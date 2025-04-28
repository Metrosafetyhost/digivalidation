import boto3
import json
import time
import re
from io import StringIO

# AWS clients
textract = boto3.client('textract', region_name='eu-west-2')
s3 = boto3.client('s3')

IMPORTANT_HEADINGS = [
    "Significant Findings and Action Plan",
    "Executive Summary",
    "Areas Identified Requiring Remedial Actions",
    "Building Description",
    "Water Scope",
    "Risk Dashboard",
    "Management Responsibilities",
    "Legionella Control Programme",
    "Audit Detail",
    "System Asset Register",
    "Water Assets",
    "Appendices",
    "Risk Assessment Checklist",
    "Legionella Control Programme of Preventative Works",
    "System Asset Register"
]

FOOTER_PATTERNS = [
    re.compile(r'Printed from SafetySMART', re.IGNORECASE),
    re.compile(r'^Page \d+ of \d+$', re.IGNORECASE)
]
DATE_RE = re.compile(r'\b\d{1,2}/\d{1,2}/\d{4}\b')

# Helpers

def normalize(text):
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()


def is_major_heading(text):
    norm = normalize(text)
    for phrase in IMPORTANT_HEADINGS:
        if all(w in norm for w in phrase.lower().split()):
            return True
    return False


def extract_key_value_pairs(blocks):
    id_map = {b['Id']: b for b in blocks}
    kv_pairs = []
    for block in blocks:
        if block['BlockType'] == 'KEY_VALUE_SET' and 'KEY' in block.get('EntityTypes', []):
            key_text = ''
            for rel in block.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    for cid in rel['Ids']:
                        child = id_map[cid]
                        if child['BlockType'] == 'WORD':
                            key_text += child['Text'] + ' '
            value_block = None
            for rel in block.get('Relationships', []):
                if rel['Type'] == 'VALUE':
                    for vid in rel['Ids']:
                        if id_map[vid]['BlockType'] == 'KEY_VALUE_SET':
                            value_block = id_map[vid]
            value_text = ''
            if value_block:
                for rel in value_block.get('Relationships', []):
                    if rel['Type'] == 'CHILD':
                        for cid in rel['Ids']:
                            child = id_map[cid]
                            if child['BlockType'] == 'WORD':
                                value_text += child['Text'] + ' '
            if key_text.strip() and value_text.strip():
                kv_pairs.append({
                    'key': key_text.strip(),
                    'value': value_text.strip(),
                    'page': block.get('Page', 1),
                    'top': block['Geometry']['BoundingBox']['Top']
                })
    return kv_pairs


def poll_for_job_completion(job_id, max_tries=20, delay=5):
    for _ in range(max_tries):
        resp = textract.get_document_analysis(JobId=job_id)
        if resp['JobStatus'] == 'SUCCEEDED':
            return get_all_pages(job_id)
        if resp['JobStatus'] == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(delay)
    raise Exception("Job did not complete in time")


def get_all_pages(job_id):
    blocks = []
    token = None
    while True:
        params = {'JobId': job_id}
        if token:
            params['NextToken'] = token
        resp = textract.get_document_analysis(**params)
        blocks.extend(resp.get('Blocks', []))
        token = resp.get('NextToken')
        if not token:
            break
    return blocks


def extract_pages_text(blocks):
    by_page = {}
    for b in blocks:
        if b['BlockType'] == 'LINE':
            text = b['Text'].strip()
            # drop footers
            if any(p.search(text) for p in FOOTER_PATTERNS):
                continue
            # remove standalone dates
            text = DATE_RE.sub('', text).strip()
            if not text:
                continue
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            by_page.setdefault(pg, []).append((top, text))
    out = {}
    for pg, lines in by_page.items():
        lines.sort(key=lambda x: x[0])
        out[pg] = [t for _, t in lines]
    return out


def extract_tables_grouped(blocks):
    tables = []
    headings = {}
    # collect headings per page
    for b in blocks:
        if b['BlockType'] == 'LINE' and is_major_heading(b.get('Text', '')):
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            headings.setdefault(pg, []).append((top, b['Text']))
    # group tables
    for b in blocks:
        if b['BlockType'] == 'TABLE':
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            # find closest heading above
            header = None
            cand = [(y, t) for y, t in headings.get(pg, []) if y < top]
            if cand:
                header = max(cand, key=lambda x: x[0])[1]
            rows = []
            for rel in b.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    cells = [c for c in blocks if c['Id'] in rel['Ids'] and c['BlockType'] == 'CELL']
                    rowm = {}
                    for c in cells:
                        ri = c['RowIndex']
                        txt = ''
                        for r2 in c.get('Relationships', []):
                            if r2['Type'] == 'CHILD':
                                for cid in r2['Ids']:
                                    w = next((x for x in blocks if x['Id'] == cid), None)
                                    if w and w['BlockType'] in ('WORD', 'LINE'):
                                        txt += w['Text'] + ' '
                        rowm.setdefault(ri, []).append(txt.strip())
                    for ri in sorted(rowm):
                        # flatten multiple cells in a row index
                        rows.append([" ".join(rowm[ri])])
            tables.append({'page': pg, 'header': header, 'rows': rows, 'bbox': b['Geometry']['BoundingBox']})
    return tables


def parse_significant_findings_table(tables):
    """
    Extract items from the 'Significant Findings and Action Plan' table.
    Expected columns: audit_ref, question, observation, priority, action_required
    """
    items = []
    for tbl in tables:
        if tbl['header'] and tbl['header'].lower().startswith('significant findings'):
            for row in tbl['rows']:
                # pad to 5 columns
                cols = row + [''] * (5 - len(row))
                items.append({
                    'audit_ref': cols[0].strip(),
                    'question': cols[1].strip(),
                    'observation': cols[2].strip(),
                    'priority': cols[3].strip(),
                    'action_required': cols[4].strip(),
                })
            break
    return items


def group_sections(pages_text, tables, kv_pairs):
    sections = []
    # find major headings
    for pg, lines in sorted(pages_text.items()):
        for line in lines:
            if is_major_heading(line):
                sections.append({
                    'name': line,
                    'start_page': pg,
                    'paragraphs': [],
                    'tables': [],
                    'fields': []
                })
    # fill each section with paragraphs and fields
    for idx, sec in enumerate(sections):
        next_sec = sections[idx+1] if idx+1 < len(sections) else None
        for pg, lines in pages_text.items():
            if pg < sec['start_page'] or (next_sec and pg >= next_sec['start_page']):
                continue
            sec['paragraphs'].extend(lines)
        sec['tables'] = [t for t in tables if t['page']==sec['start_page'] and t['header']==sec['name']]
        for kv in kv_pairs:
            if sec['start_page'] <= kv['page'] < (next_sec['start_page'] if next_sec else kv['page']+1):
                sec['fields'].append({'key': kv['key'], 'value': kv['value']})

    # specifically replace Significant Findings paragraphs with table-based items
    for sec in sections:
        if sec['name'].lower().startswith('significant findings'):
            sec['items'] = parse_significant_findings_table(tables)
            sec.pop('paragraphs', None)
            sec.pop('fields',     None)
    return sections


def process(event, context):
    input_bucket  = event.get('bucket', 'metrosafetyprodfiles')
    document_key  = event['document_key']
    output_bucket = event.get('output_bucket', 'textract-output-digival')

    resp     = textract.start_document_analysis(
       DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':document_key}},
       FeatureTypes=['TABLES','FORMS']
    )
    blocks   = poll_for_job_completion(resp['JobId'])
    pages    = extract_pages_text(blocks)
    tables   = extract_tables_grouped(blocks)
    kv_pairs = extract_key_value_pairs(blocks)
    sections = group_sections(pages, tables, kv_pairs)

    result   = {'document': document_key, 'sections': sections}
    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {'statusCode':200, 'body':json.dumps({'json_s3_key':json_key})}
