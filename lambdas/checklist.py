import boto3
import json
import time
import re
from io import StringIO

# AWS clients
textract = boto3.client('textract', region_name='eu-west-2')
s3 = boto3.client('s3')

# Important section headings
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

# Normalize text for heading detection
def normalize(text):
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()

# Detect major headings
def is_major_heading(text):
    norm = normalize(text)
    for phrase in IMPORTANT_HEADINGS:
        if all(w in norm for w in phrase.lower().split()):
            return True
    return False

# Footer detection and line cleaning
def is_footer(line):
    return bool(
        re.match(r'Printed from SafetySMART', line)
        or re.match(r'^Page \d+ of \d+', line)
        or 'Legionella Water Risk Assessment' in line
    )

def strip_dates(line):
    # Remove dates in dd/mm/yyyy or dd-mm-yyyy format
    return re.sub(r'\b\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\b', '', line)

# Extract table data into individual findings
def parse_findings_from_table(table):
    items = []
    rows = table.get('rows', [])
    if len(rows) < 2:
        return items
    header = rows[0]
    for row in rows[1:]:
        # expect columns: audit_ref, question, observation, priority, [target_date], action_required
        if len(row) >= 6:
            items.append({
                'audit_ref':       row[0].strip(),
                'question':        row[1].strip(),
                'observation':     row[2].strip(),
                'priority':        row[3].strip(),
                'action_required': row[5].strip()
            })
    return items

# Extract pages of text from Textract blocks
def extract_pages_text(blocks):
    by_page = {}
    for b in blocks:
        if b['BlockType'] == 'LINE':
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            by_page.setdefault(pg, []).append((top, b['Text']))
    out = {}
    for pg, lines in by_page.items():
        # sort by vertical position
        lines.sort(key=lambda x: x[0])
        out[pg] = [t for _, t in lines]
    return out

# Group extracted tables under headings

def extract_tables_grouped(blocks):
    tables = []
    headings = {}
    # find all headings for each page
    for b in blocks:
        if b['BlockType'] == 'LINE' and is_major_heading(b.get('Text', '')):
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            headings.setdefault(pg, []).append((top, b['Text']))
    # gather tables and assign nearest heading above
    for b in blocks:
        if b['BlockType'] == 'TABLE':
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            header = None
            cand = [(y, t) for y, t in headings.get(pg, []) if y < top]
            if cand:
                header = max(cand, key=lambda x: x[0])[1]
            # build row data
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
                        rows.append(rowm[ri])
            tables.append({'page': pg, 'header': header, 'rows': rows, 'bbox': b['Geometry']['BoundingBox']})
    return tables

# Extract key-value pairs (forms) if needed

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
            # find corresponding value block
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
                    'key':   key_text.strip(),
                    'value': value_text.strip(),
                    'page':  block.get('Page', 1),
                    'top':   block['Geometry']['BoundingBox']['Top']
                })
    return kv_pairs

# Build sections combining text, tables, and kv pairs

def group_sections(pages_text, tables, kv_pairs):
    sections = []
    # locate all major headings in document order
    for pg, lines in sorted(pages_text.items()):
        for line in lines:
            if is_major_heading(line):
                sections.append({'name': line, 'start_page': pg, 'paragraphs': [], 'tables': [], 'fields': []})
    # populate each section
    for idx, sec in enumerate(sections):
        next_sec = sections[idx+1] if idx+1 < len(sections) else None
        # paragraphs
        for pg, lines in pages_text.items():
            if pg < sec['start_page'] or (next_sec and pg >= next_sec['start_page']):
                continue
            sec['paragraphs'].extend(lines)
        # tables under this heading
        sec['tables'] = [
            t for t in tables
            if t['page'] == sec['start_page'] and t['header'] == sec['name']
        ]
        # form fields if needed
        for kv in kv_pairs:
            if sec['start_page'] <= kv['page'] < (next_sec['start_page'] if next_sec else kv['page']+1):
                sec['fields'].append({'key': kv['key'], 'value': kv['value']})
    # post-process the Significant Findings section
    for sec in sections:
        if sec['name'].lower().startswith('significant findings'):
            if sec['tables']:
                # parse box-by-box data
                sec['items'] = parse_findings_from_table(sec['tables'][0])
            else:
                sec['items'] = []
            # cleanup
            sec.pop('paragraphs', None)
            sec.pop('fields',     None)
            sec.pop('tables',     None)
    return sections

# Poll until Textract job finishes

def poll_for_job_completion(job_id, max_tries=20, delay=5):
    for _ in range(max_tries):
        resp = textract.get_document_analysis(JobId=job_id)
        status = resp['JobStatus']
        if status == 'SUCCEEDED':
            return get_all_pages(job_id)
        if status == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(delay)
    raise Exception("Job did not complete in time")

# Fetch all pages from Textract

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

# Main Lambda handler

def process(event, context):
    input_bucket  = event.get('bucket', 'metrosafetyprodfiles')
    document_key  = event['document_key']
    output_bucket = event.get('output_bucket', 'textract-output-digival')

    # start Textract job
    resp   = textract.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': input_bucket, 'Name': document_key}},
        FeatureTypes=['TABLES', 'FORMS']
    )
    blocks = poll_for_job_completion(resp['JobId'])

    # extract and clean page lines
    pages = extract_pages_text(blocks)
    for pg, lines in pages.items():
        cleaned = []
        for line in lines:
            if is_footer(line):
                continue
            clean = strip_dates(line).strip()
            if clean:
                cleaned.append(clean)
        pages[pg] = cleaned

    # tables and fields
    tables   = extract_tables_grouped(blocks)
    kv_pairs = extract_key_value_pairs(blocks)

    # group into sections
    sections = group_sections(pages, tables, kv_pairs)

    # upload result
    result   = {'document': document_key, 'sections': sections}
    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {'statusCode': 200, 'body': json.dumps({'json_s3_key': json_key})}
