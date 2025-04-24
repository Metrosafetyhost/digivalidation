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

# Helper to normalize text for heading matching
def normalize(text):
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()

# Detect major section headings
def is_major_heading(text):
    norm = normalize(text)
    for phrase in IMPORTANT_HEADINGS:
        if all(w in norm for w in phrase.lower().split()):
            return True
    return False

# Line‑based fallback parser for Significant Findings
def parse_significant_findings(lines):
    items = []
    current = None
    current_section = None

    LABELS = ['Priority', 'Observation', 'Target Date', 'Action Required']
    SUB_SECTIONS = {'management of risk', 'additional guidance', 'emergency action'}

    def flush():
        nonlocal current
        if current:
            items.append(current)
            current = None

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        lower = line.lower()

        # Pick up sub-section headings
        if lower in SUB_SECTIONS:
            current_section = line
            i += 1
            continue

        # New audit reference
        m_ref = re.match(r'^(\d+\.\d+)\.?\s+(.+)$', line)
        if m_ref:
            flush()
            current = {
                'section': current_section,
                'audit_ref': m_ref.group(1),
                'question': m_ref.group(2).strip(),
                'priority': '',
                'observation': '',
                'target_date': '',
                'action_required': ''
            }
            i += 1
            continue

        # If no item open, skip
        if not current:
            i += 1
            continue

        # Field extraction
        for label in LABELS:
            if re.match(rf'^{label}\b:?', line, re.IGNORECASE):
                field = label.lower().replace(' ', '_')
                parts = re.split(r':\s*', line, 1)

                # Inline value
                if len(parts) == 2 and parts[1].strip():
                    content = parts[1].strip()
                    i += 1
                else:
                    # Multi-line block until next label or audit ref
                    content_lines = []
                    j = i + 1
                    while j < len(lines):
                        nxt = lines[j].strip()
                        if any(re.match(rf'^{L}\b', nxt, re.IGNORECASE) for L in LABELS) or re.match(r'^\d+\.\d+', nxt):
                            break
                        content_lines.append(nxt)
                        j += 1
                    content = ' '.join(content_lines).strip()
                    i = j

                current[field] = content
                break
        else:
            i += 1

    flush()
    return items

# Extract key/value form fields (unused here but kept for completeness)
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
            # find value block
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

# Poll until Textract job completes
def poll_for_job_completion(job_id, max_tries=20, delay=5):
    for _ in range(max_tries):
        resp = textract.get_document_analysis(JobId=job_id)
        if resp['JobStatus'] == 'SUCCEEDED':
            return get_all_pages(job_id)
        if resp['JobStatus'] == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(delay)
    raise Exception("Job did not complete in time")

# Retrieve all pages of results
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

# Extract lines of text per page
def extract_pages_text(blocks):
    by_page = {}
    for b in blocks:
        if b['BlockType'] == 'LINE':
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            by_page.setdefault(pg, []).append((top, b['Text']))
    out = {}
    for pg, lines in by_page.items():
        lines.sort(key=lambda x: x[0])
        out[pg] = [t for _, t in lines]
    return out

# Group tables under headings
def extract_tables_grouped(blocks):
    tables = []
    headings = {}
    for b in blocks:
        if b['BlockType'] == 'LINE' and is_major_heading(b.get('Text', '')):
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            headings.setdefault(pg, []).append((top, b['Text']))
    for b in blocks:
        if b['BlockType'] == 'TABLE':
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            # find nearest heading above
            cand = [(y, t) for y, t in headings.get(pg, []) if y < top]
            header = None
            if cand:
                header = max(cand, key=lambda x: x[0])[1]
            # collect rows
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

# Build high‑level sections and attach tables/fields
def group_sections(pages_text, tables, kv_pairs):
    sections = []
    # detect headings
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
    # populate content
    for idx, sec in enumerate(sections):
        next_sec = sections[idx+1] if idx+1 < len(sections) else None
        for pg, lines in pages_text.items():
            if pg < sec['start_page'] or (next_sec and pg >= next_sec['start_page']):
                continue
            sec['paragraphs'].extend(lines)
        sec['tables'] = [t for t in tables if t['page'] == sec['start_page'] and t['header'] == sec['name']]
        for kv in kv_pairs:
            if sec['start_page'] <= kv['page'] < (next_sec['start_page'] if next_sec else kv['page']+1):
                sec['fields'].append({'key': kv['key'], 'value': kv['value']})
    # post‑process Significant Findings
    for sec in sections:
        if sec['name'].lower().startswith('significant findings'):
            if sec['tables']:
                tbl = sec['tables'][0]
                items = []
                # skip header row
                for row in tbl['rows'][1:]:
                    m = re.match(r'^(\d+\.\d+)\.?\s*(.+)$', row[0])
                    if m:
                        audit_ref, question = m.groups()
                    else:
                        audit_ref, question = "", row[0]
                    items.append({
                        'section': sec['name'],
                        'audit_ref': audit_ref,
                        'question': question.strip(),
                        'priority':        row[1].strip() if len(row) > 1 else "",
                        'observation':     row[2].strip() if len(row) > 2 else "",
                        'target_date':     row[3].strip() if len(row) > 3 else "",
                        'action_required': row[4].strip() if len(row) > 4 else "",
                    })
                sec['items'] = items
            else:
                sec['items'] = parse_significant_findings(sec.get('paragraphs', []))
            # drop unused
            sec.pop('paragraphs', None)
            sec.pop('fields', None)
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

    return {'statusCode':200, 'body': json.dumps({'json_s3_key': json_key})}
