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

def normalize(text):
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()

def is_major_heading(text):
    norm = normalize(text)
    return any(all(w in norm for w in phrase.lower().split()) for phrase in IMPORTANT_HEADINGS)

def is_footer(line):
    return bool(
        re.match(r'Printed from SafetySMART', line) or
        re.match(r'^Page \d+ of \d+', line) or
        'Legionella Water Risk Assessment' in line
    )

def strip_dates(line):
    return re.sub(r'\b\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\b', '', line)

# Extract lines per page

def extract_pages_text(blocks):
    by_page = {}
    for b in blocks:
        if b['BlockType'] == 'LINE':
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            by_page.setdefault(pg, []).append((top, b['Text']))
    pages = {}
    for pg, lines in by_page.items():
        lines.sort(key=lambda x: x[0])
        pages[pg] = [t for _, t in lines]
    return pages

# Group tables under their nearest heading

def extract_tables_grouped(blocks):
    # Collect all major headings with their page and vertical position
    heading_list = []
    for b in blocks:
        if b['BlockType'] == 'LINE' and is_major_heading(b.get('Text', '')):
            heading_list.append({
                'page': b.get('Page', 1),
                'top': b['Geometry']['BoundingBox']['Top'],
                'text': b['Text']
            })
    tables = []
    # For each TABLE block, find the nearest preceding heading across pages
    for b in blocks:
        if b['BlockType'] == 'TABLE':
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            # find headings before this table (either on earlier pages, or same page above)
            candidates = [h for h in heading_list if (h['page'] < pg) or (h['page'] == pg and h['top'] < top)]
            header = None
            if candidates:
                # pick the heading with maximum (page, top)
                best = max(candidates, key=lambda h: (h['page'], h['top']))
                header = best['text']
            # extract cell rows
            rows = []
            for rel in b.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    cells = [c for c in blocks if c['Id'] in rel['Ids'] and c['BlockType'] == 'CELL']
                    row_map = {}
                    for cell in cells:
                        ri = cell['RowIndex']
                        text = ''
                        for r2 in cell.get('Relationships', []):
                            if r2['Type'] == 'CHILD':
                                for cid in r2['Ids']:
                                    w = next((x for x in blocks if x['Id'] == cid), None)
                                    if w and w['BlockType'] in ('WORD', 'LINE'):
                                        text += w.get('Text', '') + ' '
                        row_map.setdefault(ri, []).append(text.strip())
                    for idx in sorted(row_map):
                        rows.append(row_map[idx])
            tables.append({'page': pg, 'header': header, 'rows': rows})
    return tables

# Parse Significant Findings table into JSON items

def parse_findings_from_table(table):
    items = []
    rows = table.get('rows', [])
    if len(rows) < 2:
        return items
    for row in rows[1:]:
        if len(row) >= 6:
            items.append({
                'audit_ref':       row[0].strip(),
                'question':        row[1].strip(),
                'observation':     row[2].strip(),
                'priority':        row[3].strip(),
                'action_required': row[5].strip(),
            })
    return items

# Group sections and include only table-based parsing for Significant Findings

def group_sections(pages, tables, kv_pairs):
    sections = []
    # detect headings in document order
    for pg, lines in sorted(pages.items()):
        for line in lines:
            if is_major_heading(line):
                sections.append({'name': line, 'start_page': pg, 'items': []})
    # associate tables
    for idx, sec in enumerate(sections):
        next_start = sections[idx+1]['start_page'] if idx+1 < len(sections) else float('inf')
        sec_tables = [t for t in tables if t['header'] == sec['name'] and sec['start_page'] <= t['page'] < next_start]
        if sec['name'].lower().startswith('significant findings') and sec_tables:
            sec['items'] = parse_findings_from_table(sec_tables[0])
    return sections

# Poll for async Textract job completion

def poll_for_job_completion(job_id, max_tries=20, delay=5):
    for _ in range(max_tries):
        resp = textract.get_document_analysis(JobId=job_id)
        if resp['JobStatus'] == 'SUCCEEDED':
            return get_all_pages(job_id)
        if resp['JobStatus'] == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(delay)
    raise Exception("Job did not complete in time")

# Retrieve all Textract blocks

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

# Lambda handler

def process(event, context):
    input_bucket  = event.get('bucket', 'metrosafetyprodfiles')
    document_key  = event['document_key']
    output_bucket = event.get('output_bucket', 'textract-output-digival')

    # start Textract analysis
    resp   = textract.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': input_bucket, 'Name': document_key}},
        FeatureTypes=['TABLES', 'FORMS']
    )
    blocks = poll_for_job_completion(resp['JobId'])

    # extract and clean pages
    pages_raw = extract_pages_text(blocks)
    pages = {}
    for pg, lines in pages_raw.items():
        cleaned = []
        for line in lines:
            if is_footer(line):
                continue
            text = strip_dates(line).strip()
            if text:
                cleaned.append(text)
        pages[pg] = cleaned

    # extract tables and kvs
    tables   = extract_tables_grouped(blocks)
    kv_pairs = []  # not used, kept for structure

    # build sections
    sections = group_sections(pages, tables, kv_pairs)

    # upload result
    result   = {'document': document_key, 'sections': sections}
    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {'statusCode': 200, 'body': json.dumps({'json_s3_key': json_key})}
