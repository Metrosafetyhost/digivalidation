import boto3
import json
import time
import re
from io import StringIO

# AWS clients
textract = boto3.client('textract', region_name='eu-west-2')
s3 = boto3.client('s3')

# Headings that define sections
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
]

def normalize(text):
    # Lowercase and strip non-alphanumeric characters
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()

def is_major_heading(text):
    norm = normalize(text)
    for phrase in IMPORTANT_HEADINGS:
        words = phrase.lower().split()
        if all(w in norm for w in words):
            return True
    return False

# --- New: Key-Value (FORM) extraction ---
def extract_key_value_pairs(blocks):
    # Map block id to block
    block_map = {b['Id']: b for b in blocks}
    key_blocks = []
    value_blocks = {}

    # Separate KEY and VALUE blocks
    for b in blocks:
        if b['BlockType'] == 'KEY_VALUE_SET' and 'EntityTypes' in b:
            types = b['EntityTypes']
            if 'KEY' in types:
                key_blocks.append(b)
            elif 'VALUE' in types:
                value_blocks[b['Id']] = b

    pairs = []
    for key_block in key_blocks:
        # Extract full text of key
        key_text = ''
        for rel in key_block.get('Relationships', []):
            if rel['Type'] == 'CHILD':
                for cid in rel['Ids']:
                    child = block_map.get(cid)
                    if child and child['BlockType'] in ('WORD', 'LINE'):
                        key_text += child['Text'] + ' '
        key_text = key_text.strip()

        # Find associated VALUE block(s)
        for rel in key_block.get('Relationships', []):
            if rel['Type'] == 'VALUE':
                for vid in rel['Ids']:
                    val_block = value_blocks.get(vid)
                    if not val_block:
                        continue
                    # Extract full text of value
                    val_text = ''
                    for rel2 in val_block.get('Relationships', []):
                        if rel2['Type'] == 'CHILD':
                            for cid2 in rel2['Ids']:
                                child2 = block_map.get(cid2)
                                if child2 and child2['BlockType'] in ('WORD', 'LINE'):
                                    val_text += child2['Text'] + ' '
                    pairs.append({
                        'key': key_text,
                        'value': val_text.strip(),
                        'page': key_block.get('Page', 1),
                        'bbox': key_block['Geometry']['BoundingBox']
                    })
    return pairs

# Existing functions for tables and pages

def poll_for_job_completion(job_id, max_tries=20, delay=5):
    for _ in range(max_tries):
        resp = textract.get_document_analysis(JobId=job_id)
        status = resp.get('JobStatus')
        if status == 'SUCCEEDED':
            return get_all_pages(job_id)
        if status == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(delay)
    raise Exception("Textract job did not complete in time")


def get_all_pages(job_id):
    all_blocks = []
    token = None
    while True:
        if token:
            resp = textract.get_document_analysis(JobId=job_id, NextToken=token)
        else:
            resp = textract.get_document_analysis(JobId=job_id)
        all_blocks.extend(resp.get('Blocks', []))
        token = resp.get('NextToken')
        if not token:
            break
    return all_blocks


def extract_pages_text(blocks):
    pages = {}
    for b in blocks:
        if b['BlockType'] == 'LINE':
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            pages.setdefault(pg, []).append((top, b['Text']))
    out = {}
    for pg, lines in pages.items():
        lines.sort(key=lambda x: x[0])
        out[pg] = [text for _, text in lines]
    return out

def extract_tables_grouped(blocks):
    tables = []
    headings_per_page = {}
    # collect headings
    for b in blocks:
        if b['BlockType'] == 'LINE' and 'Text' in b and is_major_heading(b['Text']):
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            headings_per_page.setdefault(pg, []).append((top, b['Text']))
    # extract tables
    for b in blocks:
        if b['BlockType'] == 'TABLE':
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            header = None
            candidates = [(y, t) for (y, t) in headings_per_page.get(pg, []) if y < top]
            if candidates:
                header = max(candidates, key=lambda x: x[0])[1]
            rows = []
            for rel in b.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    cells = [blk for blk in blocks if blk['Id'] in rel['Ids'] and blk['BlockType'] == 'CELL']
                    row_map = {}
                    for cell in cells:
                        ri = cell['RowIndex']
                        text = ''
                        for crel in cell.get('Relationships', []):
                            if crel['Type'] == 'CHILD':
                                for cid in crel['Ids']:
                                    child = next((x for x in blocks if x['Id'] == cid), None)
                                    if child and child['BlockType'] in ['WORD', 'LINE']:
                                        text += child['Text'] + ' '
                        row_map.setdefault(ri, []).append(text.strip())
                    for ri in sorted(row_map):
                        rows.append(row_map[ri])
            tables.append({
                'page': pg,
                'header': header,
                'boundingBox': b['Geometry']['BoundingBox'],
                'rows': rows
            })
    return tables


def group_sections(pages_text, tables, kv_pairs):
    sections = []
    # Build sections by lines
    for pg, lines in pages_text.items():
        current = None
        for line in lines:
            if is_major_heading(line):
                if current:
                    sections.append(current)
                current = {'name': line, 'page': pg, 'paragraphs': [], 'tables': [], 'fields': []}
            else:
                if current:
                    current['paragraphs'].append(line)
        if current:
            sections.append(current)
    # Attach tables
    for sec in sections:
        sec['tables'] = [t for t in tables if t['page'] == sec['page'] and t['header'] == sec['name']]
    # Attach key-value pairs under matching section
    for sec in sections:
        sec['fields'] = [
            {'key': kv['key'], 'value': kv['value']} 
            for kv in kv_pairs 
            if kv['page'] == sec['page']
        ]
    return sections


def process(event, context):
    input_bucket   = event.get('bucket', 'metrosafetyprodfiles')
    document_key   = event.get('document_key', 'WorkOrders/your-document.pdf')
    output_bucket  = event.get('output_bucket', 'textract-output-digival')

    resp = textract.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': input_bucket, 'Name': document_key}},
        FeatureTypes=['TABLES','FORMS']
    )
    job_id = resp['JobId']

    blocks = poll_for_job_completion(job_id)

    pages_text = extract_pages_text(blocks)
    tables     = extract_tables_grouped(blocks)
    kv_pairs   = extract_key_value_pairs(blocks)
    sections   = group_sections(pages_text, tables, kv_pairs)

    result = {'document': document_key, 'sections': sections}

    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {'statusCode': 200, 'body': json.dumps({'json_s3_key': json_key})}
