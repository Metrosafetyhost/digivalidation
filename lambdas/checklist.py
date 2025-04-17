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
    """Lowercases and strips non-alphanumeric characters"""
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()

def is_major_heading(text):
    """Returns True if the line matches one of the important headings"""
    norm = normalize(text)
    for phrase in IMPORTANT_HEADINGS:
        words = phrase.lower().split()
        if all(w in norm for w in words):
            return True
    return False

def poll_for_job_completion(job_id, max_tries=20, delay=5):
    """Poll Textract until the document analysis job completes"""
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
    """Retrieve all pages of Textract analysis results"""
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
    """Group LINE blocks by page, sorted by their vertical position"""
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
    """Extract tables, attach nearest heading and bounding box"""
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
            # find nearest heading above
            header = None
            candidates = [(y, t) for (y, t) in headings_per_page.get(pg, []) if y < top]
            if candidates:
                header = max(candidates, key=lambda x: x[0])[1]
            # extract rows
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

def group_sections(pages_text, tables):
    """Group paragraphs and tables into named sections"""
    sections = []
    for pg, lines in pages_text.items():
        current = None
        for line in lines:
            if is_major_heading(line):
                if current:
                    sections.append(current)
                current = {
                    'name': line,
                    'page': pg,
                    'paragraphs': [],
                    'tables': []
                }
            else:
                if current:
                    current['paragraphs'].append(line)
        if current:
            sections.append(current)
    # attach tables to matching section
    for sec in sections:
        sec['tables'] = [t for t in tables if t['page'] == sec['page'] and t['header'] == sec['name']]
    return sections

def process(event, context):
    """AWS Lambda entrypoint: runs Textract, structures output, saves JSON to S3"""
    # Safely pull event parameters or use defaults
    input_bucket   = event.get('bucket', 'metrosafetyprodfiles')
    document_key   = event.get('document_key', 'WorkOrders/your-document.pdf')
    output_bucket  = event.get('output_bucket', 'textract-output-digival')

    # Kick off Textract analysis
    resp = textract.start_document_analysis(
        DocumentLocation={
            'S3Object': {'Bucket': input_bucket, 'Name': document_key}
        },
        FeatureTypes=['TABLES', 'FORMS']
    )
    job_id = resp['JobId']

    # Wait for completion
    blocks = poll_for_job_completion(job_id)

    # Build structured JSON
    pages_text = extract_pages_text(blocks)
    tables     = extract_tables_grouped(blocks)
    sections   = group_sections(pages_text, tables)

    result = {
        'document': document_key,
        'sections': sections
    }

    # Persist JSON to S3
    json_key = f"processed/{document_key.rsplit('/', 1)[-1].replace('.pdf', '.json')}"
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {
        'statusCode': 200,
        'body': json.dumps({'json_s3_key': json_key})
    }
