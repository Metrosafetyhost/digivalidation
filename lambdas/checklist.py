import boto3
import json
import re
import time

# AWS clients
textract = boto3.client('textract', region_name='eu-west-2')
s3 = boto3.client('s3')

# List of section headers to extract
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
    "Legionella Control Programme of Preventative Works"
]

# Utils for normalisation and heading detection
def normalize(text):
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()

def is_heading(text):
    norm = normalize(text)
    for phrase in IMPORTANT_HEADINGS:
        if normalize(phrase) in norm:
            return True
    return False

# Start async Textract analysis
def start_job(bucket, document_key):
    resp = textract.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': document_key}},
        FeatureTypes=['TABLES', 'FORMS']
    )
    return resp['JobId']

# Poll until job completes and collect all blocks
def get_job_blocks(job_id, delay=5, max_tries=60):
    for _ in range(max_tries):
        status = textract.get_document_analysis(JobId=job_id)
        if status['JobStatus'] == 'SUCCEEDED':
            break
        if status['JobStatus'] == 'FAILED':
            raise RuntimeError("Textract analysis failed")
        time.sleep(delay)
    # retrieve all pages
    blocks = []
    next_token = None
    while True:
        params = {'JobId': job_id}
        if next_token:
            params['NextToken'] = next_token
        page = textract.get_document_analysis(**params)
        blocks.extend(page.get('Blocks', []))
        next_token = page.get('NextToken')
        if not next_token:
            break
    return blocks

# Extract lines with their positions per page
def extract_lines(blocks):
    pages = {}
    for b in blocks:
        if b['BlockType'] == 'LINE':
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            pages.setdefault(pg, []).append({'text': b['Text'], 'top': top})
    # sort lines by vertical position
    for pg in pages:
        pages[pg].sort(key=lambda x: x['top'])
    return pages

# Find all headings positions
def find_headings(lines_by_page):
    headings = {}
    for pg, lines in lines_by_page.items():
        for ln in lines:
            if is_heading(ln['text']):
                headings.setdefault(pg, []).append(ln)
    return headings

# Extract one section's text and tables
def extract_section_data(blocks, heading_text, lines_by_page, headings_positions):
    data = {'heading': heading_text, 'paragraphs': [], 'tables': []}
    # find this heading occurrence
    for pg, heads in headings_positions.items():
        for h in heads:
            if normalize(h['text']) == normalize(heading_text):
                page = pg
                top = h['top']
                # next heading on same page
                next_tops = [nh['top'] for nh in heads if nh['top'] > top]
                limit = min(next_tops) if next_tops else 1.0
                # collect paragraphs
                for ln in lines_by_page.get(page, []):
                    if top < ln['top'] < limit:
                        data['paragraphs'].append(ln['text'])
                # collect tables under this heading
                for tb in [b for b in blocks if b['BlockType']=='TABLE' and b['Page']==page]:
                    t_top = tb['Geometry']['BoundingBox']['Top']
                    if top < t_top < limit:
                        # parse that table into rows
                        rows = []
                        for rel in tb.get('Relationships', []):
                            if rel['Type']=='CHILD':
                                cells = [c for c in blocks if c['Id'] in rel['Ids'] and c['BlockType']=='CELL']
                                # group by row index
                                by_row = {}
                                for c in cells:
                                    ri = c['RowIndex']
                                    text = ''
                                    for r2 in c.get('Relationships', []):
                                        if r2['Type']=='CHILD':
                                            for wid in r2['Ids']:
                                                w = next((x for x in blocks if x['Id']==wid), None)
                                                if w and w['BlockType'] in ('WORD','LINE'):
                                                    text += w['Text'] + ' '
                                    by_row.setdefault(ri, []).append(text.strip())
                                for ri in sorted(by_row): rows.append(by_row[ri])
                        data['tables'].append(rows)
                return data
    return data

# Main handler

def process_document(event, context):
    bucket = event['bucket']
    key    = event['document_key']
    out_bucket = event.get('output_bucket', bucket)

    job_id = start_job(bucket, key)
    blocks = get_job_blocks(job_id)
    lines_by_page = extract_lines(blocks)
    headings_positions = find_headings(lines_by_page)

    # extract each section
    result = {'document': key, 'sections': []}
    for heading in IMPORTANT_HEADINGS:
        sec = extract_section_data(blocks, heading, lines_by_page, headings_positions)
        if sec['paragraphs'] or sec['tables']:
            result['sections'].append(sec)

    # save JSON back to S3
    json_key = key.replace('.pdf', '_sections.json')
    s3.put_object(Bucket=out_bucket, Key=json_key, Body=json.dumps(result))
    return {'json_key': json_key}
