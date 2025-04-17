import boto3
import json
import time
import re
from io import StringIO

textract = boto3.client('textract', region_name='eu-west-2')
s3 = boto3.client('s3')

# List of headings to detect as section boundaries
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
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()


def is_major_heading(text):
    norm = normalize(text)
    for phrase in IMPORTANT_HEADINGS:
        words = phrase.lower().split()
        if all(w in norm for w in words):
            return True
    return False


def poll_for_job_completion(job_id, max_tries=20, delay=5):
    for _ in range(max_tries):
        resp = textract.get_document_analysis(JobId=job_id)
        status = resp.get('JobStatus')
        if status == 'SUCCEEDED':
            return get_all_pages(job_id)
        if status == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(delay)
    raise Exception("Job did not complete in time")


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
    # similar to existing logic, but include bounding boxes
    tables = []
    # collect headings per page
    headings_per_page = {}
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
                    cells = [blk for blk in blocks if blk['Id'] in rel['Ids'] and blk['BlockType']=='CELL']
                    row_map = {}
                    for cell in cells:
                        ri = cell['RowIndex']
                        text = ''
                        for crel in cell.get('Relationships', []):
                            if crel['Type']=='CHILD':
                                for cid in crel['Ids']:
                                    child = next((x for x in blocks if x['Id']==cid), None)
                                    if child and child['BlockType'] in ['WORD','LINE']:
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
    # attach tables
    for sec in sections:
        sec['tables'] = [t for t in tables if t['page']==sec['page'] and t['header']==sec['name']]
    return sections


def process(event, context):
    # launch Textract
    input_bucket = event['bucket']
    key = event['document_key']
    out_bucket = event['output_bucket']
    resp = textract.start_document_analysis(
        DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':key}},
        FeatureTypes=['TABLES','FORMS']
    )
    job_id = resp['JobId']
    blocks = poll_for_job_completion(job_id)

    # extract structured data
    pages_text = extract_pages_text(blocks)
    tables = extract_tables_grouped(blocks)
    sections = group_sections(pages_text, tables)

    # build final JSON
    result = {
        'document': key,
        'sections': sections
    }

    # save JSON to S3
    out_key = f"processed/{key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=out_bucket, Key=out_key, Body=json.dumps(result))

    return {
        'statusCode': 200,
        'body': json.dumps({'json_s3_key': out_key})
    }
