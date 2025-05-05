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

def normalize(text):
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()

def is_major_heading(text):
    norm = normalize(text)
    for phrase in IMPORTANT_HEADINGS:
        if all(w in norm for w in phrase.lower().split()):
            return True
    return False

# New helper: parse a TABLE block into structured items
def parse_significant_findings_table(table):
    """
    Parses the Textract TABLE block rows for 'Significant Findings and Action Plan' section
    and returns a list of dicts with keys: Question, Priority, Observation, Target Date, Action Required.
    """
    items = []
    current = {}
    for row in table['rows']:
        # strip empty cells
        cells = [c.strip() for c in row if c and c.strip()]
        if not cells:
            continue
        first = cells[0]
        # detect new question by pattern '12.3.' etc
        if re.match(r'^\d+\.\d+', first):
            # flush prior
            if current:
                items.append(current)
            # start new item
            q_text = ' '.join(cells)
            current = {'Question': q_text}
            continue
        label = first.lower()
        # map each label to its value
        if label == 'priority' and len(cells) > 1:
            current['Priority'] = cells[1]
        elif label == 'observation':
            current['Observation'] = ' '.join(cells[1:]) if len(cells) > 1 else ''
        elif label == 'target date':
            current['Target Date'] = cells[1] if len(cells) > 1 else ''
        elif label == 'action required':
            current['Action Required'] = ' '.join(cells[1:]) if len(cells) > 1 else ''
    # append last
    if current:
        items.append(current)
    return items

# Extract tables grouped by detected headings (unchanged)
def extract_tables_grouped(blocks):
    tables = []
    headings = {}
    for b in blocks:
        if b['BlockType'] == 'LINE' and is_major_heading(b.get('Text','')):
            pg = b.get('Page',1)
            top = b['Geometry']['BoundingBox']['Top']
            headings.setdefault(pg,[]).append((top,b['Text']))
    for b in blocks:
        if b['BlockType'] == 'TABLE':
            pg = b.get('Page',1)
            top = b['Geometry']['BoundingBox']['Top']
            # find closest header above the table
            header = None
            cand = [(y,t) for y,t in headings.get(pg,[]) if y < top]
            if cand:
                header = max(cand, key=lambda x: x[0])[1]
            # collect rows
            rows = []
            for rel in b.get('Relationships',[]):
                if rel['Type'] == 'CHILD':
                    cells = [c for c in blocks if c['Id'] in rel['Ids'] and c['BlockType']=='CELL']
                    rowm = {}
                    for c in cells:
                        ri = c['RowIndex']
                        txt = ''
                        for r2 in c.get('Relationships',[]):
                            if r2['Type']=='CHILD':
                                for cid in r2['Ids']:
                                    w = next((x for x in blocks if x['Id']==cid), None)
                                    if w and w['BlockType'] in ('WORD','LINE'):
                                        txt += w.get('Text','') + ' '
                        rowm.setdefault(ri,[]).append(txt.strip())
                    for ri in sorted(rowm):
                        rows.append(rowm[ri])
            tables.append({'page':pg,'header':header,'rows':rows,'bbox':b['Geometry']['BoundingBox']})
    return tables

def group_sections(blocks, tables, fields):
    """
    Split the Textract output purely by heading text.
    We assume is_major_heading(text) returns True for any of your IMPORTANT_HEADINGS.
    """

    # 1) Grab all LINE blocks and sort them in reading order
    lines = [
        b for b in blocks
        if b.get("BlockType") == "LINE" and b.get("Text", "").strip()
    ]
    lines.sort(key=lambda b: (b.get("Page", 1),
                              b["Geometry"]["BoundingBox"]["Top"]))

    sections = []
    current = None

    # 2) Walk through every line in order
    for b in lines:
        txt = b["Text"].strip()

        # If this line is one of our major headings, start a new section
        if is_major_heading(txt):
            current = {
                "name":       txt,
                "paragraphs": [],
                "tables":     [t for t in tables if t["header"] == txt],
                "fields":     [f for f in fields if f["key"].startswith(txt + " ")]
            }
            sections.append(current)

        # Otherwise, dump the line into the current section’s paragraphs
        elif current:
            current["paragraphs"].append(txt)

    return sections


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
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            by_page.setdefault(pg, []).append((top, b['Text']))
    out = {}
    for pg, lines in by_page.items():
        lines.sort(key=lambda x: x[0])
        out[pg] = [t for _, t in lines]
    return out

def process(event, context):
    input_bucket  = event.get('bucket', 'metrosafetyprodfiles')
    document_key  = event['document_key']
    output_bucket = event.get('output_bucket', 'textract-output-digival')

    resp     = textract.start_document_analysis(
       DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':document_key}},
       FeatureTypes=['TABLES','FORMS']
    )
    blocks   = poll_for_job_completion(resp['JobId'])
    tables   = extract_tables_grouped(blocks)
    kv_pairs = extract_key_value_pairs(blocks)
    sections = group_sections(blocks, tables, kv_pairs,)

    result   = {'document':document_key, 'sections':sections}
    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {'statusCode':200, 'body':json.dumps({'json_s3_key':json_key})}