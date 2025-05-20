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
    "Contents",
    "Executive Summary",
    "Areas Identified Requiring Remedial Actions",
    "Building Description",
    "Accompanying the Risk Assessor",
    "Risk Review and Reassessment",
    "Water Scope",
    "Risk Dashboard",
    "Management Responsibilities",
    "Legionella Control Programme",
    "Audit Detail",
    "Water Control Scheme",
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

# Extract tables grouped by detected headings
def extract_tables_grouped(blocks):
    """
    Walk the entire document in reading order, remember the last major heading seen,
    and assign that heading to every TABLE block until the next heading appears.
    """
    tables = []
    # 1) sort all blocks by page, then vertical position
    sorted_blocks = sorted(
        blocks,
        key=lambda b: (b.get("Page", 1), b["Geometry"]["BoundingBox"]["Top"])
    )

    current_header = None
    for b in sorted_blocks:
        # whenever we hit a major heading line, update current_header
        if b["BlockType"] == "LINE" and is_major_heading(b.get("Text", "")):
            current_header = b["Text"].strip()

        # whenever we hit a TABLE, grab its rows & attach the most recent header
        if b["BlockType"] == "TABLE" and current_header:
            # collect the rows just as you were doing before
            rows = []
            for rel in b.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    cells = [c for c in blocks
                             if c["Id"] in rel["Ids"]
                             and c["BlockType"] == "CELL"]
                    rowm = {}
                    for c in cells:
                        ri = c["RowIndex"]
                        txt = ""
                        for r2 in c.get("Relationships", []):
                            if r2["Type"] == "CHILD":
                                for cid in r2["Ids"]:
                                    w = next((x for x in blocks if x["Id"] == cid), None)
                                    if w and w["BlockType"] in ("WORD", "LINE"):
                                        txt += w.get("Text", "") + " "
                        rowm.setdefault(ri, []).append(txt.strip())
                    for ri in sorted(rowm):
                        rows.append(rowm[ri])

            tables.append({
                "page": b.get("Page", 1),
                "header": current_header,
                "rows": rows,
                "bbox": b["Geometry"]["BoundingBox"]
            })

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