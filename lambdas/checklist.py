import boto3
import json
import time
import re

# AWS clients
textract = boto3.client('textract', region_name='eu-west-2')
s3      = boto3.client('s3')

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
    "Outlet Temperature Profile",
    "Water Assets",
    "Appendices",
    "Risk Assessment Checklist",
    "Legionella Control Programme of Preventative Works",
]

def normalize(text):
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()

def is_major_heading(txt):
    """True if this line is one of your named sections or matches e.g. '1.2', '3.4', etc."""
    norm = normalize(txt)
    for phrase in IMPORTANT_HEADINGS:
        if all(w in norm for w in phrase.lower().split()):
            return True
    return bool(re.match(r'^\d+(\.\d+)*\s+', txt))

def extract_tables_grouped(blocks):
    tables = []
    sorted_blocks = sorted(
        blocks,
        key=lambda b: (b.get("Page",1), b["Geometry"]["BoundingBox"]["Top"])
    )
    current_header = None
    for b in sorted_blocks:
        if b["BlockType"] == "LINE" and is_major_heading(b.get("Text","")):
            current_header = b["Text"].strip()
        if b["BlockType"] == "TABLE" and current_header:
            # collect rows...
            rows = []
            for rel in b.get("Relationships",[]):
                if rel["Type"] == "CHILD":
                    cells = [c for c in blocks if c["Id"] in rel["Ids"] and c["BlockType"]=="CELL"]
                    rowm = {}
                    for c in cells:
                        ri = c["RowIndex"]
                        txt = ""
                        for r2 in c.get("Relationships",[]):
                            if r2["Type"]=="CHILD":
                                for cid in r2["Ids"]:
                                    w = next((x for x in blocks if x["Id"]==cid), None)
                                    if w and w["BlockType"] in ("WORD","LINE"):
                                        txt += w.get("Text","") + " "
                        rowm.setdefault(ri,[]).append(txt.strip())
                    for ri in sorted(rowm):
                        rows.append(rowm[ri])
            # dedupe rows
            seen = set(); unique=[]
            for row in rows:
                key = tuple(row)
                if key not in seen:
                    seen.add(key); unique.append(row)
            tables.append({
                "page": b.get("Page",1),
                "header": current_header,
                "rows": unique,
                "bbox": b["Geometry"]["BoundingBox"]
            })
    return tables

def extract_key_value_pairs(blocks):
    id_map = {b['Id']:b for b in blocks}
    kv = []
    for b in blocks:
        if b['BlockType']=="KEY_VALUE_SET" and 'KEY' in b.get('EntityTypes',[]):
            key_txt = ""
            for rel in b.get('Relationships',[]):
                if rel['Type']=="CHILD":
                    for cid in rel['Ids']:
                        w = id_map[cid]
                        if w['BlockType']=="WORD":
                            key_txt += w['Text']+" "
            # find its VALUE block
            val_block = None
            for rel in b.get('Relationships',[]):
                if rel['Type']=="VALUE":
                    for vid in rel['Ids']:
                        if id_map[vid]['BlockType']=="KEY_VALUE_SET":
                            val_block = id_map[vid]
            val_txt = ""
            if val_block:
                for rel in val_block.get('Relationships',[]):
                    if rel['Type']=="CHILD":
                        for cid in rel['Ids']:
                            w = id_map[cid]
                            if w['BlockType']=="WORD":
                                val_txt += w['Text']+" "
            if key_txt.strip() and val_txt.strip():
                kv.append({
                    'key': key_txt.strip(),
                    'value': val_txt.strip(),
                    'page': b.get('Page',1),
                    'top':   b['Geometry']['BoundingBox']['Top']
                })
    return kv

def group_sections(blocks, tables, fields):
    # 1) sort lines by page & vertical
    lines = sorted(
      [b for b in blocks if b['BlockType']=="LINE" and b.get('Text')],
      key=lambda b:(b.get('Page',1), b['Geometry']['BoundingBox']['Top'])
    )
    sections=[]
    seen=set()
    current=None

    for b in lines:
        txt  = b['Text'].strip()
        top  = b['Geometry']['BoundingBox']['Top']
        page = b.get('Page',1)

        # new section?
        if is_major_heading(txt) and 0.06 < top < 0.85:
            if txt not in seen:
                seen.add(txt)
                current = {
                    "name": txt,
                    "paragraphs": [],
                    "tables": [t for t in tables if t["header"]==txt],
                    "fields": [f for f in fields if f["key"].startswith(txt+" ")]
                }
                sections.append(current)
            else:
                current = None
            continue

        # collect paragraphs only when inside a section, body‐zone and not a heading
        if current and not is_major_heading(txt) and 0.06 < top < 0.85:
            current["paragraphs"].append(txt)

    return sections

def poll_for_job_completion(job_id, max_tries=20, delay=5):
    for _ in range(max_tries):
        resp = textract.get_document_analysis(JobId=job_id)
        if resp['JobStatus']=='SUCCEEDED':
            return get_all_pages(job_id)
        if resp['JobStatus']=='FAILED':
            raise Exception("Textract failed")
        time.sleep(delay)
    raise Exception("Timeout")

def get_all_pages(job_id):
    blocks=[]; token=None
    while True:
        params={'JobId':job_id}
        if token: params['NextToken']=token
        resp = textract.get_document_analysis(**params)
        blocks += resp.get('Blocks',[])
        token = resp.get('NextToken')
        if not token: break
    return blocks

def process(event, context):
    # kick off Textract
    resp = textract.start_document_analysis(
      DocumentLocation={'S3Object':{
         'Bucket':event.get('bucket','metrosafetyprodfiles'),
         'Name':  event['document_key']
      }},
      FeatureTypes=['TABLES','FORMS']
    )
    blocks  = poll_for_job_completion(resp['JobId'])
    tables  = extract_tables_grouped(blocks)
    fields  = extract_key_value_pairs(blocks)
    secs    = group_sections(blocks, tables, fields)

    out = {'document': event['document_key'], 'sections': secs}
    key = f"processed/{event['document_key'].split('/')[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=event.get('output_bucket','textract-output-digival'),
                  Key=key, Body=json.dumps(out))
    return {'statusCode':200, 'body':json.dumps({'json_s3_key':key})}
