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
]

def normalize(text):
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()

def is_major_heading(text):
    norm = normalize(text)
    for phrase in IMPORTANT_HEADINGS:
        if all(w in norm for w in phrase.lower().split()):
            return True
    return False

# Extract key/value pairs

def extract_key_value_pairs(blocks):
    block_map = {b['Id']: b for b in blocks}
    kv_pairs = []

    for b in blocks:
        if b['BlockType']=='KEY_VALUE_SET' and 'KEY' in b.get('EntityTypes',[]):
            # text and bbox
            key_text = ''
            for r in b.get('Relationships',[]):
                if r['Type']=='CHILD':
                    for cid in r['Ids']:
                        w = block_map.get(cid)
                        if w and w['BlockType'] in ('WORD','LINE'):
                            key_text += w['Text'] + ' '
            key_text = key_text.strip()
            bbox = b.get('Geometry', {}).get('BoundingBox', {})
            top = bbox.get('Top')
            page = b.get('Page',1)
            # find value block
            val_text = ''
            for r in b.get('Relationships',[]):
                if r['Type']=='VALUE':
                    valb = block_map.get(r['Ids'][0])
                    if valb:
                        for r2 in valb.get('Relationships',[]):
                            if r2['Type']=='CHILD':
                                for cid2 in r2['Ids']:
                                    w2 = block_map.get(cid2)
                                    if w2 and w2['BlockType'] in ('WORD','LINE'):
                                        val_text += w2['Text'] + ' '
            kv_pairs.append({'key': key_text, 'value': val_text.strip(), 'page': page, 'top': top})
    return kv_pairs

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

def poll_for_job_completion(job_id, max_tries=20, delay=5):
    for _ in range(max_tries):
        resp = textract.get_document_analysis(JobId=job_id)
        if resp['JobStatus']=='SUCCEEDED':
            return get_all_pages(job_id)
        if resp['JobStatus']=='FAILED':
            raise Exception("Textract job failed")
        time.sleep(delay)
    raise Exception("Job did not complete in time")


def get_all_pages(job_id):
    blocks=[]
    token=None
    while True:
        params={'JobId':job_id}
        if token:
            params['NextToken']=token
        resp = textract.get_document_analysis(**params)
        blocks.extend(resp.get('Blocks', []))
        token = resp.get('NextToken')
        if not token:
            break
    return blocks


def extract_pages_text(blocks):
    by_page={}
    for b in blocks:
        if b['BlockType']=='LINE':
            pg = b.get('Page',1)
            top = b['Geometry']['BoundingBox']['Top']
            by_page.setdefault(pg, []).append((top, b['Text']))
    out={}
    for pg, lines in by_page.items():
        lines.sort(key=lambda x: x[0])
        out[pg] = [t for _, t in lines]
    return out


def extract_tables_grouped(blocks):
    tables=[]
    headings={}
    # collect headings
    for b in blocks:
        if b['BlockType']=='LINE' and is_major_heading(b.get('Text','')):
            pg = b.get('Page',1)
            top = b['Geometry']['BoundingBox']['Top']
            headings.setdefault(pg, []).append((top, b['Text']))
    # extract tables
    for b in blocks:
        if b['BlockType']=='TABLE':
            pg = b.get('Page',1)
            top = b['Geometry']['BoundingBox']['Top']
            header = None
            cand = [(y,t) for y,t in headings.get(pg, []) if y < top]
            if cand:
                header = max(cand, key=lambda x: x[0])[1]
            rows=[]
            for rel in b.get('Relationships', []):
                if rel['Type']=='CHILD':
                    cells = [c for c in blocks if c['Id'] in rel['Ids'] and c['BlockType']=='CELL']
                    rowm={}
                    for c in cells:
                        ri = c['RowIndex']
                        txt = ''
                        for r2 in c.get('Relationships', []):
                            if r2['Type']=='CHILD':
                                for cid in r2['Ids']:
                                    w = next((x for x in blocks if x['Id']==cid), None)
                                    if w and w['BlockType'] in ('WORD','LINE'):
                                        txt += w['Text'] + ' '
                        rowm.setdefault(ri, []).append(txt.strip())
                    for ri in sorted(rowm):
                        rows.append(rowm[ri])
            tables.append({'page':pg, 'header':header, 'rows':rows, 'bbox':b['Geometry']['BoundingBox']})
    return tables


def group_sections(blocks, pages_text, tables, kv_pairs):
    heads=[]
    for b in blocks:
        if b['BlockType']=='LINE' and is_major_heading(b.get('Text','')):
            heads.append({'name':b['Text'], 'page':b.get('Page',1), 'top':b['Geometry']['BoundingBox']['Top']})
    # sort headings
    heads.sort(key=lambda x: (x['page'], x['top']))
    sections=[]
    for i,h in enumerate(heads):
        page = h['page']
        start = h['top']
        # determine end bound
        end = 1.0
        for j in range(i+1, len(heads)):
            if heads[j]['page'] == page:
                end = heads[j]['top']
                break
        # paragraphs
        paras=[]
        for b in blocks:
            if b['BlockType']=='LINE' and b.get('Page',1)==page:
                top = b['Geometry']['BoundingBox']['Top']
                if start < top < end:
                    paras.append(b['Text'])
        # tables
        secs = [t for t in tables if t['page']==page and start < t['bbox']['Top'] < end]
        # key-values (ensure 'top' exists)
        fields = []
        for kv in kv_pairs:
            kv_top = kv.get('top')
            if kv.get('page')==page and kv_top is not None and start < kv_top < end:
                fields.append({'key':kv['key'], 'value':kv['value']})
        sections.append({'name':h['name'], 'page':page, 'paragraphs':paras, 'tables':secs, 'fields':fields})
    return sections


def process(event, context):
    input_bucket = event.get('bucket', 'metrosafetyprodfiles')
    document_key = event.get('document_key')
    output_bucket = event.get('output_bucket', 'textract-output-digival')

    resp = textract.start_document_analysis(
        DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':document_key}},
        FeatureTypes=['TABLES','FORMS']
    )
    job_id = resp['JobId']
    blocks = poll_for_job_completion(job_id)

    pages = extract_pages_text(blocks)
    tables = extract_tables_grouped(blocks)
    kv_pairs = extract_key_value_pairs(blocks)
    sections = group_sections(blocks, pages, tables, kv_pairs)

    result = {'document':document_key, 'sections':sections}
    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {'statusCode':200,'body':json.dumps({'json_s3_key':json_key})}
