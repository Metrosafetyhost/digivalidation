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

# Parse the Significant Findings section
def parse_significant_findings(lines):
    items = []
    current = None
    curr_section = None
    next_field = None
    for line in lines:
        text = line.strip()
        # Section headers within this block
        if text in ('Management of Risk', 'Additional Guidance', 'Emergency Action'):
            curr_section = text
            continue
        # Question lines: e.g. '12.2. Has a competent ...'
        qmatch = re.match(r'^(\d+\.\d+)\.\s*(.+)$', text)
        if qmatch:
            if current:
                items.append(current)
            current = {
                'section': curr_section,
                'audit_ref': qmatch.group(1),
                'question': qmatch.group(2).strip()
            }
            continue
        # Label lines
        if text == 'Priority':
            next_field = 'priority'
            continue
        if text == 'Observation':
            next_field = 'observation'
            continue
        if text == 'Action Required':
            next_field = 'action_required'
            continue
        if text == 'Target Date':
            next_field = 'target_date'
            continue
        # Assign the value of the previous label
        if next_field and current:
            current[next_field] = text
            next_field = None
            continue
    if current:
        items.append(current)
    return items

# Extract key/value pairs from Textract blocks
def extract_key_value_pairs(blocks):
    id_map = {b['Id']: b for b in blocks}
    kv_pairs = []
    for block in blocks:
        if block['BlockType']=='KEY_VALUE_SET' and 'KEY' in block.get('EntityTypes', []):
            key_text = ''
            for rel in block.get('Relationships', []):
                if rel['Type']=='CHILD':
                    for cid in rel['Ids']:
                        child = id_map[cid]
                        if child['BlockType']=='WORD':
                            key_text += child['Text'] + ' '
            value_text = ''
            # find related value block
            for rel in block.get('Relationships', []):
                if rel['Type']=='VALUE':
                    for vid in rel['Ids']:
                        vb = id_map.get(vid)
                        if vb and vb['BlockType']=='KEY_VALUE_SET':
                            for r2 in vb.get('Relationships', []):
                                if r2['Type']=='CHILD':
                                    for cid in r2['Ids']:
                                        w = id_map[cid]
                                        if w['BlockType']=='WORD':
                                            value_text += w['Text'] + ' '
            if key_text.strip() and value_text.strip():
                kv_pairs.append({
                    'key': key_text.strip(),
                    'value': value_text.strip(),
                    'page': block.get('Page', 1),
                    'top': block['Geometry']['BoundingBox']['Top']
                })
    return kv_pairs

# Poll for Textract job to complete
def poll_for_job_completion(job_id, max_tries=20, delay=5):
    for _ in range(max_tries):
        resp = textract.get_document_analysis(JobId=job_id)
        if resp['JobStatus']=='SUCCEEDED':
            return get_all_pages(job_id)
        if resp['JobStatus']=='FAILED':
            raise Exception('Textract job failed')
        time.sleep(delay)
    raise Exception('Job did not complete in time')

# Retrieve all pages of Textract response
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

# Extract plain text lines by page
def extract_pages_text(blocks):
    by_page = {}
    for b in blocks:
        if b['BlockType']=='LINE':
            pg = b.get('Page',1)
            top = b['Geometry']['BoundingBox']['Top']
            by_page.setdefault(pg, []).append((top, b['Text']))
    out = {}
    for pg, lines in by_page.items():
        lines.sort(key=lambda x: x[0])
        out[pg] = [t for _,t in lines]
    return out

# Extract tables and group by header
def extract_tables_grouped(blocks):
    tables = []
    headings = {}
    for b in blocks:
        if b['BlockType']=='LINE' and is_major_heading(b.get('Text','')):
            pg = b.get('Page',1)
            top = b['Geometry']['BoundingBox']['Top']
            headings.setdefault(pg, []).append((top, b['Text']))
    for b in blocks:
        if b['BlockType']=='TABLE':
            pg = b.get('Page',1)
            top = b['Geometry']['BoundingBox']['Top']
            header = None
            cand = [(y,t) for y,t in headings.get(pg, []) if y<top]
            if cand:
                header = max(cand, key=lambda x: x[0])[1]
            rows = []
            for rel in b.get('Relationships', []):
                if rel['Type']=='CHILD':
                    cells = [c for c in blocks if c['Id'] in rel['Ids'] and c['BlockType']=='CELL']
                    rowm = {}
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
            tables.append({'page':pg, 'header':header,'rows':rows,'bbox':b['Geometry']['BoundingBox']})
    return tables

# Group into sections
def group_sections(pages_text, tables, kv_pairs):
    sections = []
    # detect headings
    for pg, lines in sorted(pages_text.items()):
        for line in lines:
            if is_major_heading(line):
                sections.append({'name':line,'start_page':pg,'paragraphs':[],'tables':[],'fields':[]})
    # collect content
    for idx,sec in enumerate(sections):
        nxt = sections[idx+1] if idx+1<len(sections) else None
        for pg,lines in pages_text.items():
            if pg<sec['start_page'] or (nxt and pg>=nxt['start_page']): continue
            sec['paragraphs'].extend(lines)
        sec['tables'] = [t for t in tables if t['page']==sec['start_page'] and t['header']==sec['name']]
        sec['fields'] = [kv for kv in kv_pairs if sec['start_page']<=kv['page'] < (nxt['start_page'] if nxt else kv['page']+1)]
    # parse Significant Findings
    for sec in sections:
        if sec['name']=='Significant Findings and Action Plan':
            sec['items'] = parse_significant_findings(sec['paragraphs'])
            sec.pop('paragraphs', None)
            sec.pop('fields', None)
    return sections

def process(event, context):
    input_bucket = event.get('bucket','metrosafetyprodfiles')
    document_key = event.get('document_key')
    output_bucket = event.get('output_bucket','textract-output-digival')

    resp = textract.start_document_analysis(
        DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':document_key}},
        FeatureTypes=['TABLES','FORMS']
    )
    job_id = resp['JobId']
    blocks = poll_for_job_completion(job_id)
    pages = extract_pages_text(blocks)
    tables = extract_tables_grouped(blocks)
    kv_pairs = extract_key_value_pairs(blocks)
    sections = group_sections(pages, tables, kv_pairs)

    result = {'document':document_key,'sections':sections}
    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=output_bucket,Key=json_key,Body=json.dumps(result))
    return {'statusCode':200,'body':json.dumps({'json_s3_key':json_key})}
