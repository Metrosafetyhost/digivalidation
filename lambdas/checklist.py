import boto3
import json
import time
import re
from io import StringIO

# AWS clients
textract = boto3.client('textract', region_name='eu-west-2')
s3      = boto3.client('s3')

# Headings to identify sections in the PDF
IMPORTANT_HEADINGS = [
    "Significant Findings and Action Plan",
    "Executive Summary",
    "Areas Identified Requiring Remedial Actions",
    "Building Description",
    "Water Scope",
    "Risk Dashboard",
    "Management Responsibilities",
    "Legionella Control Programme of Preventative Works",
    "Audit Detail",
    "System Asset Register",
    "Water Assets",
    "Appendices",
    "Risk Assessment Checklist"
]

# Normalize text for comparison
def normalize(text):
    return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).strip()

# Detect major section headings
def is_major_heading(text):
    norm = normalize(text)
    for phrase in IMPORTANT_HEADINGS:
        if all(w in norm for w in phrase.lower().split()):
            return True
    return False

# Extract key-value pairs (forms) from Textract blocks
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
                    'key':   key_text.strip(),
                    'value': value_text.strip(),
                    'page':  block.get('Page', 1),
                    'top':   block['Geometry']['BoundingBox']['Top']
                })
    return kv_pairs

# Poll Textract job until completion
def poll_for_job_completion(job_id, max_tries=20, delay=5):
    for _ in range(max_tries):
        resp = textract.get_document_analysis(JobId=job_id)
        status = resp['JobStatus']
        if status == 'SUCCEEDED':
            return get_all_pages(job_id)
        if status == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(delay)
    raise Exception("Textract job did not complete in time")

# Retrieve all pages from a completed Textract job
def get_all_pages(job_id):
    blocks = []
    token  = None
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

# Extract lines of text grouped by page
def extract_pages_text(blocks):
    by_page = {}
    for b in blocks:
        if b['BlockType'] == 'LINE':
            pg  = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            by_page.setdefault(pg, []).append((top, b['Text']))
    out = {}
    for pg, lines in by_page.items():
        lines.sort(key=lambda x: x[0])
        out[pg] = [t for _, t in lines]
    return out

# Extract and group tables with their nearest heading
def extract_tables_grouped(blocks):
    tables   = []
    headings = {}
    # Record headings per page
    for b in blocks:
        if b['BlockType'] == 'LINE' and is_major_heading(b.get('Text', '')):
            pg  = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            headings.setdefault(pg, []).append((top, b['Text']))
    # Collect table cells into rows
    for b in blocks:
        if b['BlockType'] == 'TABLE':
            pg   = b.get('Page', 1)
            top  = b['Geometry']['BoundingBox']['Top']
            # Find the nearest heading above the table
            cand = [(y, t) for y, t in headings.get(pg, []) if y < top]
            header = max(cand, key=lambda x: x[0])[1] if cand else None
            rows = []
            for rel in b.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    cells = [c for c in blocks if c['Id'] in rel['Ids'] and c['BlockType']=='CELL']
                    row_map = {}
                    for c in cells:
                        ri = c['RowIndex']
                        txt = ''
                        for r2 in c.get('Relationships', []):
                            if r2['Type'] == 'CHILD':
                                for cid in r2['Ids']:
                                    w = next((x for x in blocks if x['Id']==cid), None)
                                    if w and w['BlockType'] in ('WORD','LINE'):
                                        txt += w['Text'] + ' '
                        row_map.setdefault(ri, []).append(txt.strip())
                    for ri in sorted(row_map):
                        rows.append(row_map[ri])
            tables.append({'page':pg, 'header':header, 'rows':rows, 'bbox':b['Geometry']['BoundingBox']})
    return tables

# Group sections, override Significant Findings to use table rows as items
def group_sections(pages_text, tables, kv_pairs):
    sections = []
    # Identify section headings
    for pg, lines in sorted(pages_text.items()):
        for line in lines:
            if is_major_heading(line):
                sections.append({
                    'name':       line,
                    'start_page': pg,
                    'paragraphs': [],
                    'tables':     [],
                    'fields':     []
                })
    # Populate paragraphs, tables, fields for each section
    for idx, sec in enumerate(sections):
        next_sec = sections[idx+1] if idx+1 < len(sections) else None
        for pg, lines in pages_text.items():
            if pg < sec['start_page'] or (next_sec and pg >= next_sec['start_page']):
                continue
            sec['paragraphs'].extend(lines)
        sec['tables'] = [t for t in tables if t['page']==sec['start_page'] and t['header']==sec['name']]
        for kv in kv_pairs:
            if sec['start_page'] <= kv['page'] < (next_sec['start_page'] if next_sec else kv['page']+1):
                sec['fields'].append({'key':kv['key'], 'value':kv['value']})
    # Override for Significant Findings
    for sec in sections:
        if sec['name'].lower().startswith('significant findings'):
            items = []
            # Use each row in the table as its own finding
            for table in [t for t in sec['tables']]:
                for row in table['rows']:
                    section_cell    = row[0] if len(row)>0 else ''
                    ref_q           = row[1] if len(row)>1 else ''
                    parts           = ref_q.split(' ',1)
                    audit_ref       = parts[0]
                    question        = parts[1] if len(parts)>1 else ''
                    priority        = row[2] if len(row)>2 else ''
                    observation     = row[3] if len(row)>3 else ''
                    target_date     = row[4] if len(row)>4 else ''
                    action_required = row[5] if len(row)>5 else ''
                    items.append({
                        'section':         section_cell,
                        'audit_ref':       audit_ref,
                        'question':        question,
                        'priority':        priority,
                        'observation':     observation,
                        'target_date':     target_date,
                        'action_required': action_required
                    })
            sec['items'] = items
            # remove unused keys
            sec.pop('paragraphs', None)
            sec.pop('fields',     None)
    return sections

# Main Lambda handler
def process(event, context):
    input_bucket  = event.get('bucket', 'metrosafetyprodfiles')
    document_key  = event['document_key']
    output_bucket = event.get('output_bucket', 'textract-output-digival')

    # Start Textract job
    resp   = textract.start_document_analysis(
        DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':document_key}},
        FeatureTypes=['TABLES','FORMS']
    )
    blocks = poll_for_job_completion(resp['JobId'])

    # Extract content
    pages    = extract_pages_text(blocks)
    tables   = extract_tables_grouped(blocks)
    kv_pairs = extract_key_value_pairs(blocks)

    # Group into sections
    sections = group_sections(pages, tables, kv_pairs)

    # Build result
    result   = {'document': document_key, 'sections': sections}
    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"

    # Save back to S3
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {'statusCode':200, 'body':json.dumps({'json_s3_key': json_key})}
