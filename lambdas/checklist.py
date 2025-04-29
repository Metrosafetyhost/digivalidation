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


# … keep your imports, clients, IMPORTANT_HEADINGS, normalize, is_major_heading …

def extract_tables_grouped(blocks, id_map):
    tables = []
    headings = {}

    # first, gather all major headings by page & vertical position
    for b in blocks:
        if b['BlockType'] == 'LINE' and is_major_heading(b.get('Text', '')):
            pg = b['Page']
            top = b['Geometry']['BoundingBox']['Top']
            headings.setdefault(pg, []).append((top, b['Text']))

    # then, for each TABLE block, capture its header (closest heading above)
    # and also keep its raw CELL blocks for deeper parsing
    for b in blocks:
        if b['BlockType'] == 'TABLE':
            pg = b['Page']
            top = b['Geometry']['BoundingBox']['Top']

            # find the nearest heading above this table on the same page
            header = None
            if pg in headings:
                above = [(y, t) for y, t in headings[pg] if y < top]
                if above:
                    header = max(above, key=lambda x: x[0])[1]

            # collect the CELL blocks that belong to this table
            cell_blocks = []
            for rel in b.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    for cid in rel['Ids']:
                        cell = id_map.get(cid)
                        if cell and cell['BlockType'] == 'CELL':
                            cell_blocks.append(cell)

            tables.append({
                'page':  pg,
                'header': header,
                'block':  b,
                'cells':  cell_blocks,
                'bbox':   b['Geometry']['BoundingBox']
            })

    return tables


def parse_significant_table(table, id_map):
    """
    Given a single TABLE entry for 'Significant Findings…', build a dict
    mapping each header cell (by ColumnIndex) to the cell directly below it.
    """
    # split CELL blocks by row
    rows = {}
    for cell in table['cells']:
        ri = cell['RowIndex']
        rows.setdefault(ri, []).append(cell)

    header_row = rows.get(1, [])  # assume first row is headers
    data_row   = rows.get(2, [])  # assume second row is the values

    # map ColumnIndex → header text
    headers = {}
    for cell in header_row:
        col = cell['ColumnIndex']
        text = ''
        for rel in cell.get('Relationships', []):
            if rel['Type'] == 'CHILD':
                for wid in rel['Ids']:
                    w = id_map.get(wid)
                    if w and w['BlockType'] == 'WORD':
                        text += w['Text'] + ' '
        headers[col] = text.strip()

    # map ColumnIndex → value text
    values = {}
    for cell in data_row:
        col = cell['ColumnIndex']
        text = ''
        for rel in cell.get('Relationships', []):
            if rel['Type'] == 'CHILD':
                for wid in rel['Ids']:
                    w = id_map.get(wid)
                    if w and w['BlockType'] == 'WORD':
                        text += w['Text'] + ' '
        values[col] = text.strip()

    # build final dict: header → corresponding value
    data = {}
    for col, h in headers.items():
        data[h] = values.get(col, '')
    return data


def group_sections(blocks, pages_text, tables, kv_pairs, id_map):
    # 1) locate each major heading in order
    sections = []
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

    # 2) populate paragraphs, tables and key–value fields per section
    for idx, sec in enumerate(sections):
        nxt = sections[idx+1] if idx+1 < len(sections) else None

        # paragraphs
        for pg, lines in pages_text.items():
            if pg < sec['start_page'] or (nxt and pg >= nxt['start_page']):
                continue
            sec['paragraphs'].extend(lines)

        # tables
        sec['tables'] = [
            t for t in tables
            if sec['start_page'] <= t['page'] < (nxt['start_page'] if nxt else t['page']+1)
        ]

        # form fields
        for kv in kv_pairs:
            if sec['start_page'] <= kv['page'] < (nxt['start_page'] if nxt else kv['page']+1):
                sec['fields'].append({'key': kv['key'], 'value': kv['value']})

    # 3) special-case the Significant Findings section
    for sec in sections:
        if sec['name'].lower().startswith('significant findings'):
            tbl = sec['tables'][0] if sec['tables'] else None
            sec['data'] = parse_significant_table(tbl, id_map) if tbl else {}
            # drop the old buckets
            for k in ('paragraphs', 'fields', 'tables'):
                sec.pop(k, None)

    return sections

def process(event, context):
    input_bucket  = event.get('bucket', 'metrosafetyprodfiles')
    document_key  = event['document_key']
    output_bucket = event.get('output_bucket', 'textract-output-digival')

    resp     = textract.start_document_analysis(
       DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':document_key}},
       FeatureTypes=['TABLES','FORMS']
    )
    blocks   = poll_for_job_completion(resp['JobId'])
    # build a quick lookup of every block by Id
    id_map   = {b['Id']: b for b in blocks}
    pages    = extract_pages_text(blocks)
    tables = extract_tables_grouped(blocks, id_map)
    kv_pairs = extract_key_value_pairs(blocks)
    sections = group_sections(blocks, pages, tables, kv_pairs, id_map)

    # debug dump for “Significant Findings…” only:
    for sec in sections:
        if sec['name'].startswith('Significant Findings'):
            print(">> Parsed Significant Findings table:\n",
                  json.dumps(sec['data'], indent=2))

    result   = {'document':document_key, 'sections':sections}
    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {'statusCode':200, 'body':json.dumps({'json_s3_key':json_key})}
