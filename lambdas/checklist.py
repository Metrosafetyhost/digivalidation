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

# existing parser for fallback

# def parse_significant_findings(lines):
#     items = []
#     current = None
#     i = 0

#     while i < len(lines):
#         line = lines[i].strip()
#         m_ref = re.match(r'^(\d+\.\d+)\.?\s+(.+)$', line)
#         if m_ref:
#             if current:
#                 items.append(current)
#             current = { 'audit_ref': m_ref.group(1), 'question': m_ref.group(2) }
#             i += 1
#             continue

#         if current:
#             if line.lower() == 'priority' and i+1 < len(lines):
#                 current['priority'] = lines[i+1].strip()
#                 i += 2
#                 continue

#             if line.lower().startswith('observation'):
#                 obs = ''
#                 if line.lower() == 'observation':
#                     j = i + 1
#                 else:
#                     obs = line.partition(':')[2].strip()
#                     j = i + 1
#                 while j < len(lines) and not re.match(r'^(Priority|Target Date|Action Required)$', lines[j].strip(), re.IGNORECASE) and not re.match(r'^\d+\.\d+', lines[j].strip()):
#                     obs += ' ' + lines[j].strip()
#                     j += 1
#                 current['observation'] = obs.strip()
#                 i = j
#                 continue

#             if line.lower().startswith('target date'):
#                 parts = line.split(':',1)
#                 if len(parts) == 2 and parts[1].strip():
#                     current['target_date'] = parts[1].strip()
#                     i += 1
#                 elif i+1 < len(lines):
#                     current['target_date'] = lines[i+1].strip()
#                     i += 2
#                 else:
#                     i += 1
#                 continue

#             if line.lower().startswith('action required'):
#                 action = ''
#                 parts = line.split(':',1)
#                 if len(parts) == 2:
#                     action = parts[1].strip()
#                 j = i + 1
#                 while j < len(lines) and not re.match(r'^\d+\.\d+', lines[j].strip()):
#                     action += ' ' + lines[j].strip()
#                     j += 1
#                 current['action_required'] = action.strip()
#                 i = j
#                 continue

#         i += 1

#     if current:
#         items.append(current)
#     return items


def extract_key_value_pairs(blocks):
    id_map = { b['Id']: b for b in blocks }
    kv_pairs = []
    for block in blocks:
        if block['BlockType'] == 'KEY_VALUE_SET' and 'KEY' in block.get('EntityTypes', []):
            # unchanged
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


def extract_tables_grouped(blocks):
    tables = []
    headings = {}
    # collect major-heading lines
    for b in blocks:
        if b['BlockType'] == 'LINE' and is_major_heading(b.get('Text','')):
            pg  = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            headings.setdefault(pg, []).append((top, b['Text']))

    for b in blocks:
        if b['BlockType'] == 'TABLE':
            pg  = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            header = None
            cand = [(y, t) for y, t in headings.get(pg, []) if y < top]
            if cand:
                header = max(cand, key=lambda x: x[0])[1]

            # build rows with correct column order
            # collect CELL blocks
            cell_blocks = []
            for rel in b.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    for cid in rel['Ids']:
                        cb = next((x for x in blocks if x['Id']==cid and x['BlockType']=='CELL'), None)
                        if cb:
                            cell_blocks.append(cb)

            # map text by row & col
            table_map = {}
            for c in cell_blocks:
                ri = c['RowIndex']
                ci = c.get('ColumnIndex', 1)
                txt = ''
                for r2 in c.get('Relationships', []):
                    if r2['Type'] == 'CHILD':
                        for wid in r2['Ids']:
                            w = next((x for x in blocks if x['Id']==wid), None)
                            if w and w['BlockType'] in ('WORD','LINE'):
                                txt += w['Text'] + ' '
                table_map.setdefault(ri, {})[ci] = txt.strip()

            rows = []
            for ri in sorted(table_map):
                row = [ table_map[ri].get(ci, '') for ci in sorted(table_map[ri]) ]
                rows.append(row)

            tables.append({
                'page': pg,
                'header': header,
                'rows': rows,
                'bbox': b['Geometry']['BoundingBox']
            })
    return tables


def group_sections(pages_text, tables, kv_pairs):
    sections = []
    # identify all major headings
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

    # fill each section
    for idx, sec in enumerate(sections):
        next_sec = sections[idx+1] if idx+1 < len(sections) else None
        for pg, lines in pages_text.items():
            if pg < sec['start_page'] or (next_sec and pg >= next_sec['start_page']):
                continue
            sec['paragraphs'].extend(lines)
        sec['tables'] = [
            t for t in tables
            if t['page']==sec['start_page'] and t['header']==sec['name']
        ]
        for kv in kv_pairs:
            if sec['start_page'] <= kv['page'] < (next_sec['start_page'] if next_sec else kv['page']+1):
                sec['fields'].append({'key': kv['key'], 'value': kv['value']})

    # post-process Significant Findings and Action Plan
    for sec in sections:
        if sec['name'].lower().startswith('significant findings'):
            # try to map the "Management of Risk" table directly
            tbl = next((t for t in sec.get('tables', []) if t['header']=='Management of Risk'), None)
            # if tbl:
            r = tbl['rows']
            sec['data'] = {
                'Question':       r[0][0] if len(r)>0 and len(r[0])>0 else '',
                'Priority':       r[2][0] if len(r)>2 and len(r[2])>0 else '',
                'Observation':    r[2][1] if len(r)>2 and len(r[2])>1 else '',
                'Target Date':    r[4][0] if len(r)>4 and len(r[4])>0 else '',
                'Action Required':r[4][1] if len(r)>4 and len(r[4])>1 else ''
                }
            # else:
            #     # fallback to line-based parsing
            #     sec['items'] = parse_significant_findings(sec['paragraphs'])

            # remove now-redundant fields
            for k in ('paragraphs', 'fields', 'tables'):
                sec.pop(k, None)

    return sections


def process(event, context):
    input_bucket  = event.get('bucket', 'metrosafetyprodfiles')
    document_key  = event['document_key']
    output_bucket = event.get('output_bucket', 'textract-output-digival')

    resp   = textract.start_document_analysis(
       DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':document_key}},
       FeatureTypes=['TABLES','FORMS']
    )
    blocks   = poll_for_job_completion(resp['JobId'])
    pages    = extract_pages_text(blocks)
    tables   = extract_tables_grouped(blocks)
    kv_pairs = extract_key_value_pairs(blocks)
    sections = group_sections(pages, tables, kv_pairs)

    result   = {'document': document_key, 'sections': sections}
    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {'statusCode':200, 'body':json.dumps({'json_s3_key': json_key})}

