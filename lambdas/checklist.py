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

# Extract lines with their vertical position for clustering
def extract_pages_lines(blocks):
    pages = {}
    for b in blocks:
        if b['BlockType'] == 'LINE':
            pg = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            pages.setdefault(pg, []).append((top, b['Text']))
    # sort by position
    for pg in pages:
        pages[pg].sort(key=lambda x: x[0])
    return pages  # { page: [(top, text), ...] }

# Extract key-value pairs (forms) from Textract blocks
def extract_key_value_pairs(blocks):
    id_map = {b['Id']: b for b in blocks}
    kv_pairs = []
    for block in blocks:
        if block['BlockType'] == 'KEY_VALUE_SET' and 'KEY' in block.get('EntityTypes', []):
            # extract key text
            key_text = ''
            for rel in block.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    for cid in rel['Ids']:
                        w = id_map[cid]
                        if w['BlockType'] == 'WORD':
                            key_text += w['Text'] + ' '
            # find the value block
            val_block = None
            for rel in block.get('Relationships', []):
                if rel['Type'] == 'VALUE':
                    for vid in rel['Ids']:
                        if id_map[vid]['BlockType'] == 'KEY_VALUE_SET':
                            val_block = id_map[vid]
            val_text = ''
            if val_block:
                for rel in val_block.get('Relationships', []):
                    if rel['Type'] == 'CHILD':
                        for cid in rel['Ids']:
                            w = id_map[cid]
                            if w['BlockType'] == 'WORD':
                                val_text += w['Text'] + ' '
            if key_text.strip() and val_text.strip():
                kv_pairs.append({
                    'key':   key_text.strip(),
                    'value': val_text.strip(),
                    'page':  block.get('Page', 1)
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

# Extract lines of text grouped by page (for paragraphs)
def extract_pages_text(blocks):
    pages = {}
    for b in blocks:
        if b['BlockType'] == 'LINE':
            pg  = b.get('Page', 1)
            top = b['Geometry']['BoundingBox']['Top']
            pages.setdefault(pg, []).append((top, b['Text']))
    out = {}
    for pg, lines in pages.items():
        lines.sort(key=lambda x: x[0])
        out[pg] = [t for _, t in lines]
    return out

# Extract and group tables with their nearest heading
def extract_tables_grouped(blocks):
    tables   = []
    headings = {}
    for b in blocks:
        if b['BlockType'] == 'LINE' and is_major_heading(b.get('Text', '')):
            pg, top = b.get('Page', 1), b['Geometry']['BoundingBox']['Top']
            headings.setdefault(pg, []).append((top, b['Text']))
    for b in blocks:
        if b['BlockType'] == 'TABLE':
            pg, top = b.get('Page', 1), b['Geometry']['BoundingBox']['Top']
            # find nearest heading above
            cand = [(y, t) for y, t in headings.get(pg, []) if y < top]
            header = max(cand, key=lambda x: x[0])[1] if cand else None
            rows = []
            for rel in b.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    cells = [c for c in blocks if c['Id'] in rel['Ids'] and c['BlockType'] == 'CELL']
                    rowmap = {}
                    for c in cells:
                        ri, txt = c['RowIndex'], ''
                        for r2 in c.get('Relationships', []):
                            if r2['Type'] == 'CHILD':
                                for cid in r2['Ids']:
                                    w = next((x for x in blocks if x['Id'] == cid), None)
                                    if w and w['BlockType'] in ('WORD', 'LINE'):
                                        txt += w['Text'] + ' '
                        rowmap.setdefault(ri, []).append(txt.strip())
                    for i in sorted(rowmap):
                        rows.append(rowmap[i])
            tables.append({'page': pg, 'header': header, 'rows': rows})
    return tables

# Free-text parser for Significant Findings
def parse_significant_findings(lines):
    items, current, i = [], None, 0
    while i < len(lines):
        line = lines[i].strip()
        m_ref = re.match(r'^(\d+\.\d+)\.\s*(.+)$', line)
        if m_ref:
            if current:
                items.append(current)
            current = {'audit_ref': m_ref.group(1), 'question': m_ref.group(2).strip()}
            i += 1
            continue
        if current:
            low = line.lower()
            if low.startswith('priority'):
                parts = line.split(':', 1)
                current['priority'] = parts[1].strip() if len(parts)>1 else ''
                i += 1; continue
            if low.startswith('observation'):
                obs = line.split(':',1)[1].strip() if ':' in line else ''
                j = i+1
                while j < len(lines) and not re.match(r'^(Priority|Target Date|Action Required|\d+\.\d+)', lines[j], re.IGNORECASE):
                    obs += ' ' + lines[j].strip(); j += 1
                current['observation'] = obs.strip(); i = j; continue
            if low.startswith('target date'):
                parts = line.split(':',1)
                current['target_date'] = parts[1].strip() if len(parts)>1 else ''
                i += 1; continue
            if low.startswith('action required'):
                act = line.split(':',1)[1].strip() if ':' in line else ''
                j = i+1
                while j < len(lines) and not re.match(r'^\d+\.\d+', lines[j]):
                    act += ' ' + lines[j].strip(); j += 1
                current['action_required'] = act.strip(); i = j; continue
        i += 1
    if current: items.append(current)
    return items

# Cluster free-text lines into boxes by vertical gap
def extract_sf_items_from_lines(lines_with_pos, gap=0.02):
    if not lines_with_pos: return []
    clusters, curr = [], [lines_with_pos[0]]
    for (t0, txt0), (t1, txt1) in zip(lines_with_pos, lines_with_pos[1:]):
        if t1 - t0 > gap:
            clusters.append(curr); curr = []
        curr.append((t1, txt1))
    clusters.append(curr)
    items = []
    for cluster in clusters:
        texts = [txt for _, txt in cluster]
        # only parse if looks like an entry
        if any(re.match(r'^\d+\.\d+', t) for t in texts):
            items.extend(parse_significant_findings(texts))
    return items

# Group sections, using table or clustered fallback for SF
def group_sections(pages_text, pages_lines, tables, kv_pairs):
    sections = []
    # detect headings
    for pg, lines in sorted(pages_text.items()):
        for l in lines:
            if is_major_heading(l):
                sections.append({'name':l,'start_page':pg,'paragraphs':[], 'tables':[], 'fields':[]})
    # fill content
    for idx, sec in enumerate(sections):
        nxt = sections[idx+1] if idx+1<len(sections) else None
        for pg, lines in pages_text.items():
            if pg<sec['start_page'] or (nxt and pg>=nxt['start_page']): continue
            sec['paragraphs'].extend(lines)
        sec['tables'] = [t for t in tables if t['page']==sec['start_page'] and t['header']==sec['name']]
        for kv in kv_pairs:
            if sec['start_page']<=kv['page']< (nxt['start_page'] if nxt else kv['page']+1):
                sec['fields'].append({'key':kv['key'],'value':kv['value']})
    # post process SF
    for sec in sections:
        if sec['name'].lower().startswith('significant findings'):
            if sec['tables']:
                # table-based
                items = []
                for tbl in sec['tables']:
                    for r in tbl['rows']:
                        parts = r[1].split(' ',1) if len(r)>1 else ['','']
                        items.append({
                            'section':         r[0] if len(r)>0 else '',
                            'audit_ref':       parts[0],
                            'question':        parts[1] if len(parts)>1 else '',
                            'priority':        r[2] if len(r)>2 else '',
                            'observation':     r[3] if len(r)>3 else '',
                            'target_date':     r[4] if len(r)>4 else '',
                            'action_required': r[5] if len(r)>5 else ''
                        })
            else:
                # clustered free-text fallback
                items = extract_sf_items_from_lines(pages_lines.get(sec['start_page'], []))
            sec['items'] = items
            sec.pop('paragraphs',None); sec.pop('fields',None)
    return sections

# Main Lambda handler
def process(event, context):
    input_bucket  = event.get('bucket', 'metrosafetyprodfiles')
    document_key  = event['document_key']
    output_bucket = event.get('output_bucket', 'textract-output-digival')

    resp   = textract.start_document_analysis(
        DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':document_key}},
        FeatureTypes=['TABLES','FORMS']
    )
    blocks = poll_for_job_completion(resp['JobId'])

    pages_text  = extract_pages_text(blocks)
    pages_lines = extract_pages_lines(blocks)
    tables      = extract_tables_grouped(blocks)
    kv_pairs    = extract_key_value_pairs(blocks)

    sections = group_sections(pages_text, pages_lines, tables, kv_pairs)

    result   = {'document': document_key, 'sections': sections}
    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {'statusCode':200, 'body': json.dumps({'json_s3_key': json_key})}
