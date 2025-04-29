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

# Revised group_sections to use table parsing for Significant Findings
def group_sections(pages_text, tables, kv_pairs):
    sections = []
    # detect section headings
    for pg, lines in sorted(pages_text.items()):
        for line in lines:
            if is_major_heading(line):
                sections.append({'name':line,'start_page':pg,'paragraphs':[],'tables':[], 'fields':[]})
    # assign content
    for idx, sec in enumerate(sections):
        next_sec = sections[idx+1] if idx+1 < len(sections) else None
        for pg, lines in pages_text.items():
            if pg < sec['start_page'] or (next_sec and pg >= next_sec['start_page']):
                continue
            sec['paragraphs'].extend(lines)
        # include tables on same page or next page for Significant Findings
        sec['tables'] = [
            t for t in tables
            if (t['header']==sec['name'] and t['page']==sec['start_page'])
               or (sec['name'].lower().startswith('significant findings') and t['page']==sec['start_page']+1)
        ]
        # collect key-values as before
        for kv in kv_pairs:
            if sec['start_page'] <= kv['page'] < (next_sec['start_page'] if next_sec else kv['page']+1):
                sec['fields'].append({'key':kv['key'],'value':kv['value']})
    # build final output, replacing line-based parsing for Significant Findings
    final_sections = []
    for sec in sections:
        if sec['name'].lower().startswith('significant findings'):
            for table in sec['tables']:
                items = parse_significant_findings_table(table)
                for item in items:
                    final_sections.append({'name':sec['name'],'data':item})
        else:
            final_sections.append(sec)
    return final_sections

def process(event, context):
    input_bucket  = event.get('bucket', 'metrosafetyprodfiles')
    document_key  = event['document_key']
    output_bucket = event.get('output_bucket', 'textract-output-digival')

    resp     = textract.start_document_analysis(
       DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':document_key}},
       FeatureTypes=['TABLES','FORMS']
    )
    blocks   = poll_for_job_completion(resp['JobId'])
    pages    = extract_pages_text(blocks)
    tables   = extract_tables_grouped(blocks)
    kv_pairs = extract_key_value_pairs(blocks)
    sections = group_sections(pages, tables, kv_pairs)

    result   = {'document':document_key, 'sections':sections}
    json_key = f"processed/{document_key.rsplit('/',1)[-1].replace('.pdf','.json')}"
    s3.put_object(Bucket=output_bucket, Key=json_key, Body=json.dumps(result))

    return {'statusCode':200, 'body':json.dumps({'json_s3_key':json_key})}