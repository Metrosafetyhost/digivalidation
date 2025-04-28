import json
import time
import boto3

# AWS clients
textract = boto3.client('textract', region_name='eu-west-2')
s3       = boto3.client('s3')

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

def extract_significant_findings_from_table(section):
    """
    Treat the first table under this section as:
      Row0: "Management of Risk" header → skip
      Row1: the real question (e.g. "12.2. …?")
      Row2: ["Priority", "Observation"]
      Row3: ["Medium", "…observations…"]
      Row4: ["Target Date", "Action Required"]
      Row5: ["20/04/2025", "…action text…"]

    Returns a dict with keys:
      Question, Priority, Observation, Target Date, Action Required
    """
    tables = section.get('tables', [])
    if not tables:
        return {}

    table = tables[0]

    # 1) Find the question row: skip any top row that matches "Management of Risk"
    question_row = 0
    top = table[0][0].strip().lower()
    if top.startswith(section['name'].lower()) or "management of risk" in top:
        question_row = 1

    # 2) Pull the question
    question = table[question_row][0].strip()

    # 3) Locate the first label row ("Priority")
    label_row_idx = next(
        (i for i in range(question_row+1, len(table))
         if any(cell.strip()=="Priority" for cell in table[i])),
        None
    )
    if label_row_idx is None or label_row_idx+1 >= len(table):
        return {"Question": question}

    # 4) Map the two pairs of label→value
    result = {"Question": question}
    i = label_row_idx
    while i+1 < len(table):
        labels = table[i]
        values = table[i+1]
        for col,label in enumerate(labels):
            val = values[col] if col < len(values) else ""
            result[label.strip()] = val.strip()
        i += 2

    return result


# section name → extractor
SECTION_EXTRACTORS = {
    "Significant Findings and Action Plan": extract_significant_findings_from_table
}


def process(event, context):
    # ─── determine input/output ───────────────────────────────────────────────
    input_bucket  = event.get('bucket', 'metrosafetyprodfiles')
    document_key  = event['document_key']
    output_bucket = event.get('output_bucket','textract-output-digival')

    # ─── start Textract ───────────────────────────────────────────────────────
    start = textract.start_document_analysis(
        DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':document_key}},
        FeatureTypes=['TABLES','FORMS']
    )
    job_id = start['JobId']

    # ─── poll until done ───────────────────────────────────────────────────────
    while True:
        status = textract.get_document_analysis(JobId=job_id)['JobStatus']
        if status in ('SUCCEEDED','FAILED'):
            break
        time.sleep(5)
    if status != 'SUCCEEDED':
        raise RuntimeError(f"Textract failed: {status}")

    # ─── retrieve all blocks ───────────────────────────────────────────────────
    blocks   = []
    next_tok = None
    while True:
        kwargs = {'JobId': job_id}
        if next_tok:
            kwargs['NextToken'] = next_tok
        resp = textract.get_document_analysis(**kwargs)
        blocks.extend(resp['Blocks'])
        next_tok = resp.get('NextToken')
        if not next_tok:
            break

    # ─── group into sections ──────────────────────────────────────────────────
    sections = parse_sections(blocks)

    # ─── extract per‐section data ──────────────────────────────────────────────
    output_secs = []
    for sec in sections:
        name      = sec['name']
        extractor = SECTION_EXTRACTORS.get(name, lambda s: s['tables'])
        data      = extractor(sec)
        output_secs.append({'name': name, 'data': data})

    # ─── write JSON back to s3 ────────────────────────────────────────────────
    result   = {'document': document_key, 'sections': output_secs}
    json_key = 'processed/' + document_key.rsplit('/',1)[-1].replace('.pdf','.json')
    s3.put_object(
        Bucket=output_bucket,
        Key=json_key,
        Body=json.dumps(result, indent=2).encode('utf-8'),
        ContentType='application/json'
    )

    return {
        'statusCode': 200,
        'body': json.dumps({'json_s3_key': json_key})
    }


# ─────────────────────────────────────────────────────────────────────────────
# your existing parse_sections & extract_table unchanged below
# ─────────────────────────────────────────────────────────────────────────────
def parse_sections(blocks):
    id_map = {b['Id']: b for b in blocks}
    page_lines,page_tables = {},{}
    for b in blocks:
        if b['BlockType']=='LINE':
            page_lines.setdefault(b['Page'],[]).append(b)
        elif b['BlockType']=='TABLE':
            page_tables.setdefault(b['Page'],[]).append(b)

    sections = []
    for page in sorted(page_lines):
        lines  = sorted(page_lines[page],
                        key=lambda x: x['Geometry']['BoundingBox']['Top'])
        tables = page_tables.get(page,[])
        table_data = [extract_table(t,blocks,id_map) for t in tables]
        t_idx = 0
        current = None

        for line in lines:
            text = line['Text'].strip()
            if text in IMPORTANT_HEADINGS:
                current = {'name':text,'paragraphs':[],'tables':[]}
                sections.append(current)
                continue
            if not current:
                continue

            while (t_idx < len(tables)
                   and tables[t_idx]['Geometry']['BoundingBox']['Top']
                     > line['Geometry']['BoundingBox']['Top']):
                current['tables'].append(table_data[t_idx])
                t_idx += 1
            current['paragraphs'].append(text)

        while t_idx < len(tables) and current:
            current['tables'].append(table_data[t_idx])
            t_idx += 1

    return sections

def extract_table(tbl, blocks, id_map):
    cells = [b for b in blocks
             if b['BlockType']=='CELL'
             and b.get('Table',{}).get('Id')==tbl['Id']]
    if not cells:
        return []

    rows = {}
    for cell in cells:
        r,c = cell['RowIndex'],cell['ColumnIndex']
        txt = ""
        for rel in cell.get('Relationships',[]):
            if rel['Type']=='CHILD':
                for cid in rel['Ids']:
                    w = id_map[cid]
                    if w['BlockType']=='WORD':
                        txt += w['Text']+" "
                    elif (w['BlockType']=='SELECTION_ELEMENT'
                          and w['SelectionStatus']=='SELECTED'):
                        txt += "[X] "
        rows.setdefault(r,{})[c] = txt.strip()

    max_col = max(max(cols.keys()) for cols in rows.values())
    table   = []
    for r in sorted(rows):
        table.append([ rows[r].get(c,"") for c in range(1,max_col+1) ])
    return table
