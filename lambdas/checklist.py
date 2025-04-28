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
def process(event, context):
    # 1. Get input location
    rec          = event['Records'][0]['s3']
    input_bucket = rec['bucket']['name']
    document_key = rec['object']['key']

    # 2. Kick off Textract (tables + forms)
    resp = textract.start_document_analysis(
        DocumentLocation={'S3Object':{'Bucket':input_bucket,'Name':document_key}},
        FeatureTypes=['TABLES','FORMS']
    )
    job_id = resp['JobId']

    # 3. Poll until done
    while True:
        status = textract.get_document_analysis(JobId=job_id)['JobStatus']
        if status in ('SUCCEEDED','FAILED'):
            break
        time.sleep(5)
    if status != 'SUCCEEDED':
        raise RuntimeError(f"Textract failed: {status}")

    # 4. Retrieve all blocks
    blocks    = []
    next_tok  = None
    while True:
        kwargs = {'JobId': job_id}
        if next_tok:
            kwargs['NextToken'] = next_tok
        resp = textract.get_document_analysis(**kwargs)
        blocks.extend(resp['Blocks'])
        next_tok = resp.get('NextToken')
        if not next_tok:
            break

    # 5. Parse into sections
    sections = parse_sections(blocks)

    # 6. Write JSON back to the processed/ folder in textract-output-digival
    output_bucket = 'textract-output-digival'
    json_key      = 'processed/' + document_key.rsplit('/',1)[-1].replace('.pdf', '.json')
    result        = {'document': document_key, 'sections': sections}

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

def poll_for_job_completion(job_id, max_tries=60, delay=5):
    """Poll until Textract job succeeds, then return all blocks."""
    for _ in range(max_tries):
        status = textract.get_document_analysis(JobId=job_id)['JobStatus']
        if status == 'SUCCEEDED':
            break
        if status == 'FAILED':
            raise RuntimeError("Textract analysis failed")
        time.sleep(delay)

    all_blocks = []
    next_token = None
    while True:
        kwargs = {'JobId': job_id}
        if next_token:
            kwargs['NextToken'] = next_token
        resp = textract.get_document_analysis(**kwargs)
        all_blocks.extend(resp['Blocks'])
        next_token = resp.get('NextToken')
        if not next_token:
            break

    return all_blocks


def parse_sections(blocks):
    """Organise LINEs and TABLEs under each IMPORTANT_HEADINGS."""
    # map by ID for fast lookup
    id_map = {b['Id']: b for b in blocks}

    # collect lines & tables per page
    page_lines, page_tables = {}, {}
    for b in blocks:
        if b['BlockType']=='LINE':
            page_lines.setdefault(b['Page'], []).append(b)
        elif b['BlockType']=='TABLE':
            page_tables.setdefault(b['Page'], []).append(b)

    sections = []
    for page in sorted(page_lines):
        lines  = sorted(page_lines[page],
                        key=lambda x: x['Geometry']['BoundingBox']['Top'])
        tables = page_tables.get(page, [])
        # pre-extract table arrays
        table_data = [extract_table(tbl, blocks, id_map) for tbl in tables]
        t_idx = 0
        current = None

        for line in lines:
            text = line['Text'].strip()
            # start new section?
            if text in IMPORTANT_HEADINGS:
                current = {'name': text, 'paragraphs': [], 'tables': []}
                sections.append(current)
                continue

            if not current:
                continue

            # attach any tables whose top lies below this line
            while (t_idx < len(tables) and
                   tables[t_idx]['Geometry']['BoundingBox']['Top']
                   > line['Geometry']['BoundingBox']['Top']):
                current['tables'].append(table_data[t_idx])
                t_idx += 1

            # accumulate paragraph text
            current['paragraphs'].append(text)

        # leftover tables go to the last section on this page
        while t_idx < len(tables) and current:
            current['tables'].append(table_data[t_idx])
            t_idx += 1

    return sections


def extract_table(tbl_block, blocks, id_map):
    """Convert one TABLE block into a 2D list of cell texts."""
    # find cells belonging to this table
    cells = [b for b in blocks
             if b['BlockType']=='CELL' and
                any(rel['Type']=='CHILD' for rel in b.get('Relationships', []))
             and b.get('Table', {}).get('Id') == tbl_block['Id']]

    # organise by row/col
    rows = {}
    for cell in cells:
        r = cell['RowIndex']
        c = cell['ColumnIndex']
        text = ""
        for rel in cell.get('Relationships', []):
            if rel['Type']=='CHILD':
                for cid in rel['Ids']:
                    w = id_map[cid]
                    if w['BlockType']=='WORD':
                        text += w['Text'] + ' '
                    elif (w['BlockType']=='SELECTION_ELEMENT' and
                          w['SelectionStatus']=='SELECTED'):
                        text += '[X] '
        rows.setdefault(r, {})[c] = text.strip()

    # build row-ordered list of lists
    max_col = max(max(cols.keys()) for cols in rows.values())
    table = []
    for r in sorted(rows):
        row = [rows[r].get(c, "") for c in range(1, max_col+1)]
        table.append(row)

    return table