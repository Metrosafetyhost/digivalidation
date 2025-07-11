import json
import os
import time
import boto3
import logging
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)

textract       = boto3.client("textract", region_name="eu-west-2")
s3             = boto3.client("s3")
lambda_client  = boto3.client("lambda")

PROOFING_LAMBDA_ARN_WRA = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-checklist_proofing"
PROOFING_LAMBDA_ARN_FRA = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-fra_checklist_proofing"
PROOFING_LAMBDA_ARN_HSA = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-hsa_checklist_proofing"

IMPORTANT_HEADINGS = [
    "Significant Findings and Action Plan",
    "Contents",
    "Executive Summary",
    "Areas Identified Requiring Remedial Actions",
    "Building Description",
    "Property Description", #hsa
    "Property Site/Description"
    "Accompanying the Risk Assessor",
    "Risk Review and Reassessment",
    "Water Scope",
    "Risk Dashboard",
    "Overall Risk Rating", #hsa
    "Management Responsibilities",
    "Legionella Control Programme",
    "Audit Detail",
    "Water Control Scheme",
    "System Asset Register",
    #"Outlet Temperature Profile",
    "Water Assets",
    "Appendices",
    "Risk Assessment Checklist",
    "Legionella Control Programme of Preventative Works",
    "Building Description - The Building",  #fire
    "Building Description - Fire Safety",  #fire
    "Life Safety Risk Rating at this Premises", #fire
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
    last_tbl = None
    sorted_blocks = sorted(
        blocks,
        key=lambda b: (b.get("Page", 1), b["Geometry"]["BoundingBox"]["Top"])
    )
    current_header = None

    for b in sorted_blocks:
        if b["BlockType"] == "LINE" and is_major_heading(b.get("Text", "")):
            current_header = b["Text"].strip()

        if b["BlockType"] == "TABLE" and current_header:
            # collect rows...
            rows = []
            for rel in b.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    cells = [
                        c for c in blocks
                        if c["Id"] in rel["Ids"] and c["BlockType"] == "CELL"
                    ]
                    rowm = {}
                    for c in cells:
                        ri = c["RowIndex"]
                        txt = ""
                        for r2 in c.get("Relationships", []):
                            if r2["Type"] == "CHILD":
                                for cid in r2["Ids"]:
                                    w = next((x for x in blocks if x["Id"] == cid), None)
                                    if w and w["BlockType"] in ("WORD", "LINE"):
                                        txt += w.get("Text", "") + " "
                        rowm.setdefault(ri, []).append(txt.strip())
                    for ri in sorted(rowm):
                        rows.append(rowm[ri])

            # dedupe rows
            seen = set()
            unique = []
            for row in rows:
                key = tuple(row)
                if key not in seen:
                    seen.add(key)
                    unique.append(row)

            # **merge-only-if** under SFaAP *and* first cell is _not_ empty
            # grab the first‐row’s first cell
            first_cell = unique[0][0] if unique and unique[0] else ""

            # only merge if it's clearly a SFAP “continuation” row:
            #  • starts with the word “Priority”  (Priority Low/Medium/High)
            #  • or contains a dd/mm/yyyy date
            is_fragment = (
                current_header == "Significant Findings and Action Plan"
                and last_tbl
                and (
                    first_cell.startswith("Priority")
                    or bool(re.search(r"\b\d{2}/\d{2}/\d{4}\b", first_cell))
                )
            )

            if is_fragment:
                # drop nothing—just glue on the extra rows
                last_tbl["rows"].extend(unique)
            else:
                # this is a brand‐new table
                tbl = {
                    "page":  b.get("Page",1),
                    "header": current_header,
                    "rows":   unique,
                    "bbox":   b["Geometry"]["BoundingBox"]
                }
                tables.append(tbl)
                last_tbl = tbl

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

        # new section?
        if is_major_heading(txt) and top < 0.85:
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

lambda_client  = boto3.client("lambda")

def process(event, context):
    """
    Unified handler for two invocation styles:
      A) Direct invocation with {"bucket_name", "document_key", "workOrderId", ...}
      B) SNS invocation from Textract completion with {"Records":[{"Sns":{"Message":...}}]}
    In either case, we ultimately want to:
      1) Start Textract on the PDF (if Direct‐invoke) or skip that if SNS‐invoke.
      2) Poll for completion via poll_for_job_completion(job_id).
      3) Run your extract_tables_grouped, extract_key_value_pairs, group_sections.
      4) Write processed JSON to S3 under processed/<pdfName>.json.
      5) Invoke checklist_proofing.py with {"textract_bucket","textract_key","workOrderId"}.
    """

    # ─── Case B: SNS invocation from Textract finish ──────────────────────────────
    if event.get("Records"):
        try:
            sns_message = event["Records"][0]["Sns"]["Message"]
            msg         = json.loads(sns_message)
            job_id      = msg.get("JobId")
            status      = msg.get("Status")

            if status != "SUCCEEDED":
                logger.warning("Textract job %s did not succeed (%s); skipping.", job_id, status)
                return {"statusCode": 200, "body": "Skipped non‐SUCCEEDED job."}

            # Get the original PDF’s S3 location that we started Textract on:
            s3_loc       = msg.get("DocumentLocation", {}).get("S3Object", {})
            bucket_name  = s3_loc.get("Bucket")
            document_key = s3_loc.get("Name")  # e.g. "WorkOrders/ABC123/report.pdf"

            if not bucket_name or not document_key:
                err = f"Malformed DocumentLocation in SNS message: {json.dumps(msg)}"
                logger.error(err)
                return {"statusCode": 400, "body": err}

            # Derive workOrderId (assumes path "WorkOrders/<workOrderId>/…"):
            try:
                workOrderId = document_key.split("/")[1]
            except IndexError:
                workOrderId = ""

            emailAddress = event.get("emailAddress")
            buildingName = event.get("buildingName")
            workTypeRef = event.get("workTypeRef")
            workOrderNumber = event.get("workOrderNumber")
            resourceName  = event.get("resourceName")


            # ─── (1) Poll for blocks using your exact old snippet ───────────────────
            blocks = poll_for_job_completion(job_id)
            logger.info("Textract job %s SUCCEEDED; collected %d blocks", job_id, len(blocks))

            # ─── (2) Run your extraction logic (tables/forms → sections) ────────────
            tables = extract_tables_grouped(blocks)
            fields = extract_key_value_pairs(blocks)
            secs   = group_sections(blocks, tables, fields)
            logger.info("Grouped into %d sections for %s", len(secs), document_key)

            # ─── (3) Write combined JSON to S3: processed/<pdfName>.json ─────────
            output_bucket = os.environ.get("CHECKLIST_OUTPUT_BUCKET", "textract-output-digival")
            pdf_base      = document_key.split("/")[-1].replace(".pdf", ".json")
            processed_key = f"processed/{pdf_base}"
            combined_body = {"document": document_key, "sections": secs}

            s3.put_object(
                Bucket=output_bucket,
                Key=processed_key,
                Body=json.dumps(combined_body).encode("utf-8")
            )
            logger.info("Wrote processed JSON to s3://%s/%s", output_bucket, processed_key)

            # ─── (4) Invoke proofing Lambda (checklist_proofing.py) ──────────────
            proofing_payload = {
                "bucket_name": bucket_name,
                "document_key": document_key,
                "textract_bucket": output_bucket,
                "textract_key":    processed_key,
                "workOrderId":     workOrderId,
                "resourceName": resourceName,
                "emailAddress": emailAddress,
                "buildingName": buildingName,
                "workTypeRef": workTypeRef,
                "workOrderNumber": workOrderNumber,

            }
            if workTypeRef == "C-WRA":
                target_arn = PROOFING_LAMBDA_ARN_WRA
            elif workTypeRef == "C-FRA":
                target_arn = PROOFING_LAMBDA_ARN_FRA
            else:
                target_arn = PROOFING_LAMBDA_ARN_HSA

            lambda_client.invoke(
                FunctionName   = target_arn,
                InvocationType = "Event",
                Payload        = json.dumps(proofing_payload).encode("utf-8")
            )
            logger.info("Invoked %s for %s", target_arn, processed_key)

            return {"statusCode": 200, "body": f"Completed SNS job {job_id}"}

        except Exception as e:
            logger.error("Error in SNS branch of checklist.process: %s", e, exc_info=True)
            raise

    # ─── Case A: Direct invocation (salesforce_input) ─────────────────────────────
    bucket_name   = event.get("bucket_name")
    document_key  = event.get("document_key")
    workOrderId   = event.get("workOrderId", "")
    buildingName   = event.get("buildingName")
    resourceName = event.get("resourceName")
    output_bucket = os.environ.get("CHECKLIST_OUTPUT_BUCKET", "textract-output-digival")
    emailAddress    = event.get("emailAddress")  
    workOrderNumber = event.get("workOrderNumber")
    workTypeRef = event.get("workTypeRef")


    if not bucket_name or not document_key:
        err = f"When directly invoked, 'bucket_name' and 'document_key' must be provided. Received: {json.dumps(event)}"
        logger.error(err)
        return {"statusCode": 400, "body": err}

    try:
        # ─── (1) Start Textract job ─────────────────────────────────────────────
        tex_resp = textract.start_document_analysis(
            DocumentLocation={
                "S3Object": {"Bucket": bucket_name, "Name": document_key}
            },
            FeatureTypes=["TABLES", "FORMS"]
        )
        job_id = tex_resp["JobId"]
        logger.info("Started Textract job %s for s3://%s/%s", job_id, bucket_name, document_key)

        blocks = poll_for_job_completion(job_id)
        logger.info("Textract job %s SUCCEEDED; collected %d blocks", job_id, len(blocks))

        tables = extract_tables_grouped(blocks)
        fields = extract_key_value_pairs(blocks)
        secs   = group_sections(blocks, tables, fields)
        logger.info("Grouped into %d sections for %s", len(secs), document_key)

        #change here 
        pdf_base      = document_key.split("/")[-1].replace(".pdf", ".json")
        processed_key = f"processed/{pdf_base}"
        combined_body = {"document": document_key, "sections": secs}

        s3.put_object(
            Bucket=output_bucket,
            Key=processed_key,
            Body=json.dumps(combined_body).encode("utf-8")
        )
        logger.info("Wrote processed JSON to s3://%s/%s", output_bucket, processed_key)

        # ─── (5) Invoke proofing Lambda ───────────────────────────────────────
        proofing_payload = {
            "bucket_name": bucket_name,
            "document_key": document_key,
            "textract_bucket": output_bucket,
            "textract_key":    processed_key,
            "workOrderId":     workOrderId,
            "resourceName": resourceName,
            "workTypeRef" : workTypeRef,
            "workOrderNumber": workOrderNumber,
            "emailAddress": emailAddress,
            "buildingName" : buildingName,

        }
        if workTypeRef == "C-WRA":
            target_arn = PROOFING_LAMBDA_ARN_WRA
        elif workTypeRef == "C-FRA":
            target_arn = PROOFING_LAMBDA_ARN_FRA
        else:
            target_arn = PROOFING_LAMBDA_ARN_HSA

        lambda_client.invoke(
            FunctionName   = target_arn,
            InvocationType = "Event",
            Payload        = json.dumps(proofing_payload).encode("utf-8")
        )
        logger.info("Invoked %s for %s", target_arn, processed_key)

        return {"statusCode": 200, "body": json.dumps({"json_s3_key": processed_key})}

    except Exception as e:
        logger.error("Error in direct‐invoke branch of checklist.process: %s", e, exc_info=True)
        raise
