import json
import boto3
import logging
import uuid
import time
import io
import os
import csv
from bs4 import BeautifulSoup
import re

# Initialise logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROOFING_CHECKLIST_ARN = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-checklist"
lambda_client = boto3.client("lambda")

# AWS clients
bedrock_client = boto3.client("bedrock-runtime", region_name="eu-west-2")
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Email nots perms 
ses_client = boto3.client("ses", region_name="eu-west-2")
SENDER = "luke.gasson@metrosafety.co.uk"
RECIPIENT = "luke.gasson@metrosafety.co.uk"  

# Configurations
BEDROCK_MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"
BUCKET_NAME = "metrosafety-bedrock-output-data-dev-bedrock-lambda"
TABLE_NAME = "ProofingMetadata"
PDF_BUCKET = "metrosafetyprodfiles"

def strip_html(html):
    """Strip HTML tags from a string and return only text."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(separator=" ", strip=True)
    except Exception as e:
        logger.error(f"Error stripping HTML: {str(e)}")
        return html
    
TAG_MAP = {
    '<p>':  '[[P]]',
    '</p>':'[[/P]]',
    '<ul>':'[[UL]]',
    '</ul>':'[[/UL]]',
    '<li>':'[[LI]]',
    '</li>':'[[/LI]]',
    '<u>': '[[U]]',
    '</u>':'[[/U]]',
}

def protect_html(text):
    for real, placeholder in TAG_MAP.items():
        text = text.replace(real, placeholder)
    return text

def restore_html(text):
    for real, placeholder in TAG_MAP.items():
        text = text.replace(placeholder, real)
    return text

def proof_table_content(html, record_id):
    try:
        soup  = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            logger.warning(f"No table in record {record_id}")
            return html, []

        rows = table.find_all("tr")
        if not rows:
            logger.warning(f"No rows in table for record {record_id}")
            return html, []

        # 1) Extract & protect each cell’s HTML
        fragments = []
        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) >= 2:
                raw = tds[1].decode_contents()
                fragments.append(protect_html(raw))
            else:
                fragments.append("")

        # 2) Ask Claude to return a JSON array, no extra text
        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "system": (
                "You are a meticulous proofreader. Correct only spelling, grammar and punctuation in British English. "
                "Do NOT add, remove, reorder, split or merge any text or HTML tags. "
                "Output only the corrected JSON array of strings, matching the input array exactly."
                "Ensure each sentence ends with a full stop unless it already ends with appropriate punctuation (e.g. '.', '!', '?')"
            ),
            "messages": [{
                "role": "user",
                "content": "Proofread this JSON array of HTML fragments (no commentary):\n\n"
                           + json.dumps(fragments, ensure_ascii=False)
            }],
            "max_tokens": 3000,
            "temperature": 0.0
        }
        logger.info(f"Record {record_id}: sending {len(fragments)} fragments to Bedrock")
        response = bedrock_client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload)
        )

        # 3) Parse the JSON array from Claude’s response
        body = json.loads(response["body"].read())
        raw  = body["content"][0]["text"].strip()
        # remove a leading ```html\n or ```\n
        raw = re.sub(r"^```(?:html)?\r?\n", "", raw)
        # remove any trailing ``` fences
        raw = re.sub(r"\r?\n```$", "", raw)

        corrected_protected = json.loads(raw)

        # 4) Re-insert each fragment, restoring real tags
        log_entries = []
        for idx, tr in enumerate(rows):
            tds = tr.find_all("td")
            if len(tds) >= 2:
                restored = restore_html(corrected_protected[idx])
                tds[1].clear()
                tds[1].append(BeautifulSoup(restored, "html.parser"))

                header = tds[0].get_text(separator=" ", strip=True)
                log_entries.append({
                    "recordId": record_id,
                    "header":    header,
                    "original":  protect_html(fragments[idx]),
                    "proofed":   restored
                })

        return str(soup), log_entries

    except Exception as e:
        logger.error(f"Error in proof_table_content for {record_id}: {e}")
        return html, []


def proof_plain_text(text, record_id):
    PRESERVE_TAGS = ['<p>', '<ul>', '<li>', '<u>', '</p>', ',</ul>', '</li>', '</u>']
    if any(tag in text.lower() for tag in PRESERVE_TAGS):
        plain_text = text
    else:
        plain_text = strip_html(text)
    try:
        logger.info(f"Proofing record {record_id}. Plain text: {plain_text}")
        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "system": (
            "You are a meticulous proofreader. "
            "Correct spelling, grammar and clarity only — no extra commentary or re-structuring."
            "Ensure each sentence ends with a full stop unless it already ends with appropriate punctuation (e.g. '.', '!', '?')"
        ),
            "messages": [{
                "role": "user",
                "content": (
                    "Proofread the following text according to these strict guidelines:\n"
                    "- Do NOT add any new introductory text or explanatory sentences before or after the original content - aka  **Do not** add any introductory sentence such as “Here is the corrected text:” or similar.\n"
                    "- Spelling and grammar are corrected in British English, and spacing is corrected.\n"
                    "- Headings, section titles, and structure remain unchanged.\n"
                    "- Do NOT remove any words or phrases from the original content.\n"
                    "- Do NOT split, merge, or add any new sentences or content.\n"
                    "- Ensure that lists, bullet points, and standalone words remain intact.\n"
                    "- Ensure only to proofread once, NEVER repeat the same text twice in the output.\n\n"
                    "Text to proofread: " + plain_text
                )
            }],
            "max_tokens": 1500,
            "temperature": 0.3
            
        }
        logger.info("Sending payload to Bedrock: " + json.dumps(payload, indent=2))
        response = bedrock_client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload)
        )
        response_body = json.loads(response["body"].read().decode("utf-8"))
        proofed_text = " ".join(
            [msg["text"] for msg in response_body.get("content", []) if msg.get("type") == "text"]
        ).strip()
        return proofed_text if proofed_text else text
    except Exception as e:
        logger.error(f"Error proofing plain text for record {record_id}: {e}")
        return text

def update_logs_csv(log_entries, filename, folder):
    """
    Merge new log_entries into the existing CSV in S3, dedupe, flatten newlines, and overwrite.
    log_entries: list of dicts with keys recordId, header, original, proofed
    """
    s3_key = f"{folder}/{filename}.csv"

    # 1. Load existing rows (if any)
    existing_rows = []
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        text = obj['Body'].read().decode('utf-8')
        reader = csv.reader(io.StringIO(text))
        existing_rows = list(reader)
    except s3_client.exceptions.NoSuchKey:
        # No existing file, we'll start fresh
        pass

    # 2. Initialize merged list and seen set
    merged = []
    seen = set()

    # 3. Add header row
    header = ['Record ID', 'Header', 'Original Text', 'Proofed Text']
    if not existing_rows:
        merged.append(header)
    else:
        for row in existing_rows:
            tup = tuple(row)
            if tup not in seen:
                merged.append(row)
                seen.add(tup)

    # 4. Add each new entry, flatten any embedded newlines
    for e in log_entries:
        orig = e.get('original', '')
        proof = e.get('proofed', '')

        # Replace real line-breaks with a space
        orig_clean = re.sub(r'[\r\n]+', ' ', orig).strip()
        proof_clean = re.sub(r'[\r\n]+', ' ', proof).strip()

        row = [
            e.get('recordId', ''),
            e.get('header', ''),
            orig_clean,
            proof_clean
        ]
        tup = tuple(row)
        if tup not in seen:
            merged.append(row)
            seen.add(tup)

    # 5. Write merged list back out as a well-formed CSV
    out = io.StringIO()
    writer = csv.writer(
        out,
        delimiter=',',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        escapechar='\\',
        lineterminator='\n'
    )
    writer.writerows(merged)

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=out.getvalue().encode('utf-8')
    )

    return s3_key

def store_metadata(workorder_id, logs_s3_key, status):
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(
        Item={
            "workorder_id": workorder_id,
            "logs_s3_key": logs_s3_key,
            "status": status,  # "Proofed" or "Original"
            "timestamp": int(time.time())
        }
    )

def load_payload(event):
    """
    Extracts payload from the incoming event.
    Expected JSON structure:
      {
        "workOrderId": "...",
        "contentType": "FormQuestion" or "Action_Observation" or "Action_Required",
        "sectionContents": [ { "recordId": "...", "content": "..." }, ... ]
      }
    """
    try:
        raw_body = event.get("body", "")
        logger.info("Raw payload body received: " + raw_body)
        body = json.loads(raw_body)
        content_type = body.get("contentType", "Unknown")
        items = body.get("sectionContents", [])
        logger.info(f"Payload contentType: {content_type}, records received: {len(items)}")
        
        proofing_requests = {}
        table_data = {}
        for entry in items:
            record_id = entry.get("recordId")
            content = entry.get("content")
            if not record_id or not content:
                logger.warning(f"Skipping entry with missing recordId or content: {entry}")
                continue
            proofing_requests[record_id] = content.strip()
            table_data[record_id] = {"content": content.strip(), "record_id": record_id}
        
        return body.get("workOrderId"), content_type, proofing_requests, table_data

    except Exception as e:
        logger.error(f"Unexpected error in load_payload: {e}")
        return None, None, {}, {}
    

# def notify_run(workorder_id, status):
#     subject = f"Work Order {workorder_id} Processed: {status}"
#     body = (
#         f"Hello team,\n\n"
#         f"The proofing Lambda has just run for Work Order ID: {workorder_id}.\n"
#         f"Overall status: {status}.\n\n"
#         f"Cheers,\nYour AWS Lambda"
#     )
#     ses_client.send_email(
#         Source=SENDER,
#         Destination={ "ToAddresses": [RECIPIENT] },
#         Message={
#             "Subject": { "Data": subject },
#             "Body": { "Text": { "Data": body } }
#         }
#     )

def process(event, context):
    """
    1. Parse incoming JSON (via API Gateway).
    2. Always run the “old JSON-proofing” code exactly as before.
    3. If workTypeRef == "C-WRA", then find the latest PDF in S3 and call checklist.process(...)
       to run Textract on it and write out the Textract JSON→S3.
    4. Return the same shape of response your old Lambda returned (i.e. proofed sectionContents).
    """

    # ────────────────────────────────────────────────────────────────────────────────
    # 1) Parse incoming JSON to pull out workTypeRef, buildingName, workOrderId
    # ────────────────────────────────────────────────────────────────────────────────
    try:
        raw_body = event.get("body", "")
        logger.info("Incoming payload: %s", raw_body)
        body = json.loads(raw_body)
    except Exception as e:
        logger.error("Error parsing request body: %s", e, exc_info=True)
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON format"})
        }
    
    
    # if not assessor_email:
    #     logger.error("Missing required field 'assessor_email' in event: %s", event)
    #     return {"statusCode": 400, "body": "Missing assessor_email"}
    work_type    = body.get("workTypeRef")      # e.g. "C-WRA" or something else
    buildingName = body.get("buildingName")     # e.g. "Main Office"
    workorder_id = body.get("workOrderId")      # e.g. "00X123456789"
    emailAddress = event.get("emailAddress")
    bucket_name = "metrosafetyprodfiles"

    # if not workorder_id or not buildingName:
    #     logger.error("Missing workOrderId or buildingName in payload.")
    #     return {
    #         "statusCode": 400,
    #         "body": json.dumps({"error": "workOrderId and buildingName are required"})
    #     }

    logger.info("Parsed workTypeRef=%s, buildingName=%s, workOrderId=%s",
                work_type, buildingName, workorder_id)

    try:
        wo_id_old, content_type, proofing_requests, table_data = load_payload(event)
        if not wo_id_old:
            raise ValueError("Missing workOrderId in payload (old path).")
    except Exception as e:
        logger.error("Error in load_payload(): %s", e, exc_info=True)
        return {
            "statusCode": 200,
            "body": json.dumps({"error": "Invalid JSON format"})
        }

    if not proofing_requests:
        logger.error("No proofing items extracted from payload.")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "No proofing items found"})
        }

    # Prepare to collect all JSON-proofed results
    csv_log_entries      = []
    overall_proofed_flag = False
    proofed_entries      = []

    for record_id, content in proofing_requests.items():
        if content_type == "FormQuestion":
            # exactly as your old code:
            updated_html, log_entries = proof_table_content(content, record_id)
            for entry in log_entries:
                entry["recordId"] = record_id
                csv_log_entries.append(entry)
                if entry["original"] != entry["proofed"]:
                    overall_proofed_flag = True

            rec_data = table_data.get(record_id)
            if rec_data:
                proofed_entries.append({"recordId": record_id, "content": updated_html})
            else:
                logger.warning("No table_data found for record %s", record_id)

        else:
            # “Action_Observation” or “Action_Required” path:
            corrected_text = proof_plain_text(content, record_id)
            orig_text      = strip_html(content)
            corr_text      = strip_html(corrected_text)

            if corr_text != orig_text:
                overall_proofed_flag = True
                csv_log_entries.append({
                    "recordId": record_id,
                    "header":   "",
                    "original": orig_text,
                    "proofed":  corr_text
                })
            else:
                csv_log_entries.append({
                    "recordId": record_id,
                    "header":   "",
                    "original": "No changes needed: " + orig_text,
                    "proofed":  "No changes made."
                })

            proofed_entries.append({"recordId": record_id, "content": corrected_text})

    status_flag = "Proofed" if overall_proofed_flag else "Original"
    logger.info("Work order flagged as: %s", status_flag)
    logger.info("Updating logs CSV in S3 for old JSON-proofing path…")

    # Write exactly where your old code wrote it:
    csv_filename = f"{workorder_id}_logs"             # same as before
    csv_s3_key   = update_logs_csv(csv_log_entries, csv_filename, "logs")
    logger.info("Old JSON-proof CSV stored in S3 at key: %s", csv_s3_key)

    # Update your metadata table exactly as before
    store_metadata(workorder_id, csv_s3_key, status_flag)
    # notify_run(workorder_id, status_flag)  # if you still want that email

    # Keep this list around so we can return it below
    combined_proofed = proofed_entries.copy()

    final_response = {
    "workOrderId": wo_id_old,
    "contentType": content_type,
    "sectionContents": combined_proofed
}

    # ────────────────────────────────────────────────────────────────────────────────
    # 3) If this is a C-WRA job, run the extra “fetch latest PDF → Textract” via checklist.py
    # ────────────────────────────────────────────────────────────────────────────────
    if work_type == "C-WRA" and workorder_id:
            bucket_name = "metrosafetyprodfiles"
            prefix      = f"WorkOrders/{workorder_id}/"

            # Check for the zero-byte marker
            marker_key = prefix + ".textract_ran"
            try:
                s3_client.head_object(Bucket=bucket_name, Key=marker_key)
                logger.info("Marker exists (%s); skipping Textract for %s", marker_key, workorder_id)
            except s3_client.exceptions.ClientError as ce:
                if ce.response.get("Error", {}).get("Code") == "404":
                    # ←– replace everything here with the new block
                    resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
                    contents = resp.get("Contents", [])

                    valid_objs = [
                        obj for obj in contents
                        if not obj["Key"].endswith(".textract_ran")
                            and not obj["Key"].lower().endswith((".xlsx", ".xls", ".csv"))
                    ]

                    if not valid_objs:
                        logger.error("No suitable document found under %s", prefix)
                        return {"statusCode": 400, "body": "No document file to process."}

                    newest = max(valid_objs, key=lambda o: o["LastModified"])
                    document_key = newest["Key"]
                    logger.info("Picked newest PDF: s3://%s/%s", bucket_name, document_key)

                    # Write the zero-byte marker so we don’t re-run next time
                    s3_client.put_object(Bucket=bucket_name, Key=marker_key, Body=b"")
                    logger.info("Created marker %s", marker_key)

                    # Invoke checklist.py asynchronously
                    checklist_payload = {
                        "bucket_name":   bucket_name,
                        "document_key":  document_key,
                        "workOrderId":   workorder_id,
                        "emailAddress": emailAddress,
                        "buildingName": buildingName
                    }
                    lambda_client.invoke(
                        FunctionName   = PROOFING_CHECKLIST_ARN,
                        InvocationType = "Event",  # async “fire & forget”
                        Payload        = json.dumps(checklist_payload).encode("utf-8")
                    )
                    logger.info("Invoked checklist.py for %s", document_key)

                else:
                    # Some other S3 error
                    logger.error("Error checking marker %s: %s", marker_key, ce, exc_info=True)
                    raise

    else:
            logger.info("Not C-WRA or missing workOrderId; skipping Textract trigger.")

    return {
        "statusCode": 200,
        "headers": { "Content-Type": "application/json" },
        "body": json.dumps(final_response)
    }


def process(event, context):
    # 1) parse common fields
    body = json.loads(event.get("body",""))
    work_type    = body.get("workTypeRef")
    workorder_id = body.get("workOrderId")
    email_addr   = body.get("emailAddress")
    buildingName = body.get("buildingName")

    # 2) ALWAYS run your AI-proofing
    wo, ct, proof_reqs, table_data = load_payload(event)
    if not proof_reqs:
        return {"statusCode":400,"body":json.dumps({"error":"No proofing items found"})}

    logs, flag, proofed = [], False, []
    for rid, cont in proof_reqs.items():
        if ct == "FormQuestion":
            html, le = proof_table_content(cont, rid)
            for e in le:
                logs.append(e)
                if e["original"]!=e["proofed"]:
                    flag = True
            proofed.append({"recordId":rid,"content":html})
        else:
            txt = proof_plain_text(cont, rid)
            orig = strip_html(cont); corr=strip_html(txt)
            if corr!=orig:
                flag = True
                logs.append({"recordId":rid,"header":"","original":orig,"proofed":corr})
            else:
                logs.append({"recordId":rid,"header":"",
                             "original":"No changes needed: "+orig,
                             "proofed":"No changes made."})
            proofed.append({"recordId":rid,"content":txt})

    status = "Proofed" if flag else "Original"
    csv_key = update_logs_csv(logs, f"{workorder_id}_logs", "logs")
    store_metadata(workorder_id, csv_key, status)

    final_response = {
        "workOrderId":     workorder_id,
        "contentType":     ct,
        "sectionContents": proofed
    }

    # 3) THEN trigger Textract/checklist for C-WRA only
    if work_type == "C-WRA" and workorder_id:
        prefix = f"WorkOrders/{workorder_id}/"
        marker = prefix + ".textract_ran"
        s3 = boto3.client("s3")
        try:
            s3.head_object(Bucket=PDF_BUCKET, Key=marker)
            logger.info("Marker exists, skipping Textract.")
        except s3.exceptions.ClientError as ce:
            if ce.response["Error"]["Code"] == "404":
                # find newest PDF
                resp = s3.list_objects_v2(Bucket=PDF_BUCKET, Prefix=prefix)
                pdfs = [o for o in resp.get("Contents",[]) if o["Key"].lower().endswith(".pdf")]
                if pdfs:
                    newest = max(pdfs, key=lambda x: x["LastModified"])
                    doc_key = newest["Key"]
                    # write marker
                    s3.put_object(Bucket=PDF_BUCKET, Key=marker, Body=b"")
                    # invoke checklist
                    payload = {
                      "bucket_name":  PDF_BUCKET,
                      "document_key": doc_key,
                      "workOrderId":  workorder_id,
                      "emailAddress": email_addr,
                      "buildingName": buildingName
                    }
                    lambda_client.invoke(
                        FunctionName=PROOFING_CHECKLIST_ARN,
                        InvocationType="Event",
                        Payload=json.dumps(payload).encode("utf-8")
                    )
                else:
                    logger.error("No PDF found under %s", prefix)
            else:
                raise

    # 4) return proofed JSON every time
    return {
        "statusCode": 200,
        "headers":    {"Content-Type":"application/json"},
        "body":       json.dumps(final_response)
    }