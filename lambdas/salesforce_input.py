import json
import boto3
import logging
import uuid
import time
import io
import csv
from bs4 import BeautifulSoup
import re

# Initialise logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

    work_type    = body.get("workTypeRef")      # e.g. "C-WRA" or something else
    buildingName = body.get("buildingName")     # e.g. "Main Office"
    workorder_id = body.get("workOrderId")      # e.g. "00X123456789"

    if not workorder_id or not buildingName:
        logger.error("Missing workOrderId or buildingName in payload.")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "workOrderId and buildingName are required"})
        }

    logger.info("Parsed workTypeRef=%s, buildingName=%s, workOrderId=%s",
                work_type, buildingName, workorder_id)

    try:
        wo_id_old, content_type, proofing_requests, table_data = load_payload(event)
        if not wo_id_old:
            raise ValueError("Missing workOrderId in payload (old path).")
    except Exception as e:
        logger.error("Error in load_payload(): %s", e, exc_info=True)
        return {
            "statusCode": 400,
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

    # ────────────────────────────────────────────────────────────────────────────────
    # 3) If this is a C-WRA job, run the extra “fetch latest PDF → Textract” via checklist.py
    # ────────────────────────────────────────────────────────────────────────────────
    if work_type == "C-WRA":
        logger.info("workTypeRef == C-WRA → running checklist.process(...) for Textract…")

        bucket_name = "metrosafetyprodfiles"
        prefix = f"WorkOrders/{workorder_id}/"

        # → STEP 0: Check for an existing “marker” file
        marker_key = f"WorkOrders/{workorder_id}/.textract_ran"
        try:
            s3_client.head_object(Bucket=bucket_name, Key=marker_key)
            logger.info(
                "Marker %s already exists; skipping Textract path for workOrderId=%s",
                marker_key, workorder_id
            )
        except s3_client.exceptions.NoSuchKey:
            # Marker doesn't exist → first time for this workOrderId
            # → STEP 1: List only PDFs under WorkOrders/<workorder_id>/
            paginator = s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

            all_pdfs = []
            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.lower().endswith(".pdf"):
                        all_pdfs.append({
                            "Key":          key,
                            "LastModified": obj["LastModified"]
                        })

            if not all_pdfs:
                logger.warning("No PDFs found under %s; skipping Textract path.", prefix)
            else:
                # → STEP 2: Pick the most recently modified PDF
                latest = sorted(all_pdfs, key=lambda x: x["LastModified"], reverse=True)[0]
                latest_key = latest["Key"]
                logger.info("Most recently uploaded PDF key: %s", latest_key)

                # → STEP 3: Run checklist.process(...) one time
                textract_event = {
                    "bucket":       bucket_name,
                    "document_key": latest_key
                }
                from checklist import process as textract_process

                try:
                    tex_start = time.time()
                    resp = textract_process(textract_event, None)
                    tex_end = time.time()
                    logger.info(
                        "Textract (checklist.process) returned: %s  (took %.1f s)",
                        resp, (tex_end - tex_start)
                    )
                except Exception as e:
                    logger.error("Failed to run checklist.process(...): %s", e, exc_info=True)
                    # if you want to fail the Lambda here, return a 500 or raise.

                # → STEP 4: Create a zero-byte “.textract_ran” marker so we won’t run again
                try:
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=marker_key,
                        Body=b"" 
                    )
                    logger.info("Wrote Textract marker -> %s", marker_key)
                except Exception as e:
                    logger.error("Unable to write marker file %s: %s", marker_key, e, exc_info=True)

        # else:  # if head_object did find that marker, we skip the above block

    else:
        logger.info("workTypeRef != C-WRA → skipping Textract path.")
