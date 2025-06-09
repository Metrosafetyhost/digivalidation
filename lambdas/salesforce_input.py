
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
    Merge new log_entries into the existing CSV in S3, dedupe, and overwrite.
    log_entries: list of dicts with keys recordId, header, original, proofed
    """
    s3_key = f"{folder}/{filename}.csv"

    # 1. Load existing rows (if any)
    existing_rows = []
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        text = obj["Body"].read().decode("utf-8")
        reader = csv.reader(io.StringIO(text))
        existing_rows = list(reader)
    except s3_client.exceptions.NoSuchKey:
        pass

    # 2. Build up merged, deduped rows
    merged = []
    seen = set()

    # If no existing file, write header
    if not existing_rows:
        header = ["Record ID","Header","Original Text","Proofed Text"]
        merged.append(header)
    else:
        # Keep whatever header/existing data is there
        for row in existing_rows:
            tup = tuple(row)
            if tup not in seen:
                merged.append(row)
                seen.add(tup)

    # 3. Add each new entry only if it doesn’t already exist
    for e in log_entries:
        row = [
            e.get("recordId",""),
            e.get("header",""),
            e.get("original",""),
            e.get("proofed","")
        ]
        tup = tuple(row)
        if tup not in seen:
            merged.append(row)
            seen.add(tup)

    # 4. Write the merged list back out, overwriting the S3 object
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerows(merged)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=out.getvalue()
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
    """Main processing function"""
    try:
        workorder_id, content_type, proofing_requests, table_data = load_payload(event)
        if not workorder_id:
            raise ValueError("Missing workOrderId in payload.")
    except Exception as e:
        logger.error("Error parsing request body: " + str(e))
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid JSON format"})}

    if not proofing_requests:
        logger.error("No proofing items extracted from payload.")
        return {"statusCode": 400, "body": json.dumps({"error": "No proofing items found"})}

    csv_log_entries = []
    overall_proofed_flag = False
    proofed_entries = []

    # Process each record. Use different logic based on contentType.
    for record_id, content in proofing_requests.items():
        if content_type == "FormQuestion":
            # Process HTML table content.
            updated_html, log_entries = proof_table_content(content, record_id)
            for entry in log_entries:
                # Add record ID to each entry for logging.
                entry["recordId"] = record_id
                csv_log_entries.append(entry)
                # Check if any changes were made.
                if entry["original"] != entry["proofed"]:
                    overall_proofed_flag = True
            rec_data = table_data.get(record_id)
            if rec_data:
                proofed_entries.append({"recordId": record_id, "content": updated_html})
            else:
                logger.warning(f"No table data found for record {record_id}")
        else:
            # For Action_Observation or Action_Required, treat content as plain text.
            corrected_text = proof_plain_text(content, record_id)
            orig_text = strip_html(content)
            corr_text = strip_html(corrected_text)
            if corr_text != orig_text:
                overall_proofed_flag = True
                csv_log_entries.append({
                    "recordId": record_id,
                    "header": "",
                    "original": orig_text,
                    "proofed": corr_text
                })
            else:
                csv_log_entries.append({
                    "recordId": record_id,
                    "header": "",
                    "original": "No changes needed: " + orig_text,
                    "proofed": "No changes made."
                })
            proofed_entries.append({"recordId": record_id, "content": corrected_text})

    status_flag = "Proofed" if overall_proofed_flag else "Original"
    logger.info(f"Work order flagged as: {status_flag}")
    logger.info("Updating logs CSV in S3...")

    # Use a single file for all logs for this work order
    csv_filename = f"{workorder_id}_logs"
    # Store/append the new entries to the CSV
    logger.info(f"[Action] about to write {len(csv_log_entries)} rows → {csv_log_entries}")
    csv_s3_key = update_logs_csv(csv_log_entries, csv_filename, "logs")
    logger.info(f"CSV logs stored in S3 at key: {csv_s3_key}")

    store_metadata(workorder_id, csv_s3_key, status_flag)

    # try:
    #     notify_run(workorder_id, status_flag)
    #     logger.info(f"Notification email sent for {workorder_id}")
    # except Exception as e:
    #     logger.error(f"Failed to send notification email: {e}")


    unique_proofed_entries = {entry["recordId"]: entry for entry in proofed_entries}
    final_response = {
        "workOrderId": workorder_id,
        "contentType": content_type,
        "sectionContents": list(unique_proofed_entries.values())
    }
    logger.info("Final response: " + json.dumps(final_response, indent=2))
    return {
        "statusCode": 200,
        "body": json.dumps(final_response)
    }
