import json
import boto3
import logging
import uuid
import time
import io
import csv
from bs4 import BeautifulSoup
from botocore.exceptions import ClientError

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

def proof_table_content(html, record_id):
    try:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            logger.warning("No table found in HTML. Skipping proofing for record " + record_id)
            return html, []
        
        rows = table.find_all("tr")
        if not rows:
            logger.warning("No rows found in table for record " + record_id)
            return html, []
        
        original_texts = []
        for row in rows:
            tds = row.find_all("td")
            if len(tds) >= 2:
                # Get the inner HTML as-is; leave any <p> tags intact.
                cell_html = tds[1].decode_contents()
                original_texts.append(cell_html)
            else:
                original_texts.append("")
        
        # Use a unique delimiter to join the texts from all rows.
        delimiter = "|||ROW_DELIM|||"
        joined_content = delimiter.join(original_texts)
        
        logger.info(f"Proofing record {record_id}. Joined content: {joined_content}")
        
        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{
                "role": "user",
                "content": (
                    "Proofread the following text according to these strict guidelines:\n\n"
                    "- Do NOT add any new introductory text or explanatory sentences before or after the original content.\n"
                    "- Spelling and grammar are corrected in British English, and spacing is corrected.\n"
                    "- Headings, section titles, and structure remain unchanged.\n"
                    "- Do NOT remove any words or phrases from the original content.\n"
                    "- Do NOT split, merge, or add any new sentences or content.\n"
                    "- Ensure that lists, bullet points, and standalone words remain intact.\n"
                    "- Proofread the text while preserving the exact sequence ‘|||ROW_DELIM|||’ as a marker. Additionally, if a list is detected (i.e. multiple standalone words), insert a newline between them only after the marker.\n"
                    "- Do NOT remove or alter any HTML formatting tags (such as <p>, <ul>, <li>, and <u>)."
                    "- Ensure only to proofread once, NEVER repeat the same text twice in the output.\n\n"
                    "Text to proofread: " + joined_content
                )
            }],
            "max_tokens": 2000,
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
        
        # Split the proofed result back into segments.
        corrected_contents = proofed_text.split(delimiter)
        if len(corrected_contents) != len(original_texts):
            logger.warning(f"Expected {len(original_texts)} proofed segments, got {len(corrected_contents)} for record {record_id}")
        
        log_entries = []
        # Replace each row's second cell with the corresponding corrected text.
        for idx, row in enumerate(rows):
            tds = row.find_all("td")
            if len(tds) >= 2:
                corrected = corrected_contents[idx] if idx < len(corrected_contents) else tds[1].get_text()
                tds[1].clear()
                # Append as HTML so that <p> tags are preserved for rich text rendering.
                tds[1].append(BeautifulSoup(corrected, "html.parser"))
                header = tds[0].get_text(separator=" ", strip=True)
                logger.info(f"Record {record_id} row {idx+1} header: {header}. Original: {original_texts[idx]}. Proofed: {corrected}")
                log_entries.append({"header": header, "original": original_texts[idx], "proofed": corrected})
        return str(soup), log_entries
    except Exception as e:
        logger.error(f"Error proofing table content for record {record_id}: {str(e)}")
        return html, []

def proof_plain_text(text, record_id):
    if any(tag in text.lower() for tag in ['<p>', '<ul>', '<li>', '<u>']):
        plain_text = text
    else:
        plain_text = strip_html(text)

    try:
        logger.info(f"Proofing record {record_id}. Plain text: {plain_text}")
        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{
                "role": "user",
                "content": (
                    "Proofread and correct the following text while ensuring:\n"
                    "- Spelling and grammar are corrected in British English, and spacing is corrected.\n"
                    "- Headings, section titles, and structure remain unchanged.\n"
                    "- Do NOT remove any words or phrases from the original content.\n"
                    "- Do NOT split, merge, or add any new sentences or content.\n"
                    "- Ensure NOT to add any introductory text or explanations ANYWHERE.\n"
                    "- Ensure that lists, bullet points, and standalone words remain intact.\n"
                    "- Ensure only to proofread once, NEVER repeat the same text twice in the output.\n\n"
                    "Correct this text: " + plain_text
                )
            }],
            "max_tokens": 1000,
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
    Appends new log entries to a single CSV file in S3 (creating it if needed).
    Each entry is a dict with keys: recordId, header, original, proofed.
    """
    s3_key = f"{folder}/{filename}.csv"

    existing_rows = []
    try:
        # Attempt to read the existing CSV file from S3
        existing_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        existing_csv = existing_obj["Body"].read().decode("utf-8")
        reader = csv.reader(io.StringIO(existing_csv))
        existing_rows = list(reader)
    except s3_client.exceptions.NoSuchKey:
        # CSV file doesn't exist yet, so we'll create it fresh
        pass

    # Prepare a new CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)

    if not existing_rows:
        # Write header row if file didn't exist or was empty
        writer.writerow(["Record ID", "Header", "Original Text", "Proofed Text"])
    else:
        # Rewrite existing rows so we keep all previous data
        for row in existing_rows:
            writer.writerow(row)

    # Now write the new log entries
    for entry in log_entries:
        writer.writerow([
            entry.get("recordId", ""),
            entry.get("header", ""),
            entry.get("original", ""),
            entry.get("proofed", "")
        ])

    # Upload the updated CSV to S3
    new_csv_data = output.getvalue()
    output.close()
    s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=new_csv_data)

    return s3_key

def store_metadata(workorder_id, logs_s3_key, status):
    table = dynamodb.Table(TABLE_NAME)
    # Update logs/status/timestamp but leave 'notified' alone
    table.update_item(
        Key={"workorder_id": workorder_id},
        UpdateExpression="SET logs_s3_key = :logs, #st = :status, timestamp = :ts",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":logs": logs_s3_key,
            ":status": status,
            ":ts": int(time.time())
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
    
def mark_notified_if_needed(workorder_id):
    table = dynamodb.Table(TABLE_NAME)
    try:
        table.update_item(
            Key={"workorder_id": workorder_id},
            UpdateExpression="SET notified = :true",
            ConditionExpression="attribute_not_exists(notified)",
            ExpressionAttributeValues={":true": True}
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def notify_run(workorder_id, status, summary_lines):
    subject = f"Work Order {workorder_id} Processed: {status}"
    body = (
        f"The Work Order {workorder_id} has been proofed with the following results:\n\n"
        + "\n".join(summary_lines)
        + "\n\nCheers,\nYour AWS Lambda"
    )
    ses_client.send_email(
        Source=SENDER,
        Destination={"ToAddresses": [RECIPIENT]},
        Message={
            "Subject": {"Data": subject},
            "Body":    {"Text": {"Data": body}}
        }
    )

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
    proofed_entries   = []
    summary_lines     = []
    overall_proofed   = False

    # Process each record
    for record_id, content in proofing_requests.items():
        if content_type == "FormQuestion":
            updated_html, log_entries = proof_table_content(content, record_id)
            # determine if any changes
            changed = any(e["original"] != e["proofed"] for e in log_entries)
            summary_lines.append(f"Form Question {record_id} {'Proofed' if changed else 'No changes'}")
            for entry in log_entries:
                entry["recordId"] = record_id
                csv_log_entries.append(entry)
                if entry["original"] != entry["proofed"]:
                    overall_proofed = True
            if record_id in table_data:
                proofed_entries.append({"recordId": record_id, "content": updated_html})
            else:
                logger.warning(f"No table data found for record {record_id}")
        else:
            corrected_text = proof_plain_text(content, record_id)
            orig_text  = strip_html(content)
            corr_text  = strip_html(corrected_text)
            changed    = corr_text != orig_text
            # choose label based on type
            if content_type == "Action_Observation":
                label = f"Case {record_id} Observation"
            else:
                label = f"Case {record_id} Action"
            summary_lines.append(f"{label} {'Proofed' if changed else 'No changes'}")
            if changed:
                overall_proofed = True
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

    status_flag = "Proofed" if overall_proofed else "Original"
    logger.info(f"Work order flagged as: {status_flag}")

    # write logs
    csv_filename = f"{workorder_id}_logs"
    csv_s3_key   = update_logs_csv(csv_log_entries, csv_filename, "logs")
    logger.info(f"CSV logs stored in S3 at key: {csv_s3_key}")

    store_metadata(workorder_id, csv_s3_key, status_flag)

    # Notify once per work order
    if mark_notified_if_needed(workorder_id):
        try:
            notify_run(workorder_id, status_flag, summary_lines)
            logger.info(f"Notification email sent for {workorder_id}")
        except Exception as e:
            logger.error(f"Failed to send notification email: {e}")
    else:
        logger.info(f"Skipping notification: already sent for {workorder_id}")

    unique_proofed = {e['recordId']: e for e in proofed_entries}
    final_response = {
        "workOrderId": workorder_id,
        "contentType": content_type,
        "sectionContents": list(unique_proofed.values())
    }
    logger.info("Final response: " + json.dumps(final_response, indent=2))
    return {
        "statusCode": 200,
        "body": json.dumps(final_response)
    }