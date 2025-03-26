import json
import boto3
import logging
import uuid
import time
from bs4 import BeautifulSoup

# Initialise logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
bedrock_client = boto3.client("bedrock-runtime", region_name="eu-west-2")
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

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
    """
    Processes an entire HTML table.
    
    1. Parses the table and iterates over all rows.
    2. Extracts the content from each row's second cell.
    3. Joins these texts with a unique delimiter and sends the full block for proofing.
    4. Splits the returned corrected text by the delimiter.
    5. Replaces each row's second cell with the corresponding corrected text.
    
    Returns:
        - The updated HTML (with corrected content)
        - A list of log entries containing header, original text and proofed text.
    """
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
                original_texts.append(tds[1].get_text(separator=" ", strip=True))
            else:
                original_texts.append("")
        
        # Use a unique delimiter to join the texts from all rows.
        delimiter = "|||ROW_DELIM|||"
        joined_content = delimiter.join(original_texts)
        plain_text = joined_content  # Already plain text
        
        logger.info(f"Proofing record {record_id}. Joined content: {plain_text}")
        
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
                    "- If the text contains the delimiter `||`, do NOT remove, alter, or add spaces around it.\n"
                    "- Ensure only to proofread once, NEVER repeat the same text twice in the output.\n\n"
                    "Correct this text: " + plain_text
                )
            }],
            "max_tokens": 512,
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
                tds[1].append(corrected)
                header = tds[0].get_text(separator=" ", strip=True)
                logger.info(f"Record {record_id} row {idx+1} header: {header}. Original: {original_texts[idx]}. Proofed: {corrected}")
                log_entries.append({"header": header, "original": original_texts[idx], "proofed": corrected})
        return str(soup), log_entries
    except Exception as e:
        logger.error(f"Error proofing table content for record {record_id}: {str(e)}")
        return html, []

def proof_plain_text(text, record_id):
    """
    Proofreads plain text content.
    This function is used when the incoming content is not an HTML table (e.g. for actions).
    """
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
                    "- If the text contains the delimiter `||`, do NOT remove, alter, or add spaces around it.\n"
                    "- Ensure only to proofread once, NEVER repeat the same text twice in the output.\n\n"
                    "Correct this text: " + plain_text
                )
            }],
            "max_tokens": 512,
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

def store_in_s3(text, filename, folder):
    s3_key = f"{folder}/{filename}.txt"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=text)
    return s3_key

def store_metadata(workorder_id, original_s3_key, proofed_s3_key, status):
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(
        Item={
            "workorder_id": workorder_id,
            "original_s3_key": original_s3_key,
            "proofed_s3_key": proofed_s3_key,
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

    proofed_entries = []
    original_text_log = "=== ORIGINAL TEXT ===\n"
    proofed_text_log = "=== PROOFED TEXT ===\n"
    overall_proofed_flag = False

    # Process each record. Use different logic based on contentType.
    for record_id, content in proofing_requests.items():
        if content_type == "FormQuestion":
            # Process HTML table content.
            updated_html, log_entries = proof_table_content(content, record_id)
            for entry in log_entries:
                if entry["original"] != entry["proofed"]:
                    overall_proofed_flag = True
                    original_text_log += f"\n\n### {record_id} - {entry['header']} ###\n{entry['original']}\n"
                    proofed_text_log += f"\n\n### {record_id} - {entry['header']} ###\n{entry['proofed']}\n"
                else:
                    original_text_log += f"\n\n### {record_id} - {entry['header']} ###\nNo changes needed: {entry['original']}\n"
                    proofed_text_log += f"\n\n### {record_id} - {entry['header']} ###\nNo changes made.\n"
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
                original_text_log += f"\n\n### {record_id} ###\n{orig_text}\n"
                proofed_text_log += f"\n\n### {record_id} ###\n{corr_text}\n"
            else:
                original_text_log += f"\n\n### {record_id} ###\nNo changes needed: {orig_text}\n"
                proofed_text_log += f"\n\n### {record_id} ###\nNo changes made.\n"
            proofed_entries.append({"recordId": record_id, "content": corrected_text})

    status_flag = "Proofed" if overall_proofed_flag else "Original"
    logger.info(f"Work order flagged as: {status_flag}")
    logger.info("Storing proofed files in S3...")

    original_s3_key = store_in_s3(original_text_log, f"{workorder_id}_original", "original")
    proofed_s3_key = store_in_s3(proofed_text_log, f"{workorder_id}_proofed", "proofed")
    store_metadata(workorder_id, original_s3_key, proofed_s3_key, status_flag)

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
