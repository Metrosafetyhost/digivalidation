import json
import boto3
import logging
from bs4 import BeautifulSoup
import uuid
import time

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

# Allowed headers for form questions (the building description tables)
ALLOWED_HEADERS = [
    "Building Fire strategy",
    "Fire Service and Evacuation Lifts",
    "Mains Electrical incomers and electrical distribution boards (EDBs)",
    "Natural Gas Supplies",
    "Disabled escape arrangements",
]

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

def load_html_data(event):
    """
    Extracts relevant text from Salesforce input (for form questions).
    
    For each record in "sectionContents":
      - If the content contains a <table> tag, we parse every table row.
          • For each row, if the first cell’s text (after trimming) exactly
            matches one of the ALLOWED_HEADERS (ignoring case), we store that row.
      - Otherwise, we assume the content is plain text.
          • We split by newline and take the first nonempty line as the header.
          • If that header exactly matches one of ALLOWED_HEADERS (ignoring case),
            we use it as the key and the rest of the text as the section content.
          • If not, we skip the record.
    
    Returns:
      proofing_requests: dict mapping allowed header text to the content (to be proofed).
      table_data: dict with additional metadata (including record_id).
    """
    try:
        body = json.loads(event["body"])
        html_data = body.get("sectionContents", [])
        if not html_data:
            logger.warning("No HTML data found in event.")
            return {}, {}

        proofing_requests = {}
        table_data = {}

        for entry in html_data:
            record_id = entry.get("recordId")
            content_html = entry.get("content")
            if not record_id or not content_html:
                logger.warning(f"Skipping entry with missing recordId or content: {entry}")
                continue

            # Case 1: Content includes a table.
            if "<table" in content_html.lower():
                soup = BeautifulSoup(content_html, "html.parser")
                rows = soup.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        header_text = cells[0].get_text(strip=True)
                        content_text = cells[1].get_text(strip=True)
                        logger.debug(f"Extracted from table – Header: '{header_text}', Content: '{content_text}'")
                        # Only include if header exactly matches one of the allowed headers.
                        if any(header_text.lower() == allowed.lower() for allowed in ALLOWED_HEADERS):
                            proofing_requests[header_text] = content_text
                            table_data[header_text] = {"row": row, "record_id": record_id}
                        else:
                            logger.info(f"Skipping table row header not allowed: '{header_text}'")
            else:
                # Case 2: Plain text content. Assume the first nonempty line is the header.
                lines = [line.strip() for line in content_html.splitlines() if line.strip()]
                if lines:
                    potential_header = lines[0]
                    # Check if potential_header is one of the allowed headers (exact match ignoring case)
                    if any(potential_header.lower() == allowed.lower() for allowed in ALLOWED_HEADERS):
                        content_text = "\n".join(lines[1:]).strip()
                        proofing_requests[potential_header] = content_text
                        table_data[potential_header] = {"raw_content": content_html.strip(), "record_id": record_id}
                        logger.debug(f"Extracted from plain text – Header: '{potential_header}', Content: '{content_text}'")
                    else:
                        logger.info(f"Skipping plain text record {record_id} because first line '{potential_header}' is not allowed.")
                else:
                    logger.info(f"Skipping plain text record {record_id} because it is empty.")
        logger.info(f"✅ Extracted {len(proofing_requests)} items for proofing. Keys: {list(proofing_requests.keys())}")
        return proofing_requests, table_data
    except Exception as e:
        logger.error(f"Unexpected error in load_html_data: {e}")
        return {}, {}


def proof_html_with_bedrock(header, content):
    """Sends content for proofing and retrieves corrected version"""
    try:
        logger.info(f"🔹 Original content before proofing (Header/Key: {header}): {content}")
        # For proofing, we always send the plain text version.
        text_content = content  # For HTML, we already extracted the text from the cell

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
                    "Correct this text: " + text_content
                )
            }],
            "max_tokens": 512,
            "temperature": 0.3
        }

        response = bedrock_client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload)
        )

        response_body = json.loads(response["body"].read().decode("utf-8"))
        proofed_text = " ".join(
            [msg["text"] for msg in response_body.get("content", []) if msg.get("type") == "text"]).strip()

        logger.info(f"✅ Proofed content (Header/Key: {header}): {proofed_text}")

        # If proofed_text is empty, return original content
        return proofed_text if proofed_text else content

    except Exception as e:
        logger.error(f"❌ Bedrock API Error: {str(e)}")
        return content

def process(event, context):
    logger.info(f"🔹 Full Incoming Event: {json.dumps(event, indent=2)}")

    try:
        body = json.loads(event["body"])
    except (TypeError, KeyError, json.JSONDecodeError):
        logger.error("❌ Error parsing request body")
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid JSON format"})}

    workorder_id = body.get("workOrderId", str(uuid.uuid4()))
    proofing_requests, table_data = load_html_data(event)

    if not proofing_requests:
        logger.warning("No rows found to process.")
        return {"statusCode": 200, "body": json.dumps({
            "workOrderId": workorder_id,
            "sectionContents": []  # return empty
        })}

    proofed_entries = []
    original_text = "=== ORIGINAL TEXT ===\n"
    proofed_text = "=== PROOFED TEXT ===\n"
    proofed_flag = False

    # Go through each row we extracted
    for header_text, content in proofing_requests.items():
        row_info = table_data[header_text]
        record_id = row_info["record_id"]
        is_allowed = row_info["is_allowed"]

        if is_allowed:
            # Only proof if it's in ALLOWED_HEADERS
            corrected_content = proof_html_with_bedrock(header_text, content)
            if corrected_content.strip() != content.strip():
                proofed_flag = True

            # Update the <td> in the row
            row = row_info["row"]
            content_cell = row.find_all("td")[1]
            content_cell.clear()
            content_cell.append(corrected_content)
            updated_html = str(row)

            # We'll store the row as the final "content"
            proofed_entries.append({
                "recordId": record_id,
                "content": updated_html
            })

            original_text += f"\n\n### {header_text} ###\n{content}\n"
            proofed_text += f"\n\n### {header_text} ###\n{corrected_content}\n"
        else:
            # Not in ALLOWED_HEADERS, skip proofing but still return the row unchanged
            row = row_info["row"]
            updated_html = str(row)  # original row HTML
            proofed_entries.append({
                "recordId": record_id,
                "content": updated_html
            })
            original_text += f"\n\n### {header_text} (SKIPPED) ###\n{content}\n"
            proofed_text += f"\n\n### {header_text} (SKIPPED) ###\n{content}\n"

    status_flag = "Proofed" if proofed_flag else "Original"
    logger.info(f"Work order flagged as: {status_flag}")

    # Store in S3
    original_s3_key = store_in_s3(original_text, f"{workorder_id}_original", "original")
    proofed_s3_key = store_in_s3(proofed_text, f"{workorder_id}_proofed", "proofed")
    store_metadata(workorder_id, original_s3_key, proofed_s3_key, status_flag)

    # If you want to remove duplicates by recordId, do so here
    unique_map = {}
    for entry in proofed_entries:
        unique_map[entry["recordId"]] = entry
    final_entries = list(unique_map.values())

    return {
        "statusCode": 200,
        "body": json.dumps({"workOrderId": workorder_id, "sectionContents": final_entries})
    }