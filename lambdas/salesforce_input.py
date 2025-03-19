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

# Allowed headers for proofing
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
    """Extracts relevant text from Salesforce HTML table"""
    try:
        logger.debug(f"Full event received: {json.dumps(event, indent=2)}")
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
                logger.warning(f"⚠️ Skipping entry with missing recordId or content: {entry}")
                continue

            soup = BeautifulSoup(content_html, "html.parser")
            rows = soup.find_all("tr")

            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    header_text = cells[0].get_text(strip=True)
                    content_text = cells[1].get_text(strip=True)

                    logger.debug(f"🔍 Extracted - Header: '{header_text}', Content: '{content_text}'")

                    # Ensure header is in ALLOWED_HEADERS
                    if any(header_text.lower() == h.lower() for h in ALLOWED_HEADERS):
                        if content_text:
                            proofing_requests[header_text] = content_text
                            table_data[header_text] = {"row": row, "record_id": record_id}
                        else:
                            logger.info(f"⚠️ Skipping '{header_text}' as it has no content.")

        logger.info(f"✅ Extracted {len(proofing_requests)} items for proofing.")
        return proofing_requests, table_data

    except Exception as e:
        logger.error(f"Unexpected error in load_html_data: {e}")
        return {}, {}

def proof_html_with_bedrock(header, content):
    """Sends content for proofing and retrieves corrected version"""
    try:
        logger.info(f"🔹 Original content before proofing (Header: {header}): {content}")

        text_content = BeautifulSoup(content, "html.parser").get_text().strip()

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
            [msg["text"] for msg in response_body.get("content", []) if msg.get("type") == "text"]
        ).strip()

        logger.info(f"✅ Proofed content (Header: {header}): {proofed_text}")

        # If proofed_text is empty, return original content
        return proofed_text if proofed_text else content

    except Exception as e:
        logger.error(f"❌ Bedrock API Error: {str(e)}")
        return content

def process(event, context):
    """Main processing function"""
    logger.info(f"🔹 Full Incoming Event: {json.dumps(event, indent=2)}")

    try:
        body = json.loads(event["body"])
    except (TypeError, KeyError, json.JSONDecodeError):
        logger.error("❌ Error parsing request body")
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid JSON format"})}

    workorder_id = body.get("workOrderId", str(uuid.uuid4()))
    html_entries = body.get("sectionContents", [])

    if not html_entries:
        logger.error("❌ No section contents received from Salesforce.")
        return {"statusCode": 400, "body": json.dumps({"error": "No section contents found"})}

    proofing_requests, table_data = load_html_data(event)
    proofed_entries = []
    original_text = "=== ORIGINAL TEXT ===\n"
    proofed_text = "=== PROOFED TEXT ===\n"

    # Flag to check if any content was actually corrected
    proofed_flag = False

    for header, content in proofing_requests.items():
        corrected_content = proof_html_with_bedrock(header, content)

        # Compare original vs. corrected (ignoring extra spaces)
        if corrected_content.strip() != content.strip():
            proofed_flag = True

        if header in table_data:
            row_info = table_data[header]
            row = row_info["row"]
            record_id = row_info["record_id"]

            content_cell = row.find_all("td")[1]
            content_cell.clear()
            content_cell.append(corrected_content)

            updated_html = str(row)

            proofed_entries.append({"recordId": record_id, "content": updated_html})
            original_text += f"\n\n### {header} ###\n{content}\n"
            proofed_text += f"\n\n### {header} ###\n{corrected_content}\n"
        else:
            logger.warning(f"⚠️ No table data found for header: {header}")

    # Log which status will be flagged
    status_flag = "Proofed" if proofed_flag else "Original"
    logger.info(f"Work order flagged as: {status_flag}")

    logger.info("🔹 Storing proofed files in S3...")

    original_s3_key = store_in_s3(original_text, f"{workorder_id}_original", "original")
    proofed_s3_key = store_in_s3(proofed_text, f"{workorder_id}_proofed", "proofed")

    # Store metadata including the flag status
    store_metadata(workorder_id, original_s3_key, proofed_s3_key, status_flag)

    # Remove duplicate recordIds (keep the latest version)
    unique_proofed_entries = {}
    for entry in proofed_entries:
        unique_proofed_entries[entry["recordId"]] = entry

    proofed_entries = list(unique_proofed_entries.values())

    return {
        "statusCode": 200,
        "body": json.dumps({"workOrderId": workorder_id, "sectionContents": proofed_entries})
    }
