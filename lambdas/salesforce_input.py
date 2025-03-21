import json
import boto3
import os
# import logging

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import event_source, APIGatewayProxyEvent
from bs4 import BeautifulSoup
import uuid
import time

# Initialise logger
logger = Logger()
tracer = Tracer()

# AWS Environment Variables
AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")
BUCKET_NAME = os.getenv("BUCKET_NAME", "metrosafety-bedrock-output-data-dev-bedrock-lambda")
TABLE_NAME = os.getenv("TABLE_NAME", "ProofingMetadata")
BEDROCK_MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"

# AWS clients
bedrock_client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)


# Allowed headers for form questions (the building description tables)
ALLOWED_HEADERS = [
    "Building Fire strategy",
    "Fire Service and Evacuation Lifts",
    "Mains Electrical incomers and electrical distribution boards (EDBs)",
    "Natural Gas Supplies",
    "Disabled escape arrangements",
]

@tracer.capture_method
def store_in_s3(text: str, filename: str, folder: str) -> str:
    """Stores text in an S3 bucket and returns the S3 key."""
    s3_key = f"{folder}/{filename}.txt"
    try:
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=text)
        logger.info(f"📂 Stored file in S3: s3://{BUCKET_NAME}/{s3_key}")
        return s3_key
    except Exception as e:
        logger.error(f"❌ Failed to store file in S3: {e}")
        raise

@tracer.capture_method
def store_metadata(workorder_id: str, original_s3_key: str, proofed_s3_key: str, status: str) -> None:
    """Stores proofing metadata in DynamoDB."""
    table = dynamodb.Table(TABLE_NAME)
    try:
        table.put_item(
            Item={
                "workorder_id": workorder_id,
                "original_s3_key": original_s3_key,
                "proofed_s3_key": proofed_s3_key,
                "status": status,  # "Proofed" or "Original"
                "timestamp": int(time.time())
            }
        )
        logger.info(f"✅ Metadata stored in DynamoDB for Work Order: {workorder_id}")
    except Exception as e:
        logger.exception("❌ Failed to store metadata in DynamoDB")
        raise

@tracer.capture_method
def load_html_data(event_body: dict):
    """
    Parses and extracts Salesforce input data.

    - Extracts table data for form questions (headers as keys)
    - Uses `recordId` as the key for non-table data (actions)

    Returns:
        - proofing_requests: Dict of extracted content (keyed by headers or record IDs)
        - table_data: Dict containing extra info needed later
    """
    proofing_requests = {}
    table_data = {}

    html_data = event_body.get("sectionContents", [])
    if not html_data:
        logger.warning("No HTML data found in event.")
        return {}, {}

    for entry in html_data:
        record_id = entry.get("recordId")
        content_html = entry.get("content")

        if not record_id or not content_html:
            logger.warning(f"⚠️ Skipping entry with missing recordId or content: {entry}")
            continue

        if "<table" in content_html.lower():
            soup = BeautifulSoup(content_html, "html.parser")
            rows = soup.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    header_text = cells[0].get_text(strip=True)
                    content_text = cells[1].get_text(strip=True)

                    if header_text.lower() in map(str.lower, ALLOWED_HEADERS):
                        proofing_requests[header_text] = content_text
                        table_data[header_text] = {"row": row, "record_id": record_id}
        else:
            proofing_requests[record_id] = content_html.strip()
            table_data[record_id] = {"content": content_html.strip(), "record_id": record_id}

    logger.info({"proofed_requests": len(proofing_requests), "keys": list(proofing_requests.keys())})
    return proofing_requests, table_data



def proof_html_with_bedrock(header, content):
    try:
        logger.info(f"🔹 Original content before proofing (Header/Key: {header}): {content}")
        # for p[roofing, send plain text version.
        text_content = content 

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

        logger.info(f"✅ Proofed content (Header/Key: {header}): {proofed_text}")

        # if proofed_text is empty, return original content
        return proofed_text if proofed_text else content

    except Exception as e:
        logger.error(f"❌ Bedrock API Error: {str(e)}")
        return content

@logger.inject_lambda_context(log_event=True)
def process(event: dict, context: LambdaContext) -> str:

    #logger.info(f"🔹 Full Incoming Event: {json.dumps(event, indent=2)}")

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
    proofed_flag = False

    for key, content in proofing_requests.items():
        corrected_content = proof_html_with_bedrock(key, content)
        if corrected_content.strip() != content.strip():
            proofed_flag = True

        # check if key corresponds to a form question (HTML) or an action (plain text)
        if key.lower() in [h.lower() for h in ALLOWED_HEADERS]:
            # for form questions, update the HTML table row.
            row_info = table_data.get(key)
            if row_info:
                row = row_info["row"]
                record_id = row_info["record_id"]

                content_cell = row.find_all("td")[1]
                content_cell.clear()
                content_cell.append(corrected_content)
                updated_html = str(row)

                proofed_entries.append({"recordId": record_id, "content": updated_html})
                original_text += f"\n\n### {key} ###\n{content}\n"
                proofed_text += f"\n\n### {key} ###\n{corrected_content}\n"
            else:
                logger.warning(f"⚠️ No table data found for header: {key}")
        else:
            # fdor actions, key is the recordId and  content is plain text.
            rec_data = table_data.get(key)
            if rec_data:
                record_id = rec_data["record_id"]

                proofed_entries.append({"recordId": record_id, "content": corrected_content})
                original_text += f"\n\n### {record_id} ###\n{content}\n"
                proofed_text += f"\n\n### {record_id} ###\n{corrected_content}\n"
            else:
                logger.warning(f"⚠️ No table data found for action record: {key}")

    status_flag = "Proofed" if proofed_flag else "Original"
    logger.info(f"Work order flagged as: {status_flag}")

    logger.info("🔹 Storing proofed files in S3...")
    original_s3_key = store_in_s3(original_text, f"{workorder_id}_original", "original")
    proofed_s3_key = store_in_s3(proofed_text, f"{workorder_id}_proofed", "proofed")

    store_metadata(workorder_id, original_s3_key, proofed_s3_key, status_flag)

    # remove duplicate recordIds
    unique_proofed_entries = {}
    for entry in proofed_entries:
        unique_proofed_entries[entry["recordId"]] = entry
    proofed_entries = list(unique_proofed_entries.values())

    return {
        "statusCode": 200,
        "body": json.dumps({"workOrderId": workorder_id, "sectionContents": proofed_entries})
    }
