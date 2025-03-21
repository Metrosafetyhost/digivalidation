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
@tracer.capture_method
def load_html_data(event_body: dict):
    """
    Parses Salesforce input data.
    
    - If the 'content' contains table markup, it is assumed to be a Report Section record.
      In that case, the function iterates over each row and extracts only those rows
      whose header (the first cell) matches one of the allowed headers (case-insensitive).
    - Otherwise, it assumes the record is an Action record and simply stores the plain text,
      using the recordId as the key.
    
    Returns:
        proofing_requests: Dict mapping keys (allowed header for form questions, or recordId for actions)
                           to the content that should be proofed.
        table_data: Additional metadata keyed in the same way.
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
            logger.warning(f"Skipping entry with missing recordId or content: {entry}")
            continue

        # Check if the content has table markup
        if "<table" in content_html.lower():
            soup = BeautifulSoup(content_html, "html.parser")
            rows = soup.find_all("tr")
            allowed_found = False

            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    header_text = cells[0].get_text(strip=True)
                    content_text = cells[1].get_text(strip=True)
                    # Only include rows where the header matches one of our allowed headers
                    if header_text.lower() in (h.lower() for h in ALLOWED_HEADERS):
                        proofing_requests[header_text] = content_text
                        table_data[header_text] = {"row": row, "record_id": record_id}
                        allowed_found = True

            if not allowed_found:
                logger.info(f"Table entry for record {record_id} did not contain any allowed header; skipping.")
        else:
            # For plain text (action records), simply use the recordId as the key.
            proofing_requests[record_id] = content_html.strip()
            table_data[record_id] = {"raw_content": content_html.strip(), "record_id": record_id}

    logger.info({"proofed_requests": len(proofing_requests), "keys": list(proofing_requests.keys())})
    return proofing_requests, table_data




@tracer.capture_method
def proof_html_with_bedrock(header: str, content: str) -> str:
    """Sends content to AWS Bedrock for proofing and returns corrected text."""
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
        logger.exception("❌ Bedrock API Error")
        return content

@logger.inject_lambda_context(log_event=True)
@event_source(data_class=APIGatewayProxyEvent)
@tracer.capture_lambda_handler
def process(event: APIGatewayProxyEvent, context: LambdaContext):
    """
    AWS Lambda entrypoint. Extracts event data, processes text, stores proofed content in S3, and logs metadata.
    """

    try:
        body = event.json_body
        # body = json.loads(event["body"])
        workorder_id = body.get("workOrderId", str(uuid.uuid4()))
        proofing_requests, table_data = load_html_data(body)

        if not proofing_requests:
            return {"statusCode": 400, "body": json.dumps({"error": "No section contents found"})}

        proofed_entries = []
        original_text, proofed_text = "=== ORIGINAL TEXT ===\n", "=== PROOFED TEXT ===\n"
        proofed_flag = False

        for key, content in proofing_requests.items():
                corrected_content = proof_html_with_bedrock(key, content)
                proofed_flag = proofed_flag or (corrected_content.strip() != content.strip())

                row_info = table_data.get(key)
                record_id = row_info["record_id"] if row_info else key
                proofed_entries.append({"recordId": record_id, "content": corrected_content})
                original_text += f"\n\n### {key} ###\n{content}\n"
                proofed_text += f"\n\n### {key} ###\n{corrected_content}\n"

        status_flag = "Proofed" if proofed_flag else "Original"
        original_s3_key = store_in_s3(original_text, f"{workorder_id}_original", "original")
        proofed_s3_key = store_in_s3(proofed_text, f"{workorder_id}_proofed", "proofed")
        store_metadata(workorder_id, original_s3_key, proofed_s3_key, status_flag)

        return {"statusCode": 200, "body": json.dumps({"workOrderId": workorder_id, "sectionContents": proofed_entries})}

    except Exception as e:
        logger.exception("❌ Error processing request")
        return {"statusCode": 500, "body": json.dumps({"error": "Internal Server Error"})}
