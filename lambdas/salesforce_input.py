import json
import boto3
import logging
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
    Extracts the payload from the incoming event.
    Assumes a JSON structure with:
      - workOrderId
      - contentType: "FormQuestion" or "Action"
      - sectionContents: a list of objects {recordId, content}
    """
    logger.info("=== load_payload - FULL EVENT ===")
    logger.info(json.dumps(event, indent=2))
    
    try:
        raw_body = event.get("body", "")
        logger.info("=== load_payload - RAW BODY STRING ===")
        logger.info(raw_body)
        body = json.loads(raw_body)
        logger.info("=== load_payload - PARSED BODY ===")
        logger.info(json.dumps(body, indent=2))
        
        # Extract contentType and sectionContents directly
        content_type = body.get("contentType", "Unknown")
        items = body.get("sectionContents", [])
        
        logger.info(f"Payload contentType: {content_type}")
        logger.info(f"Number of sectionContents items: {len(items)}")
        
        # Build dictionaries for processing (mapping recordId to content)
        proofing_requests = {}
        table_data = {}
        for entry in items:
            record_id = entry.get("recordId")
            content = entry.get("content")
            if not record_id or not content:
                logger.warning(f"⚠️ Skipping entry with missing recordId or content: {entry}")
                continue
            proofing_requests[record_id] = content.strip()
            table_data[record_id] = {"content": content.strip(), "record_id": record_id}
        
        logger.info(f"✅ Extracted {len(proofing_requests)} items for proofing.")
        return body.get("workOrderId"), content_type, proofing_requests, table_data

    except Exception as e:
        logger.error(f"Unexpected error in load_payload: {e}")
        return None, None, {}, {}

def proof_html_with_bedrock(record_id, content):
    """Sends content for proofing and retrieves corrected version"""
    try:
        logger.info(f"🔹 Original content before proofing (recordId: {record_id}): {content}")

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
                    "- IF text seperated by ||, proof content, but leave the || uncahnged. \n"
                    "- Ensure only to proofread once, NEVER repeat the same text twice in the output.\n\n"
                    "Correct this text: " + content
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

        logger.info(f"✅ Proofed content (recordId: {record_id}): {proofed_text}")
        return proofed_text if proofed_text else content

    except Exception as e:
        logger.error(f"❌ Bedrock API Error for recordId {record_id}: {str(e)}")
        return content

def process(event, context):
    """Main processing function"""
    logger.info("=== process - FULL EVENT AS RECEIVED ===")
    logger.info(json.dumps(event, indent=2))

    try:
        # Extract workOrderId, contentType, and payload dictionaries
        workorder_id, content_type, proofing_requests, table_data = load_payload(event)
        if not workorder_id:
            raise ValueError("Missing workOrderId in payload.")
    except Exception as e:
        logger.error("❌ Error parsing request body: " + str(e))
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid JSON format"})}

    if not proofing_requests:
        logger.error("❌ No proofing items extracted from the payload.")
        return {"statusCode": 400, "body": json.dumps({"error": "No proofing items found"})}

    proofed_entries = []
    original_text = "=== ORIGINAL TEXT ===\n"
    proofed_text = "=== PROOFED TEXT ===\n"
    proofed_flag = False

    # Process each record in the payload's sectionContents
    for record_id, content in proofing_requests.items():
        corrected_content = proof_html_with_bedrock(record_id, content)
        
        if corrected_content.strip() != content.strip():
            proofed_flag = True
            logger.info(f"Record {record_id} was proofed:\nOriginal: {content}\nCorrected: {corrected_content}")
        else:
            logger.info(f"Record {record_id} did not require changes: {content}")

        rec_data = table_data.get(record_id)
        if rec_data:
            proofed_entries.append({"recordId": record_id, "content": corrected_content})
            original_text += f"\n\n### {record_id} ###\n{content}\n"
            proofed_text += f"\n\n### {record_id} ###\n{corrected_content}\n"
        else:
            logger.warning(f"⚠️ No table data found for recordId: {record_id}")

    status_flag = "Proofed" if proofed_flag else "Original"
    logger.info(f"Work order flagged as: {status_flag}")

    logger.info("🔹 Storing proofed files in S3...")
    original_s3_key = store_in_s3(original_text, f"{workorder_id}_original", "original")
    proofed_s3_key = store_in_s3(proofed_text, f"{workorder_id}_proofed", "proofed")

    store_metadata(workorder_id, original_s3_key, proofed_s3_key, status_flag)

    # Remove duplicate recordIds (keeping the latest version)
    unique_proofed_entries = {}
    for entry in proofed_entries:
        unique_proofed_entries[entry["recordId"]] = entry
    proofed_entries = list(unique_proofed_entries.values())

    # Optionally, you can include the contentType in your response if needed
    response_body = {
        "workOrderId": workorder_id,
        "contentType": content_type,
        "sectionContents": proofed_entries
    }
    logger.info("=== process - FINAL PROOFED ENTRIES ===")
    logger.info(json.dumps(response_body, indent=2))
    return {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }
