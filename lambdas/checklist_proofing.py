import json
import boto3
import logging

# ——— Initialise logging ———
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ——— AWS clients ———
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3       = boto3.client('s3')

def extract_json_data(json_content):
    payload = json.loads(json_content)
    for sec in payload.get("sections", []):
        if sec.get("name", "").strip().lower() == "significant findings and action plan":
            items = sec.get("items", [])
            logger.info(f"Found {len(items)} items in ‘Significant Findings and Action Plan’")
            return items
    logger.warning("Section ‘Significant Findings and Action Plan’ not found; returning empty list")
    return []

def build_user_message(question_number, items):
    if question_number == 13:
        msg = (
            "Water Hygiene/Legionella Risk Assessment QCC Query:\n\n"
            "Question 13: “Significant Findings and Action Plan” – read through the Observations & Actions, "
            "checking for spelling mistakes, grammatical errors, technical inaccuracies or poor location descriptions. "
            "Confirm that the Priority labels make sense, and note any missing supplementary photographs.\n\n"
            "--- Significant Findings and Action Plan ---\n"
            f"{items}\n\n"
            "If everything looks good, reply “PASS”. Otherwise, list each discrepancy."
        )
        logger.info("Built user message for question 13")
        return msg
    logger.error(f"No handler for question_number={question_number}; returning empty message")
    return ""

def send_to_bedrock(user_text):
    MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"

    payload = {
        "anthropic_version": "bedrock-2023-05-31",   # pins the API version
        "max_tokens":        1000,
        "temperature":       0.0,
        "system": (
            "You are a meticulous proofreader. "
            "Correct spelling, grammar and clarity only—no extra commentary or re-structuring."
        ),
        "messages": [
            {
                "role":    "user",
                "content": user_text
            }
        ]
    }

    logger.info(f"Invoking Bedrock model {MODEL_ID} with payload: {json.dumps(payload)}")
    resp = bedrock.invoke_model(
        modelId     = MODEL_ID,
        body        = json.dumps(payload),
        contentType = "application/json",
        accept      = "application/json"
    )
    response_text = resp["body"].read().decode("utf-8")
    logger.info(f"Received response from Bedrock: {response_text}")
    return response_text

def process(event, context):
    """
    Lambda entry point.
    Expects:
      - event['json_bucket'], event['json_key']
      - optional event['question_number']
    """
    logger.info(f"Event received: {json.dumps(event)}")
    bucket = event.get("json_bucket")
    key    = event.get("json_key")
    q_num  = event.get("question_number", 13)

    # 1) Fetch Textract JSON
    raw_json = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    logger.info(f"Fetched raw JSON from S3://{bucket}/{key}: {raw_json[:200]}…")

    # 2) Extract the relevant section
    items = extract_json_data(raw_json)

    # 3) Build user message & send to Bedrock
    user_msg = build_user_message(q_num, items)
    logger.info(f"User message to send: {user_msg}")
    result = send_to_bedrock(user_msg)

    # 4) Return the Bedrock result
    return {
        "statusCode": 200,
        "body":       json.dumps({"bedrock_response": result})
    }
