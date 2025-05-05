import json
import boto3
import logging

# ——— Initialise logging ———
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ——— AWS clients ———
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3       = boto3.client('s3')


def extract_json_data(json_content, question_number):
    """
    Pull out either:
      • Q4 => the Building Description string
      • Q13 => the list of Significant Findings items
    """
    payload = json.loads(json_content)

    # ————— Q4: Building Description —————
    if question_number == 4:
        for sec in payload.get("sections", []):
            name = sec.get("name", "").strip().lower()
            if name.endswith("building description"):
                # look for the table row “Description of the Property”
                for tbl in sec.get("tables", []):
                    for row in tbl.get("rows", []):
                        key = row[0].lower().strip()
                        if key.startswith("description of the property"):
                            desc = row[1].strip()
                            logger.info("Extracted Building Description")
                            return desc
        logger.warning("Section ‘Building Description’ not found; returning empty string")
        return ""

    # ————— Q13: Significant Findings and Action Plan —————
    if question_number == 13:
        for sec in payload.get("sections", []):
            if sec.get("name", "").strip().lower() == "significant findings and action plan":
                items = sec.get("items", [])
                logger.info(f"Found {len(items)} items in ‘Significant Findings and Action Plan’")
                return items
        logger.warning("Section ‘Significant Findings and Action Plan’ not found; returning empty list")
        return []

    # ————— default —————
    logger.error(f"No extractor for question_number={question_number}; returning None")
    return None


def build_user_message(question_number, content):
    """
    Build the user→Bedrock prompt for each question.
    """
    # Q4: Building Description
    if question_number == 4:
        msg = (
            "Water Hygiene/Legionella Risk Assessment QCC Query:\n\n"
            "Question 4: Read the Building Description, ensuring it’s complete, concise and relevant.\n\n"
            "--- Building Description ---\n"
            f"{content}\n\n"
            "If it’s good, reply “PASS”. Otherwise, list any missing or unclear details."
        )
        logger.info("Built user message for question 4")
        return msg

    # Q13: Significant Findings…
    if question_number == 13:
        msg = (
            "Water Hygiene/Legionella Risk Assessment QCC Query:\n\n"
            "Question 13: “Significant Findings and Action Plan” – read through the Observations & Actions, "
            "checking for spelling mistakes, grammatical errors, technical inaccuracies or poor location descriptions. "
            "Confirm that the Priority labels make sense, and note any missing supplementary photographs.\n\n"
            "--- Significant Findings and Action Plan ---\n"
            f"{content}\n\n"
            "If everything looks good, reply “PASS”. Otherwise, list each discrepancy."
        )
        logger.info("Built user message for question 13")
        return msg

    # fallback
    logger.error(f"No handler for question_number={question_number}; returning empty message")
    return ""


def send_to_bedrock(user_text):
    MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
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

    logger.info("Bedrock request payload: %s", json.dumps(payload))
    resp = bedrock.invoke_model(
        modelId     = MODEL_ID,
        body        = json.dumps(payload),
        contentType = "application/json",
        accept      = "application/json"
    )
    response_text = resp["body"].read().decode("utf-8")
    logger.info("Received response from Bedrock")
    return response_text


def process(event, context):
    """
    Lambda entry point.
    Expects: 
      - event['json_bucket'], event['json_key']
      - optional event['question_number'] (defaults to 13)
    """
    logger.info(f"Event received: {event}")
    bucket = event.get("json_bucket")
    key    = event.get("json_key")
    q_num  = event.get("question_number", 13)

    # 1) fetch your pre-processed Textract JSON
    raw_json = s3.get_object(Bucket=bucket, Key=key)["Body"]\
                  .read().decode("utf-8")

    # 2) extract the section or items
    data = extract_json_data(raw_json, q_num)

    # 3) build the Bedrock prompt & invoke
    user_msg = build_user_message(q_num, data)
    result   = send_to_bedrock(user_msg)
    logger.info(f"Bedrock response payload: {result}")

    # 4) return the AI’s proofed text
    return {
        "statusCode": 200,
        "body":       json.dumps({"bedrock_response": result})
    }
