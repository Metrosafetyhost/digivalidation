import json
import boto3

# Initialise AWS clients
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3       = boto3.client('s3')

def extract_json_data(json_content):
    """
    Parses the Textract-generated JSON, finds the
    "Significant Findings and Action Plan" section,
    and returns its items list.
    """
    payload = json.loads(json_content)
    for sec in payload.get("sections", []):
        if sec.get("name", "").strip().lower() == "significant findings and action plan":
            return sec.get("items", [])
    return []

def build_user_message(question_number, items):
    """
    Returns only the user-facing content for a given question.
    Currently implemented for question_number == 13.
    """
    if question_number == 13:
        return (
            "Water Hygiene/Legionella Risk Assessment QCC Query:\n\n"
            "Question 13: “Significant Findings and Action Plan” – read through the Observations & Actions, "
            "checking for spelling mistakes, grammatical errors, technical inaccuracies or poor location descriptions. "
            "Confirm that the Priority labels make sense, and note any missing supplementary photographs.\n\n"
            "--- Significant Findings and Action Plan ---\n"
            f"{items}\n\n"
            "If everything looks good, reply “PASS”. Otherwise, list each discrepancy."
        )
    # Future questions go here...
    return ""

def send_to_bedrock(user_text):
    """
    Invokes Claude 3 Sonnet via the Messages API.
    The 'system' role sets the assistant’s behaviour,
    the 'user' role carries your QCC query.
    """
    MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"
    payload = {
        "messages": [
            {
                # SYSTEM sets tone/instructions for Claude
                "role":    "system",
                "content": (
                    "You are a meticulous proofreader. "
                    "Correct spelling, grammar and clarity only—no extra commentary or re-structuring."
                )
            },
            {
                # USER carries the actual QCC question
                "role":    "user",
                "content": user_text
            }
        ],
        "max_tokens_to_sample": 1000,
        "temperature":           0.0
    }

    resp = bedrock.invoke_model(
        modelId     = MODEL_ID,
        body        = json.dumps(payload),
        contentType = "application/json",
        accept      = "application/json"
    )
    return resp["body"].read().decode("utf-8")

def process(event, context):
    """
    Lambda entry point.
    Expects event to have 'json_bucket', 'json_key' and optional 'question_number'.
    """
    bucket = event["json_bucket"]
    key    = event["json_key"]
    q_num  = event.get("question_number", 13)

    # 1) Fetch Textract JSON from S3
    raw_json = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

    # 2) Extract just the Significant Findings items
    items = extract_json_data(raw_json)

    # 3) Build the user message and send to Bedrock
    user_msg = build_user_message(q_num, items)
    result   = send_to_bedrock(user_msg)

    return {
        "statusCode": 200,
        "body": json.dumps({"bedrock_response": result})
    }
