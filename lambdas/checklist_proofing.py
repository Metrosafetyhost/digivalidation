import json
import boto3

# Initialise clients
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3       = boto3.client('s3')

def extract_json_data(json_content):
    payload = json.loads(json_content)
    for sec in payload.get("sections", []):
        if sec.get("name", "").strip().lower() == "significant findings and action plan":
            return sec.get("items", [])
    return []

def build_user_message(question_number, items):
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
    return ""

def send_to_bedrock(user_text):
    MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"
    payload = {
        "anthropic_version": "bedrock-2023-05-31",   # pins the API version :contentReference[oaicite:0]{index=0}
        "max_tokens":          1000,                  # required key :contentReference[oaicite:1]{index=1}
        "temperature":         0.0,
        "system": (
            "You are a meticulous proofreader. "
            "Correct spelling, grammar and clarity only—no extra commentary or re-structuring."
        ),
        "messages": [
            {
                "role":    "user",   # must be 'user' or 'assistant' (no 'system' here) :contentReference[oaicite:2]{index=2}
                "content": user_text
            }
        ]
    }

    resp = bedrock.invoke_model(
        modelId     = MODEL_ID,
        body        = json.dumps(payload),
        contentType = "application/json",
        accept      = "application/json"
    )
    return resp["body"].read().decode("utf-8")

def process(event, context):
    bucket = event["json_bucket"]
    key    = event["json_key"]
    q_num  = event.get("question_number", 13)

    raw_json = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    items    = extract_json_data(raw_json)
    user_msg = build_user_message(q_num, items)
    result   = send_to_bedrock(user_msg)

    return {
        "statusCode": 200,
        "body":       json.dumps({"bedrock_response": result})
    }
