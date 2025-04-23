import boto3
import json

# Initialize AWS clients
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3       = boto3.client('s3')

def extract_json_data(json_content):
    """
    Parses the Textract‐generated JSON, finds the real
    "Significant Findings and Action Plan" section,
    and returns its items.
    """
    payload = json.loads(json_content)
    for sec in payload.get("sections", []):
        if sec.get("name", "").strip().lower() == "significant findings and action plan":
            # returns the parsed list of audit_ref/question/priority/etc.
            return {"significant_findings": sec.get("items", [])}
    # fallback if no real section found
    return {"significant_findings": []}


def build_prompt(question_number, data):
    if question_number == 13:
        return (
            "Water Hygiene/Legionella Risk Assessment QCC Query:\n\n"
            "Question 13: “Significant Findings and Action Plan” – read through the Observations & Actions, "
            "checking for spelling mistakes, grammatical errors, technical inaccuracies or poor location descriptions. "
            "Confirm that the Priority labels make sense, and note any missing supplementary photographs.\n\n"
            f"--- Significant Findings and Action Plan ---\n{data['significant_findings']}\n\n"
            "If everything looks good, reply “PASS”. Otherwise, list each discrepancy."
        )
    # Add other question handlers here if needed
    return ""

def send_to_bedrock(prompt_text):
    MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"
    payload = {"prompt": prompt_text, "max_tokens_to_sample": 1000, "temperature": 0}
    resp = bedrock.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps(payload),      
        contentType="application/json",
        accept="application/json",
    )
    return resp["body"].read().decode("utf-8")


def process(event, context):
    # Lambda entry point
    bucket  = event["json_bucket"]
    key     = event["json_key"]
    q_num   = event.get("question_number", 13)

    # Fetch the JSON produced by Textract
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

    # Extract just the Significant Findings section
    data = extract_json_data(raw)

    # Build and send the prompt to Bedrock
    prompt = build_prompt(q_num, data)
    result = send_to_bedrock(prompt)

    return {
        "statusCode": 200,
        "body": json.dumps({"bedrock_response": result})
    }
