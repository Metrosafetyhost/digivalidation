import boto3
import json
import re

bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3       = boto3.client('s3')

def extract_section(text, section_title):
    pattern = re.compile(
        rf"{re.escape(section_title)}\s*[:\-–]*\s*(.*?)(?=\n\S+\s*[:\-–]|\Z)",
        re.DOTALL | re.IGNORECASE
    )
    m = pattern.search(text)
    return m.group(1).strip() if m else ""

def extract_csv_data(csv_content):
    return {
        # … your existing sections …
        "significant_findings": extract_section(csv_content, "Significant Findings and Action Plan"),
    }

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
    # … expand for other question_numbers …

def send_to_bedrock(prompt):
    resp = bedrock.invoke_model(
        ModelId="your-bedrock-model-id",
        Body=json.dumps({"prompt": prompt, "maxTokens": 500}),
        ContentType="application/json"
    )
    return json.loads(resp["Body"].read())

def lambda_handler(event, context):
    bucket  = event["csv_bucket"]
    key     = event["csv_key"]
    q_num   = event.get("question_number", 13)

    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    data = extract_csv_data(raw)
    prompt = build_prompt(q_num, data)
    result = send_to_bedrock(prompt)

    return {
        "statusCode": 200,
        "body": json.dumps({"bedrock_response": result})
    }
