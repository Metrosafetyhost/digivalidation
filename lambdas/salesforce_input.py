import json
import boto3
import logging
from aws_lambda_powertools.logging import Logger

# initialise logger
logger = Logger(service="bedrock-lambda-salesforce_input")

# initialise AWS Bedrock client
bedrock_client = boto3.client("bedrock-runtime", region_name="eu-west-2")

# define Bedrock model
BEDROCK_MODEL_ID = "amazon.titan-text-lite-v1"

def load_html_data(event):
    # extract HTML data directly
    try:
        logger.debug(f"🔍 Full event received: {json.dumps(event, indent=2)}")

        # check if 'htmlData' exists directly in the event
        if "htmlData" not in event:
            logger.error("❌ Missing 'htmlData' key in event.")
            return []

        html_data = event["htmlData"]

        if not html_data:
            logger.warning("⚠️ No HTML data found in event.")

        logger.info(f"✅ Loaded {len(html_data)} HTML data entries.")
        return html_data
    except Exception as e:
        logger.error(f"🚨 Unexpected error in load_html_data: {e}")
        return []

def proof_html_with_bedrock(html_text):
    # calls AWS Bedrock model to proof content
    try:
        # prompt to test with 
        prompt = f"Proofread and correct this HTML content, ensuring spelling and grammar is in British English:\n\n{html_text}"

        # Ensure correct JSON format
        payload = {
            "inputText": prompt,  # ✅ Place the full prompt here
            "textGenerationConfig": {
                "maxTokenCount": 512,
                "temperature": 0.5,
                "topP": 0.9
            }
        }

        # 🔹 Make request to AWS Bedrock
        response = bedrock_client.invoke_model(
            modelId="amazon.titan-text-lite-v1",  # ✅ Ensure correct model ID
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload)  # ✅ Ensure correct JSON format
        )

        # 🔹 Parse response
        response_body = json.loads(response["body"].read().decode("utf-8"))

        # 🔹 Titan models return proofed text inside "results"
        proofed_text = response_body.get("results", [{}])[0].get("outputText", "").strip()

        logger.info("✅ Bedrock proofing successful.")
        return proofed_text

    except Exception as e:
        logger.error(f"❌ Bedrock API Error: {str(e)}")
        return html_text  # 🔹 Return original text if error occurs

def process(event, context):
    # AWS Lambda entry point
    logger.info("🚀 Starting proofing process via AWS Bedrock...")

    # load HTML from request
    html_entries = load_html_data(event)

    # process each entry with Bedrock
    proofed_entries = [proof_html_with_bedrock(entry) for entry in html_entries]

    # return proofed HTML
    return {
        "statusCode": 200,
        "body": json.dumps({"proofed_html": proofed_entries})
    }
