import json
import boto3
import logging
from aws_lambda_powertools.logging import Logger
from bs4 import BeautifulSoup

# initialise logger
logger = Logger()

# initialise AWS Bedrock client
bedrock_client = boto3.client("bedrock-runtime", region_name="eu-west-2")

# define Bedrock model
BEDROCK_MODEL_ID = "amazon.titan-text-lite-v1"

# Define headers that need proofing
ALLOWED_HEADERS = [
    "Passenger and Disabled Access Platform Lifts (DAPL)",
    "Fire Service and Evacuation Lifts",
    "Mains Electrical incomers and electrical distribution boards (EDBs)",
    "Natural Gas Supplies",
    "Fire Safety",
    "Roof Details"
]

def load_html_data(event):
    """Extracts and filters HTML data based on allowed headers"""
    try:
        logger.debug(f"🔍 Full event received: {json.dumps(event, indent=2)}")

        if "htmlData" not in event:
            logger.error("❌ Missing 'htmlData' key in event.")
            return {}

        html_data = event["htmlData"]
        if not html_data:
            logger.warning("⚠️ No HTML data found in event.")
            return {}

        proofing_requests = {}

        # Process each HTML entry
        for html_entry in html_data:
            soup = BeautifulSoup(html_entry, "html.parser")
            rows = soup.find_all("tr")

            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    header = cells[0].get_text(strip=True)
                    content = cells[1].get_text(strip=True)

                    logger.debug(f"🔎 Checking row - Header: {header}, Content: {content}")

                    if header in ALLOWED_HEADERS:
                        proofing_requests[header] = content

        logger.info(f"✅ Extracted {len(proofing_requests)} items for proofing.")
        return proofing_requests

    except Exception as e:
        logger.error(f"🚨 Unexpected error in load_html_data: {e}")
        return {}


def proof_html_with_bedrock(html_text):
    # calls AWS Bedrock model to proof content
    try:
        logger.info(f"Original HTML text before proofing:\n{html_text}")
        # prompt to test with 
        prompt = f"Proofread and correct this HTML content, ensuring spelling and grammar is in British English:\n\n{html_text}"

        # ensure correct JSON format
        payload = {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": 512,
                "temperature": 0.5,
                "topP": 0.9
            }
        }

        # make request to AWS Bedrock
        response = bedrock_client.invoke_model(
            modelId="amazon.titan-text-lite-v1", 
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload) 
        )

        # parse response
        response_body = json.loads(response["body"].read().decode("utf-8"))

        # Titan return proofed text inside "results"
        proofed_text = response_body.get("results", [{}])[0].get("outputText", "").strip()

        logger.info(f"✅ Bedrock proofing successful. Proofed text:\n{proofed_text}")

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
