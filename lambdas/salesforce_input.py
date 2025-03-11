import json
import boto3
import logging
from aws_lambda_powertools.logging import Logger
from bs4 import BeautifulSoup
import uuid
import time

# initialise logger
logger = Logger()

# initialise AWS clients
bedrock_client = boto3.client("bedrock-runtime", region_name="eu-west-2")
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# define Bedrock model
BEDROCK_MODEL_ID = "amazon.titan-text-lite-v1"
BUCKET_NAME = f"metrosafety-bedrock-output-data-dev-bedrock-lambda"
TABLE_NAME = "ProofingMetadata"

# define headers that need proofing
ALLOWED_HEADERS = [
    "Passenger and Disabled Access Platform Lifts (DAPL)",
    "Fire Service and Evacuation Lifts",
    "Mains Electrical incomers and electrical distribution boards (EDBs)",
    "Natural Gas Supplies",
    "Disabled escape arrangements",
    "General Means of Escape Description",
    "Fire Assembly Point",
    "Optimal evacuation strategy for the building occupancy type",
    "Fire Safety Policies"
]


def store_in_s3(text, filename, folder):
    # store text in S3 under the correct folder and return object key
    s3_key = f"{folder}/{filename}.txt"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=text)
    return s3_key


def store_metadata(workorder_id, original_s3_key, proofed_s3_key):
    # store metadata in DynamoDB for tracking.
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(
        Item={
            "workorder_id": workorder_id,
            "original_s3_key": original_s3_key,
            "proofed_s3_key": proofed_s3_key,
            "status": "Pending",
            "timestamp": int(time.time())
        }
    )


def load_html_data(event):
    # extract filtered data based on allowed headers
    try:
        logger.debug(f"Full event received: {json.dumps(event, indent=2)}")

        if "htmlData" not in event:
            logger.error("❌ Missing 'htmlData' key in event.")
            return {}

        html_data = event["htmlData"]
        if not html_data:
            logger.warning("No HTML data found in event.")
            return {}

        proofing_requests = {}

        # process each entry
        for html_entry in html_data:
            soup = BeautifulSoup(html_entry, "html.parser")
            rows = soup.find_all("tr")

            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    header = cells[0].get_text(strip=True)  # get header
                    content = cells[1].get_text(strip=True)  # get content

                    logger.debug(f"Checking row - Header: {header}, Content: {content}")

                    if header in ALLOWED_HEADERS:
                        proofing_requests[header] = content 

        logger.info(f"✅ Extracted {len(proofing_requests)} items for proofing.")
        return proofing_requests  # returns content

    except Exception as e:
        logger.error(f"Unexpected error in load_html_data: {e}")
        return {}


def proof_html_with_bedrock(header, content):
    # corrects content using  Bedrock.
    try:
        # log the original content before proofing
        logger.info(f"🔹 Original content before proofing (Header: {header}): {content}")

        # prompt - to be altered if needed
        prompt = f""" Proofread and correct the following text while ensuring:
                    - Spelling and grammar are corrected in British English, and spacing and formatted corrected.
                    - Headings, section titles, and structure remain unchanged.
                    - Do NOT remove any words, phrases, from the original content.
                    - Do NOT split, merge, or add any new sentences or content.
                    - Ensure NOT to add any introductory text or explanations ANYWHERE.
                    - Ensure that lists, bullet points, and standalone words remain intact.
                    - Ensure only to proofread once, NEVER repeat the same text twice in the ouput. 
                     \nIMPORTANT: The only allowed changes are correcting spacing, spelling and grammar while keeping the original order, and structure 100% intact.
                     \nIMPORTANT: If the text is already correct, return it exactly as it is without any modifications

                    Correct this text: {content} """

                    # - Do NOT rephrase or alter any wording, even if grammatically incorrect.
                    # - Every word and punctuation in the original text must remain exactly as it is, except for spelling, spacing and grammar corrections.
                    # Ensure every original word, phrase and punctuation remains in the corrected output.
                    #- Do NOT merge separate points or section headings.
        # prepare request payload
        payload = {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": 512,
                "temperature": 0.5,
                "topP": 0.9
            }
        }

        # call AWS Bedrock API
        response = bedrock_client.invoke_model(
            modelId="amazon.titan-text-lite-v1",
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload)
        )

        # parse response
        response_body = json.loads(response["body"].read().decode("utf-8"))
        proofed_text = response_body.get("results", [{}])[0].get("outputText", "").strip()

        # log the proofed text
        logger.info(f"✅ Proofed content (Header: {header}): {proofed_text}")

        return proofed_text  # only return proofed content

    except Exception as e:
        logger.error(f"❌ Bedrock API Error: {str(e)}")
        return content  # Return original text if error occurs


def process(event, context):
    workorder_id = event.get("workorder", str(uuid.uuid4()))  # Generate an ID if missing
    html_entries = load_html_data(event)
    
    proofed_entries = {}

    for header, content in html_entries.items():
        proofed_text = proof_html_with_bedrock(header, content)
        proofed_entries[header] = proofed_text

        # Store both original and proofed text in S3 in different folders
        original_s3_key = store_in_s3(content, f"{workorder_id}_{header}_original", "original")
        proofed_s3_key = store_in_s3(proofed_text, f"{workorder_id}_{header}_proofed", "proofed")

        # Store metadata in DynamoDB
        store_metadata(workorder_id, original_s3_key, proofed_s3_key)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "workorder_id": workorder_id,
            "message": "Stored for validation",
            "proofed_entries": proofed_entries
        })
    }
