import json
import boto3
import logging
from aws_lambda_powertools.logging import Logger
from bs4 import BeautifulSoup
import uuid
import time

# initialise logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# initialise AWS clients
bedrock_client = boto3.client("bedrock-runtime", region_name="eu-west-2")
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# define Bedrock model
BEDROCK_MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0" #"anthropic.claude-3-haiku-20240307-v1:0"
BUCKET_NAME = f"metrosafety-bedrock-output-data-dev-bedrock-lambda"
TABLE_NAME = "ProofingMetadata"
# models = bedrock_client.list_foundation_models()

# print("✅ Available models in eu-west-1:")
# for model in models["modelSummaries"]:
#     print(model["modelId"])
# define headers that need proofing
ALLOWED_HEADERS = [
    "Building Fire strategy",
    "Fire Service and Evacuation Lifts",
    "Mains Electrical incomers and electrical distribution boards (EDBs)",
    "Natural Gas Supplies",
    "Disabled escape arrangements",
    # "General Means of Escape Description",
    # "Fire Assembly Point",
    # "Optimal evacuation strategy for the building occupancy type",
    # "Fire Safety Policies"
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
            "status": "Pending", # to be edited, temporary so i dont forget 
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
                    header_text = cells[0].get_text(strip=True)  # get header
                    content = cells[1].get_text(strip=True)  # get content

                    logger.debug(f"Checking row - Header: {header_text}, Content: {content}")

                   # Normalize headers to ensure flexible matching
                    if any(allowed_header.lower().strip() == header_text.lower().strip() for allowed_header in ALLOWED_HEADERS):
                        proofing_requests[header_text] = content  # Store only matched headers
                        logger.debug(f"✅ Matched Header: {header_text} -> Added for proofing.")


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
        payload = {
            "anthropic_version": "bedrock-2023-05-31", 
            "messages": [
                {"role": "user", "content": f""" Proofread and correct the following text while ensuring:
                    - Spelling and grammar are corrected in British English, and spacing and formatted corrected.
                    - Headings, section titles, and structure remain unchanged.
                    - Do NOT remove any words, phrases, from the original content.
                    - Do NOT split, merge, or add any new sentences or content.
                    - Ensure NOT to add any introductory text or explanations ANYWHERE.
                    - Ensure that lists, bullet points, and standalone words remain intact.
                    - Ensure only to proofread once, NEVER repeat the same text twice in the ouput. 
                     \nIMPORTANT: The only allowed changes are correcting spacing, spelling and grammar while keeping the original order, and structure 100% intact.
                     \nIMPORTANT: If the text is already correct, return it exactly as it is without any modifications

                    Correct this text: {content} """}
                     ],
            "max_tokens": 512,
            "temperature": 0.3
        }

                    # - Do NOT rephrase or alter any wording, even if grammatically incorrect.
                    # - Every word and punctuation in the original text must remain exactly as it is, except for spelling, spacing and grammar corrections.
                    # Ensure every original word, phrase and punctuation remains in the corrected output.
                    #- Do NOT merge separate points or section headings.
        # prepare request payload
        # payload = {
        #     "inputText": prompt,
        #     "textGenerationConfig": {
        #         "maxTokenCount": 512,
        #         "temperature": 0.5,
        #         "topP": 0.9
        #     }
        # }

        # call AWS Bedrock API
        response = bedrock_client.invoke_model(
            modelId= "anthropic.claude-3-sonnet-20240229-v1:0", #"amazon.titan-text-lite-v1",
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload)
        )


        response_body = json.loads(response["body"].read().decode("utf-8"))

        logger.info(f"🔹 Full Bedrock response: {json.dumps(response_body, indent=2)}")

        proofed_text = " ".join([msg["text"] for msg in response_body.get("content", []) if msg.get("type") == "text"]).strip()

        # parse response for titan 
        # response_body = json.loads(response["body"].read().decode("utf-8"))
        # proofed_text = response_body.get("results", [{}])[0].get("outputText", "").strip()

        # log the proofed text
        logger.info(f"✅ Proofed content (Header: {header}): {proofed_text}")

        return proofed_text if proofed_text else content  # only return proofed content

    except Exception as e:
        logger.error(f"❌ Bedrock API Error: {str(e)}")
        return content  # return original text if error 


def process(event, context):
    logger.info(f"🔹 Full Incoming Event: {json.dumps(event, indent=2)}") 
    
    # ✅ Ensure the request body is correctly parsed
    try:
        body = json.loads(event["body"])  # Extract JSON body
    except (TypeError, KeyError, json.JSONDecodeError):
        logger.error("❌ Error parsing request body")
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid JSON format"})}

    workorder_id = body.get("workOrderId", str(uuid.uuid4()))  # Ensure key matches Apex
    html_entries = body.get("sectionContents", [])  # Extract content list

    if not html_entries:
        logger.error("❌ No section contents received from Salesforce.")
        return {"statusCode": 400, "body": json.dumps({"error": "No section contents found"})}

    proofed_entries = []

    original_text = "=== ORIGINAL TEXT ===\n"
    proofed_text = "=== PROOFED TEXT ===\n"

    for entry in html_entries:
        record_id = entry.get("recordId")
        content = entry.get("content")

        if not record_id or not content:
            logger.warning(f"⚠️ Skipping invalid entry: {entry}")
            continue

        if record_id and content:
            proofed_content = proof_html_with_bedrock("Proofing", content)  # Process text
            proofed_entries.append({"recordId": record_id, "content": proofed_content})

            logger.info(f"✅ Proofed Content for {record_id}: {proofed_content}")

            # Maintain original S3/DynamoDB storage logic
            original_text += f"\n\n### {record_id} ###\n{content}\n"
            proofed_text += f"\n\n### {record_id} ###\n{proofed_content}\n"

            logger.info(f"🔹 Storing files in S3...")

    original_s3_key = store_in_s3(original_text, f"{workorder_id}_original", "original")
    proofed_s3_key = store_in_s3(proofed_text, f"{workorder_id}_proofed", "proofed")

    store_metadata(workorder_id, original_s3_key, proofed_s3_key)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "workOrderId": workorder_id,  # Match Apex naming
            "sectionContents": proofed_entries  # Match Apex expected format
        })
    }
