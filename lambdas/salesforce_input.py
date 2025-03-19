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
    try:
        logger.debug(f"Full event received: {json.dumps(event, indent=2)}")
        
        try:
            body = json.loads(event["body"])  # ✅ Extract the JSON body
            if "sectionContents" not in body:
                logger.error(f"❌ Missing 'sectionContents' key in event body. Received keys: {list(body.keys())}")
                return {}, {}
            html_data = body["sectionContents"]  # ✅ Now correctly extracts the expected format
        except json.JSONDecodeError:
            logger.error("❌ Failed to decode JSON body.")
            return {}, {}

        if not html_data:
            logger.warning("No HTML data found in event.")
            return {}, {}

        proofing_requests = {}
        table_data = {}  # Store original table rows

        for entry in html_data:
            record_id = entry.get("recordId")  # Extract record ID
            content_html = entry.get("content")  # Extract table HTML

            if not record_id or not content_html:
                logger.warning(f"⚠️ Skipping entry with missing recordId or content: {entry}")
                continue

            soup = BeautifulSoup(content_html, "html.parser")

            rows = soup.find_all("tr")  # Extract all table rows

            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    header_text = cells[0].get_text(strip=True)  # Get header text only
                    content_text = cells[1].get_text(strip=True)  # Extract clean content

                    logger.debug(f"🔍 Extracted - Header: '{header_text}', Content: '{content_text}'")

                    if header_text in ALLOWED_HEADERS:
                        # ✅ Only store if content is **non-empty**
                        if content_text:
                            proofing_requests[header_text] = content_text  # ✅ Store extracted text only
                            table_data[header_text] = row  # ✅ Store the table row for reinsertion
                        else:
                            logger.info(f"⚠️ Skipping '{header_text}' as it has no content.")

        logger.info(f"✅ Extracted {len(proofing_requests)} items for proofing.")
        return proofing_requests, table_data

    except Exception as e:
        logger.error(f"Unexpected error in load_html_data: {e}")
        return {}, {}


def proof_html_with_bedrock(header, content):
    # corrects content using  Bedrock.
    try:
        # log the original content before proofing
        logger.info(f"🔹 Original content before proofing (Header: {header}): {content}")

        text_content = BeautifulSoup(content, "html.parser").get_text().strip()  # Extract clean text


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

                    Correct this text: {text_content} """}
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

        if proofed_text:
            return proofed_text  # ✅ Return only proofed text
        else:
            return content  # ✅ Keep original if proofing fails


        # parse response for titan 
        # response_body = json.loads(response["body"].read().decode("utf-8"))
        # proofed_text = response_body.get("results", [{}])[0].get("outputText", "").strip()

        # log the proofed text

    except Exception as e:
        logger.error(f"❌ Bedrock API Error: {str(e)}")
        return content  # return original text if error 


def process(event, context):
    logger.info(f"🔹 Full Incoming Event: {json.dumps(event, indent=2)}") 

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

    # ✅ Load extracted text and original table structure
    proofing_requests, table_data = load_html_data(event)

    proofed_entries = []

    original_text = "=== ORIGINAL TEXT ===\n"
    proofed_text = "=== PROOFED TEXT ===\n"

    for header, content in proofing_requests.items():
        proofed_content = proof_html_with_bedrock(header, content)

        # ✅ Reinsert proofed text into the original HTML row structure
        if header in table_data:
            row = table_data[header]
            content_cell = row.find_all("td")[1]  # Second <td> contains the content
            content_cell.clear()
            content_cell.append(proofed_content)  # ✅ Replace text inside <td> correctly
            updated_html = str(row)  # Convert modified row back to HTML

            proofed_entries.append({"recordId": workorder_id, "content": updated_html})

            # ✅ Store original and proofed content for tracking
            original_text += f"\n\n### {header} ###\n{content}\n"
            proofed_text += f"\n\n### {header} ###\n{proofed_content}\n"
        else:
            logger.warning(f"⚠️ No table data found for header: {header}")

    logger.info(f"🔹 Storing proofed files in S3...")

    original_s3_key = store_in_s3(original_text, f"{workorder_id}_original", "original")
    proofed_s3_key = store_in_s3(proofed_text, f"{workorder_id}_proofed", "proofed")

    store_metadata(workorder_id, original_s3_key, proofed_s3_key)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "workOrderId": workorder_id,
            "sectionContents": proofed_entries  # ✅ Ensure formatted HTML is returned
        })
    }
