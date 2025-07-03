import json
import boto3
import logging
from botocore.client import Config

# initialise logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3      = boto3.client(
    "s3",
    region_name="eu-west-2",
    config=Config(signature_version="s3v4")
)
MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"

def classify_asset_text(text):

    prompt = (
        "Please categorise the following asset description into these Salesforce fields:\n"
        "• Object_Type__c: everything up to the first ' - Location'\n"
        "• Object_Category__c: the text after 'Type:'\n"
        "• Asset_Instructions__c: the text after 'Test:'\n"
        "• Label__c: the reference code in Asset_Instructions__c (e.g. 'FF1')\n"
        "• Name: combine:\n"
        "    1) the Location text (after 'Location:' up to the full stop),\n"
        "    2) the object identifier (uppercase acronym of Object_Type__c, e.g. 'Emergency Light' → 'EL'),\n"
        "    3) the Label__c\n"
        "  separated by commas.\n\n"
        f"Input: {text}\n\n"
        "Output as a single JSON object, using these exact keys:\n"
        "{\n"
        '  "Object_Type__c": "…",\n'
        '  "Object_Category__c": "…",\n'
        '  "Asset_Instructions__c": "…",\n'
        '  "Label__c": "…",\n'
        '  "Name": "…"\n'
        "}\n\n"
    )
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens":        500,
        "temperature":       0.0,
        "messages": [
            { "role": "user", "content": prompt }
        ]
    }

    resp = bedrock.invoke_model(
        modelId     = MODEL_ID,
        body        = json.dumps(payload),
        contentType = "application/json",
        accept      = "application/json"
    )
    raw = resp["body"].read().decode("utf-8")

    # parse the JSON blob out of Bedrock’s response
    try:
        data = json.loads(raw)
        # Claude returns its assistant text in data["content"]
        text_out = "".join(part.get("text", "") for part in data.get("content", []))
        return json.loads(text_out)
    except Exception as e:
        logger.error("Failed to parse classification response: %s", e)
        raise

def process(event, context):
    # 1. Read the JSON array from HTTP body
    body = json.loads(event["body"])
    samples = [obj.get("input") for obj in body]

    # 2. Classify each sample
    results = []
    for txt in samples:
        try:
            results.append(classify_asset_text(txt))
        except Exception as ex:
            results.append({ "error": str(ex), "input": txt })

    # 3. Return bare JSON array
    return {
        "statusCode": 200,
        "headers": { "Content-Type": "application/json" },
        "body": json.dumps(results)
    }

