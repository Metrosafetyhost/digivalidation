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
        # "    2) the object identifier (uppercase acronym of Object_Type__c, e.g. 'Emergency Light' → 'EL'),\n"
        # "    3) the Label__c\n"
        # "  separated by commas.\n\n"
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
    logger.info("<< classify_asset_text: raw Bedrock response: %s", raw)

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
    logger.info("<< process: received event: %s", json.dumps(event))

    # read the JSON array from HTTP body
    try:
        body = json.loads(event.get("body","[]"))
    except Exception as e:
        logger.error("process: could not decode event['body']: %s", e, exc_info=True)
        raise

    logger.info(">> process: HTTP body parsed as: %s", body)
    samples = [obj.get("input") for obj in body]
    logger.info(">> process: extracted samples: %s", samples)

    # 2. Classify each sample
    results = []
    for txt in samples:
        try:
            out = classify_asset_text(txt)
            results.append(out)
        except Exception as ex:
            logger.warning("process: classification error for input '%s': %s", txt, ex)
            results.append({ "error": str(ex), "input": txt })

    logger.info("<< process: returning results: %s", results)

    # 3. Return bare JSON array
    return {
        "statusCode": 200,
        "headers": { "Content-Type": "application/json" },
        "body": json.dumps(results)
    }

