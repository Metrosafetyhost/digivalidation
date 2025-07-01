import json
import boto3
import logging
from botocore.client import Config

# ——— Initialise logging ———
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ——— AWS clients ———
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3      = boto3.client(
    "s3",
    region_name="eu-west-2",
    config=Config(signature_version="s3v4")
)

# ——— Constants ———
MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"

def classify_asset_text(text):
    """
    Send `text` to Bedrock and return a dict with the five fields.
    """
    prompt = (
        "Please categorise the following into these six fields:\n"
        "Floor, Object Type, Object Category, Label, Enable/Security Code, Asset Instructions\n\n"
        f"Input: {text}\n\n"
        "Output as a single JSON object:\n"
        "{\n"
        '  "Floor": "...",\n'
        '  "Object Type": "...",\n'
        '  "Object Category": "...",\n'
        '  "Label": "...",\n'
        '  "Enable/Security Code": "...",\n'
        '  "Asset Instructions": "..." \n'
        "}\n"
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
    """
    Handler expects:
      {
        "bucket": "your-input-bucket",
        "key":    "samples.json"    # or .txt for a single line
      }
    """
    bucket = event.get("bucket")
    key    = event.get("key")

    logger.info("Starting asset classification for s3://%s/%s", bucket, key)

    # ——— 1) Fetch the sample(s) from S3 ———
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")

    # assume either {"samples": [...]} or a single-line text
    try:
        data = json.loads(body)
        samples = data.get("samples") or [body]
    except json.JSONDecodeError:
        samples = [body]

    # ——— 2) Classify each sample ———
    results = []
    for txt in samples:
        try:
            out = classify_asset_text(txt)
            results.append(out)
        except Exception as ex:
            logger.warning("Error classifying sample: %s", ex, exc_info=True)
            results.append({"error": str(ex), "input": txt})

    # ——— 3) Return or persist results ———
    # here we simply return; you could also write back to S3 if you prefer
    logger.info("Classification complete: %s", results)
    return {
        "statusCode": 200,
        "body": json.dumps({"classifications": results})
    }
