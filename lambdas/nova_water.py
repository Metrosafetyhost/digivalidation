import json
import boto3
import os
import re
from pathlib import Path

# Configure region & model here
REGION = os.getenv("AWS_REGION", "eu-west-2")
MODEL_ID = "amazon.nova-lite-v1:0"   # or "amazon.nova-pro-v1:0"

def nova_safe_name(key: str) -> str:
    base = Path(key).name                     # e.g., "renaming_to_test.pdf"
    stem = Path(base).stem                    # -> "renaming_to_test"
    # Replace anything not alnum, space, hyphen, parentheses, square brackets with a space
    cleaned = re.sub(r"[^A-Za-z0-9 \-\(\)\[\]]+", " ", stem)
    # Collapse multiple spaces to one, strip edges
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    # If empty after cleaning, fall back
    return cleaned or "Document"

# 🔒 Your hard-coded question lives here:
QUESTION = (
    "Does this report mention an external wall fire risk assessment? "
    "If yes, quote the exact sentence or section."
)

brt = boto3.client("bedrock-runtime", region_name=REGION)

def process(event, context):
    """
    Expected test event JSON:
    {
      "bucket": "my-pdfs-bucket",
      "key": "reports/2025/st-andrews.pdf"
    }
    """
    try:
        bucket = event["bucket"]
        key = event["key"]
    except KeyError as e:
        return {
            "statusCode": 400,
            "body": f"Missing field in event: {e}. Provide 'bucket' and 'key'."
        }

    # Build a Converse request with the PDF attached via S3 and a hard-coded question
    messages = [{
    "role": "user",
    "content": [
        {
            "document": {
                "format": "pdf",
                "name": nova_safe_name(key),
                "source": {"s3Location": {"uri": f"s3://{bucket}/{key}"}}
            }
        },
        {"text": QUESTION}
    ]
    }]

    inf = {"maxTokens": 900, "temperature": 0.2, "topP": 0.9}

    resp = brt.converse(
        modelId=MODEL_ID,
        messages=messages,
        inferenceConfig=inf
    )

    answer = resp["output"]["message"]["content"][0]["text"]

    # Optional: return both the question and the answer
    return {
        "statusCode": 200,
        "body": json.dumps({
            "bucket": bucket,
            "key": key,
            "question": QUESTION,
            "answer": answer
        })
    }
