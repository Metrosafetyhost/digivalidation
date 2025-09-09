# lambda_function.py
import os
import json
import base64
import boto3
from botocore.exceptions import ClientError
from openai import OpenAI

# ---------------------------
# Config (matches your style)
# ---------------------------
S3_BUCKET = os.environ.get("ASSET_BUCKET", "metrosafetyprod")
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

def _load_openai_key():
    arn = os.environ.get("OPENAI_SECRET_ARN")
    if arn:
        sm = boto3.client("secretsmanager")
        val = sm.get_secret_value(SecretId=arn)
        s = val.get("SecretString")
        return s if s is not None else base64.b64decode(val["SecretBinary"]).decode()
    return os.environ.get("OPENAI_API_KEY")  # fallback

OPENAI_API_KEY = _load_openai_key()

# ---------------------------
# Prompts (tweak as you like)
# ---------------------------
SYSTEM_PROMPT = (
    "You are a safety asset classifier for UK sites. "
    "Given a single site asset photo, output ALL of the following fields as compact JSON with EXACT keys: "
    "Manufacturer__c, What_Is_It__c, SerialNumber, Colour__c, Rough_Dimensions__c, "
    "Distinguishing_Features__c, Asset_Condition__c, Broken_Or_Needs_Replacement__c, "
    "Service_Provider_Or_Supplier__c, Other_Codes_Or_Numbers__c, How_To_Test__c, "
    "How_To_Replace__c, Parts_Needed__c, UK_Estimated_Price__c, Confidence__c. "
    "Always provide a best-guess for every field, even if uncertain. "
    "Base your assumptions on typical UK standards and suppliers if the photo does not show enough detail. "
    "Return realistic rough values (e.g., '120mm diameter', '£20-£40', 'Screwdriver needed'). "
    "Never leave a field blank. Confidence__c must be a number 0..1."
)

USER_INSTRUCTION = "Extract the fields from this image and return ONLY compact JSON."

# ---------------------------
# Clients
# ---------------------------
s3 = boto3.client("s3")
oai = OpenAI(api_key=OPENAI_API_KEY)

# ---------------------------
# Helpers
# ---------------------------
def find_key_by_prefix(prefix: str) -> str | None:
    """
    Return the most recently modified S3 key whose name starts with the given prefix.
    """
    try:
        paginator = s3.get_paginator("list_objects_v2")
        latest_key, latest_ts = None, 0.0
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                ts = obj["LastModified"].timestamp()
                if ts > latest_ts:
                    latest_ts = ts
                    latest_key = obj["Key"]
        return latest_key
    except ClientError as e:
        print(f"S3 list error for prefix {prefix}: {e}")
        return None

def presign(key: str, seconds: int = 900) -> str:
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=seconds,
    )

def call_openai(image_url: str) -> dict:
    """
    Call an OpenAI vision-capable model with an image URL and parse JSON out.
    """
    resp = oai.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "text", "text": USER_INSTRUCTION},
                {"type": "image_url", "image_url": {"url": image_url}},
            ]}
        ],
        temperature=0.2,
    )
    text = resp.choices[0].message.content.strip()

    # Strip common code fences
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
    if text.lower().startswith("json"):
        text = text[4:].strip()

    try:
        data = json.loads(text)
    except Exception:
        data = {"_raw_response": text}

    # Ensure all keys present; coerce Confidence__c
    defaults = {
        "Manufacturer__c": "", "What_Is_It__c": "", "SerialNumber": "", "Colour__c": "",
        "Rough_Dimensions__c": "", "Distinguishing_Features__c": "", "Asset_Condition__c": "",
        "Broken_Or_Needs_Replacement__c": "", "Service_Provider_Or_Supplier__c": "",
        "Other_Codes_Or_Numbers__c": "", "How_To_Test__c": "", "How_To_Replace__c": "",
        "Parts_Needed__c": "", "UK_Estimated_Price__c": "", "Confidence__c": 0.0
    }
    for k, v in defaults.items():
        data.setdefault(k, v)
    try:
        data["Confidence__c"] = float(data.get("Confidence__c", 0.0))
    except Exception:
        data["Confidence__c"] = 0.0

    return data

def make_error_result(msg: str) -> dict:
    """
    Produce a result object with all expected fields empty so list alignment is preserved.
    """
    base = {
        "Manufacturer__c": "", "What_Is_It__c": "", "SerialNumber": "", "Colour__c": "",
        "Rough_Dimensions__c": "", "Distinguishing_Features__c": "", "Asset_Condition__c": "",
        "Broken_Or_Needs_Replacement__c": "", "Service_Provider_Or_Supplier__c": "",
        "Other_Codes_Or_Numbers__c": "", "How_To_Test__c": "", "How_To_Replace__c": "",
        "Parts_Needed__c": "", "UK_Estimated_Price__c": "", "Confidence__c": 0.0,
        "_error": msg
    }
    return base

def parse_incoming(event):
    """
    Support both direct array (Lambda test) and API Gateway proxy (event['body']).
    Expect: [{ "ContentVersionId": "<prefix>" }, ...]
    """
    payload = event
    if isinstance(event, dict) and "body" in event:
        body = event["body"]
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode("utf-8")
        payload = json.loads(body)
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, list):
        raise ValueError("Payload must be a JSON array.")
    return payload

# ---------------------------
# Handler
# ---------------------------
def process(event, context):
    try:
        items = parse_incoming(event)
    except Exception as e:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": f"Bad request: {e}"})
        }

    results = []
    for item in items:
        prefix = (item or {}).get("ContentVersionId")
        if not prefix:
            results.append(make_error_result("Missing ContentVersionId"))
            continue

        key = find_key_by_prefix(prefix)
        if not key:
            results.append(make_error_result(f"No S3 object for prefix '{prefix}'"))
            continue

        try:
            url = presign(key)
            fields = call_openai(url)
            fields["_s3_key"] = key  # optional for debug; Apex ignores unknown keys
            results.append(fields)
        except Exception as e:
            results.append(make_error_result(f"Inference failed: {e}"))

    # Return array in SAME ORDER to match Apex's index-based mapping
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(results)
    }
