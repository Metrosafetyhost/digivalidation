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
    "Manufacturer_AI__c, What_Is_It__c, SerialNumber, Colour__c, Rough_Dimensions__c, "
    "Distinguishing_Features__c, Asset_Condition__c, Broken_Or_Needs_Replacement__c, "
    "Service_Provider_Or_Supplier__c, Other_Codes_Or_Numbers__c, How_To_Test__c, "
    "How_To_Replace__c, Parts_Needed__c, UK_Estimated_Price__c, "
    "Estimated_Unit_Replacement_Cost__c, Estimated_Replacement_Parts_Price__c,"
    "Estimated_Labour_Cost_To_Repair__c, Estimated_Labour_Cost_To_Replace__c, "
    "Estimated_Labour_Cost_To_Repair_On_Site__c, Estimated_Time_To_Replace_On_Site__c, "
    "Object_Type_AI__c, Object_Category_AI__c,"
    "Confidence__c. "
    "Always provide a best-guess for every field, even if uncertain. If there is none however, respond with N/A"
    "For Colour__c, return only a SINGLE most dominant or most likely colour (not multiple). "
    "Base your assumptions on typical UK standards and suppliers if the photo does not show enough detail. "
    "Return realistic rough values (e.g., '120mm diameter', '£20-£40', 'Screwdriver needed'). "
    "Never leave a field blank. Confidence__c must be a number 0..1."
     "If a 'building_address' is provided in the input, also infer the nearest realistic UK retail or trade supplier "
    "store location where this asset (or equivalent) could be purchased, and return this in the fields: "
    "Nearest_Store_Name__c, Nearest_Store_Address__c"
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

# EXACT picklist values from Salesforce (API values must match these)
ASSET_CONDITION_VALUES = [
    "C1 - Very Good Condition",
    "C2 - Needs cleaning",
    "C2 - Minor Defects Only",
    "C3 - Maintenance required to return to an accepted level of service",
    "C4 - Requires renewal",
    "C5 - Asset Unserviceable",
]

def normalize_asset_condition(text: str) -> str:
    t = (text or "").strip().lower()

    # C5 — clearly broken/unsafe
    if any(w in t for w in ["unserviceable", "not working", "doesn't work", "broken", "inoperative", "unsafe", "failed"]):
        return "C5 - Asset Unserviceable"

    # C4 — needs replacement / end-of-life
    if any(w in t for w in ["requires renewal", "replace", "replacement", "end of life", "obsolete", "beyond repair", "major defect"]):
        return "C4 - Requires renewal"

    # C3 — needs maintenance/repair to return to service
    if any(w in t for w in ["maintenance required", "requires maintenance", "repair", "service", "intermittent fault", "faulty"]):
        return "C3 - Maintenance required to return to an accepted level of service"

    # C2 (clean) — mainly dirty
    if any(w in t for w in ["dirty", "dust", "grime", "cleaning", "needs cleaning"]):
        return "C2 - Needs cleaning"

    # C2 (minor defects) — scuffs/scratches/loose etc.
    if any(w in t for w in ["minor defect", "minor defects", "scuff", "scratch", "crack", "loose", "wear", "worn", "cosmetic", "slight"]):
        return "C2 - Minor Defects Only"

    # C1 — good/very good/serviceable
    if any(w in t for w in ["very good", "excellent", "good", "serviceable", "ok", "works", "working"]):
        return "C1 - Very Good Condition"

    # Fallback: choose a safe middle ground if the model is vague
    return "C2 - Minor Defects Only"

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
        data["Asset_Condition__c"] = normalize_asset_condition(data.get("Asset_Condition__c"))
    except Exception:
        data = {"_raw_response": text}

    # Ensure all keys present; coerce Confidence__c
    defaults = {
        "Manufacturer_AI__c": "", "What_Is_It__c": "", "SerialNumber": "", "Colour__c": "",
        "Rough_Dimensions__c": "", "Distinguishing_Features__c": "", "Asset_Condition__c": "",
        "Broken_Or_Needs_Replacement__c": "", "Service_Provider_Or_Supplier__c": "",
        "Other_Codes_Or_Numbers__c": "", "How_To_Test__c": "", "How_To_Replace__c": "",
        "Parts_Needed__c": "", "UK_Estimated_Price__c": "",
        "Estimated_Unit_Replacement_Cost__c": "",
        "Estimated_Replacement_Parts_Price__c": "",
        "Estimated_Labour_Cost_To_Repair__c": "",
        "Estimated_Labour_Cost_To_Replace__c": "",
        "Estimated_Labour_Cost_To_Repair_On_Site__c": "",
        "Estimated_Time_To_Replace_On_Site__c": "",
        "Object_Type_AI__c": "",
        "Object_Category_AI__c": "",
        "Confidence__c": 0.0,
        "Nearest_Store_Name__c": "",
        "Nearest_Store_Address__c": "",
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
        "Manufacturer_AI__c": "", "What_Is_It__c": "", "SerialNumber": "", "Colour__c": "",
        "Rough_Dimensions__c": "", "Distinguishing_Features__c": "", "Asset_Condition__c": "",
        "Broken_Or_Needs_Replacement__c": "", "Service_Provider_Or_Supplier__c": "",
        "Other_Codes_Or_Numbers__c": "", "How_To_Test__c": "", "How_To_Replace__c": "",
        "Parts_Needed__c": "", "UK_Estimated_Price__c": "", 
        "Estimated_Unit_Replacement_Cost__c": "",
        "Estimated_Replacement_Parts_Price__c": "",
        "Estimated_Labour_Cost_To_Repair__c": "",
        "Estimated_Labour_Cost_To_Replace__c": "",
        "Estimated_Labour_Cost_To_Repair_On_Site__c": "",
        "Estimated_Time_To_Replace_On_Site__c": "",
        "Object_Type_AI__c": "",
        "Object_Category_AI__c": "",
        "Confidence__c": 0.0,
        "Nearest_Store_Name__c": "",
        "Nearest_Store_Address__c": "",
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
    print("=== Incoming Event ===")
    print(json.dumps(event, indent=2))
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
            building_address = item.get("BuildingAddress")
            fields = call_openai(url, building_address)
            results.append(fields)
        except Exception as e:
            results.append(make_error_result(f"Inference failed: {e}"))

    # Return array in SAME ORDER to match Apex's index-based mapping
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(results)
    }
