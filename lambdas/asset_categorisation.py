import os
from openai import OpenAI
import json
import logging
from urllib.parse import unquote_plus


import boto3
from openai import OpenAI

# ---------- Logging ----------
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------- Clients ----------
s3 = boto3.client("s3")
client = OpenAI

# ---------- Config via Env ----------
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")  # vision-capable
PRESIGN_TTL = int(os.environ.get("PRESIGN_TTL", "900"))  # seconds

# ---------- Prompt ----------
PROMPT = (
    "Please describe the following:\n"
    "What is it?\n"
    "Who is the manufacturer / what is the brand?\n"
    "What is the serial number?\n"
    "What is the colour of it?\n"
    "What are the rough dimensions of it if possible to determine?\n"
    "Any other disguishing features that could be mentioned if we need to find and order a new one?\n"
    "What is the condition of this unit?\n"
    "If broken what is broken and needs replacement?\n"
    "Are they details of a service provider or supplier on this unit, if so what?\n"
    "Company name, if present.\n"
    "Contact details, if present\n"
    "Are the any other codes or numbers shown on the unit?\n"
    "How do you test this unit?\n"
    "How do you replace this unit?\n"
    "What parts are needed to replace the unit?\n"
    "What would a UK price estimate of the parts be or the unit cost?\n\n"
    "Important instructions:\n"
    "- Only extract details that are clearly visible in the image or are a reasonable inference from visible context.\n"
    "- If a requested detail is not visible or cannot be determined, return null for that field—do not guess.\n"
    "- Keep answers concise but complete. Use SI units for dimensions where possible.\n"
)

# ---------- JSON Schema for Structured Output ----------
SCHEMA = {
    "name": "UnitInspectionExtraction",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "what_is_it": {"type": "string", "nullable": True},
            "manufacturer_brand": {"type": "string", "nullable": True},
            "serial_number": {"type": "string", "nullable": True},
            "colour": {"type": "string", "nullable": True},
            "rough_dimensions": {"type": "string", "nullable": True},  # e.g., "approx 30 cm x 12 cm x 8 cm"
            "distinguishing_features": {"type": "string", "nullable": True},
            "condition": {"type": "string", "nullable": True},
            "broken_or_needs_replacement": {"type": "string", "nullable": True},
            "service_provider_or_supplier": {
                "type": "object",
                "nullable": True,
                "properties": {
                    "company_name": {"type": "string", "nullable": True},
                    "contact_details": {"type": "string", "nullable": True}
                },
                "required": ["company_name", "contact_details"],
                "additionalProperties": False
            },
            "other_codes_or_numbers": {"type": "string", "nullable": True},
            "how_to_test": {"type": "string", "nullable": True},
            "how_to_replace": {"type": "string", "nullable": True},
            "parts_needed": {"type": "string", "nullable": True},
            "uk_price_estimate_gbp": {"type": "string", "nullable": True},
            "confidence": {"type": "number"}  # model's holistic confidence 0-1
        },
        "required": ["confidence"],
        "additionalProperties": False
    }
}

# ---------- Helpers ----------
def presign_s3(bucket: str, key: str) -> str:
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=PRESIGN_TTL,
    )

def parse_event_for_s3(event: dict):
    """
    For AWS console testing, send:
      { "bucket": "...", "s3_key": "path/to/object.jpg" }
    """
    body = event or {}
    # Allow API Gateway style, but not required here
    if "body" in body:
        try:
            body = json.loads(body["body"]) if isinstance(body["body"], str) else body["body"]
        except Exception:
            body = {}

    bucket = body.get("bucket")
    s3_key = body.get("s3_key") or body.get("key") or body.get("objectKey")
    if s3_key:
        s3_key = unquote_plus(s3_key)

    if not bucket or not s3_key:
        raise ValueError("Provide both 'bucket' and 's3_key' in the test event.")

    return bucket, s3_key

def call_openai_with_image(image_url: str) -> dict:
    resp = client.responses.create(
        model=MODEL,
        input=[{
            "role": "user",
            "content": [
                {"type": "input_text", "text": PROMPT},
                {"type": "input_image", "image_url": image_url},
            ],
        }],
        response_format={"type": "json_schema", "json_schema": SCHEMA},
        store=False,
    )

    payload = None
    try:
        for item in getattr(resp, "output", []) or []:
            if item.get("type") == "message":
                for c in item.get("content", []):
                    if c.get("type") == "output_json":
                        payload = c.get("parsed")
                        if payload:
                            break
            if payload:
                break
    except Exception:
        logger.exception("Failed to parse output_json from response")

    if payload is None:
        payload = {
            "what_is_it": None,
            "manufacturer_brand": None,
            "serial_number": None,
            "colour": None,
            "rough_dimensions": None,
            "distinguishing_features": None,
            "condition": None,
            "broken_or_needs_replacement": None,
            "service_provider_or_supplier": {"company_name": None, "contact_details": None},
            "other_codes_or_numbers": None,
            "how_to_test": None,
            "how_to_replace": None,
            "parts_needed": None,
            "uk_price_estimate_gbp": None,
            "confidence": 0.0,
        }

    return payload


# ---------- Lambda Handler ----------
def process(event, context):
    logger.info("Incoming event: %s", json.dumps(event)[:2000])

    try:
        bucket, key = parse_event_for_s3(event)
        url = presign_s3(bucket, key)
        result = call_openai_with_image(url)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "ok": True,
                "bucket": bucket,
                "key": key,
                "result": result
            })
        }
    except Exception as e:
        logger.exception("Error in handler")
        return {
            "statusCode": 500,
            "body": json.dumps({"ok": False, "error": str(e)})
        }

