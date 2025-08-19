# asset_categorisation.py (minimal)
import os, json
from urllib.parse import urlparse
import boto3
from openai import OpenAI

MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

# Ask your existing questions. (Replace this block with your exact prompt text if you want.)
PROMPT = (
    "Analyse the photo and answer these (UK context). "
    "Return ONLY strict JSON with these fields and null for anything not visible:\n"
    '{'
    '"what_is_it": string|null,'
    '"manufacturer_brand": string|null,'
    '"serial_number": string|null,'
    '"colour": string|null,'
    '"rough_dimensions": string|null,'
    '"distinguishing_features": string|null,'
    '"condition": string|null,'
    '"broken_or_needs_replacement": string|null,'
    '"service_provider_or_supplier": {"company_name": string|null, "contact_details": string|null}|null,'
    '"other_codes_or_numbers": string|null,'
    '"how_to_test": string|null,'
    '"how_to_replace": string|null,'
    '"parts_needed": string|null,'
    '"uk_price_estimate_gbp": string|null,'
    '"confidence": number'
    '}'
)

s3 = boto3.client("s3")
client = OpenAI(api_key=OPENAI_API_KEY)

def _presigned_url(bucket: str, key: str, expires=3600) -> str:
    return s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires
    )

def _normalize_s3(bucket: str | None, s3_key: str) -> tuple[str, str]:
    if s3_key.startswith("s3://"):
        p = urlparse(s3_key)
        return (bucket or p.netloc, p.path.lstrip("/"))
    return (bucket, s3_key)

def ask_with_image(image_url: str) -> dict | None:
    """Return the JSON dict the model gives, or None if it's not a valid JSON answer."""
    resp = client.chat.completions.create(
        model=MODEL,
        response_format={"type": "json_object"},
        temperature=0,
        max_tokens=800,
        messages=[
            {"role": "system", "content": "Answer strictly as JSON only."},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": PROMPT},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            },
        ],
    )
    try:
        content = resp.choices[0].message.content or ""
        # If the model didn't actually return JSON, this will throw
        return json.loads(content)
    except Exception:
        return None

def process(event: dict) -> dict | None:
    """
    event expects:
      { "bucket": "my-bucket", "s3_key": "path/to.jpg" }
      or { "s3_key": "s3://my-bucket/path/to.jpg" }
    Returns the JSON dict from the model, or None.
    """
    bucket = event.get("bucket")
    s3_key = event.get("s3_key")
    if not s3_key:
        return None

    bucket, key = _normalize_s3(bucket, s3_key)
    if not bucket or not key:
        return None

    img_url = _presigned_url(bucket, key)
    return ask_with_image(img_url)

def lambda_handler(event, context):
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except Exception:
            return {"statusCode": 200, "body": json.dumps(None)}
    if not isinstance(event, dict):
        return {"statusCode": 200, "body": json.dumps(None)}

    result = process(event)
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(result),  # either the JSON dict or null
    }
