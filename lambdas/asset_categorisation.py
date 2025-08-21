# asset_categorisation.py (minimal, inline-image version)
import os, json, mimetypes
from urllib.parse import urlparse
import boto3
from openai import OpenAI
import base64


def _load_openai_key():
    arn = os.environ.get("OPENAI_SECRET_ARN")
    if arn:
        sm = boto3.client("secretsmanager")
        val = sm.get_secret_value(SecretId=arn)
        s = val.get("SecretString")
        return s if s is not None else base64.b64decode(val["SecretBinary"]).decode()
    return os.environ.get("OPENAI_API_KEY")  # fallback

OPENAI_API_KEY = _load_openai_key()

MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

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

def _guess_mime(key: str, default="image/jpeg") -> str:
    mime, _ = mimetypes.guess_type(key)
    return mime or default

def s3_to_data_url(bucket: str, key: str) -> str:
    obj = s3.get_object(Bucket=bucket, Key=key)
    b = obj["Body"].read()
    # Prefer the object's ContentType if present; fall back to extension; then jpeg
    mime = obj.get("ContentType") or _guess_mime(key)
    return f"data:{mime};base64,{base64.b64encode(b).decode()}"

def ask_with_image_data(bucket: str, key: str) -> dict | None:
    """Send the S3 image inline (no external fetch). Return parsed JSON or None."""
    data_url = s3_to_data_url(bucket, key)
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
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            },
        ],
    )
    try:
        content = resp.choices[0].message.content or ""
        return json.loads(content)
    except Exception:
        return None

def _normalize_s3(bucket: str | None, s3_key: str) -> tuple[str, str]:
    if s3_key.startswith("s3://"):
        p = urlparse(s3_key)
        return (bucket or p.netloc, p.path.lstrip("/"))
    return (bucket, s3_key)

def process(event, context=None):
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

    # Inline path (no presigned URL)
    return ask_with_image_data(bucket, key)

def lambda_handler(event, context):
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except Exception:
            return {"statusCode": 200, "body": json.dumps(None)}
    if not isinstance(event, dict):
        return {"statusCode": 200, "body": json.dumps(None)}

    result = process(event, context)
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(result),
    }
