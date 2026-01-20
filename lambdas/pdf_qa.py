import os
import json
import base64
import boto3
import pymupdf  # PyMuPDF
from openai import OpenAI

S3_BUCKET = os.environ.get("ASSET_BUCKET", "metrosafetyprodfiles")
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

def _load_openai_key():
    arn = os.environ.get("OPENAI_SECRET_ARN")
    if arn:
        sm = boto3.client("secretsmanager")
        val = sm.get_secret_value(SecretId=arn)
        return val.get("SecretString") or base64.b64decode(val["SecretBinary"]).decode()
    return os.environ.get("OPENAI_API_KEY")

OPENAI_API_KEY = _load_openai_key()
s3 = boto3.client("s3")
oai = OpenAI(api_key=OPENAI_API_KEY)

def extract_cover_photo_png(pdf_bytes: bytes) -> bytes | None:
    """
    Deterministically extracts the *largest embedded image* on page 1 (index 0).
    Returns PNG bytes or None if no images found.
    """
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    try:
        page = doc[0]
        images = page.get_images(full=True)
        if not images:
            return None

        # choose largest image by pixel area
        best = None
        for img in images:
            xref = img[0]
            pix = pymupdf.Pixmap(doc, xref)
            try:
                if pix.n > 4:
                    pix = pymupdf.Pixmap(pymupdf.csRGB, pix)
                area = pix.width * pix.height
                if (best is None) or (area > best[0]):
                    best = (area, pix.tobytes("png"))
            finally:
                pix = None

        return best[1] if best else None
    finally:
        doc.close()

def process(event, context):
    try:
        payload = event if isinstance(event, dict) else json.loads(event["body"])

        bucket = payload.get("bucket", S3_BUCKET)
        pdf_key = payload["pdf_s3_key"]

        # Optional: where to store the cover image in S3
        cover_key = payload.get("cover_s3_key")  # e.g. "covers/CLIF0001.png"

        print(f"Loading PDF from bucket={bucket}, key={pdf_key}")
        obj = s3.get_object(Bucket=bucket, Key=pdf_key)
        pdf_bytes = obj["Body"].read()

        # (A) Extract cover photo in code (fast, reliable)
        cover_png_bytes = extract_cover_photo_png(pdf_bytes)
        if cover_png_bytes and cover_key:
            s3.put_object(
                Bucket=bucket,
                Key=cover_key,
                Body=cover_png_bytes,
                ContentType="image/png",
            )

        # (B) Upload PDF to OpenAI Files
        up = oai.files.create(
            file=("document.pdf", pdf_bytes),
            purpose="responses",  # if your SDK supports it; otherwise "assistants" may still work
        )
        file_id = up.id

        # (C) Ask model for strict JSON matching your Salesforce fields
        schema = {
            "name": "fra_extract",
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "uprn": {"type": ["string", "null"]},
                    "building_name": {"type": ["string", "null"]},
                    "building_address": {"type": ["string", "null"]},
                    "address_line_1": {"type": ["string", "null"]},
                    "address_line_2": {"type": ["string", "null"]},
                    "address_line_3": {"type": ["string", "null"]},
                    "address_line_4": {"type": ["string", "null"]},
                    "postcode": {"type": ["string", "null"]},

                    "awss_sprinkler_misting": {"type": ["string", "null"]},
                    "storeys": {"type": ["integer", "null"]},
                    "height_m": {"type": ["number", "null"]},
                    "basement_levels": {"type": ["integer", "null"]},
                    "below_ground_mentioned": {"type": ["boolean", "null"]},
                    "total_flats": {"type": ["integer", "null"]},

                    "evacuation_policy": {"type": ["string", "null"]},
                    "fra_completion_date_raw": {"type": ["string", "null"]},
                    "fra_completion_date_ddmmyyyy": {"type": ["string", "null"]},
                    "fra_producer": {"type": ["string", "null"]},
                    "fra_author": {"type": ["string", "null"]},
                    "year_built": {"type": ["integer", "null"]},

                    "notes": {"type": ["string", "null"]},  # optional: store anything ambiguous here
                },
                "required": [
                    "uprn","building_name","building_address","address_line_1","address_line_2",
                    "address_line_3","address_line_4","postcode","awss_sprinkler_misting","storeys",
                    "height_m","basement_levels","below_ground_mentioned","total_flats","evacuation_policy",
                    "fra_completion_date_raw","fra_completion_date_ddmmyyyy","fra_producer","fra_author",
                    "year_built","notes"
                ],
            }
        }

        instructions = """
Extract the requested building fields from the PDF. If a field is not explicitly present, return null.
Do not guess. Put uncertainties/assumptions into notes.
Dates: also output DD/MM/YYYY if present or derivable from the PDF text.
"""

        # Responses API is the recommended unified interface. :contentReference[oaicite:5]{index=5}
        resp = oai.responses.create(
            model=MODEL,
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_file", "file_id": file_id},
                        {"type": "input_text", "text": instructions},
                    ],
                }
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "json_schema": schema,
                    "strict": True
                }
            },
        )

        # Most SDKs expose resp.output_text as the JSON string; adjust if yours differs.
        extracted = json.loads(resp.output_text)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "ok": True,
                "fields": extracted,
                "cover_image_written": bool(cover_png_bytes and cover_key),
                "cover_s3_key": cover_key if (cover_png_bytes and cover_key) else None
            }),
        }

    except Exception as e:
        print("Error:", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"ok": False, "error": str(e)}),
        }
