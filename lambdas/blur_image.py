import json
import boto3
import os
import io
import re
from urllib.parse import unquote_plus
from PIL import Image, ImageFilter, ImageOps
import requests

REGION = os.getenv("AWS_REGION", "eu-west-2")

s3 = boto3.client("s3", region_name=REGION)
rekognition = boto3.client("rekognition", region_name=REGION)

OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET")

# Optional tuning
BLUR_RADIUS = int(os.getenv("BLUR_RADIUS", "25"))
MIN_TEXT_CONFIDENCE = float(os.getenv("MIN_TEXT_CONFIDENCE", "80"))
MIN_PLATE_CHARS = int(os.getenv("MIN_PLATE_CHARS", "5"))
MAX_PLATE_CHARS = int(os.getenv("MAX_PLATE_CHARS", "10"))

# env vars
SF_CALLBACK_URL = os.getenv("SF_CALLBACK_URL")
SF_CALLBACK_SECRET_ARN = os.getenv("SF_CALLBACK_SECRET_ARN")
SF_OAUTH_SECRET_ARN = os.getenv("SF_OAUTH_SECRET_ARN")

secretsmanager = boto3.client("secretsmanager", region_name=REGION)

def _get_secret(secret_arn):
    if not secret_arn:
        return None
    response = secretsmanager.get_secret_value(SecretId=secret_arn)
    return response["SecretString"]

SF_CALLBACK_SECRET = _get_secret(SF_CALLBACK_SECRET_ARN)

def _get_json_secret(secret_arn):
    if not secret_arn:
        return None
    response = secretsmanager.get_secret_value(SecretId=secret_arn)
    return json.loads(response["SecretString"])

SF_OAUTH = _get_json_secret(SF_OAUTH_SECRET_ARN)

def _get_salesforce_access_token():
    if not SF_OAUTH:
        return None

    response = requests.post(
        SF_OAUTH["token_url"],
        data={
            "grant_type": "client_credentials",
            "client_id": SF_OAUTH["client_id"],
            "client_secret": SF_OAUTH["client_secret"],
        },
        timeout=10,
    )
    response.raise_for_status()
    return response.json()["access_token"]

def _download_image_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read(), response.get("ContentType", "image/jpeg")


def _upload_image_to_s3(bucket, key, img_bytes, content_type="image/jpeg"):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=img_bytes,
        ContentType=content_type,
    )


def _extract_content_version_id_from_key(key):
    filename = key.split("/")[-1]
    if "_" not in filename:
        return None
    return filename.split("_", 1)[0]

def _notify_salesforce(content_version_id, blurred_key):
    if not SF_CALLBACK_URL or not SF_CALLBACK_SECRET or not content_version_id:
        return

    access_token = _get_salesforce_access_token()
    if not access_token:
        raise Exception("Could not retrieve Salesforce access token")

    payload = {
        "contentVersionId": content_version_id,
        "blurredKey": blurred_key,
        "sharedSecret": SF_CALLBACK_SECRET
    }

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(SF_CALLBACK_URL, json=payload, headers=headers, timeout=10)
    response.raise_for_status()

def _detect_faces(image_bytes):
    response = rekognition.detect_faces(
        Image={"Bytes": image_bytes},
        Attributes=["DEFAULT"],
    )
    return [f["BoundingBox"] for f in response.get("FaceDetails", [])]


def _normalize_plate_text(text):
    # Remove spaces and punctuation so formats like "AB12 CDE" become "AB12CDE"
    return re.sub(r"[^A-Z0-9]", "", text.upper())


def _looks_like_number_plate(text):
    """
    Generic heuristic for number plates / license plates.

    This is intentionally broad so it works reasonably across different formats.
    You can tighten this later for UK-only formats if needed.
    """
    normalized = _normalize_plate_text(text)

    if not normalized:
        return False

    if len(normalized) < MIN_PLATE_CHARS or len(normalized) > MAX_PLATE_CHARS:
        return False

    # Must contain both letters and digits
    has_letter = any(c.isalpha() for c in normalized)
    has_digit = any(c.isdigit() for c in normalized)

    if not (has_letter and has_digit):
        return False

    # Avoid obviously bad OCR results that are all one repeated char etc.
    unique_chars = len(set(normalized))
    if unique_chars < 3:
        return False

    return True


def _detect_number_plates(image_bytes):
    response = rekognition.detect_text(Image={"Bytes": image_bytes})
    text_detections = response.get("TextDetections", [])

    plate_bboxes = []

    for item in text_detections:
        if item.get("Type") != "LINE":
            continue

        confidence = item.get("Confidence", 0)
        detected_text = item.get("DetectedText", "")
        geometry = item.get("Geometry", {})
        bbox = geometry.get("BoundingBox")

        if confidence < MIN_TEXT_CONFIDENCE:
            continue

        if not bbox:
            continue

        if _looks_like_number_plate(detected_text):
            plate_bboxes.append(bbox)

    return plate_bboxes


def _blur_regions(image_bytes, bboxes):
    with Image.open(io.BytesIO(image_bytes)) as img:
        img = img.convert("RGB")

        width, height = img.size

        for bbox in bboxes:
            left = max(0, int(bbox["Left"] * width))
            top = max(0, int(bbox["Top"] * height))
            box_width = int(bbox["Width"] * width)
            box_height = int(bbox["Height"] * height)

            right = min(width, left + box_width)
            bottom = min(height, top + box_height)

            if right > left and bottom > top:
                region = img.crop((left, top, right, bottom))
                blurred_region = region.filter(ImageFilter.GaussianBlur(radius=BLUR_RADIUS))
                img.paste(blurred_region, (left, top, right, bottom))

        out_buffer = io.BytesIO()
        img.save(out_buffer, format="JPEG", quality=95)
        out_buffer.seek(0)
        return out_buffer.read(), "image/jpeg"

def _normalize_image_orientation(image_bytes):
    with Image.open(io.BytesIO(image_bytes)) as img:
        img = ImageOps.exif_transpose(img)

        out_buffer = io.BytesIO()

        # Keep output predictable for Rekognition + later processing
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        elif img.mode == "L":
            img = img.convert("RGB")

        img.save(out_buffer, format="JPEG", quality=95)
        out_buffer.seek(0)
        return out_buffer.read(), "image/jpeg"

def _build_blurred_key(key):
    if "." in key:
        base, ext = key.rsplit(".", 1)
        return f"{base}_blurred.{ext}"
    return f"{key}_blurred"


def _extract_bucket_and_key(event):
    """
    Supports both:
    1. Direct invocation:
       { "bucket": "my-bucket", "key": "path/file.jpg" }

    2. S3 event notification:
       {
         "Records": [
           {
             "s3": {
               "bucket": {"name": "my-bucket"},
               "object": {"key": "path%2Ffile.jpg"}
             }
           }
         ]
       }
    """
    if "bucket" in event and "key" in event:
        return event["bucket"], event["key"]

    if "Records" in event and len(event["Records"]) > 0:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        return bucket, key

    raise KeyError("Could not find 'bucket' and 'key' in event payload")


def process(event, context):
    try:
        bucket, key = _extract_bucket_and_key(event)
    except KeyError as e:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": f"Missing field in event: {str(e)}. Provide 'bucket' and 'key', or use an S3 trigger event."
            })
        }

    try:
        if key.lower().endswith("_blurred.jpg") or key.lower().endswith("_blurred.jpeg") or key.lower().endswith("_blurred.png"):
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Skipping already processed blurred image.",
                    "bucket": bucket,
                    "key": key
                })
            }

        original_bytes, original_content_type = _download_image_from_s3(bucket, key)

        # Normalize orientation first so detection and blur use the same image layout
        normalized_bytes, normalized_content_type = _normalize_image_orientation(original_bytes)

        # Detect regions on the normalized image
        face_bboxes = _detect_faces(normalized_bytes)
        plate_bboxes = _detect_number_plates(normalized_bytes)

        all_bboxes = face_bboxes + plate_bboxes

        face_count = len(face_bboxes)
        plate_count = len(plate_bboxes)

        output_bucket = OUTPUT_BUCKET or bucket
        output_key = _build_blurred_key(key)

        if not all_bboxes:
            _upload_image_to_s3(output_bucket, output_key, normalized_bytes, normalized_content_type)
            content_version_id = _extract_content_version_id_from_key(key)
            _notify_salesforce(content_version_id, output_key)

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No faces or number plates detected. Original copied to blurred key.",
                    "input_bucket": bucket,
                    "input_key": key,
                    "output_bucket": output_bucket,
                    "output_key": output_key,
                    "faces_detected": 0,
                    "number_plates_detected": 0
                })
            }

        blurred_bytes, blurred_content_type = _blur_regions(normalized_bytes, all_bboxes)

        _upload_image_to_s3(output_bucket, output_key, blurred_bytes, blurred_content_type)

        content_version_id = _extract_content_version_id_from_key(key)
        _notify_salesforce(content_version_id, output_key)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Faces and/or number plates blurred successfully.",
                "input_bucket": bucket,
                "input_key": key,
                "output_bucket": output_bucket,
                "output_key": output_key,
                "faces_blurred": face_count,
                "number_plates_blurred": plate_count,
                "total_regions_blurred": len(all_bboxes)
            })
        }

    except Exception as e:
        print("Error in blur Lambda:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }