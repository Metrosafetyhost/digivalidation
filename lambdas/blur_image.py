import json
import boto3
import os
import io
from urllib.parse import unquote_plus
from PIL import Image, ImageFilter

# Optional region config – or let the SDK pick it up from env/role
REGION = os.getenv("AWS_REGION", "eu-west-2")

s3 = boto3.client("s3", region_name=REGION)
rekognition = boto3.client("rekognition", region_name=REGION)

# Optional: set OUTPUT_BUCKET as an environment variable
# If not set, it will write back into the same bucket it read from.
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET")


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


def _detect_faces(image_bytes):
    response = rekognition.detect_faces(
        Image={"Bytes": image_bytes},
        Attributes=["DEFAULT"],
    )
    return [f["BoundingBox"] for f in response.get("FaceDetails", [])]


def _blur_faces(image_bytes, face_bboxes):
    with Image.open(io.BytesIO(image_bytes)) as img:
        img = img.convert("RGB")

        width, height = img.size

        for bbox in face_bboxes:
            left = max(0, int(bbox["Left"] * width))
            top = max(0, int(bbox["Top"] * height))
            box_width = int(bbox["Width"] * width)
            box_height = int(bbox["Height"] * height)

            right = min(width, left + box_width)
            bottom = min(height, top + box_height)

            if right > left and bottom > top:
                face_region = img.crop((left, top, right, bottom))
                blurred_face = face_region.filter(ImageFilter.GaussianBlur(radius=25))
                img.paste(blurred_face, (left, top, right, bottom))

        out_buffer = io.BytesIO()
        img.save(out_buffer, format="JPEG")
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
        # Prevent recursion if the S3 trigger also fires for blurred files
        if key.lower().endswith("_blurred.jpg") or key.lower().endswith("_blurred.jpeg") or key.lower().endswith("_blurred.png"):
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Skipping already processed blurred image.",
                    "bucket": bucket,
                    "key": key
                })
            }

        # Download original image from S3
        original_bytes, original_content_type = _download_image_from_s3(bucket, key)

        # Detect faces with Rekognition
        face_bboxes = _detect_faces(original_bytes)
        face_count = len(face_bboxes)

        # Decide output bucket (env var or same bucket)
        output_bucket = OUTPUT_BUCKET or bucket
        output_key = _build_blurred_key(key)

        if face_count == 0:
            # No faces found - still create the _blurred file by copying original bytes
            _upload_image_to_s3(output_bucket, output_key, original_bytes, original_content_type)

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No faces detected. Original copied to blurred key.",
                    "input_bucket": bucket,
                    "input_key": key,
                    "output_bucket": output_bucket,
                    "output_key": output_key,
                    "faces_detected": 0
                })
            }

        # Blur the faces
        blurred_bytes, blurred_content_type = _blur_faces(original_bytes, face_bboxes)

        # Upload blurred image
        _upload_image_to_s3(output_bucket, output_key, blurred_bytes, blurred_content_type)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Faces blurred successfully.",
                "input_bucket": bucket,
                "input_key": key,
                "output_bucket": output_bucket,
                "output_key": output_key,
                "faces_blurred": face_count
            })
        }

    except Exception as e:
        # Basic error log for CloudWatch
        print("Error in face blur Lambda:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }