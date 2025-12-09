import json
import boto3
import os
import io
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
    return response["Body"].read()


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
            left = int(bbox["Left"] * width)
            top = int(bbox["Top"] * height)
            box_width = int(bbox["Width"] * width)
            box_height = int(bbox["Height"] * height)

            right = left + box_width
            bottom = top + box_height

            # Crop and blur the face region
            face_region = img.crop((left, top, right, bottom))
            blurred_face = face_region.filter(ImageFilter.GaussianBlur(radius=25))

            # Paste blurred region back
            img.paste(blurred_face, (left, top, right, bottom))

        out_buffer = io.BytesIO()
        img.save(out_buffer, format="JPEG")
        out_buffer.seek(0)
        return out_buffer.read()


def process(event, context):
    """
    Expected test event JSON (same style as your Nova function):

    {
      "bucket": "my-images-bucket",
      "key": "images/sample-face.jpg"
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

    try:
        # Download original image from S3
        original_bytes = _download_image_from_s3(bucket, key)

        # Detect faces with Rekognition
        face_bboxes = _detect_faces(original_bytes)
        face_count = len(face_bboxes)

        if face_count == 0:
            # No faces found – nothing blurred
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No faces detected, nothing blurred.",
                    "bucket": bucket,
                    "key": key,
                    "faces_detected": 0
                })
            }

        # Blur the faces
        blurred_bytes = _blur_faces(original_bytes, face_bboxes)

        # Decide output bucket (env var or same bucket)
        output_bucket = OUTPUT_BUCKET or bucket

        # Name output key as "<original>_blurred.ext"
        if "." in key:
            base, ext = key.rsplit(".", 1)
            output_key = f"{base}_blurred.{ext}"
        else:
            output_key = f"{key}_blurred"

        # Upload blurred image
        _upload_image_to_s3(output_bucket, output_key, blurred_bytes)

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
