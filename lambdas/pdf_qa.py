import os
import json
import base64
import boto3
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


def process(event, context):

    try:
        payload = event if isinstance(event, dict) else json.loads(event["body"])

        bucket = payload.get("bucket", S3_BUCKET)
        pdf_key = payload["pdf_s3_key"]
        question = payload["question"]

        # Load PDF
        obj = s3.get_object(Bucket=bucket, Key=pdf_key)
        pdf_bytes = obj["Body"].read()

        # ---- STEP 1: Upload to OpenAI ----
        file_upload = oai.files.create(
            file={
                "file_name": "document.pdf",
                "data": pdf_bytes
            },
            purpose="assistants"
        )

        file_id = file_upload.id

        # ---- STEP 2: Chat completion referencing file_id ----
        response = oai.chat.completions.create(
            model=MODEL,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "file",
                            "file": { "file_id": file_id }
                        },
                        {
                            "type": "text",
                            "text": question
                        }
                    ]
                }
            ]
        )

        answer = response.choices[0].message.content

        return {
            "statusCode": 200,
            "body": json.dumps({"ok": True, "answer": answer})
        }

    except Exception as e:
        print("Error:", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"ok": False, "error": str(e)})
        }
