import os
import json
import base64
import boto3
from openai import OpenAI

S3_BUCKET = os.environ.get("ASSET_BUCKET", "metrosafetyprod")
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

def _load_openai_key():
    arn = os.environ.get("OPENAI_SECRET_ARN")
    if arn:
        sm = boto3.client("secretsmanager")
        val = sm.get_secret_value(SecretId=arn)
        if "SecretString" in val:
            return val["SecretString"]
        return base64.b64decode(val["SecretBinary"]).decode()
    return os.environ.get("OPENAI_API_KEY")

OPENAI_API_KEY = _load_openai_key()
s3 = boto3.client("s3")
oai = OpenAI(api_key=OPENAI_API_KEY)

def process(event, context):

    try:
        payload = event if isinstance(event, dict) else json.loads(event["body"])

        pdf_key = payload["pdf_s3_key"]
        question = payload["question"]

        # Load PDF bytes from S3
        obj = s3.get_object(Bucket=S3_BUCKET, Key=pdf_key)
        pdf_bytes = obj["Body"].read()

        # Encode file for OpenAI
        encoded_pdf = base64.b64encode(pdf_bytes).decode()

        # Send PDF directly to OpenAI as a binary file
        response = oai.chat.completions.create(
            model=MODEL,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_file",
                            "input_file": {
                                "file_name": "document.pdf",
                                "data": encoded_pdf,
                                "mime_type": "application/pdf"
                            }
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
        return {
            "statusCode": 500,
            "body": json.dumps({"ok": False, "error": str(e)})
        }
