import json
import uuid
import base64
import os
from datetime import datetime, timedelta, timezone
import boto3
import pymupdf  # PyMuPDF

s3 = boto3.client("s3")

PRESIGN_EXPIRES_SECONDS = 86400

def normalise_event(event):
    """
    Supports both direct Lambda invocation and API Gateway/Lambda URL invocation.
    """

    if event is None:
        raise ValueError("No event received")

    if "body" in event:
        body = event["body"]

        if event.get("isBase64Encoded") is True:
            body = base64.b64decode(body).decode("utf-8")

        if isinstance(body, str):
            return json.loads(body)

        if isinstance(body, dict):
            return body

        raise ValueError("Unsupported event body format")

    return event


def api_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }


def merge_pdfs(payload):
    front_doc = None
    report_doc = None
    merged_doc = None

    try:
        bucket = payload["bucket"]
        front_key = payload["front_key"]
        report_key = payload["report_key"]
        output_key = payload.get("output_key", f"output/final/{uuid.uuid4()}.pdf")

        front_local = "/tmp/front.pdf"
        report_local = "/tmp/report.pdf"
        output_local = "/tmp/merged.pdf"

        print(f"Downloading front PDF from s3://{bucket}/{front_key}")
        s3.download_file(bucket, front_key, front_local)

        print(f"Downloading report PDF from s3://{bucket}/{report_key}")
        s3.download_file(bucket, report_key, report_local)

        front_doc = pymupdf.open(front_local)
        report_doc = pymupdf.open(report_local)
        merged_doc = pymupdf.open()

        if len(front_doc) == 0:
            raise ValueError("Front PDF has no pages")

        if len(report_doc) == 0:
            raise ValueError("Report PDF has no pages")

        merged_doc.insert_pdf(front_doc)
        merged_doc.insert_pdf(report_doc)
        merged_doc.save(output_local)

        print(f"Uploading merged PDF to s3://{bucket}/{output_key}")
        s3.upload_file(
            output_local,
            bucket,
            output_key,
            ExtraArgs={"ContentType": "application/pdf"}
        )

        response_body = {
            "message": "PDFs merged successfully",
            "bucket": bucket,
            "output_key": output_key,
            "front_key": front_key,
            "report_key": report_key,
            "front_pages": len(front_doc),
            "report_pages": len(report_doc),
            "total_pages": len(front_doc) + len(report_doc),
        }

        return api_response(200, response_body)

    finally:
        if front_doc is not None:
            front_doc.close()
        if report_doc is not None:
            report_doc.close()
        if merged_doc is not None:
            merged_doc.close()

def generate_presigned_report_url(payload):
    bucket = payload["bucket"]
    key = payload["key"]

    expires_in = PRESIGN_EXPIRES_SECONDS

    download_file_name = payload.get("download_file_name") or key.split("/")[-1]
    download_file_name = download_file_name.replace('"', "")

    print(f"Checking object exists: s3://{bucket}/{key}")
    s3.head_object(Bucket=bucket, Key=key)

    print(f"Generating presigned URL for s3://{bucket}/{key} with expiry {expires_in} seconds")

    presigned_url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": bucket,
            "Key": key,
            "ResponseContentType": "application/pdf",
            "ResponseContentDisposition": f'attachment; filename="{download_file_name}"'
        },
        ExpiresIn=expires_in,
        HttpMethod="GET"
    )

    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    response_body = {
        "message": "Presigned URL generated successfully",
        "bucket": bucket,
        "key": key,
        "presigned_url": presigned_url,
        "expires_in": expires_in,
        "expires_at": expires_at.isoformat()
    }

    return api_response(200, response_body)

def process(event, context):
    try:
        print("Raw event:", json.dumps(event))

        payload = normalise_event(event)

        action = payload.get("action")

        if action == "generate_presigned_url":
            return generate_presigned_report_url(payload)

        return merge_pdfs(payload)

    except Exception as e:
        print("Error:", str(e))

        return api_response(500, {
            "message": str(e)
        })