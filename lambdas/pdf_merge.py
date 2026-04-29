import json
import uuid
import base64
import boto3
import pymupdf  # PyMuPDF

s3 = boto3.client("s3")

def normalise_event(event):
    """
    Supports both direct Lambda invocation and API Gateway/Lambda URL invocation.
    """

    if event is None:
        raise ValueError("No event received")

    # API Gateway / Lambda Function URL usually sends the request body here.
    if "body" in event:
        body = event["body"]

        if event.get("isBase64Encoded") is True:
            body = base64.b64decode(body).decode("utf-8")

        if isinstance(body, str):
            return json.loads(body)

        if isinstance(body, dict):
            return body

        raise ValueError("Unsupported event body format")

    # Direct Lambda test event.
    return event


def api_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }

def process(event, context):
    front_doc = None
    report_doc = None
    merged_doc = None

    try:
        print("Raw event:", json.dumps(event))

        payload = normalise_event(event)

        bucket = payload["bucket"]
        front_key = payload["front_key"]
        report_key = payload["report_key"]
        output_key = payload.get("output_key", f"output/final/{uuid.uuid4()}.pdf")

        front_local = "/tmp/front.pdf"
        report_local = "/tmp/report.pdf"
        output_local = "/tmp/merged.pdf"

        s3.download_file(bucket, front_key, front_local)
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

    except Exception as e:
        print("Error:", str(e))

        return api_response(500, {
            "message": str(e)
        })

    finally:
        if front_doc is not None:
            front_doc.close()
        if report_doc is not None:
            report_doc.close()
        if merged_doc is not None:
            merged_doc.close()