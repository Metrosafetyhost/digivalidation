import os
import uuid
import boto3
import pymupdf  # PyMuPDF

s3 = boto3.client("s3")


def process(event, context):
    """
    Expected event:
    {
        "bucket": "your-bucket-name",
        "front_key": "input/previews/preview.pdf",
        "report_key": "input/reports/completed-report.pdf",
        "output_key": "output/final/customer-pack.pdf"
    }
    """

    bucket = event["bucket"]
    front_key = event["front_key"]
    report_key = event["report_key"]
    output_key = event.get("output_key", f"output/final/{uuid.uuid4()}.pdf")

    front_local = "/tmp/front.pdf"
    report_local = "/tmp/report.pdf"
    output_local = "/tmp/merged.pdf"

    front_doc = None
    report_doc = None
    merged_doc = None

    try:
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

        return {
            "statusCode": 200,
            "message": "PDFs merged successfully",
            "output_key": output_key,
            "front_pages": len(front_doc),
            "report_pages": len(report_doc),
            "total_pages": len(front_doc) + len(report_doc),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "message": str(e)
        }

    finally:
        if front_doc is not None:
            front_doc.close()
        if report_doc is not None:
            report_doc.close()
        if merged_doc is not None:
            merged_doc.close()