import json
import uuid
import base64
import os
import posixpath
import re
from difflib import SequenceMatcher
from datetime import datetime, timedelta, timezone

import boto3
import pymupdf  # PyMuPDF
from botocore.exceptions import ClientError


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


def s3_object_exists(bucket, key):
    """
    Checks whether an exact S3 object exists.
    """

    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")

        if code in ("404", "NoSuchKey", "NotFound"):
            return False

        raise


def normalise_name(value):
    """
    Normalises a file name for safer fuzzy comparison.
    """

    name = posixpath.basename(value).lower()
    name = re.sub(r"\.pdf$", "", name)
    name = re.sub(r"[^a-z0-9]+", " ", name)

    return " ".join(name.split())


def useful_tokens(value):
    """
    Extracts useful matching tokens from a key/file name.

    Common generic terms are ignored so that matching focuses more on
    things like job number, date, type of work, and report number.
    """

    ignore = {
        "pdf",
        "preview",
        "visit",
        "report",
        "job",
        "sheet",
        "wo"
    }

    tokens = re.findall(r"[a-z0-9]+", normalise_name(value))

    return {
        token
        for token in tokens
        if len(token) > 1 and token not in ignore
    }


def match_score(requested_key, candidate_key):
    """
    Scores how similar a requested S3 key is to a candidate S3 key.

    Uses both token overlap and general string similarity.
    """

    requested_tokens = useful_tokens(requested_key)
    candidate_tokens = useful_tokens(candidate_key)

    token_score = 0

    if requested_tokens:
        token_score = len(requested_tokens & candidate_tokens) / len(requested_tokens)

    name_score = SequenceMatcher(
        None,
        normalise_name(requested_key),
        normalise_name(candidate_key)
    ).ratio()

    return max(token_score, name_score)


def list_s3_objects(bucket, prefix):
    """
    Lists S3 objects under a prefix using pagination.
    """

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj


def resolve_s3_key(bucket, requested_key, label, required_terms=None, min_score=0.70):
    """
    Resolves an S3 key safely.

    Order of checks:
    1. Exact key.
    2. Simple .pdf / no .pdf variant.
    3. Fuzzy match inside the same Work Order folder only.

    This avoids fuzzy searching the whole bucket and avoids continuing when
    the best match is ambiguous.
    """

    required_terms = [term.lower() for term in (required_terms or [])]

    # 1. Exact match first.
    if s3_object_exists(bucket, requested_key):
        print(f"{label}: using exact key: {requested_key}")
        return requested_key

    # 2. Simple extension / no-extension fallback.
    variants = []

    if requested_key.lower().endswith(".pdf"):
        variants.append(requested_key[:-4])
    else:
        variants.append(requested_key + ".pdf")

    for variant in variants:
        if s3_object_exists(bucket, variant):
            print(f"{label}: exact key missing, using variant: {variant}")
            return variant

    # 3. Fuzzy fallback inside the same Work Order folder only.
    prefix = posixpath.dirname(requested_key) + "/"

    print(
        f"{label}: exact key missing. Searching for close matches under "
        f"s3://{bucket}/{prefix}"
    )

    candidates = []

    for obj in list_s3_objects(bucket, prefix):
        key = obj["Key"]
        name = posixpath.basename(key).lower()

        if key == requested_key:
            continue

        # Keep the front/report searches separate.
        # Example:
        # front PDF requires "preview"
        # report PDF requires "job" and "sheet"
        if required_terms and not all(term in name for term in required_terms):
            continue

        score = match_score(requested_key, key)

        if score >= min_score:
            candidates.append({
                "key": key,
                "score": score,
                "last_modified": obj.get("LastModified"),
                "size": obj.get("Size", 0)
            })

    if not candidates:
        raise FileNotFoundError(
            f"{label} not found. Exact key was missing and no close match was found "
            f"under s3://{bucket}/{prefix}. Requested key: {requested_key}"
        )

    candidates.sort(
        key=lambda item: (
            item["score"],
            item["last_modified"] or datetime.min.replace(tzinfo=timezone.utc),
            item["size"]
        ),
        reverse=True
    )

    best = candidates[0]

    # Avoid guessing if two files are almost equally likely.
    if len(candidates) > 1:
        second = candidates[1]

        if best["score"] - second["score"] < 0.05:
            raise FileNotFoundError(
                f"{label} match is ambiguous. Requested key: {requested_key}. "
                f"Best matches were: {best['key']} and {second['key']}. "
                f"Please pass the exact S3 key from Salesforce."
            )

    print(
        f"{label}: exact key missing, using closest match: "
        f"{best['key']} with score {best['score']:.2f}"
    )

    return best["key"]


def merge_pdfs(payload):
    front_doc = None
    report_doc = None
    merged_doc = None

    try:
        bucket = payload["bucket"]

        requested_front_key = payload["front_key"]
        requested_report_key = payload["report_key"]

        output_key = payload.get("output_key", f"output/final/{uuid.uuid4()}.pdf")

        front_key = resolve_s3_key(
            bucket=bucket,
            requested_key=requested_front_key,
            label="front PDF",
            required_terms=["preview"],
            min_score=0.70
        )

        report_key = resolve_s3_key(
            bucket=bucket,
            requested_key=requested_report_key,
            label="report PDF",
            required_terms=["job", "sheet"],
            min_score=0.70
        )

        front_local = "/tmp/front.pdf"
        report_local = "/tmp/report.pdf"
        output_local = "/tmp/merged.pdf"

        try:
            print(f"Downloading front PDF from s3://{bucket}/{front_key}")
            s3.download_file(bucket, front_key, front_local)

        except Exception as e:
            raise Exception(
                f"Failed to download front PDF s3://{bucket}/{front_key}: {e}"
            )

        try:
            print(f"Downloading report PDF from s3://{bucket}/{report_key}")
            s3.download_file(bucket, report_key, report_local)

        except Exception as e:
            raise Exception(
                f"Failed to download report PDF s3://{bucket}/{report_key}: {e}"
            )

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

            # The resolved keys actually used by Lambda.
            "front_key": front_key,
            "report_key": report_key,

            # The originally requested keys from Salesforce.
            "requested_front_key": requested_front_key,
            "requested_report_key": requested_report_key,

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

    print(
        f"Generating presigned URL for s3://{bucket}/{key} "
        f"with expiry {expires_in} seconds"
    )

    presigned_url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": bucket,
            "Key": key,
            "ResponseContentType": "application/pdf",
            "ResponseContentDisposition": (
                f'attachment; filename="{download_file_name}"'
            )
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