
import base64
import json
import logging
import os
from typing import Any

import boto3
from openai import OpenAI


MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
ASSET_BUCKET = os.environ.get("ASSET_BUCKET", "metrosafetyprod")
MAX_IMAGES = int(os.environ.get("GAM_MAX_IMAGES", "4"))
PRESIGNED_URL_SECONDS = int(os.environ.get("PRESIGNED_URL_SECONDS", "900"))

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

s3 = boto3.client("s3")


def _load_openai_key() -> str:
    secret_arn = os.environ.get("OPENAI_SECRET_ARN")
    if secret_arn:
        value = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
        secret = value.get("SecretString")
        if secret is None:
            secret = base64.b64decode(value["SecretBinary"]).decode("utf-8")
        return secret

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_SECRET_ARN or OPENAI_API_KEY must be configured")
    return api_key


openai_client = OpenAI(api_key=_load_openai_key())


# Existing text Asset Capture fields, existing image Asset Capture fields, and
# proposed GAM classification fields. Defaults keep the response predictable.
OUTPUT_DEFAULTS: dict[str, Any] = {
    # Existing text Asset Capture
    "Object_Type__c": "",
    "Object_Category__c": "",
    "Asset_Instructions__c": "",
    "Label__c": "",
    "Name": "",
    "What3Words__c": "",
    "TEST_RESULT__c": "",
    "Floor__c": "",

    # Existing image Asset Capture
    "Manufacturer_AI__c": "",
    "What_Is_It__c": "",
    "SerialNumber": "",
    "Colour__c": "",
    "Rough_Dimensions__c": "",
    "Distinguishing_Features__c": "",
    "Asset_Condition__c": "",
    "Broken_Or_Needs_Replacement__c": "",
    "Service_Provider_Or_Supplier__c": "",
    "Other_Codes_Or_Numbers__c": "",
    "How_To_Test__c": "",
    "How_To_Replace__c": "",
    "Parts_Needed__c": "",
    "UK_Estimated_Price__c": "",
    "Estimated_Unit_Replacement_Cost__c": "",
    "Estimated_Replacement_Parts_Price__c": "",
    "Estimated_Labour_Cost_To_Repair__c": "",
    "Estimated_Labour_Cost_To_Replace__c": "",
    "Estimated_Labour_Cost_To_Repair_On_Site__c": "",
    "Estimated_Time_To_Replace_On_Site__c": "",
    "Object_Type_AI__c": "",
    "Object_Category_AI__c": "",
    "Confidence__c": 0.0,
    "Nearest_Store_Name__c": "",
    "Nearest_Store_Address__c": "",
    "Drive_Time__c": "",
    "Drive_Distance_km__c": "",
    "Price_Including_Drive_Time__c": "",
    "Opening_Hours__c": "",
    "Premises_Situation__c": "",
    "Location_Type__c": "",
    "Building_Classification__c": "",
    "Floor_Construction__c": "",
    "Building_Height_m__c": "",
    "Storeys_Above_Ground__c": "",
    "Storeys_Below_Ground__c": "",
    "Approx_Dimensions__c": "",
    "Roof_Details__c": "",
    "Vehicle_Parking__c": "",
    "General_Occupancy_Types__c": "",
    "Fire_History_Summary__c": "",
    "Obsequio_cross_sell_long__c": "",

    # New GAM classifications. These remain suggestions until dataset-backed.
    "UNSPSC_Code__c": "",
    "UNSPSC_Description__c": "",
    "UNSPSC_Codeset_Version__c": "",
    "UNSPSC_Confidence__c": 0.0,
    "UNSPSC_Verification_Status__c": "Unverified AI suggestion",
    "Uniclass_Code__c": "",
    "Uniclass_Title__c": "",
    "Uniclass_Table__c": "",
    "Uniclass_Version__c": "",
    "Uniclass_Confidence__c": 0.0,
    "Uniclass_Verification_Status__c": "Unverified AI suggestion",
    "SFG20_Schedule_Code__c": "",
    "SFG20_Schedule_Title__c": "",
    "SFG20_Schedule_Version__c": "",
    "SFG20_Confidence__c": 0.0,
    "SFG20_Criticality_Colours__c": "",
    "SFG20_Verification_Status__c": "Official data access required",
    "Classification_Review_Required__c": True,
    "Classification_Notes__c": "",
}


SYSTEM_PROMPT = """
You are GAM, a UK building asset enrichment assistant for Metro Safety.

Use all supplied Salesforce/PlanStudio text and all supplied photographs as
evidence for one asset. Return one compact JSON object using exactly the keys
listed in OUTPUT_FIELDS.

Rules:
- Preserve supplied values when they are reliable; enrich rather than overwrite.
- Treat all photographs as different views of the same asset.
- Never invent visible identifiers such as manufacturer, model or serial number.
- Clearly distinguish observed facts from estimates.
- Use concise Salesforce-ready strings. Use an empty string if there is no
  defensible value.
- Confidence fields must be numbers from 0 to 1.
- Asset_Condition__c should use one of the existing C1-C5 descriptions where
  evidence permits.
- Object_Type__c/Object_Category__c are the text-capture result;
  Object_Type_AI__c/Object_Category_AI__c are the image-assisted result.
- UNSPSC, Uniclass and SFG20 values are candidate suggestions only in this
  version because no authoritative reference dataset is supplied.
- Do not fabricate an exact classification code. If you cannot confidently
  recall a genuine code, leave the code blank and explain the likely class in
  Classification_Notes__c.
- SFG20 criticality colours belong to maintenance tasks, not the physical asset.
  Do not assert colours without official schedule/task evidence.
- Keep Classification_Review_Required__c true until authoritative datasets and
  approval rules are connected.
- Return JSON only, with no Markdown.
""".strip()


def _response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _parse_event(event: Any) -> dict[str, Any]:
    payload = event
    if isinstance(event, dict) and "body" in event:
        body = event.get("body") or "{}"
        if event.get("isBase64Encoded") and isinstance(body, str):
            body = base64.b64decode(body).decode("utf-8")
        payload = json.loads(body) if isinstance(body, str) else body
    elif isinstance(event, str):
        payload = json.loads(event)

    if not isinstance(payload, dict):
        raise ValueError("Payload must be a JSON object")
    asset = payload.get("asset")
    if not isinstance(asset, dict):
        raise ValueError("Payload must contain an 'asset' JSON object")
    if not asset.get("id"):
        raise ValueError("asset.id is required")
    return payload


def _presign_s3_key(s3_key: str) -> str:
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": ASSET_BUCKET, "Key": s3_key},
        ExpiresIn=PRESIGNED_URL_SECONDS,
    )


def _is_blurred_derivative(s3_key: str) -> bool:
    """Return True when the S3 object's filename ends in `_blurred[.ext]`."""
    filename = s3_key.rsplit("/", 1)[-1].lower()
    stem = filename.rsplit(".", 1)[0]
    return stem.endswith("_blurred")


def _find_s3_key(content_version_id: str) -> str:
    """Find the newest non-blurred object for a ContentVersion ID."""
    latest_key = None
    latest_modified = None

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=ASSET_BUCKET, Prefix=content_version_id):
        for item in page.get("Contents", []):
            key = item.get("Key")
            if not key or _is_blurred_derivative(key):
                continue
            modified = item.get("LastModified")
            if latest_modified is None or (modified and modified > latest_modified):
                latest_key = key
                latest_modified = modified

    if not latest_key:
        raise ValueError(
            f"No S3 image found in {ASSET_BUCKET} for ContentVersionId "
            f"'{content_version_id}'"
        )
    return latest_key


def _image_url(image: dict[str, Any]) -> str:
    content_version_id = str(image.get("contentVersionId") or "").strip()
    if not content_version_id:
        raise ValueError("Every asset image must contain contentVersionId")

    s3_key = _find_s3_key(content_version_id)
    logger.info(
        "Resolved ContentVersionId %s to S3 key %s",
        content_version_id,
        s3_key,
    )
    return _presign_s3_key(s3_key)


def _build_user_content(payload: dict[str, Any]) -> list[dict[str, Any]]:
    asset = payload["asset"]
    images = asset.get("images") or []
    if not isinstance(images, list):
        raise ValueError("asset.images must be a JSON array")

    # Do not send URLs/large binary values as text. Image references are added
    # separately as image_url content blocks.
    asset_context = {key: value for key, value in asset.items() if key != "images"}
    content: list[dict[str, Any]] = [
        {
            "type": "text",
            "text": "OUTPUT_FIELDS: " + json.dumps(OUTPUT_DEFAULTS),
        },
        {
            "type": "text",
            "text": "SALESFORCE_ASSET_CONTEXT: " + json.dumps(asset_context, default=str),
        },
    ]

    image_count = 0
    for image in images[:MAX_IMAGES]:
        if not isinstance(image, dict):
            raise ValueError("Every item in asset.images must be a JSON object")
        url = _image_url(image)
        if url:
            content.append({"type": "image_url", "image_url": {"url": url}})
            image_count += 1

    content.append(
        {
            "type": "text",
            "text": f"Analyse this one asset using the supplied context and {image_count} image(s).",
        }
    )
    return content


def _coerce_result(raw: dict[str, Any]) -> dict[str, Any]:
    result = dict(OUTPUT_DEFAULTS)
    for key in OUTPUT_DEFAULTS:
        if key in raw and raw[key] is not None:
            result[key] = raw[key]

    for key in (
        "Confidence__c",
        "UNSPSC_Confidence__c",
        "Uniclass_Confidence__c",
        "SFG20_Confidence__c",
    ):
        try:
            result[key] = max(0.0, min(1.0, float(result[key])))
        except (TypeError, ValueError):
            result[key] = 0.0

    # Until reference lookup exists, these values must not look approved.
    result["UNSPSC_Verification_Status__c"] = "Unverified AI suggestion"
    result["Uniclass_Verification_Status__c"] = "Unverified AI suggestion"
    result["SFG20_Verification_Status__c"] = "Official data access required"
    result["Classification_Review_Required__c"] = True
    return result


def _analyse(payload: dict[str, Any]) -> dict[str, Any]:
    response = openai_client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_content(payload)},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )
    text = response.choices[0].message.content or "{}"
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("OpenAI response was not a JSON object")
    return _coerce_result(parsed)


def process(event, context):
    """AWS Lambda handler used by POST /gam."""
    try:
        payload = _parse_event(event)
        asset = payload["asset"]
        logger.info(
            "GAM request asset_id=%s images=%s",
            asset.get("id"),
            len(asset.get("images") or []),
        )

        fields = _analyse(payload)
        return _response(
            200,
            {
                "status": "ok",
                "assetId": asset["id"],
                "planStudioId": asset.get("planStudioId"),
                "fields": fields,
            },
        )
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning("Bad GAM request: %s", exc)
        return _response(400, {"status": "error", "message": str(exc)})
    except Exception as exc:
        logger.exception("GAM processing failed")
        return _response(500, {"status": "error", "message": str(exc)})