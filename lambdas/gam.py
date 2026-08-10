import base64
import json
import logging
import os
import re
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


# Initial GAM fields agreed for the first tuning release. Parked fields are
# deliberately omitted so Salesforce receives only the current scope.
OUTPUT_DEFAULTS: dict[str, Any] = {
    # Core asset details
    "Asset_Instructions__c": "",
    "Name": "",
    "Floor__c": "",

    # Identification
    "Manufacturer_AI__c": "",
    "What_Is_It__c": "",
    "SerialNumber": "",
    "Colour__c": "",
    "Rough_Dimensions__c": "",
    "Distinguishing_Features__c": "",
    "Other_Codes_Or_Numbers__c": "",

    # Condition and maintenance
    "Asset_Condition__c": "",
    "Broken_Or_Needs_Replacement__c": "",
    "Service_Provider_Or_Supplier__c": "",
    "How_To_Test__c": "",
    "How_To_Replace__c": "",
    "Parts_Needed__c": "",
    "Suggested_Test_Frequency__c": "",
    "Test_Frequency_Standards_To_Check__c": "",
    "Test_Frequency_Confidence__c": 0.0,

    # Costs
    "UK_Estimated_Price__c": "",
    "UK_Average_Price__c": "",
    "Estimated_Asset_Age__c": "",
    "Estimated_Unit_Replacement_Cost__c": "",
    "Estimated_Replacement_Parts_Price__c": "",
    "Estimated_Labour_Cost_To_Repair__c": "",
    "Estimated_Labour_Cost_To_Replace__c": "",
    "Estimated_Labour_Cost_To_Repair_On_Site__c": "",
    "Estimated_Time_To_Replace_On_Site__c": "",

    # AI categorisation
    "Object_Type_AI__c": "",
    "Object_Category_AI__c": "",
    "Confidence__c": 0.0,

    # Fire-safety classification
    "Fire_Safety_Classification__c": "Insufficient Information",
    "Fire_Safety_Classification_Confidence__c": "Low",
    "Fire_Safety_Classification_Reasoning__c": "",
    "Fire_Safety_Evidence_Observed__c": "",

    # Additional classifications. These remain suggestions until dataset-backed.
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
    "Classification_Review_Required__c": True,
    "Classification_Notes__c": "",
}


# Exact Salesforce Asset_Condition__c picklist values.
ASSET_CONDITION_VALUES = [
    "C1 - Very Good Condition",
    "C2 - Needs cleaning",
    "C2 - Minor Defects Only",
    "C3 - Maintenance required to return to an accepted level of service",
    "C4 - Requires renewal",
    "C5 - Asset Unserviceable",
]


# Verified entries from the official Uniclass Products and Systems tables.
# Rules are deliberately narrow: an unmatched asset remains unclassified rather
# than accepting a code invented from model memory.
UNICLASS_VERSION = "Products v1.42, April 2026"
UNICLASS_RULES: tuple[
    tuple[tuple[str, ...], str, str, str, str], ...
] = (
    (("smoke and heat multi-sensor", "smoke/heat multi-sensor"),
     "Pr_75_80_30_82", "Smoke and heat multi-sensor detectors", "Products", UNICLASS_VERSION),
    (("multi-sensor detector", "multisensor detector"),
     "Pr_75_80_30_55", "Multi-sensor detectors", "Products", UNICLASS_VERSION),
    (("domestic smoke alarm", "smoke alarm"),
     "Pr_75_80_30_80", "Smoke alarms", "Products", UNICLASS_VERSION),
    (("optical smoke", "smoke detector"),
     "Pr_75_80_30_65", "Point smoke detectors", "Products", UNICLASS_VERSION),
    (("heat detector",),
     "Pr_75_80_30_64", "Point heat detectors", "Products", UNICLASS_VERSION),
    (("fire alarm panel", "fire alarm control panel"),
     "Pr_75_80_30_29", "Fire alarm panels", "Products", UNICLASS_VERSION),
    (("fire alarm sounder", "alarm sounder", "sounder"),
     "Pr_75_80_30_30", "Fire alarm sounders", "Products", UNICLASS_VERSION),
    (("manual call point", "call point"),
     "Pr_75_80_30_50", "Manual call points", "Products", UNICLASS_VERSION),
    (("fire extinguisher", "extinguisher"),
     "Pr_40_50_28_64", "Portable fire extinguishers", "Products", UNICLASS_VERSION),
    (("emergency luminaire", "emergency light", "emergency lighting"),
     "Pr_70_70_48_25", "Emergency luminaires", "Products", UNICLASS_VERSION),
    (("fire blanket",),
     "Pr_40_50_28_29", "Fire blankets", "Products", UNICLASS_VERSION),
    (("wet riser landing valve",),
     "Pr_65_54_30_97", "Wet riser landing valves", "Products", UNICLASS_VERSION),
    (("dry riser landing valve",),
     "Pr_65_54_30_24", "Dry riser landing valves", "Products", UNICLASS_VERSION),
    (("wet riser inlet", "dry riser inlet", "inlet breeching"),
     "Pr_65_54_30_42", "Inlet breechings", "Products", UNICLASS_VERSION),
    (("wet riser",),
     "Ss_55_30_96_97", "Wet riser systems", "Systems", "Systems v1.42, April 2026"),
    (("dry riser",),
     "Ss_55_30_96_25", "Dry riser systems", "Systems", "Systems v1.42, April 2026"),
    (("automatic smoke vent", "automatic opening vent", "aov"),
     "Ss_65_40_80_56", "Natural smoke and heat exhaust ventilation systems",
     "Systems", "Systems v1.42, April 2026"),
    (("smoke control system", "smoke extract system"),
     "Ss_65_40_80_80", "Smoke and heat exhaust ventilation systems",
     "Systems", "Systems v1.42, April 2026"),
    (("fire and smoke damper", "fire smoke damper"),
     "Pr_65_65_24_29", "Fire and smoke dampers", "Products", UNICLASS_VERSION),
    (("smoke damper",),
     "Pr_65_65_24_80", "Smoke dampers", "Products", UNICLASS_VERSION),
)


def normalize_asset_condition(text: str) -> str:
    """Map the model's condition wording to an exact Salesforce picklist value."""
    value = (text or "").strip()
    if not value:
        return ""
    if value in ASSET_CONDITION_VALUES:
        return value

    normalised = value.lower()

    if any(term in normalised for term in (
        "unserviceable", "not working", "doesn't work", "broken",
        "inoperative", "unsafe", "failed",
    )):
        return "C5 - Asset Unserviceable"

    if any(term in normalised for term in (
        "requires renewal", "replace", "replacement", "end of life",
        "obsolete", "beyond repair", "major defect",
    )):
        return "C4 - Requires renewal"

    if any(term in normalised for term in (
        "maintenance required", "requires maintenance", "repair", "service",
        "intermittent fault", "faulty",
    )):
        return "C3 - Maintenance required to return to an accepted level of service"

    if any(term in normalised for term in (
        "dirty", "dust", "grime", "cleaning", "needs cleaning",
    )):
        return "C2 - Needs cleaning"

    if any(term in normalised for term in (
        "minor defect", "minor defects", "scuff", "scratch", "crack",
        "loose", "wear", "worn", "cosmetic", "slight", "fair condition",
    )):
        return "C2 - Minor Defects Only"

    if any(term in normalised for term in (
        "very good", "excellent", "good", "serviceable", "ok", "working",
    )):
        return "C1 - Very Good Condition"

    return ""


def _context_text(payload: dict[str, Any], result: dict[str, Any]) -> str:
    asset = payload["asset"]
    values = [
        asset.get("name"),
        asset.get("Object_Type__c"),
        asset.get("Object_Category__c"),
        result.get("Object_Type_AI__c"),
        result.get("Object_Category_AI__c"),
        result.get("What_Is_It__c"),
    ]
    return " ".join(str(value) for value in values if value).casefold()


def _apply_uniclass_mapping(
    result: dict[str, Any], payload: dict[str, Any]
) -> None:
    """Resolve known assets to current official Uniclass Product entries."""
    context = _context_text(payload, result)
    for phrases, code, title, table, version in UNICLASS_RULES:
        if not any(phrase in context for phrase in phrases):
            continue

        result["Uniclass_Code__c"] = code
        result["Uniclass_Title__c"] = title
        result["Uniclass_Table__c"] = table
        result["Uniclass_Version__c"] = version
        result["Uniclass_Confidence__c"] = 0.9
        result["Uniclass_Verification_Status__c"] = (
            "Matched to official Uniclass table; asset match requires review"
        )
        result["Classification_Review_Required__c"] = True
        note = (
            f"Uniclass candidate {code} ({title}) was selected from "
            f"{version} using the identified asset type."
        )
        existing = str(result.get("Classification_Notes__c") or "").strip()
        result["Classification_Notes__c"] = (
            f"{existing} {note}".strip() if existing else note
        )
        return

    # Do not trust an unvalidated code returned from the model's memory.
    result["Uniclass_Code__c"] = ""
    result["Uniclass_Title__c"] = ""
    result["Uniclass_Table__c"] = ""
    result["Uniclass_Version__c"] = ""
    result["Uniclass_Confidence__c"] = 0.0
    result["Uniclass_Verification_Status__c"] = "No validated match"


def _strengthen_fire_classification(
    result: dict[str, Any], payload: dict[str, Any]
) -> None:
    """Make classification consistent when the supplied asset identity is clear."""

    context = _context_text(payload, result)
    mixed_terms = (
        "fire door hold open", "fire door hold-open", "automatic fire curtain",
        "powered smoke damper", "actuated smoke damper",
    )
    afp_terms = (
        "smoke detector", "heat detector", "fire alarm", "manual call point",
        "fire extinguisher", "extinguisher", "sprinkler", "hose reel",
        "emergency lighting", "emergency light",
        "emergency door release", "firefighting lift", "fire fighting lift",
        "smoke control", "automatic smoke vent", "smoke vent",
        "automatic opening vent", "aov", "wet riser", "dry riser",
        "riser inlet", "fire hydrant",
    )
    pfp_terms = (
        "fire door", "fd30", "fd60", "fd90", "fd120", "fire stopping",
        "firestop", "cavity barrier", "fire-resisting wall",
        "fire resisting wall", "fire-resistant glazing",
        "fire resistant glazing", "structural fire protection", "door",
    )
    fsm_terms = (
        "fire action notice", "evacuation plan", "fire risk assessment",
        "fire warden", "emergency procedure", "inspection record",
        "maintenance record",
    )

    classification = None
    if any(term in context for term in mixed_terms):
        classification = "Mixed or Combined System"
    elif any(term in context for term in afp_terms):
        classification = "Active Fire Protection (AFP)"
    elif any(term in context for term in pfp_terms):
        classification = "Passive Fire Protection (PFP)"
    elif any(term in context for term in fsm_terms):
        classification = "Fire Safety Management (FSM)"

    if classification:
        result["Fire_Safety_Classification__c"] = classification
        result["Fire_Safety_Classification_Confidence__c"] = "High"
        result["Fire_Safety_Classification_Reasoning__c"] = (
            f"The supplied asset identity and image context identify this as "
            f"{classification}; certification is not required to determine the "
            "classification category."
        )
        if not str(result.get("Fire_Safety_Evidence_Observed__c") or "").strip():
            result["Fire_Safety_Evidence_Observed__c"] = (
                "Supplied asset name/type and associated photograph(s)."
            )


def _first_source_value(asset: dict[str, Any], *keys: str) -> Any:
    plan_fields = asset.get("planStudioFields")
    sources = [asset, plan_fields if isinstance(plan_fields, dict) else {}]
    for source in sources:
        folded = {str(key).casefold(): value for key, value in source.items()}
        for key in keys:
            value = folded.get(key.casefold())
            if value not in (None, ""):
                return value
    return None


def _apply_deterministic_fields(
    result: dict[str, Any], payload: dict[str, Any]
) -> None:
    """Set source-owned fields deterministically instead of asking the model."""
    asset = payload["asset"]
    source_mappings = {
        "Name": ("name",),
        "Floor__c": ("Floor__c", "floor"),
    }
    for output_key, source_keys in source_mappings.items():
        value = _first_source_value(asset, *source_keys)
        if value not in (None, ""):
            result[output_key] = str(value)

    _strengthen_fire_classification(result, payload)
    _apply_uniclass_mapping(result, payload)


SYSTEM_PROMPT = """
You are GAM, a UK building asset enrichment assistant for Metro Safety.

Use all supplied Salesforce/PlanStudio text and all supplied photographs as
evidence for one asset. Return one compact JSON object using exactly the keys
listed in OUTPUT_FIELDS.

Success criteria:
- Identify the asset as specifically as the evidence permits.
- Produce useful Salesforce-ready enrichment without presenting estimates as
  observed facts.
- Populate practical condition, maintenance, cost and verification fields when
  a reasonable asset-type-level assessment can be made.
- Prioritise correct asset identification and useful classification.

Evidence rules:
- Preserve supplied values when they are reliable; enrich rather than overwrite.
- Treat all photographs as different views of the same asset.
- Never invent manufacturer, model, serial number, certification, ratings,
  service dates, label text, codes or markings. These observed-only fields must
  be empty when they are not visible or explicitly supplied.
- Do not claim that an item works, is compliant or has passed a test based only
  on appearance.
- Clearly label estimates using wording such as "Estimated", "Typical" or
  "Approximate" and prefer a realistic range over false precision.
- For an inapplicable field return "Not applicable". Leave observed-only fields
  blank when the required evidence is absent; do not fill them with generic text.
- Use concise British-English Salesforce-ready strings.
- Confidence fields must be numbers from 0 to 1.
- Asset_Condition__c must be exactly one of these Salesforce values when visible
  evidence permits a condition assessment:
  C1 - Very Good Condition;
  C2 - Needs cleaning;
  C2 - Minor Defects Only;
  C3 - Maintenance required to return to an accepted level of service;
  C4 - Requires renewal;
  C5 - Asset Unserviceable.
  Base it only on visible condition; do not imply functional testing. If the
  photographs are insufficient, leave Asset_Condition__c empty and explain the
  limitation in Broken_Or_Needs_Replacement__c.
- Object_Type_AI__c is the broad system or asset family, for example
  "Fire Alarm System". Object_Category_AI__c is the specific asset, for example
  "Smoke Detector". Never reverse these meanings. Use the supplied
  Object_Type__c and Object_Category__c as context, but make these two AI fields
  image-assisted descriptions. Leave them blank when identification is unreliable.

Field completion rules:
- Floor__c is source-owned. Copy it only when an explicit floor value is present
  in the supplied context; never infer it from the photograph or asset name.
- Asset_Instructions__c and How_To_Test__c: provide concise, safe, non-invasive
  routine guidance suitable for the identified asset. State when a competent
  person is required; do not provide unsafe repair instructions.
- Broken_Or_Needs_Replacement__c: always state what is visibly observed and the
  limitation, for example that no obvious defect is visible but operation and
  serviceability have not been confirmed.
- How_To_Replace__c: describe the appropriate high-level replacement route and
  whether a competent person is likely required.
- Parts_Needed__c: list only normally associated parts that are defensible from
  the identified asset type. Otherwise say what information is needed.
- Service_Provider_Or_Supplier__c is observed-only. Populate it only when a
  company or service-provider name is clearly visible on a sticker, label or
  supplied text. Record the exact visible name; otherwise return an empty string.
- Manufacturer_AI__c is observed-only. Inspect every image carefully for a
  readable manufacturer name, logo or nameplate, particularly on panels and
  equipment labels. Return the exact visible brand only. If it is obscured,
  unreadable or merely inferred from appearance, return an empty string.
- Rough_Dimensions__c: provide an approximate range only when the identified
  asset type has a defensible typical size; otherwise state that scale or model
  information is required.
- Cost fields: when the asset type is reasonably identified, provide broad UK
  market ranges for unit, parts and labour costs. Prefix estimates with
  "Estimated" and state the main assumption, such as type, capacity or access.
  Use "Not applicable" where repair parts or labour genuinely do not apply.
- UK_Average_Price__c: provide the estimated midpoint/typical price as one GBP
  amount as well as the broader range in UK_Estimated_Price__c.
- Estimated_Asset_Age__c: estimate an age range only where visual design, wear,
  a visible date code or supplied context provides a defensible basis. Prefix it
  with "Estimated" and state the evidence briefly. Otherwise leave it blank.
- Estimated_Time_To_Replace_On_Site__c: provide a broad time range when a normal
  replacement scenario is reasonably foreseeable, noting access or specialist
  assumptions where relevant.
- Fire_Safety_Classification_Reasoning__c and
  Fire_Safety_Evidence_Observed__c must not be blank. Explain the classification
  using the known asset name/type/category and what the photographs show.
- Other_Codes_Or_Numbers__c: transcribe visible identifiers exactly. If a QR or
  barcode is visible and readable, return its decoded URL/text/number. Do not
  merely say "QR code present". If it cannot be decoded, state "QR code visible
  but payload unreadable" and do not invent its contents.

Additional-classification rules:
- UNSPSC values are unverified candidate suggestions because no authoritative
  UNSPSC reference dataset is connected.
- For UNSPSC, where the asset is clearly identified and a genuine candidate is
  known, return one 8-digit commodity code in UNSPSC_Code__c. Use
  UNSPSC_Description__c to show all four hierarchy levels in this form:
  "Segment > Family > Class > Commodity". Never invent a code; leave it blank
  if uncertain and explain the likely classification family in
  Classification_Notes__c.
- Do not generate Uniclass fields. The Lambda validates the identified asset
  against its connected official Uniclass mapping after the model responds.
- Keep Classification_Review_Required__c true until authoritative datasets and
  approval rules are connected.

Testing-frequency rules:
- Suggest the routine test frequency normally associated with the identified
  asset type in the UK, such as weekly, monthly, six-monthly or annually.
- Put the frequency in Suggested_Test_Frequency__c and the potentially relevant
  British Standard or UK guidance in Test_Frequency_Standards_To_Check__c.
- This is a scheduling suggestion, not a compliance conclusion. Do not claim a
  standard definitely applies unless the asset type and supplied context support
  it. If uncertain, explain what must be checked in
  Test_Frequency_Standards_To_Check__c rather than guessing.

Fire-safety classification rules:
- Fire_Safety_Classification__c must be exactly one of:
  Passive Fire Protection (PFP); Active Fire Protection (AFP);
  Fire Safety Management (FSM); Mixed or Combined System;
  Not a Fire-Safety Asset; Insufficient Information.
- Passive Fire Protection means built-in products or systems intended to resist,
  contain, limit or delay fire, heat or smoke, normally without activation.
- Active Fire Protection means equipment that detects, activates, moves,
  discharges, operates or is manually used to warn, control smoke, suppress fire
  or support firefighting.
- Fire Safety Management means administrative, procedural or organisational
  controls rather than physical fire-protection equipment.
- Mixed or Combined System requires meaningful passive and active elements.
- Fire_Safety_Classification_Confidence__c must be High, Medium or Low.
- Use the photographs and supplied asset information together. Distinguish the
  asset itself from nearby equipment and do not classify from colour, shape,
  label or apparent location alone.
- Classification is about what kind of fire-safety asset it is, not whether it
  is certified or compliant. A visible certificate or rating is not required
  to classify an asset whose purpose is clear from its supplied name/type and
  photograph. Use Insufficient Information only when the asset itself or the
  operational characteristic needed to choose a class cannot be identified.
- Strong examples: smoke detector, fire-alarm panel, manual call point, fire
  extinguisher, sprinkler, smoke vent, wet/dry riser, emergency door release and
  firefighting lift are AFP; a door supplied to GAM, fire door/FD30/FD60, fire
  stopping or cavity barrier is PFP; a fire-action notice or evacuation plan is
  FSM. For this GAM dataset, assume a supplied door asset is a fire door unless
  the supplied information explicitly identifies it as a non-fire door.
- Do not assume a sealed penetration is compliant fire stopping or a damper is
  passive/active without operational evidence.
- Do not infer fire-resistance ratings such as FD30, FD60, EI30 or EI60 unless
  visible or explicitly supplied.
- Fire_Safety_Evidence_Observed__c must record only observed or supplied facts.
- Do not assess compliance or certification in this response.
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

    result["Asset_Condition__c"] = normalize_asset_condition(
        str(result.get("Asset_Condition__c") or "")
    )

    unspsc_code = re.sub(r"\D", "", str(result.get("UNSPSC_Code__c") or ""))
    result["UNSPSC_Code__c"] = unspsc_code if len(unspsc_code) == 8 else ""

    for key in (
        "Confidence__c",
        "Test_Frequency_Confidence__c",
        "UNSPSC_Confidence__c",
        "Uniclass_Confidence__c",
    ):
        try:
            result[key] = max(0.0, min(1.0, float(result[key])))
        except (TypeError, ValueError):
            result[key] = 0.0

    # Until reference lookup exists, these values must not look approved.
    result["UNSPSC_Verification_Status__c"] = "Unverified AI suggestion"
    result["Uniclass_Verification_Status__c"] = "Unverified AI suggestion"
    result["Classification_Review_Required__c"] = True

    allowed_fire_classes = {
        "Passive Fire Protection (PFP)",
        "Active Fire Protection (AFP)",
        "Fire Safety Management (FSM)",
        "Mixed or Combined System",
        "Not a Fire-Safety Asset",
        "Insufficient Information",
    }
    if result["Fire_Safety_Classification__c"] not in allowed_fire_classes:
        result["Fire_Safety_Classification__c"] = "Insufficient Information"

    if result["Fire_Safety_Classification_Confidence__c"] not in {"High", "Medium", "Low"}:
        result["Fire_Safety_Classification_Confidence__c"] = "Low"
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
    result = _coerce_result(parsed)
    _apply_deterministic_fields(result, payload)
    return result


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
