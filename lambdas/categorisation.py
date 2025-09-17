import json
import boto3
import logging
from botocore.client import Config
import re

# initialise logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3      = boto3.client(
    "s3",
    region_name="eu-west-2",
    config=Config(signature_version="s3v4")
)
MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"

def _ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"

def detect_floor(text: str) -> str | None:
    """
    Parse a free-text location description and return a canonical Floor__c label.
    Examples returned: 'Ground Floor', 'Grd Mezzanine', 'Basement 2', 'B3 Mezzanine',
                       '9th Floor', '15th Mezzanine', 'Lower Ground', 'Under Ground',
                       'External Wall', 'Roof'.
    """
    if not text:
        return None
    s = text.lower()

    # Fixed phrases first (they should win over anything else)
    if re.search(r'\broof\b', s):
        return "Roof"
    if re.search(r'\bexternal\s+wall\b', s):
        return "External Wall"
    if re.search(r'\blower\s+ground\b|\blg\b(?![a-z])', s):
        return "Lower Ground"
    if re.search(r'\bunder\s*ground\b', s):
        return "Under Ground"

    # Basement mezzanine:  "Basement Mezzanine B1|B2|..."
    m = re.search(r'\bb(?:asement)?\s*mezzanine\s*b?(\d+)\b', s)
    if m:
        return f"Basement Mezzanine B{int(m.group(1))}"

    # "B2 Mezzanine", "B5 Mezzanine"
    m = re.search(r'\bb(\d{1,2})\s*mezz(?:anine)?\b', s)
    if m:
        return f"B{int(m.group(1))} Mezzanine"

    # "Basement 5", "Basement 2"
    m = re.search(r'\b(?:basement|b)\s*([1-9]\d?)\b', s)
    if m:
        return f"Basement {int(m.group(1))}"

    # Bare "B5" => treat as "Basement 5"
    m = re.search(r'\bb(\d{1,2})\b', s)
    if m:
        return f"Basement {int(m.group(1))}"

    # Generic "Basement" with no number => choose Basement 1
    if re.search(r'\bbasement\b', s):
        return "Basement 1"

    # Ground Floor / Ground Mezzanine
    if re.search(r'\bgr(?:ou)?nd\s+floor\b|\bgf\b(?![a-z])', s):
        return "Ground Floor"
    if re.search(r'\bgrd\s*mezz(?:anine)?\b|\bground\s*mezz(?:anine)?\b', s):
        return "Grd Mezzanine"

    # "5th Mezzanine" / "12 Mezzanine"
    m = re.search(r'\b(\d{1,2})(?:st|nd|rd|th)?\s*mezz(?:anine)?\b', s)
    if m:
        n = int(m.group(1))
        return f"{_ordinal(n)} Mezzanine"

    # "9th Floor" / "9 Floor" / "9th" (when clearly in floor context)
    m = re.search(r'\b(\d{1,2})(?:st|nd|rd|th)?\s*(?:floor|flr)\b', s)
    if m:
        n = int(m.group(1))
        return f"{_ordinal(n)} Floor"

    # Sometimes it’s written as the ordinal without the word “floor”, but nearby context implies it.
    m = re.search(r'\b(\d{1,2})(st|nd|rd|th)\b', s)
    if m and "floor" in s:
        n = int(m.group(1))
        return f"{_ordinal(n)} Floor"

    return None

def classify_asset_text(text):

    prompt = (
        "Please categorise the following asset description into these Salesforce fields:\n"
        "• Object_Type__c: everything up to the first ' - Location'\n"
        "• Object_Category__c: the text after 'Type:'\n"
        "• Asset_Instructions__c: the text after 'Test:'\n"
        "• Label__c: the reference code in Asset_Instructions__c (e.g. 'FF1')\n"
        "• Floor__c: Look at the text right after the word 'Location:'. If the first phrase after 'Location:' mentions a floor (e.g., “Basement 2”, “Ground Floor”, “3rd Floor”, “15th Mezzanine”, “Roof”, “External Wall”). "
        " return its canonical name exactly as it appears up until the first comma. Capitalise the first letter of each word. If no floor is mentioned, return null"
        "• Name: combine:\n"
        "    1) the Location text (after 'Location:' up to the full stop),\n"
        "    2) the object identifier (uppercase acronym of Object_Type__c, e.g. 'Emergency Light' → 'EML') Note this always has to be three letters (if o words, first two letters of first word, and first letter of Second. If three words, first letter of each word),\n"
        "    3) the Label__c\n"
        "  separated by commas.\n\n"
        f"Input: {text}\n\n"
        "Output as a single JSON object, using these exact keys:\n"
        "{\n"
        '  "Object_Type__c": "…",\n'
        '  "Object_Category__c": "…",\n'
        '  "Asset_Instructions__c": "…",\n'
        '  "Label__c": "…",\n'
        '  "Floor__c": "…"\n' 
        '  "Name": "…"\n' 
        "}\n\n"
    )
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens":        1000,
        "temperature":       0.0,
        "messages": [
            { "role": "user", "content": prompt }
        ]
    }

    resp = bedrock.invoke_model(
        modelId     = MODEL_ID,
        body        = json.dumps(payload),
        contentType = "application/json",
        accept      = "application/json"
    )
    raw = resp["body"].read().decode("utf-8")
    logger.info("<< classify_asset_text: raw Bedrock response: %s", raw)

    # parse the JSON blob out of Bedrock’s response
    try:
        data = json.loads(raw)
        # Claude returns its assistant text in data["content"]
        text_out = "".join(part.get("text", "") for part in data.get("content", []))
        return json.loads(text_out)
    except Exception as e:
        logger.error("Failed to parse classification response: %s", e)
        raise

def process(event, context):
    logger.info("<< process: received event: %s", json.dumps(event))

    # 1) Parse HTTP body (JSON array)
    try:
        body = json.loads(event.get("body", "[]"))
    except Exception as e:
        logger.error("process: could not decode event['body']: %s", e, exc_info=True)
        raise

    logger.info(">> process: HTTP body parsed as: %s", body)

    # 2) Extract inputs for Claude, but also pick up description/contentVersionId for later use
    samples = []
    metadata = []   # <-- NEW: capture, but don't use yet
    for obj in body:
        base = (obj.get("input") or "")
        desc = obj.get("description")
        cvid = obj.get("contentVersionId")
        samples.append(base)
        metadata.append({"description": desc, "contentVersionId": cvid})

    logger.info(">> process: assembled samples for model: %s", samples)
    logger.info(">> process: collected metadata (unused for now): %s", metadata)

    # 3) Classify each sample (Claude sees ONLY the input text)
    results = []
    for txt in samples:
        try:
            out = classify_asset_text(txt)
            results.append(out)   # unchanged contract
        except Exception as ex:
            logger.warning("process: classification error for input '%s': %s", txt, ex, exc_info=True)
            results.append({"error": str(ex), "input": txt})

    logger.info("<< process: returning results: %s", results)

    # 4) Return bare JSON array (same as before, no new fields returned)
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(results)
    }


