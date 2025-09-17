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

# Exact Salesforce picklist (trim to what you actually have)
CANONICAL_FLOORS_SET = set(
    ["Ground Floor", "Lower Ground", "Under Ground", "External Wall", "Roof", "Mezzanine", "Grd Mezzanine"] +
    [f"Basement {n}" for n in range(1,6)] +
    [f"{n}{'st' if n==1 else 'nd' if n==2 else 'rd' if n==3 else 'th'} Floor" for n in range(1,51)] +
    [f"{n}{'st' if n==1 else 'nd' if n==2 else 'rd' if n==3 else 'th'} Mezzanine" for n in range(1,51)] +
    [f"Basement Mezzanine B{n}" for n in (1,2,3)]
)

def to_picklist_or_none(v: str | None) -> str | None:
    v = (v or "").strip()
    return v if v in CANONICAL_FLOORS_SET else None


CANONICAL_FLOORS = {
    # exact tokens -> canonical picklist value
    "ground floor": "Ground Floor",
    "gf": "Ground Floor",
    "grd": "Ground Floor",
    "g/f": "Ground Floor",

    "lower ground": "Lower Ground",
    "lg": "Lower Ground",
    "l.g.": "Lower Ground",
    "lgf": "Lower Ground",

    "mezzanine": "1st Mezzanine",
    "mezzanine floor": "1st Mezzanine",
    "roof": "Roof",
    "external wall": "External Wall",
    "underground": "Under Ground",
    "under ground": "Under Ground",

    "grd mezzanine": "Grd Mezzanine",
    "ground mezzanine": "Grd Mezzanine",
    "ground floor mezzanine": "Grd Mezzanine",
}

# build basement mappings B1..B5
for n in range(1, 6):
    CANONICAL_FLOORS[f"basement {n}"] = f"Basement {n}"
    CANONICAL_FLOORS[f"b{n}"] = f"Basement {n}"

# basement mezzanine variants
CANONICAL_FLOORS["basement mezzanine b1"] = "Basement Mezzanine B1"
CANONICAL_FLOORS["basement mezzanine b2"] = "Basement Mezzanine B2"
CANONICAL_FLOORS["basement mezzanine b3"] = "Basement Mezzanine B3"

ORDINALS = {  # for 1..50
    1: "1st Floor", 2: "2nd Floor", 3: "3rd Floor",
    **{n: f"{n}th Floor" for n in range(4, 51)}
}
MEZZ_ORDINALS = {
    1: "1st Mezzanine", 2: "2nd Mezzanine", 3: "3rd Mezzanine",
    **{n: f"{n}th Mezzanine" for n in range(4, 51)}
}

# compile regexes once
RE_LEVEL = re.compile(r"\b(?:level|lvl|lv)\s*(\d{1,2})\b", re.I)
RE_FLOOR_NUM = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)?\s*(?:floor|flr|fl)\b", re.I)
RE_MEZZ_NUM = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)?\s*(?:mezz|mezzanine)\b", re.I)
RE_BASEMENT_MEZZ = re.compile(r"\bbasement\s+mezz(?:anine)?\s*(b[1-3])\b", re.I)

# to bias toward the "Location:" line
def nearest_to_location(text, matches):
    if not matches: return None
    loc_idx = text.lower().find("location")
    if loc_idx == -1:
        return sorted(matches, key=lambda m: m.start())[0]
    return min(matches, key=lambda m: abs(m.start() - loc_idx))

def normalise(s):
    return re.sub(r"\s+", " ", s.lower()).strip()

def extract_floor(raw: str) -> str | None:
    if not raw: return None
    txt = " ".join(raw.split())  # collapse whitespace
    low = txt.lower()

    # 1) explicit basement mezz: "Basement Mezzanine B2"
    m = nearest_to_location(txt, list(RE_BASEMENT_MEZZ.finditer(txt)))
    if m:
        token = f"basement mezzanine {m.group(1).lower()}"
        return CANONICAL_FLOORS.get(token)

    # 2) simple dictionary hits (GF/LG/Mezz/Roof/External)
    for token, canon in CANONICAL_FLOORS.items():
        if re.search(rf"\b{re.escape(token)}\b", low):
            return canon

    # 3) basement B# / "Basement #"
    m = nearest_to_location(txt, list(re.finditer(r"\b(?:basement(?:\s*(\d))?|b\s*(\d))\b", txt, re.I)))
    if m:
        g1, g2 = m.group(1), m.group(2)
        if g1 or g2:
            n = int(g1 or g2)
            if 1 <= n <= 5:
                return f"Basement {n}"
    else:
        # just the word "Basement"
        return "Basement 1"

    # 4) "Level 7" => "7th Floor"
    m = nearest_to_location(txt, list(RE_LEVEL.finditer(txt)))
    if m:
        n = int(m.group(1))
        if 1 <= n <= 50:
            return ORDINALS[n]

    # 5) "3rd floor", "4 fl", "2 flr"
    m = nearest_to_location(txt, list(RE_FLOOR_NUM.finditer(txt)))
    if m:
        n = int(m.group(1))
        if 1 <= n <= 50:
            return ORDINALS[n]

    # 6) "2nd mezz", "1 mezzanine"
    m = nearest_to_location(txt, list(RE_MEZZ_NUM.finditer(txt)))
    if m:
        n = int(m.group(1))
        if 1 <= n <= 50:
            return MEZZ_ORDINALS[n]

    return None

def classify_asset_text(text):

    prompt = (
        "Please categorise the following asset description into these Salesforce fields:\n"
        "• Object_Type__c: everything up to the first ' - Location', if nothing, return null\n"
        "• Object_Category__c: the text after 'Type:'\n"
        "• Asset_Instructions__c: the text after 'Test:'\n"
        "• Label__c: the reference code in Asset_Instructions__c (e.g. 'FF1')'\n"
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
            # Deterministic first
            floor = extract_floor(txt)
            floor = to_picklist_or_none(floor)

            logger.info("Floor extracted: %s | from text: %s", floor, txt[:200])

            # Ensure we only return a valid picklist value (or null)
            out["Floor__c"] = floor  # None -> JSON null; Apex will set blank
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


