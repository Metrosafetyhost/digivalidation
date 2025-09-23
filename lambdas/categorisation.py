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
    ["Ground Floor", "Lower Ground", "Under Ground", "External Wall", "Roof", "Grd Mezzanine", "Basement", ] +
    [f"Basement {n}" for n in range(1,6)] +
    [f"B{n} Mezzanine" for n in range(1, 6)] +
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

    "mezzanine floor": "1st Mezzanine",
    "roof": "Roof",
    "external wall": "External Wall",
    "underground": "Under Ground",
    "under ground": "Under Ground",

    "grd mezzanine": "Grd Mezzanine",
    "ground mezzanine": "Grd Mezzanine",
    "ground floor mezzanine": "Grd Mezzanine",

    "basement": "Basement 1"


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
RE_B_MEZZ = re.compile(r"\bb\s*(\d{1,2})\s*mezz(?:anine)?\b", re.I)


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
    
    # 1b) "B5 Mezzanine" => "B5 Mezzanine"
    m = nearest_to_location(txt, list(RE_B_MEZZ.finditer(txt)))
    if m:
        n = int(m.group(1))
        if 1 <= n <= 50:
            return f"B{n} Mezzanine"


    # 2) simple dictionary hits (GF/LG/Mezz/Roof/External)
    for token in sorted(CANONICAL_FLOORS.keys(), key=len, reverse=True):
        if re.search(rf"\b{re.escape(token)}\b", low):
            return CANONICAL_FLOORS[token]

    # 3) basement B# / "Basement #"
    m = nearest_to_location(
        txt,
        list(re.finditer(r"\b(?:basement(?:\s*(\d))?|b\s*(\d))\b(?!\s*mezz)", txt, re.I)))
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

    prompt = f"""
    You are classifying facility safety assets for Metro Safety. The input may be well structured
    (e.g., "Emergency Light - Location: 7th Floor ... Type: ... Test: ...") OR free text with no labels.
    Your job is to extract or sensibly infer the following Salesforce fields and output ONLY a single JSON object.

    Return EXACTLY these keys:
    - "Object_Type__c"
    - "Object_Category__c"
    - "Asset_Instructions__c"
    - "Label__c"
    - "Name"

    ## How to interpret inputs

    1) OBJECT TYPE (Object_Type__c)
    - If structured, it's the text before " - Location".
    - If unstructured, infer from keywords/synonyms:
        emergency light|em light|EL|exit sign|fire extinguisher|FE|call point|MCP|manual call point|
        smoke detector|heat detector|sounder|beacon|sprinkler|riser|hose reel|fire door|alarm panel. etc
    - Use the clean, human-readable form, e.g., "Emergency Light", "Manual Call Point", "Fire Extinguisher".

    2) OBJECT CATEGORY (Object_Category__c)
    - If structured, take the text after "Type:".
    - Otherwise infer from shape/technology words:
        LED, Square, Dial Meter, Round, Button, Key, Flick Fuse, beacon, twin.
    - Normalize to Title Case, e.g., "LED Square", "Bulkhead Twinspot".
    - If none found, return null.

    3) ASSET INSTRUCTIONS (Asset_Instructions__c)
    - If structured, take the text after "Test:".
    - Otherwise, capture any instruction-like phrase with verbs such as: test, activate, isolate, reset, silence.
    - If nothing meaningful, return null.

    4) LABEL (Label__c)
    - Prefer a short code present anywhere in the text, typically one of:
        - FF\\d+, FK\\d+, EL\\d+, EM\\d+, CP\\d+, MCP\\d+, SD\\d+, HD\\d+, SB\\d+, R\\d+
        - Or more general pattern: [A-Z]{{1,3}}\\d{{1,3}}
    - Return as UPPERCASE (e.g., "FF1", "FK11"). If not found, return null.

    5) NAME (Name)
    - Build as: "<Location Guess>, <Object Type Acronym>, <Label>"
    - Location Guess:
        * If "Location:" is present, use the text after it up to the next period/full-stop if present;
            else up to the end of the location phrase.
        * Otherwise, infer a concise location phrase from the text (e.g., "Ground Floor Fire Exit Stairwell").
        * Normalize floors: "GF"/"ground"/"g" -> "Ground Floor"; "7th" -> "7th Floor"; "B1" -> "Basement 1"; etc.
    - Object Type Acronym:
        * 1 word -> first 2 letters (e.g., "Emergency" -> "EM")
        * 2 words -> first letter of each (e.g., "Emergency Light" -> "EL")
        * 3+ words -> first letter of each (e.g., "Manual Call Point" -> "MCP")
    - Label:
        * Use Label__c if present else omit that trailing part.
    - Examples:
        * Location="7th Floor, Kitchen Store Cupboard", Object_Type="Emergency Light", Label="FF1"
            -> "7th Floor, Kitchen Store Cupboard, EL, FF1"
        * If Label is null, end without it: "Ground Floor Stairwell, EL"

    IMPORTANT RULES
    - Be helpful but conservative: infer when strong cues exist; otherwise return null.
    - Use Title Case for Object_Category__c; keep Object_Type__c in normal case (e.g., "Emergency Light").
    - Uppercase Label__c.
    - Output must be STRICT JSON with ONLY the 5 fields, no extra text.

    ## Examples

    INPUT (structured)
    Emergency Light - Location: 7th Floor, Crawford and Co office space kitchen store cupboard. Type: LED Square. Test: Activate FF1

    OUTPUT
    {{
    "Object_Type__c": "Emergency Light",
    "Object_Category__c": "LED Square",
    "Asset_Instructions__c": "Activate FF1",
    "Label__c": "FF1",
    "Name": "7th Floor, Crawford and Co office space kitchen store cupboard, EL, FF1"
    }}

    INPUT (unstructured)
    Ground floor fire exit stairwell ceiling emergency light square FK11

    OUTPUT
    {{
    "Object_Type__c": "Emergency Light",
    "Object_Category__c": "LED Square",
    "Asset_Instructions__c": null,
    "Label__c": "FK11",
    "Name": "Ground Floor Fire Exit Stairwell Ceiling, EL, FK11"
    }}

    Now classify this input:

    {text}
    """
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens":        2000,
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


