"""
Luke Gasson - Metro Safety Asset Photo Categorisation Lambda

Purpose:
    Classifies uploaded site asset photos and returns Salesforce-ready asset,
    condition, pricing, supplier and building-context fields.

Notes:
    - Works with S3 object prefixes from Salesforce ContentVersion records.
    - Uses an OpenAI vision model for asset identification and field extraction.
    - Keeps the response order aligned with the incoming request order.

Author: Luke Gasson
"""

import os
import json
import base64
import logging
import time
import traceback
import boto3
from botocore.exceptions import ClientError
from openai import OpenAI

# ---------------------------
# Runtime configuration
# ---------------------------
S3_BUCKET = os.environ.get("ASSET_BUCKET", "metrosafetyprod")
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

SENSITIVE_HEADER_NAMES = {
    "authorization",
    "cookie",
    "x-api-key",
    "x-amz-security-token",
}

def log_event(level: str, message: str, **fields):
    """
    Emit one structured JSON log line for CloudWatch Logs Insights.
    Do not log API keys, presigned URLs, or full image payloads.
    """
    payload = {"message": message, **fields}
    logger.log(getattr(logging, level.upper(), logging.INFO), json.dumps(payload, default=str))

def safe_headers(headers: dict | None) -> dict:
    safe = {}
    for k, v in (headers or {}).items():
        if str(k).lower() in SENSITIVE_HEADER_NAMES:
            safe[k] = "***REDACTED***"
        else:
            safe[k] = v
    return safe

def safe_event_summary(event: dict) -> dict:
    """
    Keep the useful request information, but avoid dumping huge/noisy API Gateway payloads.
    """
    if not isinstance(event, dict):
        return {"event_type": type(event).__name__}

    body = event.get("body")
    body_summary = {"present": body is not None}
    if body is not None:
        body_summary["length"] = len(body) if isinstance(body, str) else None
        try:
            decoded_body = body
            if event.get("isBase64Encoded") and isinstance(body, str):
                decoded_body = base64.b64decode(body).decode("utf-8")
            parsed_body = json.loads(decoded_body) if isinstance(decoded_body, str) else decoded_body
            if isinstance(parsed_body, list):
                body_summary["item_count"] = len(parsed_body)
                body_summary["content_version_ids"] = [
                    (item or {}).get("ContentVersionId") for item in parsed_body if isinstance(item, dict)
                ]
                body_summary["has_building_address"] = [
                    bool((item or {}).get("BuildingAddress")) for item in parsed_body if isinstance(item, dict)
                ]
            else:
                body_summary["parsed_type"] = type(parsed_body).__name__
        except Exception as e:
            body_summary["parse_error"] = str(e)

    return {
        "resource": event.get("resource"),
        "path": event.get("path"),
        "httpMethod": event.get("httpMethod"),
        "requestId": (event.get("requestContext") or {}).get("requestId"),
        "sourceIp": ((event.get("requestContext") or {}).get("identity") or {}).get("sourceIp"),
        "userAgent": ((event.get("requestContext") or {}).get("identity") or {}).get("userAgent"),
        "headers": safe_headers(event.get("headers")),
        "body": body_summary,
        "isBase64Encoded": event.get("isBase64Encoded"),
    }

def _load_openai_key():
    arn = os.environ.get("OPENAI_SECRET_ARN")
    if arn:
        sm = boto3.client("secretsmanager")
        val = sm.get_secret_value(SecretId=arn)
        s = val.get("SecretString")
        return s if s is not None else base64.b64decode(val["SecretBinary"]).decode()
    return os.environ.get("OPENAI_API_KEY")  # local/dev fallback

OPENAI_API_KEY = _load_openai_key()

# ---------------------------
# Model prompt and output contract
# ---------------------------
SYSTEM_PROMPT = (
    "You are a safety asset classifier for UK sites. "
    "Given a single site asset photo, output ALL of the following fields as a JSON with the EXACT keys: "
    "Manufacturer_AI__c, What_Is_It__c, SerialNumber, Colour__c, Rough_Dimensions__c, "
    "Distinguishing_Features__c, Asset_Condition__c, Broken_Or_Needs_Replacement__c, "
    "Service_Provider_Or_Supplier__c, Other_Codes_Or_Numbers__c, How_To_Test__c, "
    "How_To_Replace__c, Parts_Needed__c, UK_Estimated_Price__c, "
    "Estimated_Unit_Replacement_Cost__c, Estimated_Replacement_Parts_Price__c,"
    "Estimated_Labour_Cost_To_Repair__c, Estimated_Labour_Cost_To_Replace__c, "
    "Estimated_Labour_Cost_To_Repair_On_Site__c, Estimated_Time_To_Replace_On_Site__c, "
    "Object_Type_AI__c, Object_Category_AI__c, Confidence__c, "
    "Nearest_Store_Name__c, Nearest_Store_Address__c, "
    "Drive_Time__c, Price_Including_Drive_Time__c, Opening_Hours__c, "
    "Premises_Situation__c, Location_Type__c, Building_Classification__c, "
    "Floor_Construction__c, Building_Height_m__c, Storeys_Above_Ground__c, "
    "Storeys_Below_Ground__c, Approx_Dimensions__c, Roof_Details__c, "
    "Vehicle_Parking__c, General_Occupancy_Types__c, Fire_History_Summary__c. "
    "Drive_Distance_km__c, "
    "Obsequio_cross_sell_long__c"
    "Always provide a best-guess for every field, even if uncertain. If there is none however, respond with N/A. "
    "For Colour__c, return only a SINGLE most dominant or most likely colour (not multiple). "
    "Base your assumptions on typical UK standards and suppliers if the photo does not show enough detail. "
    "Return realistic rough values (e.g., '120mm diameter', '£20-£40', 'Screwdriver needed'). "
    "Never leave a field blank. Confidence__c must be a number 0..1. "

    "For Object_Type_AI__c, you MUST pick exactly one key from OBJECT_MAP (or 'N/A' if nothing fits). "
    "For Object_Category_AI__c, you MUST pick exactly one allowed subtype for the chosen key; if the key has no subtypes "
    "or none fit, use 'N/A'. "
    "Do not invent labels outside OBJECT_MAP; match strings exactly (case and spacing).\n"
    "If a 'building_address' is provided in the input, identify the nearest realistic UK retail or trade supplier "
    "store location where this asset (or equivalent) could be purchased. "
    "Return BOTH the brand name AND a plausible full branch address in the fields: "
    "Nearest_Store_Name__c and Nearest_Store_Address__c. "
    "Also include the estimated round-trip drive time in minutes in Drive_Time__c, "
    "the estimated total price including travel in Price_Including_Drive_Time__c, "
    "and typical store opening hours in Opening_Hours__c. "

    "Using the building_address and any visible context in the image, also best-guess the high-level "
    "building description fields (Premises_Situation__c, Location_Type__c, Building_Classification__c, "
    "Floor_Construction__c, Building_Height_m__c, Storeys_Above_Ground__c, Storeys_Below_Ground__c, "
    "Approx_Dimensions__c, Roof_Details__c, Vehicle_Parking__c,  Drive_Distance_km__c,"
    "General_Occupancy_Types__c, Fire_History_Summary__c) in the same style as UK fire risk "
    "assessments"

    "For ALL fields, if the information is missing, unclear, or not visible, you MUST provide a realistic"
    "estimated value or range based on typical UK assets, buildings, construction practices, or dimensions."
    "Never simply return N/A unless there is absolutely no reasonable inference that can be made."

    "When providing estimates:"
    "- Use realistic ranges (e.g., “8–12 m”, “20–40 minutes”, “£80–£120”)"
    "- Include only the final estimated value/range in the JSON (no explanation)"
    "- Internally reason about building type, age, materials, and UK norms, but do not expose chain-of-thought"
    "- Ensure the estimate is plausible, concise, and formatted as a usable value"
    "- If forced to return N/A, do so only when no reasonable inference exists"

    "Confidence__c must be a number between 0 and 1 representing your overall certainty for"
    "the classification and estimates."

    "Using publicly available information only, analyse the identified asset and determine which specific "
    "Obsequio Group company or companies could provide installation, replacement, maintenance, servicing, "
    "monitoring, inspection, compliance, or IoT connectivity for this asset within the UK or Ireland. "

    "The Obsequio Group companies and their primary capabilities include:\n"
    "- CDS – Fire protection, life safety, and security solutions provider.\n"
    "- Genex – Water hygiene and energy compliance specialists.\n"
    "- Drax360 – Fire alarm lifecycle services, testing, maintenance, and compliance.\n"
    "- Drax Technology – Smart building compliance platform and alarm monitoring / digital connectivity.\n"
    "- APS – Fire and intruder alarms and security systems (NSI Gold accredited).\n"
    "- Brunel – Advanced security and life protection solutions.\n"
    "- Bryland – Fire suppression, detection, and bespoke life safety systems.\n"
    "- Metro Safety – Fire and health & safety consultancy and statutory compliance services.\n"
    "- Dakro Air & Water – Air quality and water hygiene compliance specialists.\n"
    "- Premier Fire Security – Fire and security systems specialist (South Coast UK).\n"
    "- Environtec – UKAS-accredited asbestos management and environmental compliance services.\n"
    "- PLP Fire & Security (PLP) – Fire safety systems design, installation, and maintenance.\n"
    "- Atlas World – Multi-disciplinary fire, security, energy, and assistive technology solutions "
    "with UK and Ireland presence.\n"

    "Your analysis must include:\n"
    "1. A clear identification of the asset type.\n"
    "2. The typical fire, safety, water hygiene, electrical, asbestos, building maintenance, "
    "or statutory compliance services that this asset usually requires.\n"
    "3. A mapping of those service needs to the known capabilities of each relevant Obsequio Group company.\n"
    "4. A clear statement of which companies can most realistically:\n"
    "   - install the asset\n"
    "   - maintain or service the asset\n"
    "   - upgrade or replace the asset\n"
    "   - monitor the asset remotely\n"
    "   - provide digital or IoT integration\n"
    "(Select all companies that reasonably apply based on capability.)\n"

    "For IoT, digital connectivity, or smart monitoring:\n"
    "- Identify whether technology can reasonably be applied to this asset type.\n"
    "- Specify what technology could be used (e.g. cloud monitoring, sensors, alarms, compliance platforms).\n"
    "- Explain how it would be used in practice.\n"
    "- Clearly state the benefits from the perspective of a building owner or facilities manager "
    "(e.g. compliance assurance, reduced risk, cost savings, remote visibility, predictive maintenance).\n"

    "Finally, provide a sales-ready summary ONLY in the field "
    "Obsequio_cross_sell_long__c that:\n"
    "- Names the relevant Obsequio Group company or companies\n"
    "- Clearly states what service or services each could provide\n"
    "- Highlights the most relevant up-sell and cross-sell opportunities for this specific asset\n"
    "- Is concise, practical, commercially useful, and tailored to the identified asset\n"
)

USER_INSTRUCTION = "Respond ONLY with a compact single JSON object containing all fields listed in the system prompt"

# ---------------------------
# AWS and OpenAI clients
# ---------------------------
s3 = boto3.client("s3")
oai = OpenAI(api_key=OPENAI_API_KEY)

OBJECT_MAP: dict[str, list[str]] = {
  "Access": [],
  "Activation Point": ["Button (Test)", "Check LED", "Distribution Board", "Fish Key", "Fish Key (Own)", "Fish Key (Single Tooth)", "Fish Key (Thin)", "Fish Key Bank", "Fish Key Switch", "Flick Fuse", "Flick Switch", "Fuse (Ceramic)", "Fuse (Pull)", "Key (Flat)", "Switch (Push)", "Switch (Rocker)", "Switch (Test)", "Testing Panel", "Unlisted"],
  "Alarm Gong": [],
  "Aquamist": [],
  "Assembly Point": [],
  "Beacon": [],
  "Boiler": [],
  "BSRA": [],
  "Building": [],
  "Building Plan": [],
  "Building Generator": [],
  "Burns Kit": [],
  "Call Point": ["Button (Test)", "Flick Fuse", "Key (Allen)", "Key (Apollo)", "Key (Cylindrical)", "Key (Fork)", "Key (GFE)", "Key (KAC)", "Key (Long)", "Key (Newlec)", "Key (Old Flag)", "Key (Pin)", "Key (Raffiki)", "Key (Side)", "Key (STI)", "Key (Sycall)", "Key (TOK)", "Key (Triangle)", "Key (UP)", "Key (White Flag)", "Unlisted"],
  "Calorifier": [],
  "CTTV": [],
  "Damper": [],
  "Diesel Storage Tank": [],
  "Disabled Emergency Refugee Point": [],
  "Door Released Switch": [],
  "Dry Riser": [],
  "Electric Meter": ["Dial Meter", "Digital Meter", "Prepayment Meter", "Smart Meter", "Standard Meter", "Variable-rate Meter"],
  "Emergency Light": ["Bulb", "Check LED", "Cone", "Coved", "Decorative", "Flood Lamp", "Flood Light", "Fluorescent Tube", "Fluro Square", "Halogen", "Hanging", "Hexagon", "LED Spot Light", "Oblong", "Round", "Running Man", "Semi Circular", "Spot Light", "Square", "Strip Light", "Strip Tubes", "Twin Spots", "Unlisted"],
  "Emergency Stop Activation Switch": [],
  "Emergency Stop Beacon": [],
  "Emergency Stop Button": [],
  "Emergency Stop Reset Button": [],
  "Emergency Stop Reset Key": [],
  "Evacuation Plan": [],
  "External Wall": [],
  "Extinguisher": [],
  "Eye Wash Kit": [],
  "Fire Alarm Panel": ["Key (TOK)", "Key Panel (1001)", "Key Panel (134)", "Key Panel (801)", "Key Panel (827)", "Key Panel (901)", "Key Panel (Black Plastic Flag)", "Key Panel (Plastic RED)", "Key Panel (Plastic Tok)", "Key Panel (TOK 001)", "Key Panel (TOK 003)", "Key Panel (TOK 007)", "Unlisted"],
  "Fire Blanket": [],
  "Fire Door - Communal": [],
  "Fire Door - Door and a Half": [],
  "Fire Door - Double": [],
  "Fire Door - Flat Front": [],
  "Fire Door - Single": [],
  "Fire Shutter": [],
  "First Aid Kit": [],
  "Floor": [
    "10th Floor", "10th Mezzanine", "11th Floor", "11th Mezzanine",
    "12th Floor", "12th Mezzanine", "13th Floor", "13th Mezzanine",
    "14th Floor", "14th Mezzanin", "15th Floor", "15th Mezzanine",
    "16th Floor", "16th Mezzanine", "17th Floor", "17th Mezzanine",
    "18th Floor", "18th Mezzanine", "19th Floor", "19th Mezzanine",
    "1st Floor", "1st Mezzanine", "20th Floor", "20th Mezzanine",
    "21st Floor", "21st Mezzanine", "22nd Floor", "22nd Mezzanine",
    "23rd Floor", "23rd Mezzanine", "24th Floor", "24th Mezzanine",
    "25th Floor", "25th Mezzanine", "26th Floor", "26th Mezzanine",
    "27th Floor", "27th Mezzanine", "28th Floor", "28th Mezzanine",
    "29th Floor", "29th Mezzanine", "2nd Floor", "2nd Mezzanine",
    "30th Floor", "30th Mezzanine", "31st Floor", "31st Mezzanine",
    "32nd Floor", "32nd Mezzanine", "33rd Floor", "33rd Mezzanine",
    "34th Floor", "34th Mezzanine", "35th Floor", "35th Mezzanine",
    "36th Floor", "36th Mezzanine", "37th Floor", "37th Mezzanine",
    "38th Floor", "38th Mezzanine", "39th Floor", "39th Mezzanine",
    "3rd Floor", "3rd Mezzanine", "40th Floor", "40th Mezzanine",
    "41st Floor", "41st Mezzanine", "42nd Floor", "42nd Mezzanine",
    "43rd Floor", "43rd Mezzanine", "44th Floor", "44th Mezzanine",
    "45th Floor", "45th Mezzanine", "46th Floor", "46th Mezzanine",
    "47th Floor", "47th Mezzanine", "48th Floor", "48th Mezzanine",
    "49th Floor", "49th Mezzanine", "4th Floor", "4th Mezzanine",
    "50th Floor", "50th Mezzanine", "5th Floor", "5th Mezzanine",
    "6th Floor", "6th Mezzanine", "7th Floor", "7th Mezzanine",
    "8th Floor", "8th Mezzanine", "9th Floor", "9th Mezzanine",
    "B1 Mezzanine", "B2 Mezzanine", "B3 Mezzanine",
    "Basement 1", "Basement 2", "Basement 3", "Basement 4", "Basement 5",
    "Grd Mezzanine", "Ground Floor"
  ],
  "Flow Switch": [],
  "Foam Inlet": [],
  "Gas Meter": ["Dial Meter", "Digital Meter", "Prepayment Meter", "Smart Meter", "Standard Meter", "Variable-rate Meter"],
  "Heat / Smoke Detector": [],
  "Heat Detector": [],
  "Hose Reel": [],
  "Hydrant": [],
  "Installation Valve": ["Dry", "Wet"],
  "Isolation Switch": [],
  "Jet Fan": ["Key (Fork)", "Smoke Generator"],
  "Key Safe": ["Combination", "Key"],
  "Large Step Ladder": [],
  "Led Fluro": [],
  "Lightning Conductor": [],
  "Logbook": ["Customers", "Metro"],
  "Logbook Cabinet": [],
  "Magnetic Door Release": [],
  "Meter": ["Dial Meter", "Digital Meter", "Electric Meter", "Electric Multi Read Meter", "Gas Meter", "Gas Multi Read Meter", "Prepayment Meter", "Smart Meter", "Standard Meter", "Variable-rate Meter", "Water Meter"],
  "Mobile Elevated Work Platform": [],
  "Monitoring Appliance": [],
  "Multi-Heat": [],
  "Optical Smoke": [],
  "Pressure Gauge": [],
  "Pump Test Valve": [],
  "Refuge Alarm": [],
  "Refuge Point Alarm Panel": [],
  "Region": [
    "Administration Office", "Annexe", "Attic", "Auditorium", "Bank",
    "Bathroom", "Bike Store", "Bin Room", "Bin Store", "Boardroom",
    "Boiler Room", "Cafe", "Caretakers Office", "Car Park", "Cellar",
    "Changing Room", "Cleaner Storage", "Computer Room", "Conference Room", "Corridor",
    "Corridor (LH)", "Corridor (RH)", "Corridor (Service)", "Courtyard", "Cupboard",
    "Dance Hall", "Dining Room", "Electrical Intake Room", "Electrical Riser", "Electrical Room",
    "Entrance", "Entrance Gates", "Entrance Lobby", "External Area", "External Plant Area",
    "External Plant Room", "External Walkway", "Extractor Room", "Fire Escape Stairs", "Fire Exit",
    "Fire Exit Lobby", "Flat Lobby", "Function Room", "Gas Intake room", "Gas Room",
    "Generator Room", "Gym", "Hall", "Kitchen", "Landing",
    "Laundry Room", "Lift Lobby", "Lift Machine Room", "Lift Motor Room", "Loading Bay",
    "Lobby", "Locker Room", "Lounge Room", "Meeting Room", "Meter Room",
    "Office (Other)", "Office Lobby", "Operations Room", "Photocopying Room", "Plant Room",
    "Playroom", "Pump House", "Pump Room", "Reception Area", "Refuse Area",
    "Riser", "Rooftop Area", "Room (Other)", "Seating Area", "Security Office",
    "Server Room", "Shop", "Shop Floor", "Shower Room", "Sprinkler Pump Room",
    "Staff Area", "Staffroom", "Stair Landing", "Stairs", "Stock Room",
    "Storage Area", "Store Room", "Studio", "Suite", "Tank Room",
    "Toilet (Disabled)", "Toilet (Female)", "Toilet (Lobby)", "Toilet (Male)", "Toilet (Unisex)",
    "Training Room", "Unlisted", "Utility Room", "Waiting Area", "Walkway",
    "Wall", "Wall (LHS)", "Wall (RHS)", "Warehouse", "Water Storage Space"
  ],
  "Relay Loop Module": [],
  "Remote Monitoring Panel": ["Key (TOK)"],
  "Roof Asset": ["Lift Motor Room", "Stairs Tank Room", "CCTV", "Cell Phone Tower", "Cooling Tower", "Crane / Lifting Equipment", "Exhaust", "Eye Bolt", "Fall Arrest System", "Fan Room", "Ladder", "Lightning Rod / Conductor", "Solar Panel"],
  "Security Alarm": [],
  "Security Alarm Panel": [],
  "Security Sensor": [],
  "Shower": [],
  "Shutter": [],
  "Small Step Ladder": [],
  "Small Step Podium": [],
  "Smoke Activation Point": ["Button (Test)", "Fish Key", "Fuse (Push)", "Key (Fork)", "Key (Side)", "Switch (Test)", "Unlisted"],
  "Smoke Control Panel": [],
  "Smoke Detector (Automatic)": [],
  "Smoke Detector (Domestic)": [],
  "Smoke Extractor": [],
  "Smoke Hatch": [],
  "Smoke Head of Shaft Vent": [],
  "Smoke Head of Stair Vent": [],
  "Smoke Shaft Door": [],
  "Smoke Vent": [],
  "Smoke Vent Door": [],
  "Smoke Vent Louvre": [],
  "Smoke Vent Panel": [],
  "Smoke Vent Reset Button": [],
  "Smoke Vent Reset Switch": ["Button", "Switch"],
  "Smoke Window": [],
  "Sounder": [],
  "Sprinkler Control Panel (Diesel)": [],
  "Sprinkler Control Panel (Electrical)": [],
  "Sprinkler Control Panel (Jockey)": [],
  "Sprinkler Pump (Diesel)": [],
  "Sprinkler Pump (Electric)": [],
  "Sprinkler Pump (Jockey)": [],
  "Sprinkler Pump Controller": [],
  "Surface Water Sump Pump": [],
  "Tap - Boiling": [],
  "Tap - Fountain": [],
  "Tap - Mixer": [],
  "Tap - Push Button": [],
  "Tap - Single": [],
  "Tenant List": [],
  "Testing Procedures": [],
  "Towns Main Water Supply": [],
  "Water Closet": [],
  "Water Heater": [],
  "Water Meter": ["Dial Meter", "Digital Meter", "Prepayment Meter", "Smart Meter", "Standard Meter", "Variable-rate Meter"],
  "Water Storage Tank": [],
  "Wet Riser": [],
  "Wet Riser Pump (Electric)": ["Hi Pressure (for High Rise Blocks)", "Low Pressure (for High Rise Blocks)", "Standard" ],
  "Wet Riser Pump (Jockey)": ["Hi Pressure (for High Rise Blocks)", "Low Pressure (for High Rise Blocks)", "Standard"],
  "Zone Map": []
}

# ---------------------------
# Helper functions
# ---------------------------
def find_key_by_prefix(prefix: str) -> str | None:
    """
    Return the most recently modified S3 key whose name starts with the given prefix.
    """
    started = time.time()
    log_event("INFO", "s3_lookup_started", bucket=S3_BUCKET, prefix=prefix)
    try:
        paginator = s3.get_paginator("list_objects_v2")
        latest_key, latest_ts = None, 0.0
        page_count = 0
        object_count = 0

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            page_count += 1
            contents = page.get("Contents", [])
            object_count += len(contents)
            for obj in contents:
                ts = obj["LastModified"].timestamp()
                if ts > latest_ts:
                    latest_ts = ts
                    latest_key = obj["Key"]

        log_event(
            "INFO",
            "s3_lookup_finished",
            bucket=S3_BUCKET,
            prefix=prefix,
            found=bool(latest_key),
            latest_key=latest_key,
            pages_scanned=page_count,
            objects_matched=object_count,
            duration_ms=round((time.time() - started) * 1000, 2),
        )
        return latest_key
    except ClientError as e:
        log_event(
            "ERROR",
            "s3_lookup_failed",
            bucket=S3_BUCKET,
            prefix=prefix,
            error_code=e.response.get("Error", {}).get("Code"),
            error_message=str(e),
            duration_ms=round((time.time() - started) * 1000, 2),
        )
        return None

def presign(key: str, seconds: int = 900) -> str:
    started = time.time()
    url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=seconds,
    )
    log_event(
        "INFO",
        "s3_presign_created",
        bucket=S3_BUCKET,
        key=key,
        expires_in_seconds=seconds,
        url_length=len(url),
        duration_ms=round((time.time() - started) * 1000, 2),
    )
    return url

# EXACT picklist values from Salesforce (API values must match these)
ASSET_CONDITION_VALUES = [
    "C1 - Very Good Condition",
    "C2 - Needs cleaning",
    "C2 - Minor Defects Only",
    "C3 - Maintenance required to return to an accepted level of service",
    "C4 - Requires renewal",
    "C5 - Asset Unserviceable",
]

def normalize_asset_condition(text: str) -> str:
    t = (text or "").strip().lower()

    # C5 — clearly broken/unsafe
    if any(w in t for w in ["unserviceable", "not working", "doesn't work", "broken", "inoperative", "unsafe", "failed"]):
        return "C5 - Asset Unserviceable"

    # C4 — needs replacement / end-of-life
    if any(w in t for w in ["requires renewal", "replace", "replacement", "end of life", "obsolete", "beyond repair", "major defect"]):
        return "C4 - Requires renewal"

    # C3 — needs maintenance/repair to return to service
    if any(w in t for w in ["maintenance required", "requires maintenance", "repair", "service", "intermittent fault", "faulty"]):
        return "C3 - Maintenance required to return to an accepted level of service"

    # C2 (clean) — mainly dirty
    if any(w in t for w in ["dirty", "dust", "grime", "cleaning", "needs cleaning"]):
        return "C2 - Needs cleaning"

    # C2 (minor defects) — scuffs/scratches/loose etc.
    if any(w in t for w in ["minor defect", "minor defects", "scuff", "scratch", "crack", "loose", "wear", "worn", "cosmetic", "slight"]):
        return "C2 - Minor Defects Only"

    # C1 — good/very good/serviceable
    if any(w in t for w in ["very good", "excellent", "good", "serviceable", "ok", "works", "working"]):
        return "C1 - Very Good Condition"

    # Fallback: choose a safe middle ground if the model is vague
    return "C2 - Minor Defects Only"

def call_openai(image_url: str, building_address: str) -> dict:
    started = time.time()
    log_event(
        "INFO",
        "openai_request_started",
        model=MODEL,
        has_api_key=bool(OPENAI_API_KEY),
        has_building_address=bool(building_address),
        object_map_keys=len(OBJECT_MAP),
    )

    resp = oai.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "text", "text": USER_INSTRUCTION},
                {"type": "text", "text": f"Building address: {building_address}"},
                {"type": "text", "text": "OBJECT_MAP (allowed values): " + json.dumps(OBJECT_MAP)},
                {"type": "image_url", "image_url": {"url": image_url}},
            ]}
        ],
        temperature=0.2,
    )
    text = resp.choices[0].message.content.strip()

    usage = getattr(resp, "usage", None)
    log_event(
        "INFO",
        "openai_response_received",
        model=MODEL,
        response_id=getattr(resp, "id", None),
        finish_reason=getattr(resp.choices[0], "finish_reason", None),
        response_chars=len(text),
        prompt_tokens=getattr(usage, "prompt_tokens", None) if usage else None,
        completion_tokens=getattr(usage, "completion_tokens", None) if usage else None,
        total_tokens=getattr(usage, "total_tokens", None) if usage else None,
        duration_ms=round((time.time() - started) * 1000, 2),
    )

    # Strip common code fences
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
    if text.lower().startswith("json"):
        text = text[4:].strip()

    try:
        data = json.loads(text)
        data["Asset_Condition__c"] = normalize_asset_condition(data.get("Asset_Condition__c"))
        log_event(
            "INFO",
            "openai_json_parsed",
            returned_keys=len(data),
            object_type=data.get("Object_Type_AI__c"),
            object_category=data.get("Object_Category_AI__c"),
            confidence=data.get("Confidence__c"),
        )
    except Exception as e:
        log_event(
            "ERROR",
            "openai_json_parse_failed",
            error_message=str(e),
            raw_response_excerpt=text[:1000],
        )
        data = {"_raw_response": text}

    # Ensure all keys present; coerce Confidence__c
    defaults = {
        "Manufacturer_AI__c": "", "What_Is_It__c": "", "SerialNumber": "", "Colour__c": "",
        "Rough_Dimensions__c": "", "Distinguishing_Features__c": "", "Asset_Condition__c": "",
        "Broken_Or_Needs_Replacement__c": "", "Service_Provider_Or_Supplier__c": "",
        "Other_Codes_Or_Numbers__c": "", "How_To_Test__c": "", "How_To_Replace__c": "",
        "Parts_Needed__c": "", "UK_Estimated_Price__c": "",
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
        "Drive_Distance_km__c": "",
    }
    for k, v in defaults.items():
        data.setdefault(k, v)
    try:
        data["Confidence__c"] = float(data.get("Confidence__c", 0.0))
    except Exception:
        data["Confidence__c"] = 0.0

    return data

def make_error_result(msg: str) -> dict:
    """
    Produce a result object with all expected fields empty so list alignment is preserved.
    """
    base = {
        "Manufacturer_AI__c": "", "What_Is_It__c": "", "SerialNumber": "", "Colour__c": "",
        "Rough_Dimensions__c": "", "Distinguishing_Features__c": "", "Asset_Condition__c": "",
        "Broken_Or_Needs_Replacement__c": "", "Service_Provider_Or_Supplier__c": "",
        "Other_Codes_Or_Numbers__c": "", "How_To_Test__c": "", "How_To_Replace__c": "",
        "Parts_Needed__c": "", "UK_Estimated_Price__c": "", 
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
        "Drive_Distance_km__c": "",
        "_error": msg
    }
    return base

def parse_incoming(event):
    """
    Support both direct array (Lambda test) and API Gateway proxy (event['body']).
    Expect: [{ "ContentVersionId": "<prefix>" }, ...]
    """
    payload = event
    log_event("INFO", "parse_started", event_type=type(event).__name__)

    if isinstance(event, dict) and "body" in event:
        body = event["body"]
        log_event(
            "INFO",
            "api_gateway_body_detected",
            body_length=len(body) if isinstance(body, str) else None,
            is_base64_encoded=bool(event.get("isBase64Encoded")),
        )
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode("utf-8")
        payload = json.loads(body)

    if isinstance(payload, str):
        log_event("INFO", "string_payload_detected", payload_length=len(payload))
        payload = json.loads(payload)

    if not isinstance(payload, list):
        raise ValueError("Payload must be a JSON array.")

    log_event(
        "INFO",
        "parse_finished",
        item_count=len(payload),
        content_version_ids=[(item or {}).get("ContentVersionId") for item in payload if isinstance(item, dict)],
    )
    return payload

# ---------------------------
# Lambda entry point
# ---------------------------
def process(event, context):
    request_started = time.time()
    aws_request_id = getattr(context, "aws_request_id", None)

    log_event(
        "INFO",
        "lambda_invocation_started",
        aws_request_id=aws_request_id,
        function_name=getattr(context, "function_name", None),
        function_version=getattr(context, "function_version", None),
        memory_limit_mb=getattr(context, "memory_limit_in_mb", None),
        bucket=S3_BUCKET,
        model=MODEL,
        has_openai_api_key=bool(OPENAI_API_KEY),
        event=safe_event_summary(event),
    )

    try:
        items = parse_incoming(event)
    except Exception as e:
        log_event(
            "ERROR",
            "bad_request",
            aws_request_id=aws_request_id,
            error_message=str(e),
            traceback=traceback.format_exc(),
        )
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": f"Bad request: {e}"})
        }

    results = []
    for index, item in enumerate(items):
        item_started = time.time()
        prefix = (item or {}).get("ContentVersionId")
        building_address = (item or {}).get("BuildingAddress")

        log_event(
            "INFO",
            "item_processing_started",
            aws_request_id=aws_request_id,
            index=index,
            prefix=prefix,
            has_building_address=bool(building_address),
        )

        if not prefix:
            msg = "Missing ContentVersionId"
            log_event("ERROR", "item_failed", aws_request_id=aws_request_id, index=index, error_message=msg)
            results.append(make_error_result(msg))
            continue

        key = find_key_by_prefix(prefix)
        if not key:
            msg = f"No S3 object for prefix '{prefix}'"
            log_event("ERROR", "item_failed", aws_request_id=aws_request_id, index=index, prefix=prefix, error_message=msg)
            results.append(make_error_result(msg))
            continue

        try:
            url = presign(key)
            fields = call_openai(url, building_address)
            results.append(fields)
            log_event(
                "INFO",
                "item_processing_finished",
                aws_request_id=aws_request_id,
                index=index,
                prefix=prefix,
                s3_key=key,
                object_type=fields.get("Object_Type_AI__c"),
                object_category=fields.get("Object_Category_AI__c"),
                confidence=fields.get("Confidence__c"),
                has_error="_error" in fields,
                has_raw_response="_raw_response" in fields,
                duration_ms=round((time.time() - item_started) * 1000, 2),
            )
        except Exception as e:
            msg = f"Inference failed: {e}"
            log_event(
                "ERROR",
                "item_failed",
                aws_request_id=aws_request_id,
                index=index,
                prefix=prefix,
                s3_key=key,
                error_message=msg,
                traceback=traceback.format_exc(),
                duration_ms=round((time.time() - item_started) * 1000, 2),
            )
            results.append(make_error_result(msg))

    error_count = sum(1 for r in results if "_error" in r or "_raw_response" in r)
    response_body = json.dumps(results)

    log_event(
        "INFO" if error_count == 0 else "ERROR",
        "lambda_invocation_finished",
        aws_request_id=aws_request_id,
        status_code=200,
        item_count=len(results),
        error_count=error_count,
        response_body_chars=len(response_body),
        response_preview=response_body[:4000],
        duration_ms=round((time.time() - request_started) * 1000, 2),
    )

    # Return array in SAME ORDER to match Apex's index-based mapping
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": response_body
    }
