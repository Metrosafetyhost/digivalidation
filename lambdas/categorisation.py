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

# ------------------------------------------------------------
# Exact Salesforce picklist (your existing floor logic)
# ------------------------------------------------------------
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
    "ground floor": "Ground Floor",
    "gf": "Ground Floor",
    "grd": "Ground Floor",
    "g/f": "Ground Floor",

    "lower ground": "Lower Ground",
    "l.g.": "Lower Ground",
    "lg": "Lower Ground",
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

ORDINALS = { 1: "1st Floor", 2: "2nd Floor", 3: "3rd Floor", **{n: f"{n}th Floor" for n in range(4, 51)} }
MEZZ_ORDINALS = { 1: "1st Mezzanine", 2: "2nd Mezzanine", 3: "3rd Mezzanine", **{n: f"{n}th Mezzanine" for n in range(4, 51)} }

# compile regexes once
RE_LEVEL = re.compile(r"\b(?:level|lvl|lv)\s*(\d{1,2})\b", re.I)
RE_FLOOR_NUM = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)?\s*(?:floor|flr|fl)\b", re.I)
RE_MEZZ_NUM = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)?\s*(?:mezz|mezzanine)\b", re.I)
RE_BASEMENT_MEZZ = re.compile(r"\bbasement\s+mezz(?:anine)?\s*(b[1-3])\b", re.I)
RE_B_MEZZ = re.compile(r"\bb\s*(\d{1,2})\s*mezz(?:anine)?\b", re.I)

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

# ------------------------------------------------------------
# CLOSED-WORLD ENUMS (Object Types & Categories) + guardrail
#   1) Bind your dictionary to OBJECT_MAP (it was raw at file end)
#   2) Sanitise, build enums JSON for the prompt
#   3) Validate model output against OBJECT_MAP
# ------------------------------------------------------------
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

OBJECT_TYPES = list(OBJECT_MAP.keys())
ENUMS_JSON = json.dumps({"OBJECT_TYPES": OBJECT_TYPES, "CATEGORIES_BY_TYPE": OBJECT_MAP}, ensure_ascii=False)

def validate_extraction(result: dict) -> dict:
    """Clamp Object_Type__c/Object_Category__c to the closed lists; smart-normalise Label__c."""
    safe = {
        "Object_Type__c": None,
        "Object_Category__c": None,
        "Asset_Instructions__c": None,
        "Label__c": None,
        "Name": None
    }
    if not isinstance(result, dict):
        return safe
    out = {**safe, **result}

    t = out.get("Object_Type__c")
    c = out.get("Object_Category__c")

    if t not in OBJECT_MAP:
        out["Object_Type__c"] = None
        out["Object_Category__c"] = None
    else:
        allowed = set(OBJECT_MAP.get(t, []))
        if not allowed or c not in allowed:
            out["Object_Category__c"] = None

    # only uppercase when it's a short code token like FF1, MCP12, etc.
    lbl = out.get("Label__c")
    if isinstance(lbl, str):
        lbl_stripped = lbl.strip()
        if re.fullmatch(r"[A-Za-z]{1,3}\d{1,3}", lbl_stripped):
            out["Label__c"] = lbl_stripped.upper()
        else:
            out["Label__c"] = lbl_stripped  # preserve case for sentences like "Step 7: ..."

    return out

# Extract "Step <number>: <text>" or "Step <number> - <text>" anywhere in the capture.
RE_STEP_LINE = re.compile(r"(?:^|\b)(Step\s*\d+\s*[:\-]\s*.*)", re.IGNORECASE)

def extract_first_step_line(text: str):
    if not text:
        return None
    m = RE_STEP_LINE.search(text)
    return m.group(1).strip() if m else None


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

    ## HARD CONSTRAINTS (do not violate)
    - Valid object types are ONLY those in OBJECT_TYPES.
    - For a chosen type, valid categories are ONLY those in CATEGORIES_BY_TYPE[type].
    - If the text does not clearly specify a category, set Object_Category__c to an empty string. Do not guess.
    - If the text does not clearly specify a type, set Object_Type__c to an dmepty string and also set Object_Category__c to an empty string.
    - Uppercase Label__c. Never leave Name blank.

    ## ENUMS (closed world for choices)
    {ENUMS_JSON}

    ## How to interpret inputs

    1) OBJECT TYPE (Object_Type__c)
    - PRECEDENCE: If the “Testing Procedures header override” in Step 5 applies, set Object_Type__c = "Testing Procedures" (and do not use any other object type).
    - If the override does NOT apply:
    - If structured, it's the text before " - Location".
    - If unstructured, infer from keywords/synonyms (conservatively) but only choose from OBJECT_TYPES.
    - Use the clean, human-readable form, e.g., "Emergency Light".


    2) OBJECT CATEGORY (Object_Category__c)
    - If structured, take the text after "Type:".
    - Otherwise infer from shape/technology words:
        LED, Square, Dial Meter, Round, Button, Key, Flick Fuse, Beacon.
    - Normalize to Title Case, e.g., "LED Square", "Bulkhead Twinspot".
    - If none found, return an emptry string.

    3) ASSET INSTRUCTIONS (Asset_Instructions__c)
    - If the input contains a “Testing Instructions”/“Testing Instruction”/“Testing Procedures”/“Testing Procedure”/“Test"
    header (case-insensitive) anywhere in the text:
    → Set Asset_Instructions__c to the FULL block of text starting immediately after that header marker
        (after any delimiter like “:”, “-”, or “;”) through to the END of the input. Do NOT stop at the first sentence.
    - Else if the input has a structured “Test:” field, use the text after “Test:” (entire value).
    - Else, capture the first clear imperative testing sentence (e.g., “Activate …”, “Open …”, “Isolate …”).
    - If nothing meaningful exists, set Asset_Instructions__c to "".

    4) LABEL (Label__c)
    - Priority order:
    1) If an explicit label is provided via a “Label:” field, use that exact value.
    2) ELSE if the text contains any “Step <number>” token (e.g., “Step 10: …”, “Step 10 - …”, or “... Step 10 ...”),
        set Label__c to exactly "Step <number>" (JUST the word “Step” and the number, no following text).
        Examples: “Step 7: Open the valve …” → Label__c = "Step 7"; “... proceed to Step 3 ...” → Label__c = "Step 3".
    3) ELSE prefer a short asset code anywhere in the text, typically one of:
        FF\d+, FK\d+, EL\d+, EM\d+, CP\d+, MCP\d+, SD\d+, HD\d+, SB\d+, R\d+, or generally [A-Z]{1,3}\d{1,3}.
    - Return Label__c as-is for sentence-like labels; uppercase only the short code tokens (e.g., "FF1", "FK11").
    - If none of the above are found, set Label__c to an empty string "".

    5) NAME (Name)  — and the “Testing Procedures” override

    A) When to apply the Testing Procedures HEADER override
    - Apply this override ONLY if the input begins with an object phrase and is immediately followed by a testing header phrase in the opening title/header segment (i.e., before any normal fields like “Label:”, “Type:”, “Location:”).
    - Qualifying header markers include (case-insensitive), with typical delimiters like "-", ":", ";", or ".":
    "Testing Instructions", "Testing Instruction", "Testing Procedures", "Testing Procedure", "Instructions", "Procedure".
    - Treat it as a header if the testing phrase appears within the first ~10 characters after the object name and clearly functions as part of the title/header, not mid-paragraph or after fields.

    If the HEADER override applies:
    - Set Object_Type__c = "Testing Procedures".
    - Set Object_Category__c = "".
    - Set Name = "Testing Procedures - <Object>", where <Object> is the object named in the opening phrase (e.g., "Electric Pump", "Jockey Pump", "Installation Valve").
    - For both Label__c and Asset_Instructions__c, if a “Step <number>:” sentence exists (e.g., “Step 7: …”), use the FIRST such step line verbatim for BOTH fields. If no step line exists, use the first clear imperative testing sentence for both; otherwise "".

    Examples (override applies):
    - "Electric Pump activation test valve - Testing Instructions; Step 7: Open the test valve slowly …"
    => Object_Type__c: "Testing Procedures"; Name: "Testing Procedures - Electric Pump"
    - "Diesel Pump – Testing Procedures: Step 3: Verify auto start from jockey pump pressure switch…"
    => Object_Type__c: "Testing Procedures"; Name: "Testing Procedures - Diesel Pump": Label__c - Step 7

    B) Normal description (NO override)
    - If “testing instructions/procedures” appears later in a normal/freeform description (e.g., after "Label:", "Type:", "Location:", or mid-paragraph), DO NOT use the override.
    - Keep Object_Type__c as the actual asset (e.g., "Installation Valve", "Electric Pump"), chosen from OBJECT_TYPES.
    - Set Object_Category__c from an explicit subtype (e.g., "Wet System"); otherwise "".
    - Label__c: If a “Step <number>: …” line exists anywhere, use the FULL step line. Otherwise prefer a short code (e.g., FF\d+, EL\d+, etc., or [A-Z]{1,3}\d{1,3}); return null if none.
    - Asset_Instructions__c: Use the first explicit test step sentence if present; otherwise the first clear imperative testing sentence; otherwise "".
    - Name: Build using your existing non-testing format (e.g., "<Location Guess>, <Object Type Acronym>, <Label>").

    Tie-breakers
    - Only apply the HEADER override when the testing phrase is clearly part of the opening header as defined above. If unsure, treat as normal description (no override).
    - Never invent an object type. If no valid object type is evident and the override doesn’t apply, set Object_Type__c = "" and Object_Category__c = "".
    - Never leave Name null; if you cannot construct a valid Name by the above rules, set Name = "TEMPORARY - NAME NOT FOUND".


    IMPORTANT RULES
    - Be helpful but conservative: infer when strong cues exist; otherwise return nothing at all.
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
    2nd floor corridor by flat 12 & 13 ceiling emergency light round FK2

    OUTPUT
    {{
      "Object_Type__c": "Emergency Light",
      "Object_Category__c": "Round",
      "Asset_Instructions__c": "",
      "Label__c": "FK2",
      "Name": "2nd Floor Corridor by Flat 12 & 13 Ceiling, EL, FK2"
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
        text_out = "".join(part.get("text", "") for part in data.get("content", []))
        out = json.loads(text_out)
        # Enforce closed-world enums
        out = validate_extraction(out)
        return out
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
    metadata = []
    for obj in body:
        base = (obj.get("input") or "")
        desc = obj.get("description")
        cvid = obj.get("contentVersionId")
        samples.append(base)
        metadata.append({"description": desc, "contentVersionId": cvid})

    logger.info(">> process: assembled samples for model: %s", samples)
    logger.info(">> process: collected metadata (unused for now): %s", metadata)

    # 3) Classify each sample
    results = []
    for txt in samples:
        try:
            out = classify_asset_text(txt)

            # Floor canonicalisation (unchanged)
            floor = extract_floor(txt)
            floor = to_picklist_or_none(floor)
            logger.info("Floor extracted: %s | from text: %s", floor, txt[:200])

            out["Floor__c"] = floor  # append floor as before
            results.append(out)
        except Exception as ex:
            logger.warning("process: classification error for input '%s': %s", txt, ex, exc_info=True)
            results.append({"error": str(ex), "input": txt})

    logger.info("<< process: returning results: %s", results)

    # 4) Return bare JSON array
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(results)
    }
