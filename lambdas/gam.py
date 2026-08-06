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


def _floor_categories() -> list[str]:
    """Return the Floor subtypes used by the existing Asset Capture Lambda."""
    categories: list[str] = []
    for number in range(1, 51):
        suffix = "th"
        if number % 100 not in {11, 12, 13}:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(number % 10, "th")
        mezzanine = (
            "14th Mezzanin"
            if number == 14
            else f"{number}{suffix} Mezzanine"
        )
        categories.extend((f"{number}{suffix} Floor", mezzanine))
    categories.extend((
        "B1 Mezzanine", "B2 Mezzanine", "B3 Mezzanine",
        "Basement 1", "Basement 2", "Basement 3", "Basement 4", "Basement 5",
        "Grd Mezzanine", "Ground Floor",
    ))
    return categories


# The same constrained Salesforce type/category map used by
# asset_categorisation.py. Object type is a key; object category is one of the
# corresponding values (or N/A when that type has no subtype).
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
    "Floor": _floor_categories(),
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
    "Region": ["Administration Office", "Annexe", "Attic", "Auditorium", "Bank", "Bathroom", "Bike Store", "Bin Room", "Bin Store", "Boardroom", "Boiler Room", "Cafe", "Caretakers Office", "Car Park", "Cellar", "Changing Room", "Cleaner Storage", "Computer Room", "Conference Room", "Corridor", "Corridor (LH)", "Corridor (RH)", "Corridor (Service)", "Courtyard", "Cupboard", "Dance Hall", "Dining Room", "Electrical Intake Room", "Electrical Riser", "Electrical Room", "Entrance", "Entrance Gates", "Entrance Lobby", "External Area", "External Plant Area", "External Plant Room", "External Walkway", "Extractor Room", "Fire Escape Stairs", "Fire Exit", "Fire Exit Lobby", "Flat Lobby", "Function Room", "Gas Intake room", "Gas Room", "Generator Room", "Gym", "Hall", "Kitchen", "Landing", "Laundry Room", "Lift Lobby", "Lift Machine Room", "Lift Motor Room", "Loading Bay", "Lobby", "Locker Room", "Lounge Room", "Meeting Room", "Meter Room", "Office (Other)", "Office Lobby", "Operations Room", "Photocopying Room", "Plant Room", "Playroom", "Pump House", "Pump Room", "Reception Area", "Refuse Area", "Riser", "Rooftop Area", "Room (Other)", "Seating Area", "Security Office", "Server Room", "Shop", "Shop Floor", "Shower Room", "Sprinkler Pump Room", "Staff Area", "Staffroom", "Stair Landing", "Stairs", "Stock Room", "Storage Area", "Store Room", "Studio", "Suite", "Tank Room", "Toilet (Disabled)", "Toilet (Female)", "Toilet (Lobby)", "Toilet (Male)", "Toilet (Unisex)", "Training Room", "Unlisted", "Utility Room", "Waiting Area", "Walkway", "Wall", "Wall (LHS)", "Wall (RHS)", "Warehouse", "Water Storage Space"],
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
    "Wet Riser Pump (Electric)": ["Hi Pressure (for High Rise Blocks)", "Low Pressure (for High Rise Blocks)", "Standard"],
    "Wet Riser Pump (Jockey)": ["Hi Pressure (for High Rise Blocks)", "Low Pressure (for High Rise Blocks)", "Standard"],
    "Zone Map": [],
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


def _exact_allowed_value(value: Any, allowed: list[str]) -> str | None:
    """Return the canonical allowed value using a case-insensitive match."""
    candidate = str(value or "").strip().casefold()
    return next((item for item in allowed if item.casefold() == candidate), None)


def _validate_object_classification(result: dict[str, Any]) -> None:
    """Enforce the same OBJECT_MAP contract as asset_categorisation.py."""
    object_type = _exact_allowed_value(
        result.get("Object_Type_AI__c"), list(OBJECT_MAP)
    )
    if object_type is None:
        result["Object_Type_AI__c"] = "N/A"
        result["Object_Category_AI__c"] = "N/A"
        result["Confidence__c"] = 0.0
        return

    result["Object_Type_AI__c"] = object_type
    allowed_categories = OBJECT_MAP[object_type]
    if not allowed_categories:
        result["Object_Category_AI__c"] = "N/A"
        return

    object_category = _exact_allowed_value(
        result.get("Object_Category_AI__c"), allowed_categories
    )
    if object_category is None:
        object_category = "Unlisted" if "Unlisted" in allowed_categories else "N/A"
    result["Object_Category_AI__c"] = object_category


def _context_text(payload: dict[str, Any], result: dict[str, Any]) -> str:
    asset = payload["asset"]
    values = [
        asset.get("name"),
        asset.get("objectType"),
        asset.get("objectCategory"),
        result.get("Object_Type_AI__c"),
        result.get("Object_Category_AI__c"),
        result.get("What_Is_It__c"),
    ]
    return " ".join(str(value) for value in values if value).casefold()


def _strengthen_fire_classification(
    result: dict[str, Any], payload: dict[str, Any]
) -> None:
    """Avoid 'Insufficient Information' when the known asset type is decisive."""
    if result.get("Fire_Safety_Classification__c") != "Insufficient Information":
        return

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
        "smoke control", "automatic opening vent", "aov",
    )
    pfp_terms = (
        "fire door", "fd30", "fd60", "fd90", "fd120", "fire stopping",
        "firestop", "cavity barrier", "fire-resisting wall",
        "fire resisting wall", "fire-resistant glazing",
        "fire resistant glazing", "structural fire protection",
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
        if result.get("Fire_Safety_Classification_Confidence__c") == "Low":
            result["Fire_Safety_Classification_Confidence__c"] = "Medium"
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
    }
    for output_key, source_keys in source_mappings.items():
        value = _first_source_value(asset, *source_keys)
        if value not in (None, ""):
            result[output_key] = str(value)

    _validate_object_classification(result)
    _strengthen_fire_classification(result, payload)


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
- Object_Type_AI__c/Object_Category_AI__c are the image-assisted result. Use
  only the exact values supplied in OBJECT_MAP. Object_Type_AI__c must be one
  OBJECT_MAP key. Object_Category_AI__c must be one subtype belonging to that
  chosen key. If the type has no subtypes, return "N/A" for the category. If no
  type fits, return "N/A" for both fields. Never invent or reword these labels.

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
- UNSPSC and Uniclass values are unverified candidate suggestions because no
  authoritative reference dataset is connected.
- For UNSPSC, where the asset is clearly identified and a genuine candidate is
  known, return one 8-digit commodity code in UNSPSC_Code__c. Use
  UNSPSC_Description__c to show all four hierarchy levels in this form:
  "Segment > Family > Class > Commodity". Never invent a code; leave it blank
  if uncertain and explain the likely classification family in
  Classification_Notes__c.
- For Uniclass, use the most appropriate product/system/entity table for the
  identified asset. Return a candidate code/title/table only when reasonably
  confident. Do not leave the title/table blank merely because the exact code
  is uncertain: return the likely table/title, leave the code blank, use a low
  confidence, and explain the uncertainty in Classification_Notes__c.
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
  extinguisher, sprinkler, emergency door release and firefighting lift are AFP;
  an explicitly identified fire door/FD30/FD60, fire stopping or cavity barrier
  is PFP; a fire-action notice or evacuation plan is FSM. An ordinary door is
  Not a Fire-Safety Asset unless reliable evidence identifies a fire function.
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
            "text": "OBJECT_MAP (allowed values): " + json.dumps(OBJECT_MAP),
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
