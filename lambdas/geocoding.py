import json
import boto3
import os
import re
from typing import Any

REGION = os.getenv("AWS_REGION", "eu-west-2")
PLACE_INDEX = os.getenv("PLACE_INDEX_NAME")
loc = boto3.client("location", region_name=REGION)

# AWS typically returns ISO-3166-1 alpha-3 (e.g. GBR, IRL).
# Salesforce Country/Territory picklist expects ISO-3166-1 alpha-2 (e.g. GB, IE).
ISO3_TO_ISO2 = {
    "GBR": "GB",
    "IRL": "IE",
    "NLD": "NL",
    "BEL": "BE",
    "FRA": "FR",
    "ESP": "ES",
    "DEU": "DE",
}

BUILDING_KEYWORDS = {
    "apartments", "apartment", "block", "building", "estate",
    "heights", "house", "lodge", "manor", "mews",
    "plaza", "residence", "residences", "tower",
    "towers", "view", "villas", "works", "wharf", "centre", "center",
}

STREET_KEYWORDS = {
    "road", "rd", "street", "st", "avenue", "ave", "lane", "ln", "close",
    "crescent", "way", "drive", "dr", "walk", "hill", "gardens", "terrace",
    "place", "court", "square", "boulevard", "blvd", "highway",
}

STOPWORDS = {
    "the", "and", "london", "united", "kingdom", "great", "britain",
}

UK_POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b",
    re.IGNORECASE,
)

LEADING_RANGE_RE = re.compile(r"^\s*\d+[\-/]\d+[A-Z]?\b")
SINGLE_HOUSE_NUMBER_RE = re.compile(r"^\s*\d+[A-Z]?\b")
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
RANGE_PREFIX_RE = re.compile(r"^\s*(\d+\s*[-/]\s*\d+[A-Z]?)\b", re.IGNORECASE)

def is_range_street_address(query: str) -> bool:
    if not query:
        return False

    q = normalize_text(query)
    has_leading_range = bool(LEADING_RANGE_RE.search(query))
    has_street_word = any(word in q.split() for word in STREET_KEYWORDS)

    return has_leading_range and has_street_word

def to_iso2(country_code: str | None):
    if not country_code:
        return None
    c = str(country_code).strip().upper()
    if len(c) == 2:
        return c
    if len(c) == 3:
        return ISO3_TO_ISO2.get(c)
    return None


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return NON_ALNUM_RE.sub(" ", value.lower()).strip()


def tokens(value: str | None) -> set[str]:
    txt = normalize_text(value)
    return {t for t in txt.split() if t and t not in STOPWORDS}


def looks_like_address(query: str) -> bool:
    q = normalize_text(query)
    has_house_number = bool(SINGLE_HOUSE_NUMBER_RE.search(query or ""))
    has_street_word = any(word in q.split() for word in STREET_KEYWORDS)
    has_postcode = bool(UK_POSTCODE_RE.search(query or ""))
    has_range_street = is_range_street_address(query)

    return has_range_street or ((has_house_number or has_postcode) and has_street_word)


def looks_like_building(query: str) -> bool:
    q_tokens = tokens(query)
    has_building_keyword = any(word in q_tokens for word in BUILDING_KEYWORDS)

    # A leading numeric range on its own is NOT enough to call something a building.
    # Example: "63-64 Margaret Street" should be treated as address-like.
    if has_building_keyword:
        return True

    return False


def classify_input(query: str) -> str:
    building = looks_like_building(query)
    address = looks_like_address(query)
    if building and address:
        return "mixed"
    if building:
        return "building"
    return "address"


def extract_postal_code(query: str) -> str | None:
    match = UK_POSTCODE_RE.search(query or "")
    if not match:
        return None
    return re.sub(r"\s+", " ", match.group(1).upper()).strip()


def extract_address_fallback_query(query: str) -> str:
    # For mixed building strings, strip the leading building/range component but keep
    # the street/postcode locality to obtain a postal coordinate fallback when needed.
    s = re.sub(r"^\s*\d+[\-/]\d+[A-Z]?\s+", "", query or "", count=1)
    s = re.sub(r"^\s*[A-Za-z0-9'\-]+\s+(Heights|House|Court|Tower|Block|Apartments|Lodge|Manor)\b[\s,]*", "", s, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", s).strip(", ")


def build_search_args(text: str, event: dict, max_results: int | None = None) -> dict:
    args: dict[str, Any] = {
        "IndexName": PLACE_INDEX,
        "Text": text,
        "MaxResults": max_results or int(event.get("maxResults", 5) or 5),
    }

    bias = event.get("biasPosition")
    if isinstance(bias, list) and len(bias) == 2:
        args["BiasPosition"] = bias

    countries = event.get("filterCountries")
    if isinstance(countries, list) and countries:
        args["FilterCountries"] = countries

    return args


def map_place_result(result: dict) -> dict:
    place = result.get("Place", {})
    point = ((place.get("Geometry") or {}).get("Point") or [None, None])
    label = place.get("Label")
    street = place.get("Street")
    house_number = place.get("AddressNumber")

    return {
        "countryCode": to_iso2(place.get("Country")),
        "city": place.get("Municipality"),
        "postalCode": place.get("PostalCode"),
        "street": street,
        "houseNumber": house_number,
        "confidence": result.get("Relevance"),
        "label": label,
        "position": {
            "longitude": point[0] if len(point) > 0 else None,
            "latitude": point[1] if len(point) > 1 else None,
        },
        "raw": {
            "resultId": result.get("PlaceId"),
            "place": place,
        },
    }


def build_output_fields(mapped: dict, query: str, input_type: str, matched_by: str, fallback_used: bool) -> dict:
    house = mapped.get("houseNumber")
    street = mapped.get("street")
    range_match = RANGE_PREFIX_RE.search(query or "")

    street_out = None

    if input_type == "address" and range_match and street:
        range_text = re.sub(r"\s+", "", range_match.group(1))
        street_out = f"{range_text} {street}".strip()
    elif house and street:
        street_out = f"{house} {street}".strip()
    else:
        street_out = (street or "").strip() or None

    return {
        "originalInput": query,
        "inputType": input_type,
        "matchedBy": matched_by,
        "fallbackUsed": fallback_used,
        "countryCode": mapped.get("countryCode"),
        "city": mapped.get("city"),
        "postalCode": mapped.get("postalCode"),
        "street": street_out,
        "houseNumber": mapped.get("houseNumber"),
        "label": mapped.get("label"),
        "confidence": mapped.get("confidence"),
        "position": mapped.get("position"),
    }


def text_overlap_score(query: str, candidate: dict) -> float:
    q_tokens = tokens(query)
    label_tokens = tokens(candidate.get("label"))
    street_tokens = tokens(candidate.get("street"))
    postal = extract_postal_code(query)
    candidate_postal = (candidate.get("postalCode") or "").upper().strip()

    if not q_tokens:
        return 0.0

    overlap = len(q_tokens.intersection(label_tokens.union(street_tokens))) / max(len(q_tokens), 1)
    postal_bonus = 0.2 if postal and candidate_postal and postal == candidate_postal else 0.0
    return min(1.0, overlap + postal_bonus)


def number_mismatch_penalty(query: str, candidate: dict) -> float:
    # Penalise cases like "24-55 Magnus Heights" resolving to "24 Hampden Road",
    # but do NOT penalise normal ranged street addresses like "63-64 
    if is_range_street_address(query):
        return 0.0

    leading_range = LEADING_RANGE_RE.search(query or "")
    house_number = str(candidate.get("houseNumber") or "").strip().lower()
    if not leading_range:
        return 0.0

    range_text = leading_range.group(0).strip().lower()
    if house_number and house_number in range_text and house_number != range_text:
        return 0.35
    return 0.0


def building_match_score(query: str, candidate: dict) -> float:
    base_relevance = float(candidate.get("confidence") or 0.0)
    overlap = text_overlap_score(query, candidate)
    label_txt = normalize_text(candidate.get("label"))
    q_txt = normalize_text(query)
    exactish_bonus = 0.15 if q_txt and q_txt in label_txt else 0.0
    penalty = number_mismatch_penalty(query, candidate)
    return round((base_relevance * 0.55) + (overlap * 0.45) + exactish_bonus - penalty, 4)


def address_match_score(query: str, candidate: dict) -> float:
    base_relevance = float(candidate.get("confidence") or 0.0)
    overlap = text_overlap_score(query, candidate)
    penalty = number_mismatch_penalty(query, candidate)
    return round((base_relevance * 0.7) + (overlap * 0.3) - penalty, 4)


def search_text(query: str, event: dict, max_results: int | None = None) -> list[dict]:
    search_args = build_search_args(query, event, max_results=max_results)
    resp = loc.search_place_index_for_text(**search_args)
    return [map_place_result(r) for r in resp.get("Results", [])]


def pick_best_address_result(query: str, candidates: list[dict]) -> dict | None:
    scored = []
    for c in candidates:
        score = address_match_score(query, c)
        scored.append((score, c))
    scored.sort(key=lambda item: item[0], reverse=True)
    if not scored:
        return None
    best_score, best = scored[0]
    return best if best_score >= 0.45 else None


def pick_best_building_result(query: str, candidates: list[dict], min_score: float) -> dict | None:
    scored = []
    for c in candidates:
        score = building_match_score(query, c)
        scored.append((score, c))
    scored.sort(key=lambda item: item[0], reverse=True)
    if not scored:
        return None
    best_score, best = scored[0]
    return best if best_score >= min_score else None


def geocode_query(query: str, event: dict) -> tuple[dict | None, str, bool]:
    input_type = classify_input(query)
    allow_address_fallback = bool(event.get("allowAddressFallbackForBuilding", False))
    min_building_score = float(event.get("minBuildingScore", 0.72))

    # 1) Building/mixed inputs: search the full building string first.
    if input_type in {"building", "mixed"}:
        building_candidates = search_text(query, event, max_results=max(int(event.get("maxResults", 5) or 5), 5))
        building_match = pick_best_building_result(query, building_candidates, min_score=min_building_score)
        if building_match:
            return building_match, "buildingSearch", False

        # 2) Optional fallback: strip the building portion and search the postal address.
        if allow_address_fallback:
            fallback_query = extract_address_fallback_query(query)
            if fallback_query and fallback_query != query:
                fallback_candidates = search_text(fallback_query, event, max_results=max(int(event.get("maxResults", 5) or 5), 5))
                address_match = pick_best_address_result(fallback_query, fallback_candidates)
                if address_match:
                    return address_match, "addressFallback", True

        return None, "buildingSearch", False

    # 3) Standard address handling.
    address_candidates = search_text(query, event, max_results=max(int(event.get("maxResults", 5) or 5), 5))
    address_match = pick_best_address_result(query, address_candidates)
    return address_match, "addressSearch", False


def empty_response(query: str, input_type: str, matched_by: str) -> dict:
    return {
        "originalInput": query,
        "inputType": input_type,
        "matchedBy": matched_by,
        "fallbackUsed": False,
        "countryCode": None,
        "street": None,
        "houseNumber": None,
        "city": None,
        "postalCode": None,
        "label": None,
        "confidence": None,
        "position": {"longitude": None, "latitude": None},
    }


def process(event, context):
    payload = event
    if isinstance(event, dict) and "body" in event:
        try:
            payload = json.loads(event.get("body") or "{}")
        except Exception:
            payload = {}

    address = payload.get("address")
    if not isinstance(address, str) or not address.strip():
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Provide a non-empty 'address' string."}),
        }

    if not PLACE_INDEX:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "PLACE_INDEX_NAME environment variable is not set."}),
        }

    query = address.strip()
    input_type = classify_input(query)

    try:
        result, matched_by, fallback_used = geocode_query(query, payload)
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Error calling Amazon Location", "details": str(e)}),
        }

    if not result:
        return {
            "statusCode": 200,
            "body": json.dumps(empty_response(query, input_type, "noAcceptedMatch")),
        }

    out = build_output_fields(result, query, input_type, matched_by, fallback_used)
    return {"statusCode": 200, "body": json.dumps(out)}