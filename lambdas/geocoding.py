import json
import boto3
import os
import re
import logging
from typing import Any

REGION = os.getenv("AWS_REGION", "eu-west-2")
PLACE_INDEX = os.getenv("PLACE_INDEX_NAME")
loc = boto3.client("location", region_name=REGION)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_json(message: str, **data):
    try:
        logger.info("%s | %s", message, json.dumps(data, default=str))
    except Exception:
        logger.info("%s | %s", message, str(data))

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


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return NON_ALNUM_RE.sub(" ", value.lower()).strip()


def tokens(value: str | None) -> set[str]:
    txt = normalize_text(value)
    return {t for t in txt.split() if t and t not in STOPWORDS}


def is_range_street_address(query: str) -> bool:
    if not query:
        return False

    q = normalize_text(query)
    has_leading_range = bool(LEADING_RANGE_RE.search(query))
    has_street_word = any(word in q.split() for word in STREET_KEYWORDS)

    result = has_leading_range and has_street_word
    log_json(
        "is_range_street_address",
        query=query,
        normalized=q,
        has_leading_range=has_leading_range,
        has_street_word=has_street_word,
        result=result,
    )
    return result


def to_iso2(country_code: str | None):
    if not country_code:
        return None
    c = str(country_code).strip().upper()
    if len(c) == 2:
        return c
    if len(c) == 3:
        return ISO3_TO_ISO2.get(c)
    return None


def looks_like_address(query: str) -> bool:
    q = normalize_text(query)
    has_house_number = bool(SINGLE_HOUSE_NUMBER_RE.search(query or ""))
    has_street_word = any(word in q.split() for word in STREET_KEYWORDS)
    has_postcode = bool(UK_POSTCODE_RE.search(query or ""))
    has_range_street = is_range_street_address(query)

    result = has_range_street or ((has_house_number or has_postcode) and has_street_word)
    log_json(
        "looks_like_address",
        query=query,
        normalized=q,
        has_house_number=has_house_number,
        has_street_word=has_street_word,
        has_postcode=has_postcode,
        has_range_street=has_range_street,
        result=result,
    )
    return result


def looks_like_building(query: str) -> bool:
    q_tokens = sorted(list(tokens(query)))
    has_building_keyword = any(word in q_tokens for word in BUILDING_KEYWORDS)

    result = has_building_keyword
    log_json(
        "looks_like_building",
        query=query,
        tokens=q_tokens,
        has_building_keyword=has_building_keyword,
        result=result,
    )
    return result


def classify_input(query: str) -> str:
    building = looks_like_building(query)
    address = looks_like_address(query)

    if building and address:
        input_type = "mixed"
    elif building:
        input_type = "building"
    else:
        input_type = "address"

    log_json(
        "classify_input",
        query=query,
        building=building,
        address=address,
        input_type=input_type,
    )
    return input_type


def extract_postal_code(query: str) -> str | None:
    match = UK_POSTCODE_RE.search(query or "")
    if not match:
        return None
    return re.sub(r"\s+", " ", match.group(1).upper()).strip()


def extract_address_fallback_query(query: str) -> str:
    s = re.sub(r"^\s*\d+[\-/]\d+[A-Z]?\s+", "", query or "", count=1)
    s = re.sub(
        r"^\s*[A-Za-z0-9'\-]+\s+(Heights|House|Court|Tower|Block|Apartments|Lodge|Manor)\b[\s,]*",
        "",
        s,
        flags=re.IGNORECASE,
    )
    fallback = re.sub(r"\s+", " ", s).strip(", ")
    log_json("extract_address_fallback_query", query=query, fallback=fallback)
    return fallback


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

    log_json("build_search_args", text=text, args=args)
    return args


def map_place_result(result: dict) -> dict:
    place = result.get("Place", {})
    point = ((place.get("Geometry") or {}).get("Point") or [None, None])
    label = place.get("Label")
    street = place.get("Street")
    house_number = place.get("AddressNumber")

    mapped = {
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

    log_json(
        "map_place_result",
        label=label,
        street=street,
        houseNumber=house_number,
        postalCode=place.get("PostalCode"),
        city=place.get("Municipality"),
        country=place.get("Country"),
        relevance=result.get("Relevance"),
        placeId=result.get("PlaceId"),
        point=point,
    )
    return mapped


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

    out = {
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

    log_json("build_output_fields", output=out)
    return out


def text_overlap_score(query: str, candidate: dict) -> float:
    q_tokens = tokens(query)
    label_tokens = tokens(candidate.get("label"))
    street_tokens = tokens(candidate.get("street"))
    postal = extract_postal_code(query)
    candidate_postal = (candidate.get("postalCode") or "").upper().strip()

    if not q_tokens:
        log_json("text_overlap_score", query=query, reason="no_query_tokens")
        return 0.0

    overlap_tokens = sorted(list(q_tokens.intersection(label_tokens.union(street_tokens))))
    overlap = len(overlap_tokens) / max(len(q_tokens), 1)
    postal_bonus = 0.2 if postal and candidate_postal and postal == candidate_postal else 0.0
    score = min(1.0, overlap + postal_bonus)

    log_json(
        "text_overlap_score",
        query=query,
        query_tokens=sorted(list(q_tokens)),
        label_tokens=sorted(list(label_tokens)),
        street_tokens=sorted(list(street_tokens)),
        overlap_tokens=overlap_tokens,
        overlap=overlap,
        query_postcode=postal,
        candidate_postcode=candidate_postal,
        postal_bonus=postal_bonus,
        score=score,
        candidate_label=candidate.get("label"),
    )
    return score


def number_mismatch_penalty(query: str, candidate: dict) -> float:
    if is_range_street_address(query):
        log_json(
            "number_mismatch_penalty",
            query=query,
            candidate_label=candidate.get("label"),
            reason="range_street_address_no_penalty",
            penalty=0.0,
        )
        return 0.0

    leading_range = LEADING_RANGE_RE.search(query or "")
    house_number = str(candidate.get("houseNumber") or "").strip().lower()
    if not leading_range:
        log_json(
            "number_mismatch_penalty",
            query=query,
            candidate_label=candidate.get("label"),
            reason="no_leading_range",
            penalty=0.0,
        )
        return 0.0

    range_text = leading_range.group(0).strip().lower()
    if house_number and house_number in range_text and house_number != range_text:
        log_json(
            "number_mismatch_penalty",
            query=query,
            candidate_label=candidate.get("label"),
            range_text=range_text,
            house_number=house_number,
            penalty=0.35,
        )
        return 0.35

    log_json(
        "number_mismatch_penalty",
        query=query,
        candidate_label=candidate.get("label"),
        range_text=range_text,
        house_number=house_number,
        penalty=0.0,
    )
    return 0.0


def building_match_score(query: str, candidate: dict) -> float:
    base_relevance = float(candidate.get("confidence") or 0.0)
    overlap = text_overlap_score(query, candidate)
    label_txt = normalize_text(candidate.get("label"))
    q_txt = normalize_text(query)
    exactish_bonus = 0.15 if q_txt and q_txt in label_txt else 0.0
    penalty = number_mismatch_penalty(query, candidate)
    score = round((base_relevance * 0.55) + (overlap * 0.45) + exactish_bonus - penalty, 4)

    log_json(
        "building_match_score",
        query=query,
        candidate_label=candidate.get("label"),
        base_relevance=base_relevance,
        overlap=overlap,
        exactish_bonus=exactish_bonus,
        penalty=penalty,
        final_score=score,
    )
    return score


def address_match_score(query: str, candidate: dict) -> float:
    base_relevance = float(candidate.get("confidence") or 0.0)
    overlap = text_overlap_score(query, candidate)
    penalty = number_mismatch_penalty(query, candidate)
    score = round((base_relevance * 0.7) + (overlap * 0.3) - penalty, 4)

    log_json(
        "address_match_score",
        query=query,
        candidate_label=candidate.get("label"),
        base_relevance=base_relevance,
        overlap=overlap,
        penalty=penalty,
        final_score=score,
    )
    return score


def search_text(query: str, event: dict, max_results: int | None = None) -> list[dict]:
    search_args = build_search_args(query, event, max_results=max_results)
    resp = loc.search_place_index_for_text(**search_args)

    raw_results = resp.get("Results", [])
    log_json(
        "search_text_response",
        query=query,
        requested_max_results=search_args.get("MaxResults"),
        result_count=len(raw_results),
        raw_labels=[(r.get("Place") or {}).get("Label") for r in raw_results],
    )

    mapped = [map_place_result(r) for r in raw_results]
    return mapped


def pick_best_address_result(query: str, candidates: list[dict]) -> dict | None:
    scored = []
    for c in candidates:
        score = address_match_score(query, c)
        scored.append((score, c))

    scored.sort(key=lambda item: item[0], reverse=True)

    log_json(
        "pick_best_address_result",
        query=query,
        threshold=0.35,
        scored_candidates=[
            {
                "score": s,
                "label": c.get("label"),
                "street": c.get("street"),
                "houseNumber": c.get("houseNumber"),
                "postalCode": c.get("postalCode"),
                "confidence": c.get("confidence"),
            }
            for s, c in scored
        ],
    )

    if not scored:
        log_json("pick_best_address_result", query=query, decision="no_candidates")
        return None

    best_score, best = scored[0]
    accepted = best_score >= 0.35

    log_json(
        "pick_best_address_result_decision",
        query=query,
        best_score=best_score,
        threshold=0.35,
        accepted=accepted,
        best_label=best.get("label"),
    )
    return best if accepted else None


def pick_best_building_result(query: str, candidates: list[dict], min_score: float) -> dict | None:
    scored = []
    for c in candidates:
        score = building_match_score(query, c)
        scored.append((score, c))

    scored.sort(key=lambda item: item[0], reverse=True)

    log_json(
        "pick_best_building_result",
        query=query,
        threshold=min_score,
        scored_candidates=[
            {
                "score": s,
                "label": c.get("label"),
                "street": c.get("street"),
                "houseNumber": c.get("houseNumber"),
                "postalCode": c.get("postalCode"),
                "confidence": c.get("confidence"),
            }
            for s, c in scored
        ],
    )

    if not scored:
        log_json("pick_best_building_result", query=query, decision="no_candidates")
        return None

    best_score, best = scored[0]
    accepted = best_score >= min_score

    log_json(
        "pick_best_building_result_decision",
        query=query,
        best_score=best_score,
        threshold=min_score,
        accepted=accepted,
        best_label=best.get("label"),
    )
    return best if accepted else None


def geocode_query(query: str, event: dict) -> tuple[dict | None, str, bool]:
    input_type = classify_input(query)
    allow_address_fallback = bool(event.get("allowAddressFallbackForBuilding", False))
    min_building_score = float(event.get("minBuildingScore", 0.72))

    log_json(
        "geocode_query_start",
        query=query,
        input_type=input_type,
        allow_address_fallback=allow_address_fallback,
        min_building_score=min_building_score,
        requested_max_results=event.get("maxResults"),
    )

    if input_type in {"building", "mixed"}:
        building_candidates = search_text(
            query,
            event,
            max_results=max(int(event.get("maxResults", 5) or 5), 5),
        )
        building_match = pick_best_building_result(query, building_candidates, min_score=min_building_score)
        if building_match:
            log_json("geocode_query_end", query=query, matched_by="buildingSearch", fallback_used=False)
            return building_match, "buildingSearch", False

        if allow_address_fallback:
            fallback_query = extract_address_fallback_query(query)
            if fallback_query and fallback_query != query:
                fallback_candidates = search_text(
                    fallback_query,
                    event,
                    max_results=max(int(event.get("maxResults", 5) or 5), 5),
                )
                address_match = pick_best_address_result(fallback_query, fallback_candidates)
                if address_match:
                    log_json(
                        "geocode_query_end",
                        query=query,
                        matched_by="addressFallback",
                        fallback_used=True,
                        fallback_query=fallback_query,
                    )
                    return address_match, "addressFallback", True

        log_json("geocode_query_end", query=query, matched_by="buildingSearch", fallback_used=False, result=None)
        return None, "buildingSearch", False

    address_candidates = search_text(
        query,
        event,
        max_results=max(int(event.get("maxResults", 5) or 5), 5),
    )
    address_match = pick_best_address_result(query, address_candidates)

    log_json(
        "geocode_query_end",
        query=query,
        matched_by="addressSearch",
        fallback_used=False,
        matched=bool(address_match),
    )
    return address_match, "addressSearch", False


def empty_response(query: str, input_type: str, matched_by: str) -> dict:
    out = {
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
    log_json("empty_response", output=out)
    return out


def process(event, context):
    payload = event
    log_json("process_start", event_type=str(type(event)), has_body=isinstance(event, dict) and "body" in event)

    if isinstance(event, dict) and "body" in event:
        try:
            payload = json.loads(event.get("body") or "{}")
        except Exception as e:
            log_json("body_parse_failed", error=str(e), body=event.get("body"))
            payload = {}

    log_json("payload_parsed", payload=payload)

    address = payload.get("address")
    if not isinstance(address, str) or not address.strip():
        log_json("validation_failed", reason="missing_or_blank_address", address=address)
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Provide a non-empty 'address' string."}),
        }

    if not PLACE_INDEX:
        log_json("validation_failed", reason="missing_place_index_name")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "PLACE_INDEX_NAME environment variable is not set."}),
        }

    query = address.strip()
    input_type = classify_input(query)

    try:
        result, matched_by, fallback_used = geocode_query(query, payload)
    except Exception as e:
        log_json("process_exception", query=query, error=str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Error calling Amazon Location", "details": str(e)}),
        }

    if not result:
        log_json("process_no_result", query=query, input_type=input_type, matched_by="noAcceptedMatch")
        return {
            "statusCode": 200,
            "body": json.dumps(empty_response(query, input_type, "noAcceptedMatch")),
        }

    out = build_output_fields(result, query, input_type, matched_by, fallback_used)
    log_json("process_success", query=query, output=out)
    return {"statusCode": 200, "body": json.dumps(out)}