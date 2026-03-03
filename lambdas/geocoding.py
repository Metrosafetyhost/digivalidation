import json
import boto3
import os

REGION = os.getenv("AWS_REGION", "eu-west-2")
PLACE_INDEX = os.getenv("PLACE_INDEX_NAME")
loc = boto3.client("location", region_name=REGION)

COUNTRY_NAME_MAP = {
    "GBR": "United Kingdom",
    "GB": "United Kingdom",
    "UK": "United Kingdom",
    "IRL": "Ireland",
    "IE": "Ireland",
    "NLD": "Netherlands",
    "NL": "Netherlands",
    "BEL": "Belgium",
    "BE": "Belgium",
}

def standardise_country(code):
    if not code:
        return None
    c = str(code).strip().upper()
    return COUNTRY_NAME_MAP.get(c, c)


def build_search_args(address: str, event: dict) -> dict:
    args = {
        "IndexName": PLACE_INDEX,
        "Text": address,
        "MaxResults": event.get("maxResults", 1),
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

    return {
        "country": standardise_country(place.get("Country")),
        "city": place.get("Municipality"),
        "postalCode": place.get("PostalCode"),
        "street": place.get("Street"),
        "houseNumber": place.get("AddressNumber"),
        "confidence": result.get("Relevance"),
        "label": place.get("Label"),
    }


def build_output_fields(mapped: dict) -> dict:
    # Build "37A Waterloo Street" style street
    house = mapped.get("houseNumber")
    street = mapped.get("street")

    if house and street:
        street_out = f"{house} {street}".strip()
    else:
        street_out = (street or "").strip()

    return {
        "country": mapped.get("country"),
        "city": mapped.get("city"),
        "postalCode": mapped.get("postalCode"),
        "street": street_out,
        # optional: keep confidence for debugging
        "confidence": mapped.get("confidence"),
    }


def geocode_single_address(address: str, event: dict) -> list[dict]:
    search_args = build_search_args(address, event)
    resp = loc.search_place_index_for_text(**search_args)
    return [map_place_result(r) for r in resp.get("Results", [])]


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

    try:
        results = geocode_single_address(address, payload)
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Error calling Amazon Location", "details": str(e)}),
        }

    if not results:
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "country": None,
                    "street": None,
                    "city": None,
                    "postalCode": None,
                    "confidence": None,
                }
            ),
        }

    best = results[0]
    out = build_output_fields(best)

    return {"statusCode": 200, "body": json.dumps(out)}
