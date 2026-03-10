import json
import boto3
import os

REGION = os.getenv("AWS_REGION", "eu-west-2")
PLACE_INDEX = os.getenv("PLACE_INDEX_NAME")
loc = boto3.client("location", region_name=REGION)

# AWS typically returns ISO-3166-1 alpha-3 (e.g. GBR, IRL).
# salesroce Country/Territory picklist expects ISO-3166-1 alpha-2 (e.g. GB, IE).
ISO3_TO_ISO2 = {
    "GBR": "GB",
    "IRL": "IE",
    "NLD": "NL",
    "BEL": "BE",
    "FRA": "FR",
    "ESP": "ES",
    "DEU": "DE",
    # Add more as needed
}

def to_iso2(country_code: str):
    if not country_code:
        return None
    c = str(country_code).strip().upper()
    if len(c) == 2:
        return c
    if len(c) == 3:
        return ISO3_TO_ISO2.get(c)
    return None

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

    aws_country = place.get("Country")
    iso2 = to_iso2(aws_country)

    return {
        "countryCode": iso2,
        "city": place.get("Municipality"),
        "postalCode": place.get("PostalCode"),
        "street": place.get("Street"),
        "houseNumber": place.get("AddressNumber"),
        "confidence": result.get("Relevance"),
        "label": place.get("Label"),
    }

def build_output_fields(mapped: dict) -> dict:
    house = mapped.get("houseNumber")
    street = mapped.get("street")

    if house and street:
        street_out = f"{house} {street}".strip()
    else:
        street_out = (street or "").strip()

    return {
        "countryCode": mapped.get("countryCode"),
        "city": mapped.get("city"),
        "postalCode": mapped.get("postalCode"),
        "street": street_out,
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
                    "countryCode": None,
                    "street": None,
                    "city": None,
                    "postalCode": None,
                    "confidence": None,
                }
            ),
        }

    out = build_output_fields(results[0])
    return {"statusCode": 200, "body": json.dumps(out)}