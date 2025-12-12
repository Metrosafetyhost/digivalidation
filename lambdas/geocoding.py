import json
import boto3
import os

REGION = os.getenv("AWS_REGION", "eu-west-2")
PLACE_INDEX = os.getenv("PLACE_INDEX_NAME")
loc = boto3.client("location", region_name=REGION)


def build_search_args(address: str, event: dict) -> dict:
    args = {
        "IndexName": PLACE_INDEX,
        "Text": address,
        "MaxResults": event.get("maxResults", 1)
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
    point = place.get("Geometry", {}).get("Point", [None, None])
    lon, lat = point if len(point) == 2 else (None, None)

    return {
        "label": place.get("Label"),
        "latitude": lat,
        "longitude": lon,
        "confidence": result.get("Relevance"),
        "country": place.get("Country"),
        "region": place.get("Region"),
        "subRegion": place.get("SubRegion"),
        "municipality": place.get("Municipality"),
        "postalCode": place.get("PostalCode"),
        "street": place.get("Street"),
        "houseNumber": place.get("AddressNumber"),
        "raw": place,
    }


def geocode_single_address(address: str, event: dict) -> list[dict]:
    search_args = build_search_args(address, event)
    resp = loc.search_place_index_for_text(**search_args)
    return [map_place_result(r) for r in resp.get("Results", [])]


def process(event, context):
    if not PLACE_INDEX:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "PLACE_INDEX environment variable is not set."
            })
        }

    address = event.get("address")
    if not isinstance(address, str) or not address.strip():
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Provide a non-empty 'address' string."})
        }

    try:
        results = geocode_single_address(address, event)
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "Error calling Amazon Location",
                "details": str(e)
            })
        }

    return {
        "statusCode": 200,
        "body": json.dumps({
            "query": address,
            "results": results
        })
    }
