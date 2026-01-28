import os
import json
import base64
import time
import boto3
import pymupdf  # PyMuPDF
from openai import OpenAI

S3_BUCKET = os.environ.get("ASSET_BUCKET", "metrosafetyprodfiles")
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

# Keep margin under Salesforce synchronous callout response ceiling (6MB).
# This threshold is for the *entire JSON response* (fields + cover base64 + overhead).
MAX_RESPONSE_BYTES = int(os.environ.get("MAX_RESPONSE_BYTES", "5500000"))

def _safe_json_dumps(obj: dict) -> str:
    # Compact JSON to reduce payload size
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

def _estimate_response_size_bytes(body_obj: dict) -> int:
    return len(_safe_json_dumps(body_obj).encode("utf-8"))

def _load_openai_key():
    arn = os.environ.get("OPENAI_SECRET_ARN")
    if arn:
        sm = boto3.client("secretsmanager")
        val = sm.get_secret_value(SecretId=arn)
        return val.get("SecretString") or base64.b64decode(val["SecretBinary"]).decode()
    return os.environ.get("OPENAI_API_KEY")

OPENAI_API_KEY = _load_openai_key()
s3 = boto3.client("s3")
oai = OpenAI(api_key=OPENAI_API_KEY)

def extract_cover_photo_png(pdf_bytes: bytes) -> bytes | None:
    """
    Extracts the image that occupies the largest displayed area on page 1 (index 0).
    Returns PNG bytes or None if no placed images found.
    """
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    try:
        page = doc[0]
        placed = []
        for img in page.get_images(full=True):
            xref = img[0]
            rects = page.get_image_rects(xref)
            if not rects:
                continue
            max_rect = max(rects, key=lambda r: r.width * r.height)
            placed.append((xref, max_rect.width * max_rect.height))

        if not placed:
            return None

        best_xref, _ = max(placed, key=lambda t: t[1])
        pix = pymupdf.Pixmap(doc, best_xref)
        try:
            if pix.n > 4:
                pix = pymupdf.Pixmap(pymupdf.csRGB, pix)
            return pix.tobytes("png")
        finally:
            pix = None
    finally:
        doc.close()

def extract_text_by_page(pdf_bytes: bytes, max_chars: int = 120_000) -> str:
    """
    Extract searchable text with page markers.
    Cap total chars to keep prompts stable (adjust as needed).
    """
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    try:
        chunks = []
        total = 0
        for i, page in enumerate(doc):
            txt = page.get_text("text") or ""
            if not txt.strip():
                continue
            block = f"\n[Page {i+1}]\n{txt}\n"
            if total + len(block) > max_chars:
                remaining = max_chars - total
                if remaining > 0:
                    chunks.append(block[:remaining])
                break
            chunks.append(block)
            total += len(block)
        return "".join(chunks).strip()
    finally:
        doc.close()


BASE_RULES = """
You are extracting facts from a Fire Risk Assessment PDF.

Rules:
- Do NOT guess. Only use information explicitly stated in the PDF (or clearly stated in the extracted text provided).
- If a field is not explicitly present:
  - For free-text fields, return "No specific information provided".
  - For multi-select enum fields: return the most appropriate explicit fallback from the allowed options (prefer "Not Applicable", then "Not Known", then "No specific information provided" if present).
  - For numeric/date fields, return null.
- If something is ambiguous, put details in notes (but still keep the field null if not explicit).
- Numbers must be numbers (no units). Height in meters should be numeric.
- Dates: if a completion date is stated, also provide DD/MM/YYYY.
- For multi-select classification fields: return an array of allowed enum values only.
"""

def call_extract(file_id: str, extracted_text: str, schema_name: str, schema: dict, section_instructions: str) -> dict:
    resp = oai.responses.create(
        model=MODEL,
        input=[{
            "role": "user",
            "content": [
                {"type": "input_file", "file_id": file_id},
                {"type": "input_text", "text": BASE_RULES},
                {"type": "input_text", "text": f"Section instructions:\n{section_instructions}".strip()},
                # Optional: enable this later if needed (but watch TPM)
                # {"type": "input_text", "text": f"Extracted text (page-tagged):\n{extracted_text}".strip()},
            ],
        }],
        text={
            "format": {
                "name": schema_name,
                "type": "json_schema",
                "schema": schema,
                "strict": True,
            }
        },
    )
    return json.loads(resp.output_text)

#retry wrapper to handle 429 TPM rate limits gracefully

def apply_schema_defaults(schema: dict, data: dict) -> dict:
    """Post-process model output to avoid empty/None answers where we want explicit fallbacks."""
    props = (schema or {}).get("properties", {})
    for key, prop_schema in props.items():
        if key not in data:
            continue
        val = data.get(key)

        # Fill empty multi-select arrays with an explicit fallback, if allowed by the enum.
        if prop_schema.get("type") == "array" and isinstance(val, list) and len(val) == 0:
            enum = (((prop_schema.get("items") or {}).get("enum")) or [])
            for fallback in ("Not Applicable", "Not Known", "No specific information provided"):
                if fallback in enum:
                    data[key] = [fallback]
                    break
            continue

        # Fill nullable free-text fields with the requested placeholder (except notes fields).
        t = prop_schema.get("type")
        if isinstance(t, list) and "string" in t and "null" in t and val is None:
            if not key.lower().startswith("notes"):
                data[key] = "No specific information provided"

    return data


def call_extract_with_retry(
    file_id: str,
    extracted_text: str,
    schema_name: str,
    schema: dict,
    section_instructions: str,
    retries: int = 5
) -> dict:
    for attempt in range(retries):
        try:
            result = call_extract(
                file_id=file_id,
                extracted_text=extracted_text,
                schema_name=schema_name,
                schema=schema,
                section_instructions=section_instructions,
            )
            return apply_schema_defaults(schema, result)
        except Exception as e:
            msg = str(e).lower()
            if "429" in msg or "rate limit" in msg or "tpm" in msg:
                sleep_s = 1.5 * (attempt + 1)  # 1.5s, 3s, 4.5s, 6s, 7.5s
                print(f"[{schema_name}] Rate limited (TPM). Sleeping {sleep_s:.1f}s then retrying...")
                time.sleep(sleep_s)
                continue
            raise

    raise RuntimeError(f"[{schema_name}] Failed after {retries} retries due to rate limiting")

def bytes_to_mb(n: int, precision: int = 2) -> float:
    return round(n / (1024 * 1024), precision)

# Schemas (6 passes)

def schema_identity_address():
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "uprn": {"type": ["string", "null"]},
            "building_name": {"type": ["string", "null"]},
            "building_address": {"type": ["string", "null"]},
            "address_line_1": {"type": ["string", "null"]},
            "address_line_2": {"type": ["string", "null"]},
            "address_line_3": {"type": ["string", "null"]},
            "address_line_4": {"type": ["string", "null"]},
            "postcode": {"type": ["string", "null"]},
            "notes_identity_address": {"type": ["string", "null"]},
        },
        "required": [
            "uprn","building_name","building_address","address_line_1","address_line_2",
            "address_line_3","address_line_4","postcode","notes_identity_address"
        ],
    }

def schema_fire_strategy_systems():
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "awss_sprinkler_misting": {"type": ["string", "null"]},
            "evacuation_policy": {"type": ["string", "null"]},
            "fra_completion_date_raw": {"type": ["string", "null"]},
            "fra_completion_date_ddmmyyyy": {"type": ["string", "null"]},
            "fra_producer": {"type": ["string", "null"]},
            "fra_author": {"type": ["string", "null"]},
            "notes_fire_strategy_systems": {"type": ["string", "null"]},
        },
        "required": [
            "awss_sprinkler_misting","evacuation_policy","fra_completion_date_raw",
            "fra_completion_date_ddmmyyyy","fra_producer","fra_author","notes_fire_strategy_systems"
        ],
    }

def schema_geometry_below_ground():
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "storeys": {"type": ["integer", "null"]},
            "height_m": {"type": ["number", "null"]},
            "basement_levels": {"type": ["integer", "null"]},
            "below_ground_mentioned": {"type": ["boolean", "null"]},
            "notes_geometry_below_ground": {"type": ["string", "null"]},
        },
        "required": [
            "storeys","height_m","basement_levels","below_ground_mentioned","notes_geometry_below_ground"
        ],
    }

def schema_occupancy_use():
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "total_flats": {"type": ["integer", "null"]},
            "building_uses": {"type": ["string", "null"]},
            "general_needs": {"type": ["string", "null"]},
            "main_occupancy_classification": {"type": ["string", "null"]},
            "total_building_occupancy": {"type": ["integer", "null"]},
            "other_occupancies": {"type": ["string", "null"]},
            "residents_per_flat": {"type": ["integer", "null"]},
            "uses_in_addition_to_residential": {"type": ["string", "null"]},
            "notes_occupancy_use": {"type": ["string", "null"]},
        },
        "required": [
            "total_flats","building_uses","general_needs","main_occupancy_classification",
            "total_building_occupancy","other_occupancies","residents_per_flat",
            "uses_in_addition_to_residential","notes_occupancy_use"
        ],
    }

def schema_construction_external_walls():
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "main_external_wall_type": {"type": ["string", "null"]},
            "walling_infill": {"type": ["string", "null"]},
            "proximity_to_escape_routes": {"type": ["string", "null"]},
            "proximity_to_openings": {"type": ["string", "null"]},
            "main_walling_type_percent": {"type": ["number", "null"]},
            "year_built": {"type": ["integer", "null"]},
            "building_construction_description": {"type": ["string", "null"]},
            "notes_construction_external_walls": {"type": ["string", "null"]},
        },
        "required": [
            "main_external_wall_type","walling_infill","proximity_to_escape_routes",
            "proximity_to_openings","main_walling_type_percent","year_built",
            "building_construction_description","notes_construction_external_walls"
        ],
    }

def schema_classifications():
    building_classification_enum = [
        "Residential 1a (Flats)",
        "Residential 1b",
        "Residential 1c",
        "Residential (Institutional) 2a",
        "Residential (other) 2b",
        "Office 3",
        "Shop & Commercial 4",
        "Assembly & Recreation 5",
        "Industrial 6",
        "Storage & Other non-residential 7(a)",
        "Car Parks 7(b)",
        "Not Applicable",
    ]

    structural_frame_enum = [
        "Traditional Masonry Cavity Wall",
        "Solid Masonry Wall",
        "Concrete Structural Frame",
        "Steel Structural Frame",
        "SFS",
        "Timber Frame",
        "SIP Panel",
        "MMC",
        "Not Known",
    ]

    infill_wall_types_enum = [
        "Brick / Block",
        "Lightweight Timber Framing (LTF)",
        "Lightweight Steel Framing (LSF)",
        "SIP Panel (N/A)",
        "SFS",
        "MMC",
        "Not Applicable",
        "Timber Structural Frame",
    ]

    external_wall_types_enum = [
        "Masonry Cavity Wall (Fully Filled with Mineral Wool/Rockwool)",
        "Solid Masonry/Stone",
        "Metal Panel Ventilated Rainscreen (with non-combustible insulation)",
        "Metal Panel Ventilated Rainscreen (with combustible insulation)",
        "Ceramic / Stone Ventilated Rainscreen (with non-combustible insulation)",
        "Ceramic / Stone Ventilated Rainscreen (with combustible insulation)",
        "Cementitious Panel Ventilated Rainscreen (with non-combustible insulation)",
        "HPL Ventilated Rainscreen (with non-combustible insulation)",
        "HPL Ventilated Rainscreen (with combustible insulation)",
        "Render / EPS",
        "Render / Rockwool",
        "Timber Cladding",
        "ACM",
        "Metal Spandrel Panel",
        "UPVC Spandrel Panel",
        "Glazed Curtain Walling",
        "Metal Sandwich Panel",
        "Not Applicable",
        "Masonry Cavity Wall (partial fill combustible insulation)",
        "Render / Concrete Block",
        "Render / Unknown (on Traditional Masonry Cavity Wall Building)",
        "Metal Standing Seam",
        "Tiled (mansard face / dorner etc)",
    ]

    balcony_materials_enum = [
        "Not Applicable",
        "Metal",
        "Timber",
        "Glass",
        "HPL",
    ]

    attachment_types_enum = [
        "Not Applicable",
        "Timber Decking",
        "Decking (non-combustible)",
        "Timber Railings",
        "Timber Framing (Balcony)",
        "Steel Framing (Balcony)",
        "Breize Soleil (Combustible)",
        "Breize Soleil (non-combustible)",
        "Solar Shading (combustible)",
        "Solar Shading (non-combustible)",
        "Not Known",
    ]

    use_classification_enum = [
        "No specific information provided",
        "Flat 1a",
        "Residential Institutional 2a",
        "Residential Other 2b",
        "Office 3",
        "Shop / Commercial 4",
        "Assembly & Recreation 5",
        "Industrial 6",
        "Storage 7a",
        "Car Park 7b (<2.5 tonnes)",
    ]

    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "building_classification_relevant": {
                "type": "array",
                "items": {"type": "string", "enum": building_classification_enum},
            },
            "structural_frame_classifications": {
                "type": "array",
                "items": {"type": "string", "enum": structural_frame_enum},
            },
            "infill_wall_type_classifications": {
                "type": "array",
                "items": {"type": "string", "enum": infill_wall_types_enum},
            },
            "external_wall_types_relevant": {
                "type": "array",
                "items": {"type": "string", "enum": external_wall_types_enum},
            },
            "balcony_materials": {
                "type": "array",
                "items": {"type": "string", "enum": balcony_materials_enum},
            },
            "attachment_types_relevant": {
                "type": "array",
                "items": {"type": "string", "enum": attachment_types_enum},
            },
            "secondary_use_classification": {
                "type": "array",
                "items": {"type": "string", "enum": use_classification_enum},
            },
            "third_use_classification": {
                "type": "array",
                "items": {"type": "string", "enum": use_classification_enum},
            },
            "fourth_use_classification": {
                "type": "array",
                "items": {"type": "string", "enum": use_classification_enum},
            },
            "notes_classifications": {"type": ["string", "null"]},
        },
        "required": [
            "building_classification_relevant",
            "structural_frame_classifications",
            "infill_wall_type_classifications",
            "external_wall_types_relevant",
            "balcony_materials",
            "attachment_types_relevant",
            "secondary_use_classification",
            "third_use_classification",
            "fourth_use_classification",
            "notes_classifications",
        ],
    }


def process(event, context):
    try:
        payload = event if isinstance(event, dict) else json.loads(event["body"])
        bucket = payload.get("bucket", S3_BUCKET)
        pdf_key = payload["pdf_s3_key"]
        cover_key = payload.get("cover_s3_key")  # optional

        include_cover_bytes = payload.get("include_cover_bytes", True)

        print(f"Loading PDF from bucket={bucket}, key={pdf_key}")
        obj = s3.get_object(Bucket=bucket, Key=pdf_key)
        pdf_bytes = obj["Body"].read()

        # Extract cover image
        cover_png_bytes = extract_cover_photo_png(pdf_bytes)

        # Write cover image to S3 for traceability/fallback (generate a key if not provided)
        cover_written = False
        if cover_png_bytes:
            if not cover_key:
                base = pdf_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
                cover_key = f"WorkOrders/covers/{base}.png"

            s3.put_object(
                Bucket=bucket,
                Key=cover_key,
                Body=cover_png_bytes,
                ContentType="image/png",
            )
            cover_written = True

        # Extract searchable text (not currently passed to model)
        extracted_text = extract_text_by_page(pdf_bytes)

        # Upload PDF once
        up = oai.files.create(
            file=("document.pdf", pdf_bytes),
            purpose="user_data",
        )
        file_id = up.id

        pass_specs = [
            {"name": "identity_address", "schema": schema_identity_address(),
             "instructions": "Extract: UPRN, building name, full address, address lines 1-4, postcode."},

            {"name": "fire_strategy_systems", "schema": schema_fire_strategy_systems(),
             "instructions": "Extract: AWSS/sprinkler/misting info, evacuation policy, FRA completion date, producer/company, author/assessor."},

            {"name": "geometry_below_ground", "schema": schema_geometry_below_ground(),
             "instructions": "Extract: storeys, height_m, basement_levels, below_ground_mentioned."},

            {"name": "occupancy_use", "schema": schema_occupancy_use(),
             "instructions": "Extract: total flats, uses, general needs, main occupancy classification, occupancy, other occupancies, residents per flat, uses in addition to residential."},

            {"name": "construction_external_walls", "schema": schema_construction_external_walls(),
             "instructions": "Extract: external wall type, infill, proximity escape routes/openings, % coverage, year built, construction description."},

            {"name": "classifications", "schema": schema_classifications(),
             "instructions": "Fill classification lists ONLY using explicit evidence; return empty arrays if not supported."},
        ]

        fields = {}
        pass_errors = {}

        for spec in pass_specs:
            try:
                out = call_extract_with_retry(
                    file_id=file_id,
                    extracted_text=extracted_text,
                    schema_name=spec["name"],
                    schema=spec["schema"],
                    section_instructions=spec["instructions"],
                )
                fields.update(out)
            except Exception as e:
                pass_errors[spec["name"]] = str(e)

        # Build base response (single call)
        body_obj = {
            "ok": True,
            "fields": fields,
            "pass_errors": pass_errors,
            "cover_image_written": cover_written,
            "cover_s3_key": cover_key if cover_written else None,
            "cover": None,  # filled conditionally below
        }

        # Conditionally include cover bytes inline (only if total response stays below threshold)
        if include_cover_bytes and cover_png_bytes:
            cover_b64 = base64.b64encode(cover_png_bytes).decode("utf-8")

            candidate = dict(body_obj)
            candidate["cover"] = {
                "content_type": "image/png",
                "bytes_base64": cover_b64,
            }

            est = _estimate_response_size_bytes(candidate)
            if est <= MAX_RESPONSE_BYTES:
                body_obj = candidate
            else:
                body_obj["cover_too_large"] = True
                body_obj["cover_estimated_response_bytes"] = est
                body_obj["max_response_bytes"] = MAX_RESPONSE_BYTES
                
        final_size = _estimate_response_size_bytes(body_obj)
        final_size_bytes = _estimate_response_size_bytes(body_obj)

        body_obj["response_size"] = {
            "bytes": final_size_bytes,
            "megabytes": bytes_to_mb(final_size_bytes),
        }
        print(f"[DEBUG] Final response size (bytes): {final_size}")

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": _safe_json_dumps(body_obj),
        }

    except Exception as e:
        print("Error:", e)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": False, "error": str(e)}),
        }
