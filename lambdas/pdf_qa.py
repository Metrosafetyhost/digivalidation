import os
import json
import base64
import time
import boto3
import pymupdf  # PyMuPDF
import re
from openai import OpenAI

# ----------------------------
# Async job infra
# ----------------------------
DEWRRA_JOBS_TABLE = os.environ.get("DEWRRA_JOBS_TABLE", "dewrra_jobs")

# Where the worker writes the FINAL JSON result for GET /dewrra/results/{jobId}
# Store results under the SAME WorkOrder folder as the PDF/cover:
#   s3://ASSET_BUCKET/WorkOrders/<workOrderId>/results/<jobId>.json
RESULTS_FOLDER = os.environ.get("DEWRRA_RESULTS_FOLDER", "results")
COVER_BUCKET = os.environ.get("COVER_BUCKET", "metrosafetyprod")

S3_BUCKET = os.environ.get("ASSET_BUCKET", "metrosafetyprodfiles")
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

# Vision / photo analysis model (used only when enable_photo_analysis=true in the request)
PHOTO_ANALYSIS_MODEL = os.environ.get("PHOTO_ANALYSIS_MODEL", MODEL)

# Vision / photo annotation model (used only when enable_photo_annotation=true in the request)
PHOTO_ANNOTATION_MODEL = os.environ.get("PHOTO_ANNOTATION_MODEL", "gpt-image-1")

# Keep margin under Salesforce synchronous callout response ceiling (6MB).
MAX_RESPONSE_BYTES = int(os.environ.get("MAX_RESPONSE_BYTES", "5500000"))

# Include extracted text to improve consistency (bounded)
INCLUDE_EXTRACTED_TEXT_DEFAULT = os.environ.get("INCLUDE_EXTRACTED_TEXT_DEFAULT", "false").lower() == "true"
EXTRACTED_TEXT_MAX_CHARS_PER_CALL = int(os.environ.get("EXTRACTED_TEXT_MAX_CHARS_PER_CALL", "8000"))


# ----------------------------
# Photo analysis (inlined from qa_photo_analysis.py)
# ----------------------------

PHOTO_RULES = """
You are analysing a building exterior photo shown on the front cover of a Fire Risk Assessment report.

Rules:
- Be helpful, but do not invent facts. If something is unclear, say "Not known from the photo".
- Infer only where it is reasonable from visible cues (e.g., basement lightwells/steps/grade changes).
- Write a single summary suitable for a Salesforce long text field.
- Keep it structured and easy to read (short labelled lines).
- Do NOT output JSON keys other than "summary".
""".strip()


def photo_summary_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {"summary": {"type": "string"}},
        "required": ["summary"],
    }


def analyse_cover_png(oai_client: OpenAI, model: str, cover_png_bytes: bytes) -> dict:
    """Return {"summary": "..."} using OpenAI Responses API."""
    img_b64 = base64.b64encode(cover_png_bytes).decode("utf-8")
    image_url = f"data:image/png;base64,{img_b64}"

    schema = photo_summary_schema()

    prompt = (
        "Write ONE summary string starting with exactly:\n"
        "\"From the image displayed on the front cover of this report:\"\n\n"
        "Answer the following as short labelled lines (use the label text exactly):\n"
        "- Floors visible:\n"
        "- Basement obvious:\n"
        "- Estimated height (m):\n"
        "- Estimated year built (or era):\n"
        "- Main external wall type:\n"
        "- Other external wall types visible:\n"
        "- Approx. wall type coverage (overall, % by material):\n"
        "- Balconies present:\n"
        "- Balcony materials:\n"
        "- Materials near openings (and approx distance):\n"
        "- Materials near escape doors (and approx distance):\n"
        "- Notes/uncertainties:\n\n"
        "Guidance:\n"
        "- For 'Basement obvious', infer from visible lightwells, stepped entrances, raised/lowered ground levels, or ventilation grilles; otherwise say 'Not known from the photo'.\n"
        "- For distances, approximate in metres (e.g., 'within ~1m', '2–3m').\n"
        "- For 'Approx. wall type coverage (overall, % by material)', estimate the visible proportion of each wall material as percentages that sum to ~100%. Format like: 'Brick ~65%, Timber cladding ~35%'. If only one material is visible, use ~100% for that material.\n"
        "- If uncertain for any line, write 'Not known from the photo'."
    )

    resp = oai_client.responses.create(
        model=model,
        input=[
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": PHOTO_RULES},
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": image_url},
                ],
            }
        ],
        text={
            "format": {
                "name": "dewrra_photo_summary",
                "type": "json_schema",
                "schema": schema,
                "strict": True,
            }
        },
    )

    return json.loads(resp.output_text)


def annotate_cover_png_openai(oai_client: OpenAI, model: str, cover_png_bytes: bytes, summary: str | None = None) -> bytes:
    """Return annotated cover PNG bytes using OpenAI Images edit endpoint."""
    prompt = (
        "Create a professional building-photo annotation overlay.\n"
        "\n"
        "STYLE (must follow):\n"
        "- Use light grey rounded callout boxes with a thin dark border.\n"
        "- Use ONE consistent font and consistent sizing across all labels.\n"
        "- Use leader lines with arrowheads from each callout box to the referenced feature.\n"
        "- Use dashed outlines ONLY to indicate wall-material zones (keep outlines thin; do not cover details).\n"
        "- Place callouts near image edges where possible; avoid covering doors/windows/signage.\n"
        "\n"
        "CONTENT (only if visible):\n"
        "- Add a 'Floors' callout (e.g., Ground/First/Second/Attic/Loft if visible).\n"
        "- Add an estimated height callout in meters.\n"
        "- Add an estimated build year/era callout.\n"
        "- Identify main external wall materials. For each material, label it and include an approximate % coverage range.\n"
        "- If balconies are visible, add a balcony callout and (if visible) balcony material.\n"
        "\n"
        "QUALITY RULES:\n"
        "- SPELLING MUST BE CORRECT.\n"
        "- Do NOT invent details not visible. If uncertain, omit that callout.\n"
        "- Keep to a maximum of 8 callouts.\n"
        "- Make the overlay clean, aligned, and consistent.\n"
        "\n"
        "NO DUPLICATES / CONSOLIDATION:\n"
        "- Do NOT duplicate information or create near-duplicate callouts.\n"
        "- Only ONE callout per category:\n"
        "  * Age/era (exactly one)\n"
        "  * Floors/storeys (exactly one)\n"
        "  * Height (exactly one)\n"
        "  * Each wall material (at most one per material)\n"
        "  * Balcony (at most one)\n"
        "- If uncertain between two eras/values, choose ONE and express uncertainty as a range/approx in a SINGLE callout\n"
        "  (e.g., 'Estimated age: c. 1900–1930' or 'Early 20th century (approx.)'). Do NOT add multiple era labels.\n"
    )

    if summary:
        prompt += "\nUse these findings for labels (do not add extra facts):\n" + summary

    # openai-python supports multipart tuples: (filename, bytes, content_type)
    img_file = ("cover.png", cover_png_bytes, "image/png")

    resp = oai_client.images.edit(
        model=model,
        image=[img_file],
        prompt=prompt,
    )

    b64 = resp.data[0].b64_json
    return base64.b64decode(b64)


# ----------------------------
# Helpers
# ----------------------------

def _safe_json_dumps(obj: dict) -> str:
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
ddb = boto3.resource("dynamodb")
jobs_table = ddb.Table(DEWRRA_JOBS_TABLE)

oai = OpenAI(api_key=OPENAI_API_KEY)


def _now() -> int:
    return int(time.time())


def _ddb_update(job_id: str, **attrs):
    attrs["updatedAt"] = _now()

    expr_parts = []
    ean = {}
    eav = {}

    for k, v in attrs.items():
        ean[f"#{k}"] = k
        eav[f":{k}"] = v
        expr_parts.append(f"#{k} = :{k}")

    jobs_table.update_item(
        Key={"jobId": job_id},
        UpdateExpression="SET " + ", ".join(expr_parts),
        ExpressionAttributeNames=ean,
        ExpressionAttributeValues=eav,
    )


def _ddb_get(job_id: str) -> dict | None:
    res = jobs_table.get_item(Key={"jobId": job_id})
    return res.get("Item")


def _write_result_to_s3(job_id: str, workorder_id: str, body_obj: dict) -> tuple[str, str]:
    bucket = S3_BUCKET
    key = f"WorkOrders/{workorder_id}/{RESULTS_FOLDER}/{job_id}.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=_safe_json_dumps(body_obj).encode("utf-8"),
        ContentType="application/json",
    )
    return bucket, key


def find_any_pdf_key(bucket: str, workorder_id: str) -> str:
    prefix = f"WorkOrders/{workorder_id}/"
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".pdf") and obj.get("Size", 0) > 0:
                return key

    raise FileNotFoundError(f"No PDF found under s3://{bucket}/{prefix}")


def normalize_uk_postcode(value: str | None) -> str | None:
    if not value:
        return value
    raw = str(value).upper()
    compact = re.sub(r"[^A-Z0-9]", "", raw)
    if len(compact) >= 5:
        return compact[:-3] + " " + compact[-3:]
    return compact


def join_address_lines(*parts: str | None) -> str | None:
    cleaned = []
    for p in parts:
        if p is None:
            continue
        s = str(p).strip()
        if not s:
            continue
        if s == "No specific information provided":
            continue
        cleaned.append(s)
    return ", ".join(cleaned) if cleaned else None


def extract_cover_photo_png(pdf_bytes: bytes) -> bytes | None:
    """Extract most likely cover photo from page 1."""
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    try:
        page = doc[0]
        page_rect = page.rect
        page_area = page_rect.width * page_rect.height
        page_h = page_rect.height

        candidates = []
        for img in page.get_images(full=True):
            xref = img[0]
            rects = page.get_image_rects(xref)
            if not rects:
                continue

            rect = max(rects, key=lambda r: r.width * r.height)
            area = rect.width * rect.height
            area_ratio = area / page_area if page_area else 0

            try:
                pix = pymupdf.Pixmap(doc, xref)
                pix_area = pix.width * pix.height
            finally:
                pix = None

            candidates.append((xref, rect, area, area_ratio, pix_area))

        if not candidates:
            return None

        non_tiny = [c for c in candidates if c[4] >= 150_000] or candidates
        non_bg = [c for c in non_tiny if c[3] < 0.85]
        pool = non_bg if non_bg else non_tiny

        def score(c):
            _xref, rect, area, _ratio, _pix_area = c
            y_mid = (rect.y0 + rect.y1) / 2.0
            lower_bias = 0.7 + 0.6 * (y_mid / page_h)
            return area * lower_bias

        best_xref, *_ = max(pool, key=score)

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
""".strip()


def call_extract(
    file_id: str,
    extracted_text: str,
    schema_name: str,
    schema: dict,
    section_instructions: str,
    include_extracted_text: bool = False,
    extracted_text_max_chars: int = EXTRACTED_TEXT_MAX_CHARS_PER_CALL,
) -> dict:
    content = [
        {"type": "input_file", "file_id": file_id},
        {"type": "input_text", "text": BASE_RULES},
        {"type": "input_text", "text": f"Section instructions:\n{section_instructions}".strip()},
    ]
    if include_extracted_text and extracted_text:
        snippet = extracted_text[:extracted_text_max_chars]
        content.append({"type": "input_text", "text": f"Extracted text (page-tagged, truncated):\n{snippet}".strip()})

    resp = oai.responses.create(
        model=MODEL,
        input=[{"role": "user", "content": content}],
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


def apply_schema_defaults(schema: dict, data: dict) -> dict:
    props = (schema or {}).get("properties", {})
    for key, prop_schema in props.items():
        if key not in data:
            continue
        val = data.get(key)

        if prop_schema.get("type") == "array" and isinstance(val, list) and len(val) == 0:
            enum = (((prop_schema.get("items") or {}).get("enum")) or [])
            for fallback in ("Not Applicable", "Not Known", "No specific information provided"):
                if fallback in enum:
                    data[key] = [fallback]
                    break
            continue

        t = prop_schema.get("type")
        if isinstance(t, list) and "string" in t and "null" in t and val is None:
            if not key.lower().startswith("notes"):
                data[key] = "No specific information provided"

    if "postcode" in data and data.get("postcode"):
        data["postcode"] = normalize_uk_postcode(data.get("postcode"))

    if "building_name" in data and "building_address" in data:
        bn = data.get("building_name")
        ba = data.get("building_address")
        if (bn is None or str(bn).strip() == "" or bn == "No specific information provided"):
            if ba is not None and str(ba).strip() != "" and ba != "No specific information provided":
                data["building_name"] = ba

    if "building_address" in data:
        ba = data.get("building_address")
        if ba is None or str(ba).strip() == "" or ba == "No specific information provided":
            fallback = join_address_lines(
                data.get("address_line_1"),
                data.get("address_line_2"),
                data.get("address_line_3"),
                data.get("address_line_4"),
                data.get("postcode"),
            )
            if fallback:
                data["building_address"] = fallback

    if "building_name" in data and "building_address" in data:
        bn = data.get("building_name")
        ba = data.get("building_address")
        if (bn is None or str(bn).strip() == "" or bn == "No specific information provided"):
            if ba is not None and str(ba).strip() != "" and ba != "No specific information provided":
                data["building_name"] = ba

    if "total_flats" in data and "residents_per_flat" in data and "total_building_occupancy" in data:
        tf = data.get("total_flats")
        rpf = data.get("residents_per_flat")
        tbo = data.get("total_building_occupancy")

        if tbo is None and isinstance(tf, int) and isinstance(rpf, int):
            data["total_building_occupancy"] = tf * rpf
            tbo = data["total_building_occupancy"]

        if data.get("residents_per_flat") is None and isinstance(tbo, int) and isinstance(tf, int) and tf > 0:
            if tbo % tf == 0:
                data["residents_per_flat"] = tbo // tf
                rpf = data["residents_per_flat"]

        if data.get("total_flats") is None and isinstance(tbo, int) and isinstance(rpf, int) and rpf > 0:
            if tbo % rpf == 0:
                data["total_flats"] = tbo // rpf

    return data


def call_extract_with_retry(
    file_id: str,
    extracted_text: str,
    schema_name: str,
    schema: dict,
    section_instructions: str,
    retries: int = 5,
    include_extracted_text: bool = False,
) -> dict:
    for attempt in range(retries):
        try:
            result = call_extract(
                file_id=file_id,
                extracted_text=extracted_text,
                schema_name=schema_name,
                schema=schema,
                section_instructions=section_instructions,
                include_extracted_text=include_extracted_text,
            )
            return apply_schema_defaults(schema, result)
        except Exception as e:
            msg = str(e).lower()
            if "429" in msg or "rate limit" in msg or "tpm" in msg:
                sleep_s = 4.0 * (attempt + 1)
                print(f"[{schema_name}] Rate limited (TPM). Sleeping {sleep_s:.1f}s then retrying...")
                time.sleep(sleep_s)
                continue
            raise

    raise RuntimeError(f"[{schema_name}] Failed after {retries} retries due to rate limiting")


def bytes_to_mb(n: int, precision: int = 2) -> float:
    return round(n / (1024 * 1024), precision)


# ----------------------------
# Schemas (unchanged)
# ----------------------------

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
            "uprn",
            "building_name",
            "building_address",
            "address_line_1",
            "address_line_2",
            "address_line_3",
            "address_line_4",
            "postcode",
            "notes_identity_address",
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
            "awss_sprinkler_misting",
            "evacuation_policy",
            "fra_completion_date_raw",
            "fra_completion_date_ddmmyyyy",
            "fra_producer",
            "fra_author",
            "notes_fire_strategy_systems",
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
            "storeys",
            "height_m",
            "basement_levels",
            "below_ground_mentioned",
            "notes_geometry_below_ground",
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
            "main_occupancy_classification": {
                "type": ["string", "null"],
                "enum": [
                    "Residential 1a (Flats)",
                    "Residential 1b",
                    "Residential 1c",
                    "Residential (Institutional) 2a",
                    "Residential 2(b) Other",
                    "Office 3",
                    "Shop & Commercial 4",
                    "Assembly & Recreation 5",
                    "Industrial 6",
                    "Storage & Other non-residential 7(a)",
                    "Car Parks 7(b)",
                    "Not Applicable",
                    "No specific information provided",
                ],
            },
            "total_building_occupancy": {"type": ["integer", "null"]},
            "other_occupancies": {"type": ["string", "null"]},
            "residents_per_flat": {"type": ["integer", "null"]},
            "uses_in_addition_to_residential": {"type": ["string", "null"]},
            "notes_occupancy_use": {"type": ["string", "null"]},
        },
        "required": [
            "total_flats",
            "building_uses",
            "general_needs",
            "main_occupancy_classification",
            "total_building_occupancy",
            "other_occupancies",
            "residents_per_flat",
            "uses_in_addition_to_residential",
            "notes_occupancy_use",
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
            "main_external_wall_type",
            "walling_infill",
            "proximity_to_escape_routes",
            "proximity_to_openings",
            "main_walling_type_percent",
            "year_built",
            "building_construction_description",
            "notes_construction_external_walls",
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

    balcony_materials_enum = ["Not Applicable", "Metal", "Timber", "Glass", "HPL"]

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


# ----------------------------
# Main logic
# ----------------------------

def _run_pdfqa_logic(payload: dict, event: dict | None = None) -> dict:
    bucket = payload.get("bucket", S3_BUCKET)

    workorder_id = payload.get("workOrderId") or payload.get("workorder_id") or payload.get("work_order_id")

    if "pdf_s3_key" in payload and payload["pdf_s3_key"]:
        pdf_key = payload["pdf_s3_key"]
    else:
        if not workorder_id:
            raise KeyError("Missing 'workOrderId' (or 'pdf_s3_key') in request body")
        pdf_key = find_any_pdf_key(bucket=bucket, workorder_id=workorder_id)

    cover_key = None
    if workorder_id:
        cover_key = f"WorkOrders/{workorder_id}/covers/{workorder_id}_cover.png"

    include_cover_bytes = payload.get("include_cover_bytes", True)
    include_extracted_text = payload.get("include_extracted_text", INCLUDE_EXTRACTED_TEXT_DEFAULT)

    print(f"Loading PDF from bucket={bucket}, key={pdf_key}")
    obj = s3.get_object(Bucket=bucket, Key=pdf_key)
    pdf_bytes = obj["Body"].read()

    cover_png_bytes = extract_cover_photo_png(pdf_bytes)

    cover_written = False
    if cover_png_bytes:
        if not cover_key:
            base = pdf_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            cover_key = f"WorkOrders/covers/{base}.png"

        s3.put_object(
            Bucket=COVER_BUCKET,
            Key=cover_key,
            Body=cover_png_bytes,
            ContentType="image/png",
        )
        cover_written = True

    # OPTIONAL photo analysis
    enable_photo_analysis = (payload.get("enable_photo_analysis") is True)
    photo_summary = None
    photo_error = None

    # OPTIONAL photo annotation
    enable_photo_annotation = (payload.get("enable_photo_annotation") is True)
    photo_annotated_written = False
    photo_annotated_key = None
    photo_annotation_error = None

    if enable_photo_analysis:
        if not cover_png_bytes:
            photo_error = "Cover image could not be extracted from PDF"
        else:
            try:
                out = analyse_cover_png(oai_client=oai, model=PHOTO_ANALYSIS_MODEL, cover_png_bytes=cover_png_bytes)
                photo_summary = out.get("summary")
            except Exception as e:
                photo_error = str(e)

    if enable_photo_annotation:
        if not cover_png_bytes:
            photo_annotation_error = "Cover image could not be extracted from PDF"
        else:
            try:
                annotated_png_bytes = annotate_cover_png_openai(
                    oai_client=oai,
                    model=PHOTO_ANNOTATION_MODEL,
                    cover_png_bytes=cover_png_bytes,
                    summary=photo_summary,
                )

                if workorder_id:
                    photo_annotated_key = f"WorkOrders/{workorder_id}/covers/{workorder_id}_cover_annotated.png"
                else:
                    base = pdf_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
                    photo_annotated_key = f"WorkOrders/covers/{base}_annotated.png"

                s3.put_object(
                    Bucket=COVER_BUCKET,
                    Key=photo_annotated_key,
                    Body=annotated_png_bytes,
                    ContentType="image/png",
                )
                photo_annotated_written = True
            except Exception as e:
                photo_annotation_error = str(e)

    extracted_text = extract_text_by_page(pdf_bytes)

    up = oai.files.create(
        file=("document.pdf", pdf_bytes),
        purpose="user_data",
    )
    file_id = up.id

    pass_specs = [
        {
            "name": "identity_address",
            "schema": schema_identity_address(),
            "instructions": "Extract: UPRN, building name, full address, address lines 1-4, postcode.",
            "use_extracted_text": True,
        },
        {
            "name": "fire_strategy_systems",
            "schema": schema_fire_strategy_systems(),
            "instructions": "Extract: AWSS/sprinkler/misting info, evacuation policy, FRA completion date, producer/company, author/assessor.",
            "use_extracted_text": False,
        },
        {
            "name": "geometry_below_ground",
            "schema": schema_geometry_below_ground(),
            "instructions": "Extract: storeys, height_m, basement_levels, below_ground_mentioned.",
            "use_extracted_text": False,
        },
        {
            "name": "occupancy_use",
            "schema": schema_occupancy_use(),
            "instructions": "Extract: total flats, uses, general needs, main occupancy classification, occupancy, other occupancies, residents per flat, uses in addition to residential.",
            "use_extracted_text": False,
        },
        {
            "name": "construction_external_walls",
            "schema": schema_construction_external_walls(),
            "instructions": (
                "Extract: external wall type, infill, proximity escape routes/openings, % coverage, year built, construction description.\n"
                "If a 'Construction information' table is present, use that as the primary source for Main External Wall Type / wall construction details."
            ),
            "use_extracted_text": True,
        },
        {
            "name": "classifications",
            "schema": schema_classifications(),
            "instructions": "Fill classification lists ONLY using explicit evidence",
            "use_extracted_text": False,
        },
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
                include_extracted_text=(include_extracted_text and spec.get("use_extracted_text", False)),
            )
            fields[spec["name"]] = out

            if spec["name"] == "classifications" and isinstance(fields.get("classifications"), dict):
                cls = fields["classifications"]

                def _join_if_list(v):
                    if isinstance(v, list):
                        return ", ".join([str(x) for x in v if x is not None and str(x).strip() != ""])
                    return v

                cls["building_classification_relevant"] = _join_if_list(cls.get("building_classification_relevant"))
                cls["structural_frame_classifications"] = _join_if_list(cls.get("structural_frame_classifications"))
                cls["infill_wall_type_classifications"] = _join_if_list(cls.get("infill_wall_type_classifications"))
                cls["external_wall_types_relevant"] = _join_if_list(cls.get("external_wall_types_relevant"))
                cls["balcony_materials"] = _join_if_list(cls.get("balcony_materials"))
                cls["attachment_types_relevant"] = _join_if_list(cls.get("attachment_types_relevant"))
                cls["secondary_use_classification"] = _join_if_list(cls.get("secondary_use_classification"))
                cls["third_use_classification"] = _join_if_list(cls.get("third_use_classification"))
                cls["fourth_use_classification"] = _join_if_list(cls.get("fourth_use_classification"))

                fields["classifications"] = cls

        except Exception as e:
            pass_errors[spec["name"]] = str(e)

    if photo_summary:
        fields["photo_summary"] = photo_summary
    if photo_error:
        pass_errors["photo_summary"] = photo_error
    if photo_annotated_written:
        fields["photo_annotated_s3_key"] = photo_annotated_key
    if photo_annotation_error:
        pass_errors["photo_annotation"] = photo_annotation_error

    body_obj = {
        "ok": True,
        "fields": fields,
        "pass_errors": pass_errors,
        "cover_image_written": cover_written,
        "cover_s3_key": cover_key if cover_written else None,
        "cover_annotated_written": photo_annotated_written,
        "cover_annotated_s3_key": photo_annotated_key if photo_annotated_written else None,
        "cover": None,
    }

    if include_cover_bytes and cover_png_bytes:
        cover_b64 = base64.b64encode(cover_png_bytes).decode("utf-8")

        candidate = dict(body_obj)
        candidate["cover"] = {"content_type": "image/png", "bytes_base64": cover_b64}

        est = _estimate_response_size_bytes(candidate)
        if est <= MAX_RESPONSE_BYTES:
            body_obj = candidate
        else:
            body_obj["cover_too_large"] = True
            body_obj["cover_estimated_response_bytes"] = est
            body_obj["max_response_bytes"] = MAX_RESPONSE_BYTES

    final_size_bytes = _estimate_response_size_bytes(body_obj)
    body_obj["response_size"] = {"bytes": final_size_bytes, "megabytes": bytes_to_mb(final_size_bytes)}
    print(f"[DEBUG] Final response size (bytes): {final_size_bytes}")

    return body_obj


def _is_sqs_event(event: dict) -> bool:
    return (
        isinstance(event, dict)
        and isinstance(event.get("Records"), list)
        and event["Records"]
        and "body" in event["Records"][0]
    )


def process(event, context):
    """Lambda handler."""

    if _is_sqs_event(event):
        for rec in event.get("Records", []):
            try:
                msg = json.loads(rec.get("body") or "{}")
                job_id = msg.get("jobId")
                if not job_id:
                    print("Missing jobId in SQS message body")
                    continue

                job_item = _ddb_get(job_id)
                if not job_item:
                    print(f"Job not found in DynamoDB: {job_id}")
                    continue

                workorder_id = job_item.get("workOrderId")
                if not workorder_id:
                    _ddb_update(job_id, status="FAILED", errorMessage="Missing workOrderId in DynamoDB job record")
                    continue

                _ddb_update(job_id, status="RUNNING")

                payload = {
                    "workOrderId": workorder_id,
                    "include_cover_bytes": True,
                    "include_extracted_text": False,
                    # flip to True if you want photo analysis in bulk runs:
                    "enable_photo_analysis": True,
                    "enable_photo_annotation": True,
                }

                body_obj = _run_pdfqa_logic(payload=payload, event=event)

                result_bucket, result_key = _write_result_to_s3(job_id, str(workorder_id), body_obj)

                _ddb_update(
                    job_id,
                    status="SUCCEEDED",
                    resultS3Bucket=result_bucket,
                    resultS3Key=result_key,
                    coverS3Key=body_obj.get("cover_s3_key"),
                    responseMegabytes=str((body_obj.get("response_size") or {}).get("megabytes")),
                )

            except Exception as e:
                err = str(e)
                print("Error:", err)
                try:
                    msg = json.loads(rec.get("body") or "{}")
                    job_id = msg.get("jobId")
                    if job_id:
                        _ddb_update(job_id, status="FAILED", errorMessage=err)
                except Exception:
                    pass

        return {"ok": True}

    # Legacy / manual test mode
    try:
        if isinstance(event, dict) and isinstance(event.get("body"), str):
            payload = json.loads(event["body"])
        else:
            payload = event if isinstance(event, dict) else json.loads(event["body"])

        body_obj = _run_pdfqa_logic(payload=payload, event=event)

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
