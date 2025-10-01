import json
import boto3
import logging
import uuid
import time
import io
import csv
import difflib
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta, timezone
import botocore
import boto3
from boto3.dynamodb.conditions import Key, Attr

# AWS clients
bedrock_client = boto3.client("bedrock-runtime", region_name="eu-west-2")
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")   # <— stick with "dynamodb"

# Now you can use this safely
heartbeat_tbl = dynamodb.Table("ProofingHeartbeats")
eventbridge_sched = boto3.client("scheduler", region_name="eu-west-2")

FINALIZE_LAMBDA_ARN = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-emails"
SCHEDULER_ROLE_ARN   = "arn:aws:iam::837329614132:role/eventbridge-scheduler-invoke-lambda"

# Initialise logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROOFING_CHECKLIST_ARN = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-checklist"
lambda_client = boto3.client("lambda")

# Email nots perms 
ses_client = boto3.client("ses", region_name="eu-west-2")
SENDER = "luke.gasson@metrosafety.co.uk"
RECIPIENT = "luke.gasson@metrosafety.co.uk" 

ALLOWED_EMAIL_TYPES = {"C-HSA", "C-RARA"}

def _should_email(work_type: str) -> bool:
    return (work_type or "").upper() in ALLOWED_EMAIL_TYPES

# Configurations
BEDROCK_MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"
BUCKET_NAME = "metrosafety-bedrock-output-data-dev-bedrock-lambda"
TABLE_NAME = "ProofingMetadata"
PDF_BUCKET = "metrosafetyprodfiles"

def strip_html(html):
    """Strip HTML tags from a string and return only text."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(separator=" ", strip=True)
    except Exception as e:
        logger.error(f"Error stripping HTML: {str(e)}")
        return html
    
TAG_MAP = {
    '<p>':  '[[P]]',
    '</p>':'[[/P]]',
    '<ul>':'[[UL]]',
    '</ul>':'[[/UL]]',
    '<li>':'[[LI]]',
    '</li>':'[[/LI]]',
    '<u>': '[[U]]',
    '</u>':'[[/U]]',
    '<br>':'[[BR]]',
    '<br/>':'[[BR]]',
    '<br />':'[[BR]]',
}

def protect_html(text):
    for real, placeholder in TAG_MAP.items():
        text = text.replace(real, placeholder)
    return text

def restore_html(text):
    for real, placeholder in TAG_MAP.items():
        text = text.replace(placeholder, real)
    return text

def _at_expr(dt_utc: datetime) -> str:
    # Scheduler wants no 'Z' and no offset in the string
    return f"at({dt_utc.strftime('%Y-%m-%dT%H:%M:%S')})"

def schedule_finalize(
    workorder_id: str,
    workTypeRef: str,
    buildingName: str,
    workOrderNumber: str,
    delay_seconds: int = 300,
):
    run_at_utc = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
    schedule_name = f"finalize-{workorder_id}"

    target_input = json.dumps({
        "workOrderId": workorder_id,
        "workTypeRef": workTypeRef,
        "buildingName": buildingName,
        "workOrderNumber": workOrderNumber,
    })
    kwargs = {
        "Name": schedule_name,
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "ScheduleExpression": _at_expr(run_at_utc),
        "ScheduleExpressionTimezone": "UTC",
        "Target": {
            "Arn": FINALIZE_LAMBDA_ARN,
            "RoleArn": SCHEDULER_ROLE_ARN,
            "Input": target_input,
        },
        "State": "ENABLED",
        "GroupName": "default",
    }

    try:
        eventbridge_sched.create_schedule(**kwargs)
    except eventbridge_sched.exceptions.ConflictException:
        eventbridge_sched.update_schedule(**kwargs)

def _write_heartbeat(workorder_id: str, csv_key: str):
    heartbeat_tbl.put_item(Item={
        "workorder_id": workorder_id,
        "csv_key": csv_key,
        "last_update": int(time.time()),
    })

# KNOWN_TAGS = {"P","/P","UL","/UL","LI","/LI","U","/U","BR"}

# def normalise_placeholders(text: str) -> str:
#     """Convert single-bracket tags [P], [/P], [br] into the double-bracket
#     format [[P]], [[/P]], [[BR]] so restore_html() can recognise them."""
#     def to_dbl(m):
#         tag = m.group(1).replace(" ", "").upper()
#         return f"[[{tag}]]" if tag in KNOWN_TAGS else m.group(0)
#     return re.sub(r"\[\s*([A-Za-z/ ]+?)\s*\]", to_dbl, text)

def proof_table_content(html, record_id):
    try:
        soup  = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            logger.warning(f"No table in record {record_id}")
            return html, []

        rows = table.find_all("tr")
        if not rows:
            logger.warning(f"No rows in table for record {record_id}")
            return html, []

        # 1) Extract & protect each cell’s HTML
        fragments = []
        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) >= 2:
                raw = tds[1].decode_contents()
                fragments.append(protect_html(raw))
            else:
                fragments.append("")

        # 2) Ask Claude to return a JSON array, no extra text
        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "system": (
                "You are a meticulous proofreader. Correct only spelling, grammar and punctuation in British English.\n"
                "Do NOT add, remove, reorder, split or merge any text or HTML tags.\n"
                "Output only the corrected JSON array of strings, matching the input array exactly.\n"
                "Ensure each sentence ends with a full stop unless it already ends with '.', '!', or '?'.\n"
                "\n"
                "Do not alter any codes or identifiers that match patterns like:\n"
                "- [A-Z]{2,6}\\s?\\d{1,3}\n"
                "- [A-Z]{2,6}\\d{1,3}\n"
                "- [A-Z]{2,6}\\s?\\d{1,3}/[A-Z]{2,4}\\.\\s?\\d{1,4}\n"
                "Examples (do not change spacing, punctuation, hyphens, dots, or case): 'BIR 51', 'BIR51', 'BTRB51/OP.151', 'BIRT 51/OP 151'.\n"
                "Never add punctuation inside these codes and never split or merge them.\n"
                "\n"
                "After ':' or ';', capitalise the first letter of the next word.\n"
                "If a comma is used where a sentence should end, replace it with a full stop.\n"
                "Do not apply any of these punctuation rules inside protected codes/identifiers.\n"
            ),
            "messages": [{
                "role": "user",
                "content": "Proofread this JSON array of HTML fragments (no commentary):\n\n"
                           + json.dumps(fragments, ensure_ascii=False)
            }],
            "max_tokens": 3000,
            "temperature": 0.0
        }
        logger.info(f"Record {record_id}: sending {len(fragments)} fragments to Bedrock")
        response = bedrock_client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload)
        )


        # 3) Parse the JSON array from Claude’s response
        body = json.loads(response["body"].read())
        raw  = body["content"][0]["text"].strip()
        # remove a leading ```html\n or ```\n
        raw = re.sub(r"^```(?:html)?\r?\n", "", raw)
        # remove any trailing ``` fences
        raw = re.sub(r"\r?\n```$", "", raw)

        corrected_protected = json.loads(raw)

        # 4) Re-insert each fragment, restoring real tags
        log_entries = []
        for idx, tr in enumerate(rows):
            tds = tr.find_all("td")
            if len(tds) >= 2:
                restored = restore_html(corrected_protected[idx])
                tds[1].clear()
                tds[1].append(BeautifulSoup(restored, "html.parser"))

                header = tds[0].get_text(separator=" ", strip=True)
                log_entries.append({
                    "recordId": record_id,
                    "header":    header,
                    "original":  protect_html(fragments[idx]),
                    "proofed":   restored
                })

        final_html = str(soup)
        final_html = apply_glossary(final_html)
        return final_html, log_entries

    except Exception as e:
        logger.error(f"Error in proof_table_content for {record_id}: {e}")
        return html, []


def proof_plain_text(text, record_id):
    protected = protect_html(text)

    # If the field already contains placeholders/HTML, treat as plain
    plain_text = protected if any(ph in protected for ph in TAG_MAP.values()) else strip_html(protected)

    try:
        logger.info(f"Proofing record {record_id}. Plain text: {plain_text}")

        # Hard limits to prevent expansions on very short inputs (headings/labels)
        # If it's <= 8 words, we only allow case/spacing/punctuation tweaks — no word additions.
        short_input = len(plain_text.split()) <= 8

        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "system": (
                "You are a strict proofreading function.\n"
                "TASK: Return ONLY a JSON object of the form {\"text\": \"...\"}.\n"
                "CONSTRAINTS:\n"
                "- British English spelling/grammar/punctuation only.\n"
                "- Do NOT add, remove, reorder, split, or merge words.\n"
                "- Do NOT add introductions, summaries, advice, bullets, or examples.\n"
                "- Preserve the exact meaning and structure.\n"
                "- If unsure, return the input unchanged.\n"
                "- If the input is a short fragment or heading, do NOT try to make it a full sentence.\n"
                "\n"
                "Do not alter any codes or identifiers that match patterns like:\n"
                "- [A-Z]{2,6}\\s?\\d{1,3}\n"
                "- [A-Z]{2,6}\\d{1,3}\n"
                "- [A-Z]{2,6}\\s?\\d{1,3}/[A-Z]{2,4}\\.\\s?\\d{1,4}\n"
                "Examples (do not change spacing, punctuation, hyphens, dots, or case): 'BIR 51', 'BIR51', 'BTRB51/OP.151', 'BIRT 51/OP 151'.\n"
                "Never add punctuation inside these codes and never split or merge them.\n"
                "\n"
                "Ensure each sentence ends with a full stop unless it already ends with '.', '!', or '?'.\n"
                "After ':' or ';', capitalise the first letter of the next word.\n"
                "If a comma is used where a sentence should end, replace it with a full stop.\n"
                "Do not apply any of these punctuation rules inside protected codes/identifiers.\n"
            ),
            "messages": [{
                "role": "user",
                "content": (
                    "Return EXACTLY this JSON schema with the corrected text:\n"
                    "{\"text\": \"<corrected>\"}\n\n"
                    "Requirements:\n"
                    f"- Treat this as {'a short fragment; absolutely no word additions' if short_input else 'normal text'}.\n"
                    "- No extra fields, no explanations, no code fences.\n"
                    f"INPUT:\n{plain_text}"
                )
            }],
            "max_tokens": 1500,
            "temperature": 0
        }

        logger.info("Sending payload to Bedrock: " + json.dumps(payload, indent=2))
        response = bedrock_client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload)
        )
        response_body = json.loads(response["body"].read().decode("utf-8"))
        model_text = " ".join(
            [msg.get("text","") for msg in response_body.get("content", []) if msg.get("type") == "text"]
        ).strip()

        # Parse strict JSON; fall back safely if parse fails
        corrected = None
        try:
            obj = json.loads(model_text)
            if isinstance(obj, dict) and "text" in obj and isinstance(obj["text"], str):
                corrected = obj["text"]
        except Exception:
            logger.warning("Plain-text proof: model did not return valid JSON; using original text.")
            corrected = plain_text

        # FINAL SAFETY GUARD: reject expansions (word growth > 20%) or added bullets/colons if short
        inp_words = plain_text.split()
        out_words = corrected.split()
        grew_too_much = len(out_words) > max(1, int(len(inp_words) * 1.2))
        looks_like_advice = any(sym in corrected for sym in ["•", "-", ":", "Here are", "tips", "suggestions"])

        if grew_too_much or (short_input and looks_like_advice):
            logger.warning(
                f"Plain-text proof rejected for record {record_id}: grew_too_much={grew_too_much}, "
                f"short_input={short_input}, looks_like_advice={looks_like_advice}"
            )
            corrected = plain_text  # keep original (already protected/stripped)

        restored = restore_html(corrected)
        restored = apply_glossary(restored)
        return restored if restored else text

    except Exception as e:
        logger.error(f"Error in proof_plain_text for {record_id}: {e}")
        return text

def update_logs_csv(log_entries, filename, folder):
    """
    Merge new log_entries into the existing CSV in S3, dedupe, flatten newlines, and overwrite.
    log_entries: list of dicts with keys recordId, header, original, proofed
    """
    s3_key = f"{folder}/{filename}.csv"

    # 1. Load existing rows (if any)
    existing_rows = []
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        text = obj['Body'].read().decode('utf-8')
        reader = csv.reader(io.StringIO(text))
        existing_rows = list(reader)
    except s3_client.exceptions.NoSuchKey:
        # No existing file, we'll start fresh
        pass

    # 2. Initialize merged list and seen set
    merged = []
    seen = set()

    # 3. Add header row
    header = ['Record ID', 'Header', 'Original Text', 'Proofed Text']
    if not existing_rows:
        merged.append(header)
    else:
        for row in existing_rows:
            tup = tuple(row)
            if tup not in seen:
                merged.append(row)
                seen.add(tup)

    # 4. Add each new entry, flatten any embedded newlines
    for e in log_entries:
        orig = e.get('original', '')
        proof = e.get('proofed', '')

        # Replace real line-breaks with a space
        orig_clean = re.sub(r'[\r\n]+', ' ', orig).strip()
        proof_clean = re.sub(r'[\r\n]+', ' ', proof).strip()

        row = [
            e.get('recordId', ''),
            e.get('header', ''),
            orig_clean,
            proof_clean
        ]
        tup = tuple(row)
        if tup not in seen:
            merged.append(row)
            seen.add(tup)

    # 5. Write merged list back out as a well-formed CSV
    out = io.StringIO()
    writer = csv.writer(
        out,
        delimiter=',',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        escapechar='\\',
        lineterminator='\n'
    )
    writer.writerows(merged)

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=out.getvalue().encode('utf-8')
    )

    return s3_key

def store_metadata(workorder_id, logs_s3_key, status):
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(
        Item={
            "workorder_id": workorder_id,
            "logs_s3_key": logs_s3_key,
            "status": status,  # "Proofed" or "Original"
            "timestamp": int(time.time())
        }
    )

def load_payload(event):
    """
    Extracts payload from the incoming event.
    Expected JSON structure:
      {
        "workOrderId": "...",
        "contentType": "FormQuestion" or "Action_Observation" or "Action_Required",
        "sectionContents": [ { "recordId": "...", "content": "..." }, ... ]
      }
    """
    try:
        raw_body = event.get("body", "")
        logger.info("Raw payload body received: " + raw_body)
        body = json.loads(raw_body)
        content_type = body.get("contentType", "Unknown")
        items = body.get("sectionContents", [])
        logger.info(f"Payload contentType: {content_type}, records received: {len(items)}")
        
        proofing_requests = {}
        table_data = {}
        for entry in items:
            record_id = entry.get("recordId")
            content = entry.get("content")
            if not record_id or not content:
                logger.warning(f"Skipping entry with missing recordId or content: {entry}")
                continue
            proofing_requests[record_id] = content.strip()
            table_data[record_id] = {"content": content.strip(), "record_id": record_id}
        
        return body.get("workOrderId"), content_type, proofing_requests, table_data

    except Exception as e:
        logger.error(f"Unexpected error in load_payload: {e}")
        return None, None, {}, {}
    
def apply_glossary(text):
    """
    Enforce canonical spellings for key terms.
    """
    corrections = {
        # “e scooter”, “escooter”, → “e-scooter”
        r"\b(e[\s\-]?scooter)\b": "e-scooter",
        # “flexi hose”, “flexihose”, “flexi-hose” → “flexi-hose”
        r"\b(flexi[\s\-]?hose)\b": "flexi-hose",
        # catch combi boiler / coiler / collar variants → “Combi Boiler”
        r"\bcombi[\s\-]?(boiler|coiler|collar)\b": "Combi Boiler",
        # story → storey
        r"\bstory\b": "storey",
        # single stage alarm → “Single Stage Alarm”
        r"\bsingle[\s\-]+stage[\s\-]+alarm\b": "Single Stage Alarm",
        # plc → PLC
        r"\bplc\b": "PLC",
        # are are → are
        r"\bare\s+are\b": "are",
        # Fire Safety Officer from Essex Fire Brigade
        r"\bfire safety officer from essex fire brigade\b": "Fire Safety Officer from Essex Fire Brigade",
    }
    for pattern, replacement in corrections.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text

def marker_is_fresh(bucket: str, key: str, ttl_minutes: int = 10) -> bool:
    """
    Returns True if marker exists AND is younger than ttl_minutes.
    If it exists but is older, deletes it and returns False.
    """
    try:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
        last_mod = resp['LastModified']  # UTC datetime
        if datetime.now(timezone.utc) - last_mod < timedelta(minutes=ttl_minutes):
            return True
        # expired → delete so next run will re-process
        s3_client.delete_object(Bucket=bucket, Key=key)
        return False
    except botocore.exceptions.ClientError as e:
        # 404 / NoSuchKey → marker not yet created
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            return False
        logger.error(f"Error checking marker {key}: {e}")
        return False
    
def make_diff(original: str, proofed: str) -> str:
    """
    Return a little unified-diff between original and proofed.
    """
    o_lines = original.splitlines()
    p_lines = proofed.splitlines()
    diff = difflib.unified_diff(
        o_lines,
        p_lines,
        fromfile='original',
        tofile='proofed',
        lineterm=''
    )
    return '\n'.join(diff) or '(no visible diff)'

def parse_diff(diff_str: str) -> tuple[str,str]:
    """
    Pulls out only tokens with letters/digits,
    so punctuation‑only bits (“-” or “/”) are skipped.
    """
    raw_removed = re.findall(r'-\s*([^+–]+)', diff_str)
    raw_added   = re.findall(r'\+\s*([^+–]+)', diff_str)

    def clean(tokens):
        # keep only things with a letter or digit
        kept = [t.strip() for t in tokens if re.search(r'\w', t)]
        return '; '.join(kept)

    return clean(raw_removed), clean(raw_added)

def make_word_diff(orig: str, proof: str) -> str:
    # simple word-level diff, prefixed with +/-
    diff = difflib.ndiff(orig.split(), proof.split())
    return " ".join(diff)

def drop_placeholders(text: str) -> str:
    # remove any [[TAG]] or [[/TAG]]
    return re.sub(r"\[\[\/?[A-Z]+\]\]", "", text)

def write_changes_csv(log_entries, workorder_id):
    """
    Append any *new* changes to the existing changes CSV in S3,
    merging + deduping so you end up with one row per change ever seen.
    """
    s3_key = f"changes/{workorder_id}_changes.csv"
    header = ["Record ID","Header","Original Text","Proofed Text","Removed","Added"]

    def normalize_for_compare(s: str) -> str:
        if s is None:
            return ""
        # treat whitespace and case as not-meaningful:
        return " ".join(s.split()).strip().lower()

    new_rows = []
    for e in log_entries:
        # 1) Prepare the two texts the same way you already do
        orig = drop_placeholders(strip_html(e["original"]))
        proof = drop_placeholders(strip_html(e["proofed"]))
        orig_clean  = re.sub(r"[\r\n]+", " ", orig).strip()
        proof_clean = re.sub(r"[\r\n]+", " ", proof).strip()

        # 2) If, after normalization, nothing meaningfully changed → skip this row
        if normalize_for_compare(orig_clean) == normalize_for_compare(proof_clean):
            continue

        # 3) Build a light diff as you already do
        tokens = difflib.ndiff(orig_clean.split(), proof_clean.split())
        changes = []
        for t in tokens:
            if not t.startswith(("+ ", "- ")):
                continue
            word = t[2:]
            # skip anything that is purely a tag or placeholder
            if re.fullmatch(r"</?p>|\[\[\/?[A-Z]+\]\]", word):
                continue
            changes.append(t)
        diff_text = " ".join(changes)

        # 4) Extract Removed/Added and, if both empty, skip (likely punctuation-only or tag-only)
        removed_text, added_text = parse_diff(diff_text)
        if not removed_text and not added_text:
            continue

        # 5) If we reached here, it's a meaningful change—record it
        new_rows.append([
            e["recordId"],
            e.get("header",""),
            orig_clean,
            proof_clean,
            removed_text,
            added_text
        ])

    if not new_rows:
        return None, 0

    # Load → merge → dedupe → write (unchanged from your version)
    merged = []
    try:
        obj  = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        text = obj["Body"].read().decode("utf-8")
        reader = csv.reader(io.StringIO(text))
        merged = list(reader)
    except s3_client.exceptions.NoSuchKey:
        merged = []

    rows = [header]
    if merged:
        rows.extend(merged[1:])
    rows.extend(new_rows)

    seen = set()
    deduped = [rows[0]]
    for row in rows[1:]:
        tup = tuple(row)
        if tup not in seen:
            seen.add(tup)
            deduped.append(row)

    buf = io.StringIO(newline="")
    writer = csv.writer(
        buf,
        delimiter=",",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\",
        lineterminator="\n"
    )
    writer.writerows(deduped)

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

    return s3_key, len(deduped) - 1
    

def process(event, context):
    # 1) parse common fields
    body = json.loads(event.get("body",""))
    workTypeRef = body.get("workTypeRef")
    workorder_id = body.get("workOrderId")
    email_addr   = body.get("emailAddress")
    buildingName = body.get("buildingName")
    resourceName = body.get("resourceName")
    workOrderNumber = body.get("workOrderNumber")

    # 2) ALWAYS run your AI-proofing
    wo, ct, proof_reqs, tabledata = load_payload(event)
    if not proof_reqs:
        return {"statusCode":400,"body":json.dumps({"error":"No proofing items found"})}

    logs, flag, proofed = [], False, []
    for rid, cont in proof_reqs.items():
        if ct == "FormQuestion":
            html, le = proof_table_content(cont, rid)
            for e in le:
                logs.append(e)
                if e["original"]!=e["proofed"]:
                    flag = True
            proofed.append({"recordId":rid,"content":html})
        else:
            txt  = proof_plain_text(cont, rid)
            orig = protect_html(cont)
            corr = txt

            if corr != orig:
                flag = True
                logs.append({
                    "recordId": rid,
                    "header":    "",
                    "original":  orig,
                    "proofed":   corr
                })
            else:
                logs.append({
                    "recordId": rid,
                    "header":    "",
                    "original":  "No changes needed: " + orig,
                    "proofed":   "No changes made."
                })

            proofed.append({"recordId": rid, "content": txt})

    status = "Proofed" if flag else "Original"
    csv_key = update_logs_csv(logs, f"{workorder_id}_logs", "logs")
    store_metadata(workorder_id, csv_key, status)

    real_changes = [
        e for e in logs
        if e["original"] != e["proofed"]
           and not e["original"].startswith("No changes needed")
    ]

    if real_changes:
        changed_key, change_count = write_changes_csv(real_changes, workorder_id)
        logger.info(f"Changes CSV written s3://{BUCKET_NAME}/{changed_key}; {change_count} row(s).")
        csv_for_heartbeat = changed_key or f"changes/{workorder_id}_changes.csv"
    else:
        csv_for_heartbeat = f"changes/{workorder_id}_changes.csv"

    # 2) ALWAYS write a heartbeat so checklist/FRA can fetch the link
    _write_heartbeat(workorder_id, csv_for_heartbeat)

    # 3) ONLY schedule the email for allowed types (C-HSA / C-RARA)
    if _should_email(workTypeRef):
        schedule_finalize(
            workorder_id,
            workTypeRef=workTypeRef,
            buildingName=buildingName,
            workOrderNumber=workOrderNumber,
            delay_seconds=300
        )
    else:
        logger.info(f"Skipping finalize scheduling for workTypeRef={workTypeRef}")

    final_response = {
        "workOrderId":     workorder_id,
        "contentType":     ct,
        "sectionContents": proofed
    }

    # 3) THEN trigger Textract/checklist for C-WRA only
    if workTypeRef in ("C-WRA", "C-HSRA", "C-FRA") and workorder_id:
        prefix     = f"WorkOrders/{workorder_id}/"
        marker_key = prefix + ".textract_ran"
        logger.info("About to evaluate Textract marker for workOrder %s", workorder_id)

        if marker_is_fresh(PDF_BUCKET, marker_key, ttl_minutes=10):
            logger.info("Skipping Textract – marker younger than 10 minutes.")
        else:

            logger.info("Invoking Textract for %s", workorder_id)

            logger.info("Creating new marker %s", marker_key)
            resp     = s3_client.list_objects_v2(Bucket=PDF_BUCKET, Prefix=prefix)
            contents = resp.get("Contents", [])
            valid_objs = [
                obj for obj in contents
                if not obj["Key"].endswith(".textract_ran")
                   and not obj["Key"].lower().endswith((".xlsx", ".xls", ".csv"))
            ]
            if not valid_objs:
                logger.error("No suitable document found under %s", prefix)
                return {"statusCode": 400, "body": "No document file to process."}

            newest  = max(valid_objs, key=lambda o: o["LastModified"])
            doc_key = newest["Key"]

            # write a fresh marker
            s3_client.put_object(
                Bucket=PDF_BUCKET,
                Key=marker_key,
                Body=b"",
                Tagging="marker=textract_ran"
            )
            logger.info("Wrote marker %s; invoking Textract…", marker_key)

            # fire off your proofing lambda
            payload = {
                "bucket_name":    PDF_BUCKET,
                "document_key":   doc_key,
                "workOrderId":    workorder_id,
                "emailAddress":   email_addr,
                "buildingName":   buildingName,
                "workOrderNumber":workOrderNumber,
                "resourceName":   resourceName,
                "workTypeRef":    workTypeRef,
            }
            lambda_client.invoke(
                FunctionName=PROOFING_CHECKLIST_ARN,
                InvocationType="Event",
                Payload=json.dumps(payload).encode("utf-8")
            )

    # 4) return proofed JSON every time
    return {
        "statusCode": 200,
        "headers":    {"Content-Type":"application/json"},
        "body":       json.dumps(final_response)
    }