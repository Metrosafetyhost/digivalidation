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

# Initialise logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROOFING_CHECKLIST_ARN = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-checklist"
lambda_client = boto3.client("lambda")

# AWS clients
bedrock_client = boto3.client("bedrock-runtime", region_name="eu-west-2")
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Email nots perms 
ses_client = boto3.client("ses", region_name="eu-west-2")
SENDER = "luke.gasson@metrosafety.co.uk"
RECIPIENT = "luke.gasson@metrosafety.co.uk"  

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
                "You are a meticulous proofreader. Correct only spelling, grammar and punctuation in British English. "
                "Do NOT add, remove, reorder, split or merge any text or HTML tags. "
                "Output only the corrected JSON array of strings, matching the input array exactly."
                "Ensure each sentence ends with a full stop unless it already ends with appropriate punctuation (e.g. '.', '!', '?')"
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

    if any(ph in protected for ph in TAG_MAP.values()):
        plain_text = protected
    else:
        plain_text = strip_html(protected)

    try:
        logger.info(f"Proofing record {record_id}. Plain text: {plain_text}")
        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "system": (
            "You are a meticulous proofreader. "
            "Correct spelling, grammar and clarity only — no extra commentary or re-structuring."
            "Ensure each sentence ends with a full stop unless it already ends with appropriate punctuation (e.g. '.', '!', '?')"
        ),
            "messages": [{
                "role": "user",
                "content": (
                    "Proofread the following text according to these strict guidelines:\n"
                    "- Do NOT add any new introductory text or explanatory sentences before or after the original content - aka  **Do not** add any introductory sentence such as “Here is the corrected text:” or similar.\n"
                    "- Spelling and grammar are corrected in British English, and spacing is corrected.\n"
                    "- Headings, section titles, and structure remain unchanged.\n"
                    "- Do NOT remove any words or phrases from the original content.\n"
                    "- Do NOT split, merge, or add any new sentences or content.\n"
                    "- Ensure that lists, bullet points, and standalone words remain intact.\n"
                    "- Ensure only to proofread once, NEVER repeat the same text twice in the output.\n\n"
                    "Text to proofread: " + plain_text
                )
            }],
            "max_tokens": 1500,
            "temperature": 0.3
            
        }
        logger.info("Sending payload to Bedrock: " + json.dumps(payload, indent=2))
        response = bedrock_client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload)
        )
        response_body = json.loads(response["body"].read().decode("utf-8"))
        proofed_text = " ".join(
            [msg["text"] for msg in response_body.get("content", []) if msg.get("type") == "text"]
        ).strip()
        #apply glossary to words before returning
        
        restored = restore_html(proofed_text)

        restored = apply_glossary(restored) 

        return restored if restored else text
    
    except Exception as e:
        logger.error(f"Error proofing plain text for record {record_id}: {e}")
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
    }
    for pattern, replacement in corrections.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text

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
    header = ["Record ID","Header","Original Text","Proofed Text","Diff"]

    new_rows = []
    for e in log_entries:
        orig = drop_placeholders(strip_html(e["original"]))
        proof = drop_placeholders(strip_html(e["proofed"]))
        if orig == proof or orig.startswith("No changes needed"):
            continue

        orig_clean  = re.sub(r"[\r\n]+", " ", orig).strip()
        proof_clean = re.sub(r"[\r\n]+", " ", proof).strip()

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

        new_rows.append([
            e["recordId"],
            e.get("header",""),
            orig_clean,
            proof_clean,
            diff_text
        ])

    if not new_rows:
        return None, 0

    # Load CSV
    merged = []
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        text = obj["Body"].read().decode("utf-8")
        reader = csv.reader(io.StringIO(text))
        merged = list(reader)
    except s3_client.exceptions.NoSuchKey:
        # no existing changes file yet
        merged = []

    # 3) Start fresh: header + old rows (minus their header) + new rows
    rows = [header]
    if merged:
        rows.extend(merged[1:])  # skip old header
    rows.extend(new_rows)

    # 4) Dedupe exact duplicates
    seen = set()
    deduped = [rows[0]]
    for row in rows[1:]:
        tup = tuple(row)
        if tup not in seen:
            seen.add(tup)
            deduped.append(row)

    # 5) Write it all back out
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

    # return how many *unique* rows we now have
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
        logger.info(
            f"Changes CSV written to s3://{BUCKET_NAME}/{changed_key}; "
            f"{change_count} row(s)."
        )
    final_response = {
        "workOrderId":     workorder_id,
        "contentType":     ct,
        "sectionContents": proofed
    }

    # 3) THEN trigger Textract/checklist for C-WRA only
    if workTypeRef == "C-WRA" and workorder_id: #if workTypeRef in ("C-WRA", "C-HSRA", "C-FRA") and workorder_id:
        prefix = f"WorkOrders/{workorder_id}/"
        marker = prefix + ".textract_ran"
        s3 = boto3.client("s3")
        try:
            s3.head_object(Bucket=PDF_BUCKET, Key=marker)
            logger.info("Marker exists, skipping Textract.")
        except s3_client.exceptions.ClientError as ce:
            # if the textract marker is missing…
            if ce.response.get("Error", {}).get("Code") == "404":
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

                newest   = max(valid_objs, key=lambda o: o["LastModified"])
                doc_key  = newest["Key"]

                # write your marker so you don’t re-process next time
                s3_client.put_object(Bucket=PDF_BUCKET, Key=marker, Body=b"")

                # fire off your proofing lambda
                payload = {
                    "bucket_name":   PDF_BUCKET,
                    "document_key":  doc_key,
                    "workOrderId":   workorder_id,
                    "emailAddress":  email_addr,
                    "buildingName":  buildingName,
                    "workOrderNumber": workOrderNumber,
                    "resourceName": resourceName,
                    "workTypeRef": workTypeRef,
                }
                lambda_client.invoke(
                FunctionName=PROOFING_CHECKLIST_ARN,
                InvocationType="Event",
                Payload=json.dumps(payload).encode("utf-8")
                )

            else:
                # some other S3 error – not a missing marker
                logger.error("No PDF found under %s", prefix)
                return {"statusCode": 400, "body": "No document file to process."}

    # 4) return proofed JSON every time
    return {
        "statusCode": 200,
        "headers":    {"Content-Type":"application/json"},
        "body":       json.dumps(final_response)
    }