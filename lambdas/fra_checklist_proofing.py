import json
import boto3
import logging
import re
import os
# ——— Initialise logging ———
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ——— AWS clients ———
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3       = boto3.client('s3')
ses = boto3.client('ses', region_name='eu-west-2')

BCC_ADDRESSES = ""#"peter.taylor@metrosafety.co.uk, cristian.carabus@metrosafety.co.uk"

EMAIL_QUESTIONS = {
    3: "Totals consistency check (Section 1.1 vs Significant Findings and Action Plan)",
    4: "Building Description completeness assessment",
    9: "Risk Rating & Management Control review"
}

def extract_json_data(json_content, question_number):
    payload = json.loads(json_content)
    
    # Q3: Remedial-actions vs Significant Findings count
    if question_number == 3:
        remedial_by_section = {}
        total_issues = 0

        for sec in payload.get("sections", []):
            if sec.get("name", "").startswith("1.1 Areas"):
                for tbl in sec.get("tables", []):
                # skip header row
                    for row in tbl.get("rows", [])[1:]:
                        try:
                            count = int(row[1])
                            remedial_by_section[row[0]] = count
                            total_issues += count
                        except (IndexError, ValueError):
                            continue
        sig_item_count = 0
        for sec in payload.get("sections", []):
            if sec.get("name", "").startswith("Significant Findings"):
                sig_item_count = len(sec.get("tables", []))
                break
        return {
            "remedial_by_section": remedial_by_section,
            "remedial_total":      total_issues,
            "sig_item_count":      sig_item_count
        }
    
    if question_number == 4:
        return payload
    
        # ————— Q9: Risk Dashboard – Management Control & Inherent Risk —————
    if question_number == 9:
        rr_levels    = []
        mcr_texts    = []
        inherent_txt = ""

        # 1) Find the “2.0 Risk Dashboard” section
        for sec in payload.get("sections", []):
            if sec.get("name", "").startswith("2.0 Risk Dashboard"):

                # 2) Pull the Risk Rating ⇆ Management Control table
                for tbl in sec.get("tables", []):
                    # safely unpack only the first two cells
                    hdr0, hdr1, *_ = tbl["rows"][0]
                    if hdr0.strip() == "Risk Rating" and "Management Control" in hdr1:
                        for row in tbl["rows"][1:]:
                            # grab at most two columns
                            rating = row[0].strip() if len(row) > 0 else ""
                            control = row[1].strip() if len(row) > 1 else ""
                            rr_levels.append(rating)
                            mcr_texts.append(control)

                # 3) Fallback: extract the “Inherent Risk” line from paragraphs
                paras = sec.get("paragraphs", [])
                for idx, line in enumerate(paras):
                    if re.match(r"^\s*2\.1\s+Current Risk Ratings", line):
                        # take the very next non‐footer, non‐heading line
                        for nxt in paras[idx+1:]:
                            txt = nxt.strip()
                            if not txt or txt.startswith("Overall Risk Rating") or txt.startswith("Printed from"):
                                break
                            inherent_txt = txt
                            break
                        break

                break  # no need to scan further sections

        # 4) Return exactly what build_user_message(q=6) needs
        return {
            "risk_rating_levels":        rr_levels,
            "management_control_text":   mcr_texts,
            "inherent_risk_description": inherent_txt
        }

    return None


def build_user_message(question_number, content):

         # Q3 prompt
    if question_number == 3:
        by_sec = content.get("remedial_by_section", {})
        total  = content.get("remedial_total", 0)
        sig_ct = content.get("sig_item_count", 0)
        breakdown = ", ".join(f"{k}: {v}" for k, v in by_sec.items())

        return (
            "Question 3: Compare the number of remedial‐actions raised in Section 1.1 with the\n"
            "number of items in “Significant Findings and Action Plan.”\n\n"
            f"— Section 1.1 counts: {breakdown}  (Total = {total})\n"
            f"— Significant Findings items found: {sig_ct}\n\n"
            "If the totals match, reply “PASS”. Otherwise list each discrepancy."
        )
    
    # Q9 prompt -> Inherent doesn't print as table, so if each one is the same this can be done, however would be slightly inconsistent.
    if question_number == 9:
        levels   = content["risk_rating_levels"]
        controls = content["management_control_text"]

        return (
            "Question 9: On the Risk Dashboard (Section 2.0), confirm that both of the following sections are present and populated:\n"
            "  1. Risk Rating Levels\n"
            "  2. Management Control of Legionella Risk\n\n"
            "Below are the values we found under each heading:\n\n"
            "--- Risk Rating Levels (extracted entries) ---\n"
            f"{', '.join(levels) or 'None found'}\n\n"
            "--- Management Control Text (extracted entries) ---\n"
            f"{'; '.join(controls) or 'None found'}\n\n"
            "If both lists contain at least one entry, reply:\n"
            "PASS: Both Risk Rating Levels and Management Control Text are complete. Check Legionella Inherent Risk manually and ensure no content is missing.\n"
            "Otherwise, name which section is missing or empty."
        )
    # fallback
    logger.error(f"No handler for question_number={question_number}; returning empty message")
    return ""


def send_to_bedrock(user_text):
    MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens":        1000,
        "temperature":       0.0,
        "messages": [
            {
                "role":    "user",
                "content": user_text
            }
        ]
    }

    logger.info("Bedrock request payload: %s", json.dumps(payload))
    resp = bedrock.invoke_model(
        modelId     = MODEL_ID,
        body        = json.dumps(payload),
        contentType = "application/json",
        accept      = "application/json"
    )
    response_text = resp["body"].read().decode("utf-8")
    logger.info("Received response from Bedrock")
        # ─── parse JSON and extract just the assistant text ───
    try:
        data = json.loads(response_text)
        # your responses live in data["content"], a list of { "type": "...", "text": "..." }
        plain = "".join(part.get("text", "") for part in data.get("content", []))
    except (ValueError, KeyError):
        # if parsing fails, fall back to raw
        plain = response_text
    return plain.strip()

def validate_water_assets(sections):
    """
    Locally validate Water Assets tables - Question 16: check each asset record for blank data fields (excluding photo rows),
    ensure comments row is non-empty, and note that photos are never extractable by Textract so flag them.

    Returns a list of dicts: {record: <ID>, missing: [<issues>]}
    """
    issues = []
    id_pattern = re.compile(r'^[A-Z]{2,}-\d+')

    for sec in sections:
        name = sec.get("name", "").lower()
        if "water asset" not in name:
            continue

        for table in sec.get("tables", []):
            rows = table.get("rows", [])
            if len(rows) < 2:
                continue

            # record ID from first row, second cell
            first = rows[0]
            record_id = str(first[1]).strip() if len(first) > 1 else "<unknown>"
            if not id_pattern.match(record_id):
                continue

            missing = []
            # 1) Check all data fields except photo row
            for r in rows[1:]:
                field_name = str(r[0]).strip()
                if field_name.lower().startswith("photo"):
                    continue
                # if any cell in row is blank
                for cell in r[1:]:
                    if not str(cell).strip():
                        missing.append(f"blank value in '{field_name}'")
                        break

            # 2) Comments row must have text
            comment_row = next((r for r in rows if str(r[0]).strip().lower() == "comments"), None)
            if comment_row:
                comment_text = " ".join(str(c) for c in comment_row[1:]).strip()
                if not comment_text:
                    missing.append("comments missing")
            else:
                missing.append("comments row missing")

            # 3) Photos always flagged for manual check
            missing.append("photos manual check")

            if missing:
                issues.append({"record": record_id, "missing": missing})

    return issues

def check_building_description(payload):
    """
    Returns True if any non-empty text exists in a section whose name starts with
    "Building Description" (covers both "Building Description - The Building"
    and "Building Description - Fire Safety"), including both paragraphs and table cells.
    """
 # Log all section names
    sections = payload.get("sections", [])
    logger.info("Q4: checking Building Description, payload.sections names = %s",
                [sec.get("name", "") for sec in sections])

    for sec in sections:
        name = sec.get("name", "")
        logger.info("Q4: inspecting section '%s'", name)
        if name.startswith("Building Description"):
            # collect all text fragments
            texts = []
            texts.extend(sec.get("paragraphs", []))
            logger.info("Q4: found paragraphs: %s", sec.get("paragraphs", []))

            for tbl in sec.get("tables", []):
                for row in tbl.get("rows", []):
                    joined = " ".join(cell.strip() for cell in row if cell and cell.strip())
                    texts.append(joined)
            logger.info("Q4: after tables, collected text fragments = %s", texts)

            result = any(t.strip() for t in texts)
            logger.info("Q4: result for this section = %s", result)
            return result

    logger.info("Q4: no 'Building Description' section found at all")
    return False

# def validate_outlet_temperature_table(sections):
#     """
#     Handle Q17: check Outlet Temperature Profile for out-of-range temps
#     and match against Significant Findings and Action Plan.
#     Returns a string local_response.
#     """
#     # find the Outlet Temperature Profile section (7.3)
#     ot_section = next(
#         (s for s in sections
#          if s.get("name", "").startswith("7.3 Outlet Temperature Profile")),
#         None
#     )
#     if not ot_section or not ot_section.get("tables"):
#         return "Could not find the Outlet Temperature Profile table."

#     rows = ot_section["tables"][0]["rows"]
#     anomalies = []
#     # column 13 holds the hot-water temperature
#     for row in rows:
#         try:
#             hot = float(row[13])
#         except Exception:
#             continue
#         if hot < 50 or hot > 60:
#             anomalies.append({"location": row[2], "temp": hot})

#     if not anomalies:
#         return "All hot-water temperatures are between 50 °C and 60 °C; no action needed."

#     # look for matching actions in Significant Findings and Action Plan
#     sig_section = next(
#         (s for s in sections
#          if s.get("name", "").startswith("Significant Findings and Action Plan")),
#         {}
#     )
#     actions = []
#     for tbl in sig_section.get("tables", []):
#         for row in tbl.get("rows", []):
#             text = " ".join(row).lower()
#             if "temperature" in text or "scald" in text:
#                 actions.append(text)

#     if actions:
#         return (
#             f"Out-of-range temperatures at {anomalies}; "
#             f"matching actions found: {actions}"
#         )
#     else:
#         return (
#             f"Out-of-range temperatures at {anomalies}, "
#             "but no related action in Significant Findings and Action Plan!"
#         )

def process(event, context):
    """
    Handler for SNS event from Textract Callback.

    Expects event with keys:
      - 'textract_bucket'
      - 'textract_key'
      - 'workOrderId'
      - 'assessor_email'       ← ignored here, we override below for testing
    """
    logger.info("Received proofing event: %s", json.dumps(event))

    tex_bucket      = event.get("textract_bucket")
    tex_key         = event.get("textract_key")
    work_order_id     = event.get("workOrderId")
    workOrderNumber = event.get("workOrderNumber")
    emailAddress    = event.get("emailAddress")
    buildingName    = event.get("buildingName")
    workTypeRef     = event.get("workTypeRef")
    pdf_bucket = event.get("bucket_name")
    pdf_key    = event.get("document_key")

    presigned_url = s3.generate_presigned_url(
    ClientMethod="get_object",
    Params={
        "Bucket": pdf_bucket,
        "Key":   pdf_key
    },
    ExpiresIn=86400   # link valid for 24 hours; adjust as needed
)

    # if not tex_bucket or not tex_key or not work_order_id:
    #     logger.error("Missing one of textract_bucket/textract_key/workOrderId in event: %s", event)
    #     return {"statusCode": 400, "body": "Missing required fields"}

    # ——— 2) Download the Textract JSON from S3 ———
    try:
        s3_obj  = s3.get_object(Bucket=tex_bucket, Key=tex_key)
        content = s3_obj["Body"].read().decode("utf-8")
    except Exception as e:
        logger.error(
            "Failed to download Textract JSON from s3://%s/%s: %s",
            tex_bucket, tex_key, e, exc_info=True
        )
        return {"statusCode": 500, "body": "Cannot fetch Textract JSON"}

    # ——— 3) Loop through Q1–Q15, always sending to Bedrock ———
    proofing_results = {}
    for q_num in (3, 4, 9):
        parsed = extract_json_data(content, q_num)

        # handle Q4 locally
        if q_num == 4:
            logger.info("Q4: raw payload for Q4 = %s", json.dumps(parsed))
            ok = check_building_description(parsed)
            proofing_results["Q4"] = {
                "question": 4,
                "result":   "PASS" if ok else "FAIL",
                "notes":    f"Building Description content {'found' if ok else 'missing'}"
            }
            proofing_results["Q4"] = "PASS" if ok else "FAIL"
            continue

        # for Q3 and Q9 go to Bedrock
        try:
            prompt = build_user_message(q_num, parsed)
            if not prompt:
                proofing_results[f"Q{q_num}"] = "(no prompt built)"
            else:
                ai_reply = send_to_bedrock(prompt)
                proofing_results[f"Q{q_num}"] = ai_reply or "(empty response)"
        except Exception as ex:
            logger.warning(
                "Error while processing Q%d for WorkOrder %s: %s",
                q_num, work_order_id, ex, exc_info=True
            )
            proofing_results[f"Q{q_num}"] = f"ERROR: {ex}"
    # ——— 4) Log all results ———
    logger.info(
        "Proofing results for workOrderId %s:\n%s",
        work_order_id,
        json.dumps(proofing_results, indent=2)
    )

    local_part = emailAddress.split("@")[0]                 # "firstname.lastname"
    first_name = local_part.split(".")[0].capitalize()    # "Firstname"

    question_keys = ["Q3", "Q4", "Q9"]
    results = [
        proofing_results.get(key, "").strip().upper().splitlines()[0]
        for key in question_keys
    ]
    # Check if each one exactly equals "PASS"
    digital_outcome = "PASS" if all(r == "PASS" for r in results) else "FAIL"

    # ——— 5) Build a structured plaintext email body ———
    subject = (
        f"AI || "
        f"{workOrderNumber}/"
        f"{work_order_id} || "
        f"{buildingName} || "
        f"{workTypeRef} || "
        f"{digital_outcome}"
    )
    body_lines = []
    body_lines.append(f"Hello {first_name},\n")
    body_lines.append(f"Below are the proofing outputs for '{buildingName}' (Work Order #{workOrderNumber}):\n")

    for q_num, email_heading in EMAIL_QUESTIONS.items():
        q_key = f"Q{q_num}"
        answer = proofing_results.get(q_key, "(no result)")
        # indent each line of the AI’s answer
        indented = "\n".join("  " + ln for ln in str(answer).splitlines())
        body_lines.append(f"{email_heading}:\n{indented}\n")

    body_lines.append(f"Link to Work Order in Salesforce: \n https://metrosafety.lightning.force.com/lightning/r/WorkOrder/{work_order_id}/view\n")
    body_lines.append("\n\n"
                      "You can download the original PDF here:\n"
    f"{presigned_url}")

    body_lines.append("Regards,\nQuality Team\n")
    body_text = "\n".join(body_lines)

    # ——— 6) Send the email via SES ———
    source_email = "luke.gasson@metrosafety.co.uk"
    if not source_email:
        logger.error("SES_SOURCE_EMAIL not set in environment.")
        return {"statusCode": 500, "body": "Missing SES_SOURCE_EMAIL"}

    bcc_list = [addr.strip() for addr in BCC_ADDRESSES.split(",") if addr.strip()]

    email_params = {
        "Source": source_email,
        "Destination": {
            "ToAddresses": [source_email],
            "BccAddresses": bcc_list
        },
        "Message": {
            "Subject": {"Data": subject},
            "Body": {
                "Text": {"Data": body_text}
            }
        }
    }

    try:
        ses.send_email(**email_params)
        logger.info("Sent proofing email to %s (bcc: %s)", source_email, bcc_list)
    except Exception as e:
        logger.error("Failed to send SES email: %s", e, exc_info=True)
        return {"statusCode": 500, "body": "Error sending email"}

    return {"statusCode": 200, "body": "Checklist processing complete"}