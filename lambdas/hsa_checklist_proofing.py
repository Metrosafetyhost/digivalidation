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

BCC_ADDRESSES = "peter.taylor@metrosafety.co.uk, cristian.carabus@metrosafety.co.uk"

EMAIL_QUESTIONS = {
    3: "Totals consistency check (Section 1.1 vs Significant Findings and Action Plan)",
    4: "Building Description completeness assessment",
    9: "Risk Rating & Management Control review",
    11: "Verify Content listed in Significant Findings and Action Plan is complete"
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
        desc = ""

        for sec in payload.get("sections", []):
            name = sec.get("name", "").strip().lower()

            # match either "3.1 Property Description" or the standalone "Property Site/Description"
            if name.endswith("property description") or "property site/description" in name:
                # 1) try tables first
                for tbl in sec.get("tables", []):
                    for row in tbl.get("rows", []):
                        if len(row) >= 2 and \
                           row[0].strip().lower().replace(" ", "") \
                               .startswith("propertysite/description"):
                            return row[1].strip()

                # 2) fallback to paragraphs
                if sec.get("paragraphs"):
                    # join all lines into one block
                    return " ".join(p.strip() for p in sec["paragraphs"] if p.strip())

        # if we never hit either, it's really not there
        return ""

        # ————— Q9: Risk Dashboard – Management Control & Inherent Risk —————
    if question_number == 9:
        # 1. Find the section named exactly "Overall Risk Rating"
        overall_sec = next(
            (sec for sec in payload.get("sections", [])
             if sec.get("name") == "Overall Risk Rating"),
            None
        )
        if not overall_sec:
            return {"status": "error", "message": "Section 'Overall Risk Rating' not found"}

        # 2. From that section, pick the table whose header is exactly "Overall Risk Rating"
        candidate_tables = [
            tbl for tbl in overall_sec.get("tables", [])
            if tbl.get("header") == "Overall Risk Rating"
        ]
        if not candidate_tables:
            return {"status": "error", "message": "No 'Overall Risk Rating' table present"}

        table = candidate_tables[0]
        rows = table.get("rows", [])

        # 3. Ensure no cell is blank
        blank_cells = []
        for i, row in enumerate(rows):
            for j, cell in enumerate(row):
                if cell is None or cell == "":
                    blank_cells.append({"row": i+1, "col": j+1})

        if blank_cells:
            return {
                "status": "incomplete",
                "message": "Found blank cells in Overall Risk Rating table",
                "blank_cells": blank_cells
            }

        # 4. All good
        return {
            "status": "ok",
            "header": table["header"],
            "rows": rows
        }
    
        # ——— Q11: SFAP completeness check ———
    if question_number == 11:
        issues = []
        for sec in payload.get("sections", []):
            if sec.get("name") == "Significant Findings and Action Plan":
                for tbl in sec.get("tables", []):
                    # skip the title row ["", "<something>"]
                    for row in tbl.get("rows", [])[1:]:
                        label = row[0] if len(row) > 0 else ""
                        content_val = row[1] if len(row) > 1 else ""
                        if label in ("Observation", "Target Date", "Action Required") and not content_val.strip():
                            issues.append({
                                "page": tbl.get("page"),
                                "label": label
                            })
        return {"sfap_issues": issues}

    # fallback
    logger.error(f"No handler for question_number={question_number}; returning empty message")
    return ""

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

    # Q4 prompt
    if question_number == 4:
        return (
            "Question 4: Read the Building Description, ensure that there is content within\n\n"
            f"{content}\n\n"
            "If it’s good and there is content, reply 'PASS'. Otherwise reply 'FAIL'"
        )
    
    # Q9 prompt -> Inherent doesn;t print as table, so if each one is the same this can be done, however would be slightly inconsistent.
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
    
        # ——— Q11 prompt ———
    if question_number == 11:
        issues = content.get("sfap_issues", [])
        if not issues:
            return "PASS"
        detail = "; ".join(f"page {m['page']} missing {m['label']}" for m in issues)
        return f"FAIL: {detail}"
    

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
    resourceName = event.get("resourceName")
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
    ExpiresIn=604800   # link valid for 7 days
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
    for q_num in (3, 4, 9, 11):
        try:
            parsed = extract_json_data(content, q_num)

            if q_num == 9:
                # Local check only—no Bedrock call
                status = "PASS" if parsed.get("status") == "ok" else "FAIL"
                proofing_results["Q9"] = status

            
            elif q_num == 11:
                # build_user_message already returns "PASS" or "FAIL: details"
                prompt = build_user_message(q_num, parsed)
                proofing_results["Q11"] = prompt
                continue

            else:
                # Q3 & Q4 still go to AI
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

    first_name = resourceName.split()[0] if resourceName else "there"


    question_keys = ["Q3", "Q4", "Q9", "Q11"]
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
    html_body_lines = []

    html_body_lines.append(f"<p>Hello {first_name},</p>")
    html_body_lines.append(f"<p>Below are the proofing outputs for '<strong>{buildingName}</strong>' (Work Order #{workOrderNumber}):</p>")

    for q_num, email_heading in EMAIL_QUESTIONS.items():
        q_key = f"Q{q_num}"
        answer = proofing_results.get(q_key, "(no result)")
        indented = "<br>".join(str(answer).splitlines())
        html_body_lines.append(f"<p><strong>{email_heading}:</strong><br>{indented}</p>")

    html_body_lines.append("<p>Regards,<br>Digital Validation</p>")

    html_body_lines.append(
        f'<p>Link to Work Order in Salesforce can be accessed: <a href="https://metrosafety.lightning.force.com/lightning/r/WorkOrder/{work_order_id}/view">here</a></p>'
    )

    html_body_lines.append(
        f'<p>Link to the PDF can be accessed: <a href="{presigned_url}">here</a></p>'
    )

    html_body_text = "\n".join(html_body_lines)

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
                "Html": {
                    "Data": html_body_text,
                    "Charset": "UTF-8"
                    }
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