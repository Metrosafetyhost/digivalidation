import json
import boto3
import logging
import re
from botocore.client import Config
import os
from botocore.exceptions import ClientError
# ——— Initialise logging ———
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ——— AWS clients ———
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')

s3 = boto3.client(
    "s3",
    region_name="eu-west-2",
    config=Config(signature_version="s3v4")
)
ses = boto3.client('ses', region_name='eu-west-2')

BCC_ADDRESSES = "metroit@metrosafety.co.uk"

EMAIL_QUESTIONS = {
    3: "Totals consistency check (Section 1.1 vs Significant Findings and Action Plan)",
    4: "Building Description completeness assessment",
    9: "Life Safety Risk Rating at this Premises review",
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
        return payload
    
    if question_number == 9:
        rating = None

        # 1) find the section by name
        for sec in payload.get("sections", []):
            name = sec.get("name", "").lower()
            if name.startswith("life safety risk rating at this premises"):
                # 2) scan its paragraphs for “is: <value>”
                for para in sec.get("paragraphs", []):
                    m = re.search(r"is[:\s]+(.+)", para, flags=re.IGNORECASE)
                    if m:
                        rating = m.group(1).strip()
                        break
                break

        # 3) null-check and return PASS/FAIL
        if rating:
            return {"Q9": "PASS", "value": rating}
        else:
            return {"Q9": "FAIL", "value": None}
        
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

def check_building_description(sections):
    """
    Returns (all_populated: bool, bd_sections: list)
    
    A section is considered “populated” if it has at least one table **with rows**
    OR if it has at least one non-blank paragraph.
    """
    # 1. find all “major” numbers (eg "3", "5") where there's a “.0 Building Description” heading
    bd_root = re.compile(r'^(\d+)\.0\s+Building Description', re.IGNORECASE)
    building_majors = {
        m.group(1)
        for sec in sections
        for m in [bd_root.match(sec.get('name', ''))]
        if m
    }

    if not building_majors:
        return True, []

    # 2. collect every section whose “major” digit is in that set
    major_pat = re.compile(r'^(\d+)\.\d+')
    bd_sections = [
        sec for sec in sections
        if (m := major_pat.match(sec.get('name', ''))) 
           and m.group(1) in building_majors
    ]

    # helper: does this section actually contain data?
    def has_content(sec):
        # a) any table with at least one row?
        for tbl in sec.get('tables', []):
            if tbl.get('rows'):
                return True
        # b) any non-blank paragraph?
        for p in sec.get('paragraphs', []):
            if p.strip():
                return True
        return False

    # only keep the sections that actually have content
    data_secs = [sec for sec in bd_sections if has_content(sec)]

    # if there were no real data-sections at all, we treat as PASS
    if not data_secs:
        return True, []

    # 3. ensure each data-section is populated
    for sec in data_secs:
        # if it had tables, make sure none are empty
        for tbl in sec.get('tables', []):
            if not tbl.get('rows'):
                return False, data_secs
        # if it had no tables, but had paragraphs, that's okay
    return True, data_secs

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

    CHANGES_BUCKET = "metrosafety-bedrock-output-data-dev-bedrock-lambda"
    changes_key = f"changes/{work_order_id}_changes.csv"

    changes_url = None

    try:
        # Optional existence check
        s3.head_object(Bucket=CHANGES_BUCKET, Key=changes_key)
        changes_url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": CHANGES_BUCKET, "Key": changes_key},
            ExpiresIn=604800
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            logger.warning(f"Changes CSV not found: {changes_key}")
        else:
            logger.exception("Error generating presigned URL for changes CSV")

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

    proofing_results = {}
    for q_num in (3, 4, 9, 11):
        parsed = extract_json_data(content, q_num)

        # Q4 inline (as before)
        if q_num == 4:
            sections = parsed.get("sections", [])
            all_ok, bd_secs = check_building_description(sections)
            if not all_ok:
                empty = [
                    s["name"]
                    for s in bd_secs
                    if s.get("tables") and any(not t.get("rows") for t in s["tables"])
                ]
                logger.warning("Q4 missing table-content in: %s", empty)
            proofing_results["Q4"] = "PASS" if all_ok else "FAIL"
            continue

        # Q9 inline (same style as Q4)
        if q_num == 9:
            # parsed is {"Q9": "PASS"|"FAIL", "value": "<Moderate>"|None}
            proofing_results["Q9"] = parsed.get("Q9")
            # if you want to keep the actual rating for later:
            proofing_results["Q9_value"] = parsed.get("value", "")
            continue

        if q_num == 11:
            proofing_results["Q11"] = build_user_message(11, parsed)  # PASS or FAIL: details
            continue


        # Q3 still via Bedrock
        if q_num == 3:
            try:
                prompt = build_user_message(q_num, parsed)
                if not prompt:
                    proofing_results["Q3"] = "(no prompt built)"
                else:
                    ai_reply = send_to_bedrock(prompt)
                    proofing_results["Q3"] = ai_reply or "(empty response)"
            except Exception as ex:
                logger.warning(
                    "Error while processing Q3 for WorkOrder %s: %s",
                    work_order_id, ex, exc_info=True
                )
                proofing_results["Q3"] = f"ERROR: {ex}"
            continue
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
        f"{digital_outcome} || "
        f"{workOrderNumber}/"
        f"{work_order_id} || "
        f"{buildingName} || "
        f"{workTypeRef}"
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

    # html_body_lines.append(
    #     f'<p>Link to Work Order in Salesforce can be accessed: <a href="https://metrosafety.lightning.force.com/lightning/r/WorkOrder/{work_order_id}/view">here</a></p>'
    # )

    html_body_lines.append(
         f'<p>Link to the spelling/grammar changes made to the Building Description & Actions can be found: <a href="{changes_url}">here</a></p>'
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