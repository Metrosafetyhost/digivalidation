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
    9: "Risk Rating & Management Control review"
}

def extract_json_data(json_content, question_number):
    payload = json.loads(json_content)

    # ————— Q2: Verify Contents listing for Water Assets & Appendices A–D —————
    if question_number == 2:
        # 1) find the “Contents” section
        toc_rows = []
        for sec in payload.get("sections", []):
            if sec.get("name", "").strip() == "Contents":
                toc_rows = sec.get("tables", [])[0].get("rows", [])
                break

        # 2) pull out each TOC entry (skip header row)
        headings = [r[0].strip() for r in toc_rows[1:]]

        # 3) detect any row containing “Water Assets”
        water_assets_entries = [h for h in headings if "Water Assets" in h]

        # 4) detect all Appendix A–D entries
        appendices = []
        for h in headings:
            m = re.match(r"^(APPENDIX [A-D])", h.upper())
            if m:
                appendices.append(m.group(1))

        return {
            "toc_headings":          headings,
            "water_assets_entries":  water_assets_entries,
            "appendices_found":      appendices
        }
    
    # Q3: Remedial-actions vs Significant Findings count
    if question_number == 3:
        remedial_by_section = {}
        total_issues = 0

        for sec in payload.get("sections", []):
            if sec.get("name", "").startswith("1.1"):
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

    # Q4: Building Description
    if question_number == 4:
        for sec in payload.get("sections", []):
            if sec.get("name", "").lower().endswith("building description"):
                for tbl in sec.get("tables", []):
                    for key, val in tbl.get("rows", []):
                        if key.lower().startswith("description of the property"):
                            return val.strip()
        return ""

    # ——— Q5: Water Systems vs Water Assets ———
    if question_number == 5:
        water_desc = ""
        assets     = set()

        for sec in payload.get("sections", []):
            sec_name = sec.get("name", "").lower()

            # 1) Pull the narrative under “Description of the Water Systems”
            if sec_name.endswith("building description"):
                for tbl in sec.get("tables", []):
                    for key, val in tbl.get("rows", []):
                        if key.lower().startswith("description of the water systems"):
                            water_desc = val.strip()

            # 2) Scan every table in the doc for asset IDs of the form “XXXXX-NN”
            for tbl in sec.get("tables", []):
                for row in tbl.get("rows", []):
                    candidate = row[1].strip()
                    # e.g. matches MCW-01, POU-01, MPOU-01, CWS-02, etc.
                    if re.match(r"^[A-Za-z0-9]+-\d+$", candidate):
                        assets.add(candidate)

        return {
            "description": water_desc,
            "assets":      sorted(assets)
        }
    
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

    # Q10
    if question_number == 10:
        # 1) Section 3.1 Responsible Persons (table)
        sec31 = next(
            (s for s in payload["sections"]
             if s.get("name", "").startswith("3.1 Responsible Persons")),
            None
        )
        if not sec31 or not sec31.get("tables"):
            raise ValueError("Could not find section '3.1 Responsible Persons' or its table for Q10.")
        rp_tbl = sec31["tables"][0]
        responsible_persons = [
            {"Role": row[0].strip(),
             "Name": row[1].strip(),
             "Company": row[2].strip()}
            for row in rp_tbl["rows"][1:]
            if len(row) >= 3
        ]

        # 2) Section 3.3 Accompanying the Risk Assessor (paragraphs)
        sec_3_3 = next(
            (s for s in payload["sections"]
             if s.get("name", "").startswith("3.3 Accompanying the Risk Assessor")),
            None
        )
        accompanying_assessor = (
            [p.strip() for p in sec_3_3.get("paragraphs", [])
             if p.strip() and not p.startswith("Printed from")]
            if sec_3_3 else []
        )

        # 3) Section 3.5 Risk Review and Reassessment (paragraphs)
        sec_3_5 = next(
            (s for s in payload["sections"]
             if s.get("name", "").startswith("3.5 Risk Review and Reassessment")),
            None
        )
        risk_review_reassessment = (
            [p.strip() for p in sec_3_5.get("paragraphs", [])
             if p.strip() and not p.startswith("Printed from")]
            if sec_3_5 else []
        )

        return {
            "responsible_persons":        responsible_persons,
            "accompanying_assessor":      accompanying_assessor,
            "risk_review_reassessment":   risk_review_reassessment
        }

     # ——— Q12: Written Scheme of Control ———
    if question_number == 12:
        issues = []
        # Locate the "4.1 Water Control Scheme" section
        for sec in payload.get("sections", []):
            if sec.get("name", "").startswith("4.1"):
                tables = sec.get("tables", [])
                if tables:
                    rows = tables[0].get("rows", [])
                    # Skip header row
                    for row in rows[1:]:
                        task = row[0].strip()
                        comment = row[2].strip() if len(row) > 2 else ""
                        missing = []
                        if not comment:
                            missing.append("comment")
                        # Look for a date in dd/mm/yyyy format
                        if not re.search(r"\b\d{2}/\d{2}/\d{4}\b", comment):
                            missing.append("date")
                        if missing:
                            issues.append({"task": task, "missing": missing})
                break
        return {"scheme_issues": issues}

    #Q15:
    if question_number == 15:
        # 1) Read counts from the “System Asset Register” section
        sys_counts = {}
        sys_sec = next(
            (s for s in payload.get("sections", [])
             if re.search(r"System Asset Register", s.get("name", ""), re.IGNORECASE)),
            None
        )
        if sys_sec and sys_sec.get("tables"):
            tbl = sys_sec["tables"][0]
            for asset_name, cnt_str in tbl["rows"][1:]:
                try:
                    cnt = int(cnt_str)
                except ValueError:
                    cnt = 0
                sys_counts[asset_name.strip()] = cnt

        total_sys_assets = sum(sys_counts.values())

        # 2) Find all asset-form IDs under “Water Assets”
        # IDs like CAL-01, CALP-02, MCWS-01, MULTI-01, SHOWER-04, etc.
        id_pattern = re.compile(r"^[A-Z]{2,6}-\d{2}$")
        asset_ids = []

        sections = payload.get("sections", [])
        start_idx = next(
            (i for i, s in enumerate(sections)
             if re.search(r"Water Assets\s*$", s.get("name", ""), re.IGNORECASE)),
            None
        )

        if start_idx is not None:
            for s in sections[start_idx + 1:]:
                name = s.get("name", "")
                # stop at next top-level section (e.g. “9.0 …”)
                if re.match(r"^\d+\.0\s+", name) and not re.search(r"Water Assets", name, re.IGNORECASE):
                    break

                # 2a) paragraphs: full-match ID only
                for line in s.get("paragraphs", []):
                    txt = line.strip()
                    if id_pattern.fullmatch(txt):
                        asset_ids.append(txt)

                # 2b) tables: extract all IDs anywhere in cells
                for tbl in s.get("tables", []):
                    for row in tbl.get("rows", []):
                        for cell in row:
                            for m in id_pattern.findall(cell or ""):
                                asset_ids.append(m)

                # 2c) fields: some extra extracted values
                for field in s.get("fields", []):
                    if isinstance(field, dict):
                        val = field.get("value", "").strip()
                        if id_pattern.fullmatch(val):
                            asset_ids.append(val)

        unique_ids = sorted(set(asset_ids))
        return {
            "system_counts":    sys_counts,
            "total_sys_assets": total_sys_assets,
            "asset_form_ids":   unique_ids,
            "num_asset_forms":  len(unique_ids)
        }
    
    if question_number == 16:
        # Local Water Assets validation
        issues = validate_water_assets(payload.get("sections", []))
        return {"assets_issues": issues}

    return None


def build_user_message(question_number, content):

     # ————— Q2 prompt —————
    if question_number == 2:
        headings = content.get("toc_headings", [])
        wa = content.get("water_assets_entries", [])
        ap = content.get("appendices_found", [])

        # compute which appendices A–D are missing
        expected = ["APPENDIX A", "APPENDIX B", "APPENDIX C", "APPENDIX D"]
        missing = [x for x in expected if x not in ap]

        return (
            "On the Contents page, ensure that “Water Assets” is listed and that "
            "Appendices A–D are all present.\n\n"
            "--- Table of Contents ---\n"
            f"{chr(10).join(headings)}\n\n"
            f"Water Assets entries found: {', '.join(wa) or 'None'}\n"
            f"Appendices found: {', '.join(ap) or 'None'}\n"
            f"Missing appendices: {', '.join(missing) or 'None'}\n\n"
            "If both Water Assets and all Appendices A–D appear, reply “PASS”. "
            "Otherwise list what’s missing."
        )
    
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
            "Question 4: Read the Building Description, ensuring it’s complete, concise and relevant.\n\n"
            f"{content}\n\n"
            "If it’s good, reply “PASS”. Otherwise list any missing or unclear details."
        )
    

    # Q5 prompt
    if question_number == 5:
        desc   = content.get("description", "")
        assets = content.get("assets", [])

        return (
            "Question 5: Read the Water Systems description and cross-check with the Water Assets forms.\n\n"
            "--- Water Systems Description ---\n"
            f"{desc}\n\n"
            "--- Water Asset IDs Found in Report ---\n"
            f"{', '.join(assets) or 'None found'}\n\n"
            "In the description you should see each asset type named (e.g. “Mains Cold Water Services (MCWS)”, "
            "“Point of Use (POU-01)”, “Multipoint of Use (MPOU-01)”). In the Asset Forms you should see a matching "
            "asset ID for each (e.g. MCW-01, POU-01, MPOU-01).\n\n"
            "If every asset mentioned in the description has exactly one corresponding form entry and no extras, "
            "reply “PASS”. Otherwise list what’s missing or extra."
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
    #Q10
    if question_number == 10:
        rp = content["responsible_persons"]
        ac = content["accompanying_assessor"]
        rv = content["risk_review_reassessment"]

        rp_lines = "\n".join(
            f"- {p['Role']}: {p['Name']} ({p['Company']})"
            for p in rp
        ) or "None found"

        return (
            "Question 10: ensure that:\n"
            "  Section 3.1 Responsible Persons is fully completed\n"
            "  Section 3.3 Accompanying the Risk Assessor is populated\n"
            "  Section 3.5 Risk Review and Reassessment is populated\n\n"
            "--- 3.1 Responsible Persons ---\n"
            f"{rp_lines}\n\n"
            "--- 3.3 Accompanying the Risk Assessor ---\n"
            f"{ac or 'None found'}\n\n"
            "--- 3.5 Risk Review and Reassessment ---\n"
            f"{rv or 'None found'}\n\n"
            "If all three parts are present and complete, reply “PASS”. "
            "Otherwise list which part is missing or incomplete."
        )
    
    # ——— Q12 Prompt ———
    if question_number == 12:
        issues = content.get("scheme_issues", [])
        # If no missing fields, it's a PASS
        if not issues:
            return (
                "Question 12: Section 4.0 Legionella Control Programme of Preventative Works and the Written Scheme of Control – "
                "All tasks have both a date and a comment. PASS."
            )
        # Build a list of missing items
        detail_lines = "\n".join(
            f"- {i['task']}: missing {', '.join(i['missing'])}"
            for i in issues
        )
        return (
            "Question 12: Section 4.0 Legionella Control Programme of Preventative Works and the Written Scheme of Control –ensure each task has a date (dd/mm/yyyy) and a meaningful comment.\n\n"
            f"{detail_lines}\n\n" 
            "If all entries have dates and comments, reply “PASS”. Otherwise list which tasks are missing which fields."
        )
    
    #Q15
    if question_number == 15:
        total = content["total_sys_assets"]
        forms = content["num_asset_forms"]
        ids   = content["asset_form_ids"]
        sys_ct = "\n".join(f"- {name}: {cnt}" for name, cnt in content["system_counts"].items())

        msg = (
            f"--- System Asset Register counts (present) ---\n{sys_ct}\n\n"
            f"Total assets present: {total}\n\n"
            f"--- Unique Asset Form IDs found in Water Assets ---\n- " + "\n- ".join(ids) +
            f"\n\nCount of asset forms: {forms}\n\n"
        )
        if total == forms:
            msg += "Totals match, reply “PASS”."
        else:
            diff = total - forms
            msg += (
                f"Discrepancy detected: {total} assets registered but {forms} asset forms found "
                f"(difference of {diff:+d})."
            )
        return msg
    
    # ——— Q16 Prompt ———
    if question_number == 16:
        issues = content.get("assets_issues", [])

        # Separate cases: only photos vs other issues
        photo_only = all(all(item == "photos manual check" for item in entry["missing"]) for entry in issues)
        if photo_only:
            ids = ", ".join(entry['record'] for entry in issues)
            return (
                "Question 16: Section 7.0 Water Assets – data fields and comments are present. "
                f"Please manually verify photographs for records: {ids}."
            )
        # Otherwise detail all missing
        lines = []
        for entry in issues:
            items = ", ".join(entry['missing'])
            lines.append(f"- {entry['record']}: {items}")
        detail = "\n".join(lines)
        return (
            "Question 16: Section Water Assets – please check the following asset entries for missing data/comments/photos:\n\n"
            f"{detail}\n\n"
            "Once corrected or verified, reply “PASS”."
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
    test_address   = "luke.gasson@metrosafety.co.uk"
    emailAddress   = test_address
    buildingName    = event.get("buildingName")
    workTypeRef     = event.get("workTypeRef")

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
    for q_num in range(1, 16):
        try:
            parsed_content = extract_json_data(content, q_num)
            prompt         = build_user_message(q_num, parsed_content)

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

    # ——— 5) Build a structured plaintext email body ———
    subject = f"Proofing Results: {buildingName} (WO #{workOrderNumber})"
    body_lines = []
    body_lines.append("Hello,\n")
    body_lines.append(f"Below are the proofing outputs for *{buildingName}* (Work Order #{workOrderNumber}):\n")
    body_lines.append(f"Link: https://metrosafety.lightning.force.com/lightning/r/WorkOrder/{work_order_id}/view")

    for q_num, email_heading in EMAIL_QUESTIONS.items():
        q_key = f"Q{q_num}"
        answer = proofing_results.get(q_key, "(no result)")
        # indent each line of the AI’s answer
        indented = "\n".join("  " + ln for ln in str(answer).splitlines())
        body_lines.append(f"{email_heading}:\n{indented}\n")

    body_lines.append("Regards,\nQuality Team\n")
    body_text = "\n".join(body_lines)

    # ——— 6) Send the email via SES ———
    source_email = "luke.gasson@metrosafety.co.uk"
    if not source_email:
        logger.error("SES_SOURCE_EMAIL not set in environment.")
        return {"statusCode": 500, "body": "Missing SES_SOURCE_EMAIL"}

    bcc_env  = os.environ.get("BCC_ADDRESSES", "")
    bcc_list = [addr.strip() for addr in bcc_env.split(",") if addr.strip()]

    email_params = {
        "Source": source_email,
        "Destination": {
            "ToAddresses": [emailAddress],
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
        logger.info("Sent proofing email to %s (bcc: %s)", test_address, bcc_list)
    except Exception as e:
        logger.error("Failed to send SES email: %s", e, exc_info=True)
        return {"statusCode": 500, "body": "Error sending email"}

    return {"statusCode": 200, "body": "Checklist processing complete"}