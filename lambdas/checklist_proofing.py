import json
import boto3
import logging
import re

# ——— Initialise logging ———
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ——— AWS clients ———
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3       = boto3.client('s3')


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
        # 1) Find the 1.1 Areas Identified table
        for sec in payload.get("sections", []):
            if sec.get("name", "").startswith("1.1 Areas Identified"):
                tbl  = sec["tables"][0]
                rows = tbl["rows"]

                # Skip the header row, parse column 1 (“No. of Issues”) from each data row
                issue_counts = [int(r[1]) for r in rows[1:]]
                remedial_by_sec = { r[0]: int(r[1]) for r in rows[1:] }
                total_issues   = sum(issue_counts)

                # 2) Count question-IDs in “Significant Findings and Action Plan”
                sig_ids = set()
                for s2 in payload.get("sections", []):
                    if s2.get("name", "").startswith("Significant Findings"):
                        for line in s2.get("paragraphs", []):
                            m = re.match(r"^(\d+\.\d+)", line.strip())
                            if m:
                                sig_ids.add(m.group(1))

                return {
                    "remedial_by_section": remedial_by_sec,
                    "remedial_total":      total_issues,
                    "sig_item_count":      len(sig_ids)
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
        # 1) pull 3.1 Responsible Persons from the 3.0 section as you already do
        sec30 = next(s for s in payload["sections"]
                if s["name"].startswith("3.0 Management Responsibilities"))
        rp_tbl = next(t for t in sec30["tables"]
                if t["rows"][0][0].startswith("Responsible Persons"))
        responsible_persons = [
            {"Role":r[0].strip(), "Name":r[1].strip(), "Company":r[2].strip()}
            for r in rp_tbl["rows"][1:]
            if len(r)>=3
        ]

        # 2) pull 3.3 from its own section
        sec33 = next((s for s in payload["sections"]
                    if s["name"].startswith("3.3 Accompanying the Risk Assessor")),
                    None)
        accompanying_assessor = ""
        if sec33:
            # join all paragraphs into one block, or pick the first line
            accompanying_assessor = " ".join(sec33["paragraphs"]).strip()

         # 3) pull 3.5 from its own section
        sec35 = next((s for s in payload["sections"]
                     if s["name"].startswith("3.5 Risk Review and Reassessment")),
                    None)
        risk_review_reassessment = ""
        if sec35:
            risk_review_reassessment = " ".join(sec35["paragraphs"]).strip()

        # 4) return exactly the keys your prompt-builder expects
        return {
            "responsible_persons":        responsible_persons,
            "accompanying_assessor":      accompanying_assessor,
            "risk_review_reassessment":   risk_review_reassessment
    }
    # Q13: Significant Findings items
    if question_number == 13:
        for sec in payload.get("sections", []):
            if sec.get("name", "").strip().lower() == "significant findings and action plan":
                return sec.get("items", [])
        return []
    
    #Q15:
    if question_number == 15:
        # 1) Read counts from 6.0 System Asset Register
        sys_counts = {}
        for sec in payload.get("sections", []):
            if sec.get("name", "").startswith("6.0 System Asset Register"):
                tbl = sec.get("tables", [])[0]
                # skip header row, parse name→count
                for asset_name, cnt_str in tbl["rows"][1:]:
                    try:
                        cnt = int(cnt_str)
                    except ValueError:
                        cnt = 0
                    sys_counts[asset_name.strip()] = cnt
                break

        total_sys_assets = sum(sys_counts.values())

        # 2) Find all asset IDs in 7.0 Water Assets (from paragraphs AND tables)
        asset_ids = []
        pattern = re.compile(r"^[A-Za-z0-9]+-\d+$")
        for sec in payload.get("sections", []):
            if sec.get("name", "").startswith("7.0 Water Assets"):
                # scan paragraphs
                for line in sec.get("paragraphs", []):
                    txt = line.strip()
                    if pattern.match(txt):
                        asset_ids.append(txt)
                # scan every cell in every table
                for tbl in sec.get("tables", []):
                    for row in tbl.get("rows", []):
                        for cell in row:
                            txt = cell.strip()
                            if pattern.match(txt):
                                asset_ids.append(txt)
                break

        unique_ids    = sorted(set(asset_ids))
        num_asset_ids = len(unique_ids)

        return {
            "system_counts":    sys_counts,
            "total_sys_assets": total_sys_assets,
            "asset_form_ids":   unique_ids,
            "num_asset_forms":  num_asset_ids
        }

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
        levels  = content["risk_rating_levels"]
        controls= content["management_control_text"]
        inherent= content["inherent_risk_description"]

        return (
            "Question 9: On the Risk Dashboard (Section 2.0), ensure that:\n"
            "  • Risk Rating entries are all completed (e.g. Trivial, Tolerable, Moderate…)\n"
            "  • Management Control of Legionella Risk entries are all completed\n"
            "  • An Inherent Risk narrative appears under “2.1 Current Risk Ratings”\n\n"
            "--- Risk Rating Levels ---\n"
            f"{', '.join(levels) or 'None found'}\n\n"
            "--- Management Control Text ---\n"
            f"{'; '.join(controls) or 'None found'}\n\n"
            "--- Inherent Risk Narrative ---\n"
            f"{inherent or 'None found'}\n\n"
            "If all three components are present and populated, reply “PASS”. Otherwise list which part is missing."
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
            "Question 10: Section 3.0 Management Responsibilities – ensure that:\n"
            "  • Section 3.1 Responsible Persons is fully completed\n"
            "  • Section 3.3 Accompanying the Risk Assessor is populated\n"
            "  • Section 3.5 Risk Review and Reassessment is populated\n\n"
            "--- 3.1 Responsible Persons ---\n"
            f"{rp_lines}\n\n"
            "--- 3.3 Accompanying the Risk Assessor ---\n"
            f"{ac or 'None found'}\n\n"
            "--- 3.5 Risk Review and Reassessment ---\n"
            f"{rv or 'None found'}\n\n"
            "If all three parts are present and complete, reply “PASS”. "
            "Otherwise list which part is missing or incomplete."
        )

    # Q13: Significant Findings
    if question_number == 13:
        msg = (
            "Question 13: “Significant Findings and Action Plan” – read through the Observations & Actions, "
            "checking for spelling mistakes, grammatical errors, technical inaccuracies or poor location descriptions. "
            "Confirm that the Priority labels make sense, and note any missing supplementary photographs.\n\n"
            "--- Significant Findings and Action Plan ---\n"
            f"{content}\n\n"
            "If everything looks good, reply “PASS”. Otherwise, list each discrepancy."
        )
        logger.info("Built user message for question 13")
        return msg
    
    #Q15
    if question_number == 15:
        total = content["total_sys_assets"]
        forms = content["num_asset_forms"]
        ids   = content["asset_form_ids"]
        sys_ct = "\n".join(f"- {name}: {cnt}" for name, cnt in content["system_counts"].items())

        msg = (
            f"--- System Asset Register counts (present) ---\n{sys_ct}\n\n"
            f"Total assets present: {total}\n\n"
            f"--- Unique Asset Form IDs found in Section 7.0 ---\n- " + "\n- ".join(ids) +
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
    # fallback
    logger.error(f"No handler for question_number={question_number}; returning empty message")
    return ""


def send_to_bedrock(user_text):
    MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens":        1000,
        "temperature":       0.0,
        "system": (
            "You are a meticulous proofreader. "
            "Correct spelling, grammar and clarity only — no extra commentary or re-structuring."
        ),
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
    return response_text


def process(event, context):
    """
    Lambda entry point.
    Expects: 
      - event['json_bucket'], event['json_key']
      - optional event['question_number'] (defaults to 13)
    """
    logger.info(f"Event received: {event}")
    bucket = event.get("json_bucket")
    key    = event.get("json_key")
    q_num  = event.get("question_number", 13)

    # 1) fetch your pre-processed Textract JSON
    raw_json = s3.get_object(Bucket=bucket, Key=key)["Body"]\
                  .read().decode("utf-8")

    # 2) extract the section or items
    data = extract_json_data(raw_json, q_num)

    # 3) build the Bedrock prompt & invoke
    user_msg = build_user_message(q_num, data)
    result   = send_to_bedrock(user_msg)
    logger.info(f"Bedrock response payload: {result}")

    # 4) return the AI’s proofed text
    return {
        "statusCode": 200,
        "body":       json.dumps({"bedrock_response": result})
    }
