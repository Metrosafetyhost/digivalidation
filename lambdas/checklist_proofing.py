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
        # Q6: Risk Dashboard completeness
    if question_number == 6:
        rr_levels   = []
        mcr_texts   = []
        inherent_txt = ""

        # find section 2.0
        for sec in payload.get("sections", []):
            if sec.get("name", "").startswith("2.0 Risk Dashboard"):
                # 1) pull Risk Rating ↔ Management Control table
                for tbl in sec.get("tables", []):
                    hdr = [c.strip() for c in tbl["rows"][0]]
                    if hdr[0] == "Risk Rating" and "Management Control" in hdr[1]:
                        for rating, control in tbl["rows"][1:]:
                            rr_levels.append(rating.strip())
                            mcr_texts.append(control.strip())

                # 2) fallback: parse paragraphs for the inherent‐risk narrative
                paras = sec.get("paragraphs", [])
                # find “2.1 Current Risk Ratings”
                start = next((i for i,l in enumerate(paras) 
                              if re.match(r"^\s*2\.1\s+Current Risk Ratings", l)), None)
                if start is not None:
                    lines = []
                    for line in paras[start+1:]:
                        text = line.strip()
                        # stop at footer
                        if text.startswith("Printed from") or re.match(r"^Page \d+ of", text):
                            break
                        # skip single-word cruft
                        if text.lower() in {"risk", "rating", "overall risk rating"}:
                            continue
                        lines.append(text)
                    inherent_txt = " ".join(lines)

                break

        return {
            "risk_rating_levels":       rr_levels,
            "management_control_text":  mcr_texts,
            "inherent_risk_description": inherent_txt
        }

    # Q13: Significant Findings items
    if question_number == 13:
        for sec in payload.get("sections", []):
            if sec.get("name", "").strip().lower() == "significant findings and action plan":
                return sec.get("items", [])
        return []

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
            "Water Hygiene/Legionella Risk Assessment QCC Query:\n\n"
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
    
    # Q6 prompt
    if question_number == 6:
        levels  = content["risk_rating_levels"]
        ctrls   = content["management_control_text"]
        inherent= content["inherent_risk_description"]

        return (
            "Water Hygiene/Legionella Risk Assessment QCC Query:\n\n"
            "Question 6: On the Risk Dashboard (Section 2.0), ensure that:\n"
            "  • Risk Rating entries are all completed (e.g. Trivial, Tolerable, Moderate…)\n"
            "  • Management Control of Legionella Risk entries are all completed\n"
            "  • An Inherent Risk narrative appears under “2.1 Current Risk Ratings”\n\n"
            "--- Risk Rating Levels ---\n"
            f"{', '.join(levels) or 'None found'}\n\n"
            "--- Management Control Text ---\n"
            f"{'; '.join(ctrls) or 'None found'}\n\n"
            "--- Inherent Risk Narrative ---\n"
            f"{inherent or 'None found'}\n\n"
            "If all three components are present and populated, reply “PASS”. Otherwise list which part is missing."
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
