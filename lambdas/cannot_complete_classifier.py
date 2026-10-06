import json
import logging

import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

bedrock_client = boto3.client(
    "bedrock-runtime",
    region_name="eu-west-2"
)

BEDROCK_MODEL_ID = "anthropic.claude-3-7-sonnet-20250219-v1:0"


ISSUE_TYPES = {
    "NO_ANSWER",
    "RESIDENT_REFUSED_ACCESS",
    "KEY_FOB_CODE_ISSUE",
    "APPOINTMENT_REQUIRED",
    "ESCORT_OR_STAFF_REQUIRED",
    "BUILDING_OR_ENTRANCE_IDENTIFICATION",
    "BUILDING_WORKS_OR_BOARDED_UP",
    "TEMPORARY_ACCESS_ISSUE",
    "METRO_FAILURE",
    "DRAWING_OR_PLANSTUDIO_ISSUE",
    "OTHER_INTERNAL_ISSUE",
    "OTHER_REVIEW_REQUIRED",
}

FAILURE_PARTIES = {
    "CUSTOMER",
    "METRO",
    "INTERNAL",
    "UNKNOWN",
}


SYSTEM_PROMPT = """
You classify Cannot Complete Service Appointment records for Metro Safety.

You will receive:
- Reason Not Complete: a structured Salesforce value.
- Reason Description: free text entered by a field operative.

Your task is to:
1. Classify the Cannot Complete into one allowed issue type.
2. Identify the failure party.
3. Identify whether there is an explicit access barrier.
4. Identify whether the issue is temporary.
5. Identify whether the issue requires internal review.
6. Produce a professionally written version of the original description.

Return ONLY a valid JSON object with exactly these fields:

{
  "issueType": "...",
  "failureParty": "...",
  "explicitAccessBarrier": true,
  "temporaryIssue": false,
  "requiresInternalReview": false,
  "cleanedDescription": "..."
}

Allowed issueType values:

NO_ANSWER
RESIDENT_REFUSED_ACCESS
KEY_FOB_CODE_ISSUE
APPOINTMENT_REQUIRED
ESCORT_OR_STAFF_REQUIRED
BUILDING_OR_ENTRANCE_IDENTIFICATION
BUILDING_WORKS_OR_BOARDED_UP
TEMPORARY_ACCESS_ISSUE
METRO_FAILURE
DRAWING_OR_PLANSTUDIO_ISSUE
OTHER_INTERNAL_ISSUE
OTHER_REVIEW_REQUIRED

Allowed failureParty values:

CUSTOMER
METRO
INTERNAL
UNKNOWN

CLASSIFICATION GUIDANCE

NO_ANSWER:
Use when nobody answers the door, nobody is home, there is no response,
or equivalent wording.

Examples:
- "No answer on door"
- "No one home"
- "No response"
- "Nobody answered"

RESIDENT_REFUSED_ACCESS:
Use when a resident, tenant, occupier or other person explicitly refuses
or denies access.

Examples:
- "Resident refused access"
- "Tenant would not let me in"
- "Resident denied access"
- "Resident states it is inconvenient and is not allowing me in"

KEY_FOB_CODE_ISSUE:
Use when access is prevented by missing, unavailable or non-functioning
keys, fobs, codes or other access-control credentials.

Examples:
- "Key does not work"
- "Fob does not work"
- "No key available"
- "Unable to access with entry code"

APPOINTMENT_REQUIRED:
Use when access requires an appointment, advance notice or pre-arranged visit.

Examples:
- "Need to arrange appointment in advance"
- "Resident requires notice"
- "Appointment required before attending"

ESCORT_OR_STAFF_REQUIRED:
Use when a member of staff, concierge, site representative, escort or another
authorised person must be present to allow access.

Examples:
- "Staff member needs to be present"
- "Escort required"
- "Concierge needs to provide access"

BUILDING_OR_ENTRANCE_IDENTIFICATION:
Use when the operative cannot identify the correct building, entrance,
flat, block or access point and clarification is required.

Examples:
- "Unable to locate entrance"
- "Could not identify correct block"
- "Building could not be found"

BUILDING_WORKS_OR_BOARDED_UP:
Use when the building is boarded up or inaccessible because of building works,
construction conditions or a similar physical state.

Examples:
- "Building boarded up"
- "Construction site"
- "Access blocked by building works"

TEMPORARY_ACCESS_ISSUE:
Use when access is prevented by an explicitly temporary problem which would
reasonably be expected to clear without client intervention.

Examples:
- temporary roadworks
- temporary obstruction
- temporary closure

Do not use this category for permanent access restrictions.

METRO_FAILURE:
Use when the visit failed because of Metro Safety or the operative rather than
the customer or site.

Examples:
- operative unable to attend
- Metro scheduling error
- Metro equipment/problem prevented completion

DRAWING_OR_PLANSTUDIO_ISSUE:
Use for drawing, PlanStudio, survey-plan or related technical/instruction issues.

Examples:
- drawing does not match site
- PlanStudio issue
- survey plan unclear

OTHER_INTERNAL_ISSUE:
Use for another clearly internal Metro Safety issue which does not fit the
categories above.

OTHER_REVIEW_REQUIRED:
Use when the available information is ambiguous, contradictory, unclear or
cannot confidently be classified.

FAILURE PARTY GUIDANCE

CUSTOMER:
Use when the failure is caused by customer/site access circumstances,
occupants, refusal, no answer, site arrangements, keys, fobs, codes,
appointments, escorts or similar customer-controlled circumstances.

METRO:
Use when Metro Safety or the operative caused the failure.

INTERNAL:
Use for drawing, PlanStudio, unclear instructions or another internal
Metro Safety process issue.

UNKNOWN:
Use only when the responsible party cannot reasonably be determined.

EXPLICIT ACCESS BARRIER

Set explicitAccessBarrier to true when simply returning for another unarranged
visit is unlikely to solve the issue.

Examples where explicitAccessBarrier should normally be true:
- resident refuses access
- appointment required
- advance notice required
- escort or staff member required
- key/fob/code unavailable or unusable
- building or entrance requires clarification

Do NOT set explicitAccessBarrier to true merely because nobody answered.

TEMPORARY ISSUE

Set temporaryIssue to true only when the text explicitly describes a temporary
problem that would reasonably be expected to clear without customer or internal
intervention.

Example:
- temporary roadworks preventing access

REQUIRES INTERNAL REVIEW

Set requiresInternalReview to true for:
- drawing issues
- PlanStudio issues
- unclear survey instructions
- contradictory information
- unclear internal processes
- another issue requiring Metro Safety staff to investigate

CLEANED DESCRIPTION

For cleanedDescription:
- Rewrite the operative's text as a concise, professional sentence in British English.
- Correct spelling, punctuation and grammar.
- Improve terse or informal field notes into natural professional wording.
- You may add small grammatical words such as articles, pronouns or auxiliary verbs where required to make the sentence read naturally.
- Preserve the exact meaning of the original description.
- Do not invent events, causes, people, access requirements or other facts.
- Do not remove meaningful information.
- Preserve practical details such as dates, contact instructions, names, phone numbers and access requirements.
- Prefer clear complete sentences over shorthand or fragments.
- Keep the wording concise.

Examples:

"No answer at door"
→ "There was no answer at the door."

"Resident wont allow access"
→ "The resident refused access."

"Key fob doesnt work"
→ "The key fob did not provide access."

"Need to arrange access an appointment in advance"
→ "An appointment needs to be arranged in advance to gain access."

"Rang door bell but no response from residents"
→ "The doorbell was rung, but there was no response from the residents."

IMPORTANT:
Do NOT determine the final Work Order outcome.
Do NOT return:
- ROUTINE_RETRY
- CLIENT_ACCESS_REQUIRED
- INTERNAL_REVIEW
- RESOLVED

Those final Work Order decisions are handled separately using deterministic
business rules and Service Appointment history.

Return valid JSON only.
Do not use markdown.
Do not use code fences.
Do not include any explanation outside the JSON object.
"""


def parse_event(event):
    """
    Supports both direct Lambda invocation and API Gateway-style requests.
    """
    if isinstance(event, dict) and "body" in event:
        body = event.get("body")

        if isinstance(body, str):
            return json.loads(body)

        if isinstance(body, dict):
            return body

    return event


def validate_request(payload):
    """
    Validate the expected Salesforce -> AWS request.
    """
    if not isinstance(payload, dict):
        raise ValueError("Request payload must be a JSON object.")

    required_fields = [
        "serviceAppointmentId",
        "workOrderId",
        "reasonNotComplete",
        "reasonDescription",
    ]

    missing_fields = [
        field
        for field in required_fields
        if field not in payload
    ]

    if missing_fields:
        raise ValueError(
            "Missing required fields: "
            + ", ".join(missing_fields)
        )


def get_blank_description_result(
    reason_not_complete,
    reason_description
):
    """
    Conor's explicit SNG rule:

    If the Cannot Complete reason is 'Unable to get Access'
    and the description is blank, treat the visit as
    'No answer at the door'.

    This is deterministic, so we do not call Bedrock.
    """
    if (
        reason_not_complete.strip().lower() == "unable to get access"
        and not reason_description.strip()
    ):
        return {
            "issueType": "NO_ANSWER",
            "failureParty": "CUSTOMER",
            "explicitAccessBarrier": False,
            "temporaryIssue": False,
            "requiresInternalReview": False,
            "cleanedDescription": "No answer at the door.",
        }

    return None


def call_bedrock(
    reason_not_complete,
    reason_description
):
    """
    Send the Cannot Complete reason and free text to Claude
    through Amazon Bedrock.
    """

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": (
                    "Classify this Cannot Complete record.\n\n"
                    f"Reason Not Complete:\n"
                    f"{reason_not_complete}\n\n"
                    f"Reason Description:\n"
                    f"{reason_description}"
                ),
            }
        ],
        "max_tokens": 1000,
        "temperature": 0,
    }

    logger.info(
        "Sending Cannot Complete classification request to Bedrock."
    )

    response = bedrock_client.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(payload),
    )

    response_body = json.loads(
        response["body"].read().decode("utf-8")
    )

    model_text = " ".join(
        item.get("text", "")
        for item in response_body.get("content", [])
        if item.get("type") == "text"
    ).strip()

    if not model_text:
        raise RuntimeError(
            "Bedrock returned no text content."
        )

    logger.info(
        "Bedrock classification response received."
    )

    try:
        return json.loads(model_text)

    except json.JSONDecodeError:
        logger.error(
            "Bedrock did not return valid JSON. Response: %s",
            model_text,
        )

        raise RuntimeError(
            "Bedrock did not return valid JSON."
        )


def validate_classification(result):
    """
    Validate Bedrock output before returning it to Salesforce.
    """

    if not isinstance(result, dict):
        raise ValueError(
            "Classification result must be a JSON object."
        )

    required_fields = [
        "issueType",
        "failureParty",
        "explicitAccessBarrier",
        "temporaryIssue",
        "requiresInternalReview",
        "cleanedDescription",
    ]

    missing_fields = [
        field
        for field in required_fields
        if field not in result
    ]

    if missing_fields:
        raise ValueError(
            "Bedrock response is missing required fields: "
            + ", ".join(missing_fields)
        )

    issue_type = result.get("issueType")

    if issue_type not in ISSUE_TYPES:
        raise ValueError(
            f"Invalid issueType returned: {issue_type}"
        )

    failure_party = result.get("failureParty")

    if failure_party not in FAILURE_PARTIES:
        raise ValueError(
            f"Invalid failureParty returned: {failure_party}"
        )

    boolean_fields = [
        "explicitAccessBarrier",
        "temporaryIssue",
        "requiresInternalReview",
    ]

    for field in boolean_fields:
        if not isinstance(result.get(field), bool):
            raise ValueError(
                f"{field} must be a boolean."
            )

    cleaned_description = result.get(
        "cleanedDescription"
    )

    if not isinstance(cleaned_description, str):
        raise ValueError(
            "cleanedDescription must be a string."
        )

    allowed_fields = set(required_fields)

    cleaned_result = {
        key: value
        for key, value in result.items()
        if key in allowed_fields
    }

    # Deterministic business rule:
    if cleaned_result.get("issueType") == "TEMPORARY_ACCESS_ISSUE":
        cleaned_result["explicitAccessBarrier"] = False
        cleaned_result["temporaryIssue"] = True

    return cleaned_result


def success_response(result):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(result),
    }


def error_response(status_code, message):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(
            {
                "error": message
            }
        ),
    }


def process_record(payload):
    validate_request(payload)

    service_appointment_id = (
        payload.get("serviceAppointmentId") or ""
    ).strip()

    work_order_id = (
        payload.get("workOrderId") or ""
    ).strip()

    reason_not_complete = (
        payload.get("reasonNotComplete") or ""
    ).strip()

    reason_description = (
        payload.get("reasonDescription") or ""
    ).strip()

    logger.info(
        "Processing Cannot Complete classification. "
        "serviceAppointmentId=%s workOrderId=%s",
        service_appointment_id,
        work_order_id,
    )

    deterministic_result = get_blank_description_result(
        reason_not_complete,
        reason_description,
    )

    if deterministic_result is not None:
        logger.info(
            "Applied deterministic blank-description "
            "NO_ANSWER rule."
        )

        classification = validate_classification(
            deterministic_result
        )
    else:
        classification = call_bedrock(
            reason_not_complete,
            reason_description,
        )

        classification = validate_classification(
            classification
        )

    return {
        "serviceAppointmentId": service_appointment_id,
        "workOrderId": work_order_id,
        **classification,
    }


def process(event, context):
    try:
        logger.info(
            "Cannot Complete classifier invoked."
        )

        payload = parse_event(event)

        if not isinstance(payload, dict):
            raise ValueError(
                "Request payload must be a JSON object."
            )

        #
        # BULK REQUEST
        #
        if "records" in payload:
            records = payload.get("records")

            if not isinstance(records, list):
                raise ValueError(
                    "records must be an array."
                )

            if not records:
                raise ValueError(
                    "records must contain at least one record."
                )

            if len(records) > 50:
                raise ValueError(
                    "A maximum of 50 records can be processed per request."
                )

            results = []
            errors = []

            for index, record in enumerate(records):
                try:
                    result = process_record(record)
                    results.append(result)

                except Exception as exc:
                    logger.exception(
                        "Failed processing bulk record index=%s",
                        index,
                    )

                    errors.append({
                        "index": index,
                        "serviceAppointmentId": (
                            record.get("serviceAppointmentId")
                            if isinstance(record, dict)
                            else None
                        ),
                        "workOrderId": (
                            record.get("workOrderId")
                            if isinstance(record, dict)
                            else None
                        ),
                        "error": str(exc),
                    })

            return success_response({
                "results": results,
                "errors": errors,
                "processedCount": len(results),
                "errorCount": len(errors),
            })

        #
        # SINGLE RECORD REQUEST
        #
        result = process_record(payload)

        return success_response(result)

    except ValueError as exc:
        logger.warning(
            "Invalid Cannot Complete request: %s",
            str(exc),
        )

        return error_response(
            400,
            str(exc),
        )

    except Exception as exc:
        logger.exception(
            "Cannot Complete classification failed: %s",
            str(exc),
        )

        return error_response(
            500,
            "Unable to classify Cannot Complete record.",
        )