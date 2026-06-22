import json
import os
import re
from copy import copy
from html import unescape as html_unescape
from urllib.parse import unquote_plus

import boto3
from bs4 import BeautifulSoup


REGION = os.getenv('AWS_REGION', 'eu-west-2')
s3 = boto3.client('s3', region_name=REGION)

# Claude / Bedrock settings.
# Set ENABLE_CLAUDE=true on the Lambda when you are ready to call Claude.
ENABLE_CLAUDE = os.getenv('ENABLE_CLAUDE', 'false').lower() == 'true'
CLAUDE_MODEL_ID = os.getenv(
    'CLAUDE_MODEL_ID',
    'anthropic.claude-3-7-sonnet-20250219-v1:0',
)

# Debug output setting.
# false = cleaner final response
# true = include full parsed sections, cases and evidence packs
RETURN_DEBUG_DATA = os.getenv('RETURN_DEBUG_DATA', 'false').lower() == 'true'


def _response(status_code, payload):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
        },
        'body': json.dumps(payload, ensure_ascii=False),
    }


def _load_json_body(event):
    """
    Supports API Gateway where event['body'] is a JSON string.
    Also supports direct Lambda test events where the event is already the payload.
    """
    if not isinstance(event, dict):
        raise ValueError('Event must be a JSON object')

    body = event.get('body')

    if not body:
        return event

    if isinstance(body, dict):
        return body

    return json.loads(body)


def _download_text_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    raw = response['Body'].read()

    try:
        return raw.decode('utf-8')
    except UnicodeDecodeError:
        return raw.decode('latin-1')


def _extract_payload(event):
    """
    Supports:

    1. Combined Salesforce payload:
       {
         "workOrderId": "0WO...",
         "reportType": "Water",
         "reportHtml": "<div>...</div>",
         "caseCount": 11,
         "cases": [...]
       }

    2. Older direct/API call with HTML:
       {
         "workOrderId": "0WO...",
         "reportType": "Water",
         "html": "<div>...</div>"
       }

    3. Direct/API call with S3 reference:
       {
         "workOrderId": "0WO...",
         "reportType": "Water",
         "bucket": "my-bucket",
         "key": "reports/report.html"
       }

    4. S3 trigger event.
    """

    payload = _load_json_body(event)

    if not isinstance(payload, dict):
        raise ValueError('Payload must be a JSON object')

    if 'reportHtml' in payload or 'html' in payload:
        return payload

    if 'bucket' in payload and 'key' in payload:
        html = _download_text_from_s3(payload['bucket'], payload['key'])
        payload['reportHtml'] = html
        return payload

    if 'Records' in event and event['Records']:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        html = _download_text_from_s3(bucket, key)

        return {
            'bucket': bucket,
            'key': key,
            'reportHtml': html,
            'cases': [],
            'caseCount': 0,
        }

    raise ValueError('No reportHtml, html, or S3 bucket/key found in event')


def _clean_text(value):
    if value is None:
        return ''

    return re.sub(r'\s+', ' ', str(value)).strip()


def _element_text(element):
    if not element:
        return ''

    return _clean_text(element.get_text(' ', strip=True))


def _extract_section_number(title):
    """
    Extracts section numbers from headings like:
    - 1.0 Introduction
    - 2.0 Risk Dashboard
    - 8.0 Appendices

    Important:
    This number is used for ordering/evidence only.
    It is NOT used as the main way to identify what the section means.
    """
    match = re.match(r'^\s*(\d+(?:\.\d+)*)', title or '')

    if not match:
        return None

    return match.group(1)


def _normalise_title(value):
    """
    Normalises titles/subheadings/table text so matching is not dependent on:
    - section number
    - punctuation
    - ampersands
    - capitalisation
    """

    value = _clean_text(value).lower()
    value = re.sub(r'^\d+(?:\.\d+)*\s*', '', value)
    value = value.replace('&', 'and')
    value = re.sub(r'[^a-z0-9\s]', ' ', value)
    value = re.sub(r'\s+', ' ', value).strip()

    return value


# These are meaning-based section labels. The parser keeps section numbers,
# but validation should use these section types rather than fixed section numbers.
SECTION_TYPE_ALIASES = {
    'executive_summary': [
        'executive summary',
        'introduction to the risk assessment',
        'introduction',
    ],
    'risk_dashboard': [
        'risk dashboard',
        'risk rating',
        'overall risk rating',
        'current risk ratings',
    ],
    'property_description': [
        'building description',
        'property description',
        'the property',
        'description of the property',
        'property site description',
    ],
    'records_status': [
        'records status',
        'record status',
    ],
    'site_specific_hazards': [
        'site specific hazards',
    ],
    'property_features': [
        'property features',
    ],
    'management_responsibilities': [
        'management responsibilities',
        'responsible persons',
        'responsible person',
        'management of risk',
    ],
    'control_scheme': [
        'legionella control programme',
        'preventative works',
        'water control scheme',
        'written scheme',
        'control programme',
        'control scheme',
    ],
    'audit_detail': [
        'audit detail',
        'risk assessment checklist',
        'checklist',
    ],
    'risk_assessment_checklist': [
        'risk assessment checklist',
        'audit ref hazard status',
    ],
    'asset_register': [
        'system asset register',
        'asset register',
        'confirmed as present',
        'confirmed as absent',
    ],
    'water_assets': [
        'water assets',
        'incoming mains cold water',
        'localised water heaters',
        'water heaters',
        'water asset',
        'mains cold water',
    ],
    'temperature_profile': [
        'temperature profile',
        'outlet temperature profile',
        'hot water temp',
        'cold water temp',
        'mixed hot water temp',
    ],
    'sentinel_outlets': [
        'sentinel outlets',
        'sentinel outlet',
    ],
    'schematic': [
        'schematic',
        'water services schematic diagram',
        'schematic drawing',
    ],
    'appendices': [
        'appendices',
        'appendix',
        'duty holder',
        'client responsibilities',
        'background information',
        'certificate of registration',
    ],
}


def _parse_table(table):
    rows = []

    for tr in table.find_all('tr'):
        cells = []

        for cell in tr.find_all(['th', 'td']):
            cells.append(_element_text(cell))

        if any(cells):
            rows.append(cells)

    return {
        'rowCount': len(rows),
        'columnCountEstimate': max([len(row) for row in rows], default=0),
        'header': rows[0] if rows else [],
        'rows': rows,
    }


def _nearest_heading(element):
    heading = element.find_previous(['h1', 'h2', 'h3', 'h4'])

    if not heading:
        return None

    return _element_text(heading)


def _table_search_text(tables):
    """
    Creates a compact searchable text sample from tables.
    Avoids using every cell from huge tables.
    """

    parts = []

    for table in tables or []:
        nearby_heading = table.get('nearbyHeading')
        if nearby_heading:
            parts.append(nearby_heading)

        header = table.get('header') or []
        if header:
            parts.append(' '.join(header))

        rows = table.get('rows') or []
        for row in rows[:3]:
            parts.append(' '.join(row))

    return _normalise_title(' '.join(parts))


def _classify_section_types(section):
    """
    Returns a list of meaning-based section types.

    Important:
    A single top-level section may contain multiple useful content types.
    For example, Audit Detail may also contain the Risk Assessment Checklist.
    """

    title_text = _normalise_title(section.get('title', ''))
    subheading_text = _normalise_title(' '.join(section.get('subheadings', [])))
    table_text = _table_search_text(section.get('tables', []))
    body_text = _normalise_title(section.get('text', '')[:6000])

    strong_haystack = f'{title_text} {subheading_text} {table_text}'
    weak_haystack = f'{strong_haystack} {body_text}'

    matches = []

    # Strong matches first: title, subheadings, table headers.
    for section_type, aliases in SECTION_TYPE_ALIASES.items():
        for alias in aliases:
            alias_norm = _normalise_title(alias)

            if alias_norm and alias_norm in strong_haystack:
                matches.append(section_type)
                break

    # Weak fallback: body text.
    for section_type, aliases in SECTION_TYPE_ALIASES.items():
        if section_type in matches:
            continue

        for alias in aliases:
            alias_norm = _normalise_title(alias)

            if alias_norm and alias_norm in weak_haystack:
                matches.append(section_type)
                break

    if not matches:
        return ['unknown']

    return matches


def _primary_section_type(section_types):
    """
    Picks the main type for display/debugging.
    The full sectionTypes list is still kept.
    """

    priority = [
        'executive_summary',
        'risk_dashboard',
        'property_description',
        'records_status',
        'management_responsibilities',
        'control_scheme',
        'audit_detail',
        'risk_assessment_checklist',
        'asset_register',
        'water_assets',
        'temperature_profile',
        'sentinel_outlets',
        'schematic',
        'appendices',
    ]

    for item in priority:
        if item in section_types:
            return item

    return section_types[0] if section_types else 'unknown'


def _parse_front_matter(soup):
    """
    Everything before the first h2.
    This normally includes the cover/front page area.
    """

    first_h2 = soup.find('h2')

    if not first_h2:
        return {
            'text': _element_text(soup),
            'images': [],
        }

    nodes = []

    for child in soup.children:
        if child == first_h2:
            break

        nodes.append(child)

    temp = BeautifulSoup('<div></div>', 'html.parser')
    wrapper = temp.div

    for node in nodes:
        try:
            wrapper.append(copy(node))
        except Exception:
            pass

    return {
        'text': _element_text(wrapper),
        'images': [
            {
                'src': img.get('src', ''),
                'alt': img.get('alt', ''),
            }
            for img in wrapper.find_all('img')
        ],
    }


def _build_ai_text(title, text, tables):
    """
    Creates a plain-text version that is nicer to pass into Claude.
    It keeps the section text and adds tables in a readable format.
    """

    parts = [f'SECTION: {title}']

    if text:
        parts.append('')
        parts.append('TEXT:')
        parts.append(text)

    if tables:
        parts.append('')
        parts.append('TABLES:')

        for table in tables:
            parts.append('')
            parts.append(f"Table near heading: {table.get('nearbyHeading') or 'Unknown'}")

            for row in table['rows']:
                parts.append(' | '.join(row))

    return '\n'.join(parts)


def _parse_sections(soup):
    """
    Splits the report by top-level h2 sections.

    Each section keeps its original sectionNumber for traceability,
    but also gets meaning-based sectionType/sectionTypes fields.
    """

    sections = []
    h2s = soup.find_all('h2')

    for index, h2 in enumerate(h2s, start=1):
        title = _element_text(h2)
        section_number = _extract_section_number(title)

        temp = BeautifulSoup('<div></div>', 'html.parser')
        wrapper = temp.div

        for sibling in h2.next_siblings:
            if getattr(sibling, 'name', None) == 'h2':
                break

            try:
                wrapper.append(copy(sibling))
            except Exception:
                pass

        subheadings = []

        for heading in wrapper.find_all(['h3', 'h4']):
            text = _element_text(heading)

            if text:
                subheadings.append(text)

        tables = []

        for table_index, table in enumerate(wrapper.find_all('table'), start=1):
            parsed_table = _parse_table(table)
            parsed_table['tableIndexWithinSection'] = table_index
            parsed_table['nearbyHeading'] = _nearest_heading(table)
            tables.append(parsed_table)

        images = []

        for image_index, img in enumerate(wrapper.find_all('img'), start=1):
            images.append(
                {
                    'imageIndexWithinSection': image_index,
                    'src': img.get('src', ''),
                    'alt': img.get('alt', ''),
                    'nearbyHeading': _nearest_heading(img),
                }
            )

        section_text = _element_text(wrapper)

        section = {
            'index': index,
            'sectionNumber': section_number,
            'title': title,
            'normalisedTitle': _normalise_title(title),
            'text': section_text,
            'subheadings': subheadings,
            'tables': tables,
            'images': images,
            'aiText': _build_ai_text(title, section_text, tables),
        }

        section_types = _classify_section_types(section)
        section['sectionType'] = _primary_section_type(section_types)
        section['sectionTypes'] = section_types

        sections.append(section)

    return sections


def _parse_report_html(html):
    soup = BeautifulSoup(html or '', 'html.parser')

    for unwanted in soup.find_all(['script', 'style']):
        unwanted.decompose()

    front_matter = _parse_front_matter(soup)
    sections = _parse_sections(soup)

    all_tables = []

    for section in sections:
        for table in section['tables']:
            all_tables.append(
                {
                    'sectionNumber': section['sectionNumber'],
                    'sectionTitle': section['title'],
                    'sectionType': section.get('sectionType'),
                    'sectionTypes': section.get('sectionTypes', []),
                    **table,
                }
            )

    section_type_counts = {}

    for section in sections:
        for section_type in section.get('sectionTypes', []):
            section_type_counts[section_type] = section_type_counts.get(section_type, 0) + 1

    return {
        'frontMatter': front_matter,
        'sections': sections,
        'summary': {
            'sectionCount': len(sections),
            'tableCount': len(all_tables),
            'imageCount': len(soup.find_all('img')),
            'sectionTypeCounts': section_type_counts,
            'sectionTitles': [
                {
                    'sectionNumber': section['sectionNumber'],
                    'title': section['title'],
                    'sectionType': section.get('sectionType'),
                    'sectionTypes': section.get('sectionTypes', []),
                }
                for section in sections
            ],
        },
    }


def _infer_report_type(html):
    text = _clean_text(BeautifulSoup(html or '', 'html.parser').get_text(' ')).lower()

    if 'legionella water risk assessment' in text:
        return 'Water'

    if 'fire risk assessment' in text:
        return 'Fire'

    if 'health and safety risk assessment' in text or 'health & safety risk assessment' in text:
        return 'Health & Safety'

    return 'Unknown'


def _normalise_cases(cases):
    """
    Keeps the Cases data as JSON, but cleans whitespace and unescapes HTML entities.
    """

    if not cases:
        return []

    normalised = []

    for case in cases:
        if not isinstance(case, dict):
            continue

        clean_case = {}

        for key, value in case.items():
            if isinstance(value, str):
                clean_case[key] = html_unescape(_clean_text(value))
            else:
                clean_case[key] = value

        normalised.append(clean_case)

    return normalised


def _find_section(parsed_report, section_number=None, title_contains=None):
    """
    Legacy helper. Prefer _find_sections_by_type for validation logic.
    """

    for section in parsed_report.get('sections', []):
        if section_number and section.get('sectionNumber') == section_number:
            return section

        if title_contains:
            title = section.get('title', '').lower()
            if title_contains.lower() in title:
                return section

    return None


def _find_sections_by_type(parsed_report, section_type):
    """
    Finds sections by meaning, not by fixed section number.
    """

    matches = []

    for section in parsed_report.get('sections', []):
        section_types = section.get('sectionTypes', [])

        if section.get('sectionType') == section_type or section_type in section_types:
            matches.append(section)

    return matches


def _find_first_section_by_type(parsed_report, section_type):
    matches = _find_sections_by_type(parsed_report, section_type)
    return matches[0] if matches else None


def _evidence_for_types(parsed_report, section_types):
    """
    Returns all sections matching any of the requested meaning-based types.
    """

    evidence = []
    seen_indexes = set()

    for section_type in section_types:
        for section in _find_sections_by_type(parsed_report, section_type):
            index = section.get('index')

            if index in seen_indexes:
                continue

            evidence.append(section)
            seen_indexes.add(index)

    return evidence


def _make_result(check_id, title, status, summary, issues=None, evidence=None):
    return {
        'checkId': check_id,
        'title': title,
        'status': status,
        'summary': summary,
        'issues': issues or [],
        'evidence': evidence or [],
    }


def _case_display_name(case, index):
    return (
        case.get('caseNumber')
        or case.get('id')
        or case.get('SRM_Reference_Number')
        or f'case index {index}'
    )


def _normalise_case_ref(value):
    return _clean_text(value).lower()


def _validate_cases(case_count, cases):
    """
    Generic Cases JSON checks.
    """

    results = []
    actual_count = len(cases)

    if case_count is None:
        results.append(
            _make_result(
                'cases.count_present',
                'Case count present',
                'warning',
                'caseCount was not provided. AWS can still count the cases array, but Salesforce should ideally send caseCount.',
            )
        )
    else:
        try:
            expected_count = int(case_count)
        except (TypeError, ValueError):
            expected_count = None

        if expected_count == actual_count:
            results.append(
                _make_result(
                    'cases.count_matches',
                    'Case count matches cases array',
                    'pass',
                    f'caseCount is {case_count} and cases array contains {actual_count} cases.',
                )
            )
        else:
            results.append(
                _make_result(
                    'cases.count_matches',
                    'Case count matches cases array',
                    'fail',
                    f'caseCount is {case_count}, but cases array contains {actual_count} cases.',
                )
            )

    return results


def _validate_action_plan_cases(cases):
    """
    Validates Significant Findings / Action Plan using the Cases JSON.

    This is the source of truth for action-plan questions, not the HTML.
    It answers things like:
    - Are observations present?
    - Are required actions present?
    - Are priorities present?
    - Are target dates present?
    - Are reference numbers present?
    - Are duplicate references present?
    """

    if not cases:
        return [
            _make_result(
                'action_plan.cases_present',
                'Action plan cases present',
                'fail',
                'No cases were provided. Significant Findings / Action Plan cannot be validated.',
            )
        ]

    results = []

    required_action_fields = [
        'caseNumber',
        'priority',
        'SRM_Category',
        'SRM_Hazard',
        'SRM_Observation',
        'SRM_Reference_Number',
        'SRM_Required_Action',
        'SRM_Target_Date',
    ]

    missing_issues = []
    duplicate_ref_issues = []
    reference_counts = {}

    for index, case in enumerate(cases, start=1):
        case_name = _case_display_name(case, index)

        for field in required_action_fields:
            value = case.get(field)

            if value is None or _clean_text(value) == '':
                missing_issues.append(
                    {
                        'case': case_name,
                        'field': field,
                        'message': f'{case_name} is missing {field}.',
                    }
                )

        reference = _normalise_case_ref(case.get('SRM_Reference_Number'))

        if reference:
            reference_counts[reference] = reference_counts.get(reference, 0) + 1

    for reference, count in reference_counts.items():
        if count > 1:
            duplicate_ref_issues.append(
                {
                    'referenceNumber': reference,
                    'count': count,
                    'message': f'Reference number {reference} appears {count} times in the Cases JSON.',
                }
            )

    if missing_issues:
        results.append(
            _make_result(
                'action_plan.required_fields',
                'Action plan cases have required fields',
                'fail',
                f'{len(missing_issues)} missing action-plan field(s) found in the Cases JSON.',
                issues=missing_issues,
            )
        )
    else:
        results.append(
            _make_result(
                'action_plan.required_fields',
                'Action plan cases have required fields',
                'pass',
                'All action-plan cases contain priority, category, hazard, observation, reference number, required action, and target date.',
            )
        )

    if duplicate_ref_issues:
        results.append(
            _make_result(
                'action_plan.reference_numbers_unique',
                'Action plan reference numbers are unique',
                'warning',
                f'{len(duplicate_ref_issues)} duplicate reference number issue(s) found.',
                issues=duplicate_ref_issues,
            )
        )
    else:
        results.append(
            _make_result(
                'action_plan.reference_numbers_unique',
                'Action plan reference numbers are unique',
                'pass',
                'No duplicate action-plan reference numbers were found.',
            )
        )

    priority_summary = {}
    category_summary = {}

    for case in cases:
        priority = _clean_text(case.get('priority')) or 'Blank'
        category = _clean_text(case.get('SRM_Category')) or 'Blank'

        priority_summary[priority] = priority_summary.get(priority, 0) + 1
        category_summary[category] = category_summary.get(category, 0) + 1

    results.append(
        _make_result(
            'action_plan.summary',
            'Action plan summary',
            'pass',
            f'{len(cases)} action-plan case(s) were received in JSON.',
            evidence=[
                {
                    'sourceOfTruth': 'cases_json',
                    'caseCount': len(cases),
                    'prioritySummary': priority_summary,
                    'categorySummary': category_summary,
                }
            ],
        )
    )

    return results


def _validate_front_page(parsed_report):
    front_matter = parsed_report.get('frontMatter', {})
    text = front_matter.get('text', '')
    images = front_matter.get('images', [])

    issues = []

    if not re.search(r'\b\d{1,2}/\d{1,2}/\d{4}\b', text):
        issues.append(
            {
                'message': 'No obvious front-page date found.',
                'field': 'date',
            }
        )

    if 'address' not in text.lower():
        issues.append(
            {
                'message': 'No obvious ADDRESS label found in front matter.',
                'field': 'address',
            }
        )

    if not images:
        issues.append(
            {
                'message': 'No front-page images found.',
                'field': 'images',
            }
        )

    if issues:
        return [
            _make_result(
                'front_page.basic_fields',
                'Front page has basic fields',
                'warning',
                'Some front-page fields could not be confidently detected.',
                issues=issues,
                evidence=[
                    {
                        'source': 'frontMatter',
                        'textPreview': text[:1000],
                        'imageCount': len(images),
                    }
                ],
            )
        ]

    return [
        _make_result(
            'front_page.basic_fields',
            'Front page has basic fields',
            'pass',
            'Front matter appears to include a date, address label, and image(s).',
            evidence=[
                {
                    'source': 'frontMatter',
                    'textPreview': text[:1000],
                    'imageCount': len(images),
                }
            ],
        )
    ]


def _validate_water_sections(parsed_report):
    """
    Checks for important Water report content by meaning, not fixed numbering.

    Important:
    Significant Findings / Action Plan is NOT required from HTML here.
    The source of truth for actions is the Cases JSON.
    """

    expected_types = {
        'risk_dashboard': 'Risk Dashboard',
        'property_description': 'Building / Property Description',
        'control_scheme': 'Water Control Scheme / Written Scheme',
        'audit_detail': 'Audit Detail',
        'risk_assessment_checklist': 'Risk Assessment Checklist',
        'asset_register': 'System Asset Register',
        'water_assets': 'Water Assets',
        'schematic': 'Schematic',
        'appendices': 'Appendices',
    }

    missing = []
    found = []

    for section_type, label in expected_types.items():
        matches = _find_sections_by_type(parsed_report, section_type)

        if matches:
            found.append(
                {
                    'sectionType': section_type,
                    'label': label,
                    'matches': [
                        {
                            'sectionNumber': section.get('sectionNumber'),
                            'title': section.get('title'),
                        }
                        for section in matches
                    ],
                }
            )
        else:
            missing.append(
                {
                    'sectionType': section_type,
                    'expected': label,
                    'message': f'Could not find content for {label}.',
                }
            )

    if missing:
        return [
            _make_result(
                'water.required_content',
                'Water required content present',
                'warning',
                f'{len(missing)} expected Water content area(s) could not be found by meaning.',
                issues=missing,
                evidence=found,
            )
        ]

    return [
        _make_result(
            'water.required_content',
            'Water required content present',
            'pass',
            'All expected Water content areas were found by meaning.',
            evidence=found,
        )
    ]


def _build_water_evidence_packs(parsed_report, cases):
    """
    Builds targeted evidence packs for Claude.

    Important:
    Significant Findings / Action Plan questions use the Cases JSON,
    not the HTML report body.
    """

    front_matter = parsed_report.get('frontMatter', {})

    risk_dashboard = _evidence_for_types(parsed_report, ['risk_dashboard'])
    property_description = _evidence_for_types(parsed_report, ['property_description'])
    control_scheme = _evidence_for_types(parsed_report, ['control_scheme'])
    audit_detail = _evidence_for_types(parsed_report, ['audit_detail', 'risk_assessment_checklist'])
    asset_register = _evidence_for_types(parsed_report, ['asset_register'])
    water_assets = _evidence_for_types(
        parsed_report,
        ['water_assets', 'temperature_profile', 'sentinel_outlets'],
    )
    schematic_and_appendices = _evidence_for_types(parsed_report, ['schematic', 'appendices'])

    return [
        {
            'questionId': 1,
            'question': 'Check front page for address, photo and date.',
            'evidence': {
                'frontMatter': front_matter,
            },
        },
        {
            'questionId': 3,
            'question': 'Actions in the report should match the number of cases raised.',
            'evidence': {
                'sourceOfTruth': 'cases_json',
                'cases': cases,
                'caseCount': len(cases),
            },
        },
        {
            'questionId': 4,
            'question': 'Building description is fully completed and concise.',
            'evidence': {
                'propertyDescriptionSections': property_description,
            },
        },
        {
            'questionId': 5,
            'question': 'Water description matches the Water Assets listed.',
            'evidence': {
                'propertyDescriptionSections': property_description,
                'assetRegisterSections': asset_register,
                'waterAssetSections': water_assets,
            },
        },
        {
            'questionId': 9,
            'question': 'Risk Dashboard: Risk Rating, Management Control and Inherent Risk completed.',
            'evidence': {
                'riskDashboardSections': risk_dashboard,
            },
        },
        {
            'questionId': 12,
            'question': 'Written Scheme includes dates, comments and covers all assets.',
            'evidence': {
                'controlSchemeSections': control_scheme,
                'assetRegisterSections': asset_register,
                'waterAssetSections': water_assets,
            },
        },
        {
            'questionId': 13,
            'question': 'Check Observations and Actions and ensure all action-plan fields are filled in.',
            'evidence': {
                'sourceOfTruth': 'cases_json',
                'cases': cases,
            },
        },
        {
            'questionId': 15,
            'question': 'System Asset Register matches Asset tables.',
            'evidence': {
                'assetRegisterSections': asset_register,
                'waterAssetSections': water_assets,
            },
        },
        {
            'questionId': 16,
            'question': 'Water Asset forms are fully completed with photos and comments.',
            'evidence': {
                'waterAssetSections': water_assets,
            },
        },
        {
            'questionId': 20,
            'question': 'Schematic and appendices are present where required.',
            'evidence': {
                'schematicAndAppendicesSections': schematic_and_appendices,
            },
        },
    ]


def _extract_answer_status(answer_text):
    """
    Pulls a simple status out of Claude's human-readable answer.

    Claude is not required to return JSON, but this gives the Lambda
    a useful top-level status for filtering/sorting.
    """

    text = _clean_text(answer_text).lower()

    if 'status: pass' in text or text.startswith('pass'):
        return 'pass'

    if 'status: fail' in text or text.startswith('fail'):
        return 'fail'

    if 'status: warning' in text or text.startswith('warning'):
        return 'warning'

    if (
        'status: not applicable' in text
        or 'status: n/a' in text
        or text.startswith('not applicable')
    ):
        return 'not_applicable'

    return 'answered'


def _call_claude_for_question(evidence_pack):
    """
    Calls Claude 3.7 Sonnet on AWS Bedrock.

    This version expects a human-readable answer, not strict JSON.

    The answer should explain:
    - whether it is pass/fail/warning/not applicable
    - why that decision was reached
    - what evidence was used
    """

    if not ENABLE_CLAUDE:
        return {
            'questionId': evidence_pack['questionId'],
            'question': evidence_pack['question'],
            'status': 'not_run',
            'answer': 'Claude validation is disabled. Set ENABLE_CLAUDE=true to enable Bedrock calls.',
        }

    bedrock = boto3.client('bedrock-runtime', region_name=REGION)

    prompt = f"""
You are validating a Metro Safety report.

Answer the validation question using only the evidence provided.
Do not assume facts that are not present.
Do not use outside knowledge.

Important evidence rules:
- For Significant Findings / Action Plan questions, use the Cases JSON as the source of truth.
- Do not rely on HTML action-plan text where Cases JSON is provided.
- Section numbers are evidence references only; do not assume a section's meaning from its number.

Write the answer in this exact human-readable format:

Status: Pass / Fail / Warning / Not applicable

Why:
Explain why this is a pass, fail, warning, or not applicable.
- If it is a pass, explain what evidence proves it passes.
- If it is a fail, explain exactly what is missing, incorrect, or incomplete.
- If it is a warning, explain what is present but may need review.
- If it is not applicable, explain why.

Answer:
Give a clear short answer to the validation question.

Issues found:
- List any issues found.
- If there are no issues, say "No issues found."

Evidence used:
- Briefly list the evidence you used.
- Include section titles and section numbers where useful.
- If using Cases JSON, say "Cases JSON" clearly.

Validation question:
{evidence_pack['question']}

Evidence:
{json.dumps(evidence_pack['evidence'], ensure_ascii=False)}
"""

    request_body = {
        'anthropic_version': 'bedrock-2023-05-31',
        'max_tokens': 2000,
        'temperature': 0,
        'messages': [
            {
                'role': 'user',
                'content': [
                    {
                        'type': 'text',
                        'text': prompt,
                    }
                ],
            }
        ],
    }

    response = bedrock.invoke_model(
        modelId=CLAUDE_MODEL_ID,
        body=json.dumps(request_body),
        contentType='application/json',
        accept='application/json',
    )

    response_body = json.loads(response['body'].read())

    try:
        claude_text = response_body['content'][0]['text']
    except (KeyError, IndexError, TypeError):
        return {
            'questionId': evidence_pack['questionId'],
            'question': evidence_pack['question'],
            'status': 'error',
            'answer': 'Claude response did not contain the expected content text.',
            'rawClaudeResponse': response_body,
        }

    answer = claude_text.strip()

    return {
        'questionId': evidence_pack['questionId'],
        'question': evidence_pack['question'],
        'status': _extract_answer_status(answer),
        'answer': answer,
    }


def _run_validations(parsed_report, cases, case_count, report_type):
    validation_results = []

    # Generic deterministic checks.
    validation_results.extend(_validate_cases(case_count, cases))
    validation_results.extend(_validate_front_page(parsed_report))

    # Significant Findings / Action Plan checks come from Cases JSON.
    validation_results.extend(_validate_action_plan_cases(cases))

    if report_type == 'Water':
        validation_results.extend(_validate_water_sections(parsed_report))
        evidence_packs = _build_water_evidence_packs(parsed_report, cases)
    else:
        evidence_packs = []

    question_answers = []

    for evidence_pack in evidence_packs:
        question_answers.append(_call_claude_for_question(evidence_pack))

    output = {
        'deterministicResults': validation_results,
        'questionAnswers': question_answers,
    }

    if RETURN_DEBUG_DATA:
        output['evidencePacks'] = evidence_packs

    return output


def process(event, context):
    try:
        payload = _extract_payload(event)

        # Your actual payload uses reportHtml. html is kept for older tests.
        html = payload.get('reportHtml') or payload.get('html')

        if not html:
            return _response(
                400,
                {
                    'status': 'error',
                    'message': 'Missing reportHtml. Provide reportHtml directly or provide bucket/key.',
                },
            )

        parsed_report = _parse_report_html(html)
        cases = _normalise_cases(payload.get('cases', []))
        case_count = payload.get('caseCount')
        report_type = payload.get('reportType') or _infer_report_type(html)

        validation_results = _run_validations(
            parsed_report=parsed_report,
            cases=cases,
            case_count=case_count,
            report_type=report_type,
        )

        result = {
            'status': 'ok',
            'message': 'Report payload parsed successfully.',
            'workOrderId': payload.get('workOrderId'),
            'jobNumber': payload.get('jobNumber'),
            'reportType': report_type,
            'caseCount': case_count,
            'receivedCaseCount': len(cases),
            'summary': parsed_report['summary'],
            'validationResults': validation_results,
        }

        if RETURN_DEBUG_DATA:
            result['frontMatter'] = parsed_report['frontMatter']
            result['sections'] = parsed_report['sections']
            result['cases'] = cases

        return _response(200, result)

    except Exception as e:
        print('Error processing report payload:', str(e))

        return _response(
            500,
            {
                'status': 'error',
                'message': str(e),
            },
        )
