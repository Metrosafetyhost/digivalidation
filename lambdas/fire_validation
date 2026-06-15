import json
import os
import re
from copy import copy
from urllib.parse import unquote_plus

import boto3
from bs4 import BeautifulSoup


REGION = os.getenv('AWS_REGION', 'eu-west-2')
s3 = boto3.client('s3', region_name=REGION)


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
    """
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

    1. Direct/API call with HTML:
       {
         "workOrderId": "0WO...",
         "reportType": "Fire",
         "html": "<div>...</div>"
       }

    2. Direct/API call with S3 reference:
       {
         "workOrderId": "0WO...",
         "reportType": "Fire",
         "bucket": "my-bucket",
         "key": "reports/report.html"
       }

    3. S3 trigger event.
    """

    payload = _load_json_body(event)

    if 'html' in payload:
        return payload

    if 'bucket' in payload and 'key' in payload:
        html = _download_text_from_s3(payload['bucket'], payload['key'])
        payload['html'] = html
        return payload

    if 'Records' in event and event['Records']:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        html = _download_text_from_s3(bucket, key)

        return {
            'bucket': bucket,
            'key': key,
            'html': html,
        }

    raise ValueError('No HTML or S3 bucket/key found in event')


def _clean_text(value):
    if not value:
        return ''

    return re.sub(r'\s+', ' ', value).strip()


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
    """
    match = re.match(r'^\s*(\d+(?:\.\d+)*)', title or '')

    if not match:
        return None

    return match.group(1)


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


def _parse_sections(soup):
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

        sections.append(
            {
                'index': index,
                'sectionNumber': section_number,
                'title': title,
                'text': section_text,
                'subheadings': subheadings,
                'tables': tables,
                'images': images,
                'aiText': _build_ai_text(title, section_text, tables),
            }
        )

    return sections


def _build_ai_text(title, text, tables):
    """
    Creates a plain-text version that is nicer to pass into AI.

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
                    **table,
                }
            )

    return {
        'frontMatter': front_matter,
        'sections': sections,
        'summary': {
            'sectionCount': len(sections),
            'tableCount': len(all_tables),
            'imageCount': len(soup.find_all('img')),
            'sectionTitles': [
                {
                    'sectionNumber': section['sectionNumber'],
                    'title': section['title'],
                }
                for section in sections
            ],
        },
    }


def process(event, context):
    try:
        payload = _extract_payload(event)
        html = payload.get('html')

        if not html:
            return _response(
                400,
                {
                    'status': 'error',
                    'message': 'Missing HTML. Provide html directly or provide bucket/key.',
                },
            )

        parsed_report = _parse_report_html(html)

        result = {
            'status': 'ok',
            'message': 'Report HTML parsed successfully.',
            'workOrderId': payload.get('workOrderId'),
            'reportType': payload.get('reportType') or 'Fire',
            'summary': parsed_report['summary'],
            'frontMatter': parsed_report['frontMatter'],
            'sections': parsed_report['sections'],
        }

        return _response(200, result)

    except Exception as e:
        print('Error parsing report HTML:', str(e))

        return _response(
            500,
            {
                'status': 'error',
                'message': str(e),
            },
        )