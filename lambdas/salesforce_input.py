import json
from bs4 import BeautifulSoup
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

# Define headers that need proofing
ALLOWED_HEADERS = [
    "Passenger and Disabled Access Platform Lifts (DAPL)",
    "Fire Service and Evacuation Lifts",
    "Mains Electrical incomers and electrical distribution boards (EDBs)",
    "Natural Gas Supplies",
    "Fire Safety",
    "Roof Details"
]


def load_html_data(event: dict) -> list:
    # extract HTML tables from the input JSON event.
    try:
        body = json.loads(event.get("body", "{}"))
        html_data = body.get("htmlData", [])
        logger.info(f"Loaded {len(html_data)} HTML data entries.")
        return html_data
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}")
        return []


def extract_proofing_content(html_data: str) -> dict:
    # extract key-value pairs (headers and content) that need proofing.
    soup = BeautifulSoup(html_data, 'html.parser')
    rows = soup.find_all('tr')

    proofing_requests = {}

    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 2:
            header = cells[0].get_text(strip=True)
            content = cells[1].get_text(strip=True)

            if header in ALLOWED_HEADERS:
                proofing_requests[header] = content

    logger.info(f"Extracted {len(proofing_requests)} items for proofing.")
    return proofing_requests


def call_bedrock(text: str) -> str:
    # test function for text processing.
    logger.info(f"Proofing text: {text}")

    # simulated proofing process
    proofed_text = text.replace("exemple", "example").replace("Ths", "This")

    logger.info(f"Proofed text: {proofed_text}")
    return proofed_text


def apply_proofing(html_data: str, proofed_texts: dict) -> str:
    # update the HTML content with proofed text while keeping structure.
    soup = BeautifulSoup(html_data, 'html.parser')
    rows = soup.find_all('tr')

    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 2:
            header = cells[0].get_text(strip=True)

            if header in proofed_texts:
                new_content = proofed_texts[header]
                cells[1].string = new_content  # replace text while keeping HTML structure

    logger.info("Applied proofing to HTML data.")
    return str(soup)


@logger.inject_lambda_context()
def process(event: dict, context: LambdaContext) -> dict:
    logger.info("Starting proofing process...")

    # Log the full event to ensure it's received correctly
    logger.debug(f"Event received: {json.dumps(event, indent=2)}")

    # Load HTML data
    html_data_list = load_html_data(event)
    logger.debug(f"Extracted HTML data count: {len(html_data_list)}")

    proofed_html_list = []

    for index, html_data in enumerate(html_data_list):
        logger.debug(f"Processing table {index+1}/{len(html_data_list)}: {html_data[:500]}...")  # Log first 500 chars

        # Extract proofing requests
        proofing_requests = extract_proofing_content(html_data)
        logger.debug(f"Extracted proofing requests: {proofing_requests}")

        # If no proofing requests were found, log a warning
        if not proofing_requests:
            logger.warning(f"No proofing requests found for table {index+1}.")

        # Send content to AWS Bedrock (or mock function)
        proofed_texts = {header: call_bedrock(text) for header, text in proofing_requests.items()}
        logger.debug(f"Proofed texts: {proofed_texts}")

        # If proofed_texts is empty, log a warning
        if not proofed_texts:
            logger.warning(f"No proofed texts returned for table {index+1}.")

        # Apply proofed content back to the HTML
        logger.debug(f"Before applying proofing: {html_data[:500]}...")
        proofed_html = apply_proofing(html_data, proofed_texts)
        logger.debug(f"After applying proofing: {proofed_html[:500]}...")

        proofed_html_list.append(proofed_html)

    logger.info("Finished proofing. Returning proofed HTML.")

    return {
        "statusCode": 200,
        "body": json.dumps({"proofed_html": proofed_html_list})
    }



if __name__ == "__main__":
    logger.info("Running salesforce_input.py locally...")

    # Load test JSON file
    with open("/mnt/data/salesforce.json", "r", encoding="utf-8") as f:
        json_data = json.load(f)

    test_event = {"body": json.dumps(json_data)}

    response = process(test_event, None)

    # print nicely formatted JSON
    print(json.dumps(json.loads(response["body"]), indent=4))
