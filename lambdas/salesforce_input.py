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

def load_html_data_from_file(file_path: str) -> list:
    # load HTML data from a local JSON file instead of an API event.
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info(f"Loaded JSON file: {file_path}")
        html_data = data.get("htmlData", [])

        if not html_data:
            logger.warning("No HTML data found in JSON file.")
        
        logger.info(f"Extracted {len(html_data)} HTML entries.")
        return html_data
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}")
        return []
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
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

            logger.debug(f"Checking row - Header: {header}, Content: {content}")

            if header in ALLOWED_HEADERS:
                proofing_requests[header] = content

    logger.info(f"Extracted {len(proofing_requests)} items for proofing.")
    return proofing_requests

def call_bedrock(text: str) -> str:
    # mock function for text proofing.
    logger.info(f"Proofing text: {text}")

    # Simulated proofing process
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

# commenting out API event handling for now whilst i test locally 
# @logger.inject_lambda_context()
# def process(event: dict, context: LambdaContext) -> dict:
#     Main function to handle proofing of Salesforce input data from an API event.
#     logger.info("Starting proofing process...")
#     html_data_list = load_html_data(event)  # Extract data from API request
#     proofed_html_list = []

#     for html_data in html_data_list:
#         proofing_requests = extract_proofing_content(html_data)
#         proofed_texts = {header: call_bedrock(text) for header, text in proofing_requests.items()}
#         proofed_html = apply_proofing(html_data, proofed_texts)

#         proofed_html_list.append(proofed_html)

#     logger.info("Finished proofing. Returning proofed HTML.")

#     return {
#         "statusCode": 200,
#         "body": json.dumps({"proofed_html": proofed_html_list}, indent=4)
#     }

def process_file(file_path: str) -> dict:
    # main function to handle proofing of Salesforce input data from a local file
    logger.info("Starting proofing process from file...")

    html_data_list = load_html_data_from_file(file_path)
    proofed_html_list = []

    for html_data in html_data_list:
        proofing_requests = extract_proofing_content(html_data)
        proofed_texts = {header: call_bedrock(text) for header, text in proofing_requests.items()}
        proofed_html = apply_proofing(html_data, proofed_texts)

        proofed_html_list.append(proofed_html)

    logger.info("Finished proofing. Returning proofed HTML.")

    return {
        "statusCode": 200,
        "body": json.dumps({"proofed_html": proofed_html_list}, indent=4)
    }

if __name__ == "__main__":
    file_path = "/data/conftest.json"
    response = process_file(file_path)

    # Print nicely formatted JSON output
    print(json.dumps(json.loads(response["body"]), indent=4))
