import json
import pytest
from bs4 import BeautifulSoup
from lambdas.salesforce_input import (
    load_html_data,
    extract_proofing_content,
    call_bedrock,
    apply_proofing,
    process
)
from aws_lambda_powertools.utilities.typing import LambdaContext

@pytest.fixture
def load_salesforce_json():
# Load test json
    with open("tests/salesforce.json", "r", encoding="utf-8") as f:
        return json.load(f)

def test_load_html_data(load_salesforce_json):
    """Test extracting HTML data from the actual JSON file."""
    test_event = {"body": json.dumps(load_salesforce_json)}
    html_data_list = load_html_data(test_event)

    assert isinstance(html_data_list, list)
    assert len(html_data_list) > 0
    assert "table" in html_data_list[0]  # ensures extracted HTML contains a table

    print("test_load_html_data passed")

def test_extract_proofing_content(load_salesforce_json):
    """Test extracting proofable headers from the HTML in the JSON."""
    test_event = {"body": json.dumps(load_salesforce_json)}
    html_data = load_html_data(test_event)[0]
    extracted_content = extract_proofing_content(html_data)

    assert isinstance(extracted_content, dict)
    assert any(header in extracted_content for header in [
        "Fire Safety", "Roof Details", "Natural Gas Supplies"
    ])

    print("✅ test_extract_proofing_content passed")

def test_call_bedrock():
    """Test proofing function with specific text corrections."""
    input_text = "Ths is an exemple text."
    proofed_text = call_bedrock(input_text)

    assert proofed_text == "This is an example text."
    print("✅ test_call_bedrock passed")


def test_process(load_salesforce_json):
    """Full integration test using the actual Salesforce JSON."""
    test_event = {"body": json.dumps(load_salesforce_json)}
    context = LambdaContext()
    response = process(test_event, context)

    assert response["statusCode"] == 200
    response_body = json.loads(response["body"])

    assert "proofed_html" in response_body
    assert isinstance(response_body["proofed_html"], list)

    # Validate that proofed HTML is modified correctly
    soup = BeautifulSoup(response_body["proofed_html"][0], 'html.parser')
    updated_text = soup.find_all('td')[1].get_text(strip=True)

    assert "example" in updated_text or "This is" in updated_text
    print("✅ test_process passed")

if __name__ == "__main__":
    pytest.main()
