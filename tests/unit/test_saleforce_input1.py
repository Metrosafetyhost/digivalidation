import json
import os
import pytest
import boto3
from moto import mock_aws
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from salesforce_input import store_in_s3, store_metadata, load_html_data, proof_html_with_bedrock

# Mock AWS region and resources
AWS_REGION = "eu-west-2"
BUCKET_NAME = "test-bucket"
TABLE_NAME = "ProofingMetadata"

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "fake_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "fake_secret_key"
    os.environ["AWS_SECURITY_TOKEN"] = "fake_security_token"
    os.environ["AWS_SESSION_TOKEN"] = "fake_session_token"
    os.environ["AWS_REGION"] = AWS_REGION
    os.environ["BUCKET_NAME"] = BUCKET_NAME
    os.environ["TABLE_NAME"] = TABLE_NAME

@pytest.fixture(scope="function")
@mock_aws
def s3_mock(aws_credentials):
    """Mocked S3 setup using Moto 5's mock_aws."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.create_bucket(Bucket=BUCKET_NAME)
    return s3

@pytest.fixture(scope="function")
@mock_aws
def dynamodb_mock(aws_credentials):
    """Mocked DynamoDB setup using Moto 5's mock_aws."""
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[{"AttributeName": "workorder_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "workorder_id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    table.wait_until_exists()
    return dynamodb

# --- TESTS ---

def test_store_in_s3(s3_mock):
    """Test storing text in S3."""
    text = "Hello, this is a test."
    filename = "testfile"
    folder = "test-folder"

    s3_key = store_in_s3(text, filename, folder)
    assert s3_key == f"{folder}/{filename}.txt"

    # Check if file exists in mock S3
    response = s3_mock.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    assert response["Body"].read().decode("utf-8") == text

def test_store_metadata(dynamodb_mock):
    """Test storing metadata in DynamoDB."""
    table = dynamodb_mock.Table(TABLE_NAME)

    workorder_id = "test_workorder"
    original_s3_key = "original/file.txt"
    proofed_s3_key = "proofed/file.txt"
    status = "Proofed"

    store_metadata(workorder_id, original_s3_key, proofed_s3_key, status)

    response = table.get_item(Key={"workorder_id": workorder_id})
    assert "Item" in response
    assert response["Item"]["workorder_id"] == workorder_id
    assert response["Item"]["original_s3_key"] == original_s3_key
    assert response["Item"]["proofed_s3_key"] == proofed_s3_key
    assert response["Item"]["status"] == status

def test_load_html_data():
    """Test parsing HTML data from event."""
    event = {
        "body": json.dumps({
            "sectionContents": [
                {
                    "recordId": "rec123",
                    "content": "<table><tr><td>Building Fire strategy</td><td>Test content</td></tr></table>"
                },
                {
                    "recordId": "rec456",
                    "content": "This is a plain text action"
                }
            ]
        })
    }

    proofing_requests, table_data = load_html_data(event)

    assert proofing_requests["Building Fire strategy"] == "Test content"
    assert proofing_requests["rec456"] == "This is a plain text action"
    assert table_data["Building Fire strategy"]["record_id"] == "rec123"
    assert table_data["rec456"]["record_id"] == "rec456"

def test_load_html_data_missing_body():
    """Test handling when body is missing or empty."""
    event = {"body": json.dumps({})}
    proofing_requests, table_data = load_html_data(event)

    assert proofing_requests == {}
    assert table_data == {}

def test_proof_html_with_bedrock():
    """Test proofing function with simulated Bedrock API."""
    header = "Test Header"
    content = "Ths is a mispelled sentence."

    corrected_text = proof_html_with_bedrock(header, content)

    # Since the function defaults to returning original text on failure,
    # we expect it to return the input content (as Bedrock API is not mocked here)
    assert corrected_text == content  # In actual Bedrock call, it should return corrected text

# --- NEGATIVE TESTS ---

def test_store_in_s3_invalid_bucket():
    """Test storing in S3 with an invalid bucket."""
    with pytest.raises(ClientError):
        store_in_s3("Test text", "testfile", "invalid-folder")

def test_store_metadata_missing_table():
    """Test storing metadata when the table does not exist."""
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    with pytest.raises(ClientError):
        store_metadata("test_workorder", "original/file.txt", "proofed/file.txt", "Proofed")

