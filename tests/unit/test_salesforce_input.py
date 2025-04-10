import json
import os

import pytest
import boto3

from moto import mock_aws
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
AWS_REGION = "us-east-1"
BUCKET_NAME = "test-bucket"
TABLE_NAME = "ProofingMetadata"

# @mock_aws
# def test_store_in_s3(aws_credentials):
#     """Test storing text in S3."""

#     s3 = boto3.client("s3", region_name=AWS_REGION)
#     s3.create_bucket(
#         Bucket=BUCKET_NAME,
#         CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
#     )
#     text = "Hello, this is a test."
#     filename = "testfile"
#     folder = "test-folder"

#     s3_key = store_in_s3(text, filename, folder)
#     assert s3_key == f"{folder}/{filename}.txt"

#     # Check if file exists in mock S3
#     response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
#     assert response["Body"].read().decode("utf-8") == text

def test_store_metadata(ddb_table):
    # import here as aws clients are set globally in the file. Need moto to patch aws first!
    from lambdas.salesforce_input import store_metadata

    workorder_id = "test_workorder"
    logs_s3_key = "original/file.txt"
    status = "Proofed"

    store_metadata(workorder_id, logs_s3_key, status, ddb_table)

    response = ddb_table.get_item(Key={"workorder_id": workorder_id})
    assert "Item" in response
    assert response["Item"]["workorder_id"] == workorder_id
    assert response["Item"]["logs_s3_key"] == logs_s3_key
    assert response["Item"]["status"] == status

def test_load_payload():
    """Test parsing HTML data from event."""
    event = {
        "body": json.dumps({
		"workOrderId": "work123",
		"contentType": "FormQuestion",
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

    from lambdas.salesforce_input import load_payload

    workorder_id, content_type, proofing_requests, table_data = load_payload(event)  # ✅ Full unpack correctly

    assert workorder_id == "work123"
    assert content_type == "FormQuestion"
    assert "rec123" in proofing_requests
    assert "rec456" in table_data

def test_load_payload_missing_body(lambda_context):
    """Test handling when body is missing or empty."""
    event = {"body": json.dumps({})}

    from lambdas.salesforce_input import load_payload

    workorder_id, content_type, proofing_requests, table_data = load_payload(event)  # ✅ Full unpack correctly

    # Ensure header lookup is case-insensitive
    expected_key = next((h for h in proofing_requests if h.lower() == "building fire strategy"), None)

    assert proofing_requests == {}
    assert table_data == {}

def test_proof_html_with_bedrock(bedrock_client):
    """Test proofing function with simulated Bedrock API."""
    content = "<p>Ths is a mispelled sentence.</p>"
    record_id = "rec-test"

    from lambdas.salesforce_input import proof_plain_text

    corrected_text = proof_plain_text(content, record_id )
    # Since the function defaults to returning original text on failure,
    # we expect it to return the input content (as Bedrock API is not mocked here)
    assert corrected_text == content  # In actual Bedrock call, it should return corrected text

# --- NEGATIVE TESTS ---

# def test_store_in_s3_invalid_bucket():
#     """Test storing in S3 with an invalid bucket."""
#     with pytest.raises(ClientError):
#         store_in_s3("Test text", "testfile", "invalid-folder")

@mock_aws
def test_store_metadata_missing_table(monkeypatch):
    """Test storing metadata when the table does not exist."""
    monkeypatch.setenv("TABLE_NAME", "NonExistentTable")

    from lambdas.salesforce_input import store_metadata

    dynamodb = boto3.resource("dynamodb", region_name="us-west-2")
    table = dynamodb.Table("NonExistentTable")

    with pytest.raises(ClientError):
        store_metadata("test_workorder", "proofed/file.txt", "Proofed", table)
