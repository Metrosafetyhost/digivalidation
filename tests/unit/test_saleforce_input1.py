import os
from contextlib import contextmanager
from lambdas.salesforce_input import call_bedrock
from moto import mock_aws
import boto3

# @mock_aws
# def test_process_saleforce1_json(salesforce_input1, lambda_context):

#     headers = extract_headers(salesforce_input1, lambda_context)

#     assert headers == "extracted headers"

def test_call_bedrock():

    headers = "ths etxt is exemple to be replaced"
    extract = call_bedrock(headers)

    assert extract == "this etxt is example to be replaced"
