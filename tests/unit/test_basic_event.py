import os
from contextlib import contextmanager
from lambdas.basic_event import process
from moto import mock_aws
import boto3

@mock_aws
def test_process_saleforce1_json(sqs_event, lambda_context):

    response = process(sqs_event, lambda_context)

    assert response == "hola mundo"
