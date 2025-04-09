
import json

import pytest
import os
from moto import mock_aws
import boto3

mp = pytest.MonkeyPatch()
mp.setenv("POWERTOOLS_METRICS_NAMESPACE", "testLambdas")

@pytest.fixture(scope="session", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["BUCKET_NAME"] = "test-bucket"
    os.environ["TABLE_NAME"] = "ProofingMetdata"

@pytest.fixture
def s3_resource(aws_credentials):
    with mock_aws():
        yield boto3.resource("s3", region_name="us-east-1")

@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")

@pytest.fixture
def dynamodb(aws_credentials):
    with mock_aws():
        yield boto3.client("dynamodb")

# @pytest.fixture
# @mock_aws
# def dynamodb(aws_credentials):
#     return boto3.resource("dynamodb")

@pytest.fixture
def events(aws_credentials):
    with mock_aws():
        yield boto3.client("events")

@pytest.fixture
def sqs_client(aws_credentials):
    with mock_aws():
        yield boto3.client("sqs", region_name="us-east-1")

@pytest.fixture
def bedrock_client(aws_credentials):
    with mock_aws():
        yield boto3.client("bedrock-runtime", region_name="us-east-1")

@pytest.fixture
def lambda_context():
    class LambdaContext:
        def __init__(self):
            self.function_name = "fabulous"
            self.memory_limit_in_mb = 128
            self.invoked_function_arn = "arn:aws:lambda:eu-west-1:012345678910:function:example-powertools-HelloWorldFunction-1P1Z6B39FLU73"
            self.aws_request_id = "899856cb-83d1-40d7-8611-9e78f15f32f4"

    return LambdaContext()

# @pytest.fixture(scope="function")
# def dynamodb_mock():
#     """Mocked DynamoDB setup using Moto 5's mock_aws."""
#     dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
#     table = dynamodb.create_table(
#         TableName=TABLE_NAME,
#         KeySchema=[{"AttributeName": "workorder_id", "KeyType": "HASH"}],
#         AttributeDefinitions=[{"AttributeName": "workorder_id", "AttributeType": "S"}],
#         ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
#     )
#     table.wait_until_exists()
#     return dynamodb

@pytest.fixture(scope="module")
def sqs_event():
    return {
        "version": "1.0",
        "timestamp": "2020-07-03T14:44:59.367Z",
        "requestContext": {
            "requestId": "2c3b6b36-f136-4ad7-af0a-e2c29fc79f0d",
            "functionArn": "arn:aws:lambda:ap-southeast-2:318356030799:function:dev-failure-lambda:$LATEST",
            "condition": "RetriesExhausted",
            "approximateInvokeCount": 3,
        },
        "requestPayload": {
            "version": "0",
            "id": "b9071294-275a-d84b-de0a-3b13104d5394",
            "detail-type": "Scheduled Event",
            "source": "aws.events",
            "account": "318356030799",
            "time": "2020-07-03T14:41:40Z",
            "region": "ap-southeast-2",
            "resources": [
                "arn:aws:events:ap-southeast-2:318356030799:rule/kapua-dlq-dev-FailureDashlambdaEventsRuleSchedule1-15MLGRLHSV7JW"
            ],
            "detail": {
                "someData": str(
                    ["data" for i in range(600)]
                )  # Simulate a very large request payload
            },
        },
        "responseContext": {
            "statusCode": 200,
            "executedVersion": "$LATEST",
            "functionError": "Unhandled",
        },
        "responsePayload": {
            "errorMessage": "An error occurred (StateMachineDoesNotExist) when calling the StartExecution operation: "
            "State Machine Does Not Exist: 'arn:aws:states:ap-southeast-2: "
            "318356030799:stateMachine:DoesNotExist'",
            "errorType": "StateMachineDoesNotExist",
            "stackTrace": [
                '  File "/var/task/lambdas/failure_lambda.py", line 32, in process\n response = sfn.start_execution(\n',
                '  File "/var/runtime/botocore/client.py", line 316, in _api_call\n    '
                "return self._make_api_call(operation_name, kwargs)\n",
                '  File "/var/runtime/botocore/client.py", line 626, in _make_api_call\n '
                " raise error_class(parsed_response, operation_name)\n",
            ],
        },
    }
