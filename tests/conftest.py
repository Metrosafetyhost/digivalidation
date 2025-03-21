import json

import pytest
import os
import moto
import boto3

mp = pytest.MonkeyPatch()
mp.setenv("POWERTOOLS_METRICS_NAMESPACE", "testLambdas")

@pytest.fixture
def lambda_context():
    class LambdaContext:
        def __init__(self):
            self.function_name = "fabulous"
            self.memory_limit_in_mb = 128
            self.invoked_function_arn = "arn:aws:lambda:eu-west-1:012345678910:function:example-powertools-HelloWorldFunction-1P1Z6B39FLU73"
            self.aws_request_id = "899856cb-83d1-40d7-8611-9e78f15f32f4"

    return LambdaContext()

@pytest.fixture
def s3(aws_credentials):
    with moto.mock_aws():
        yield boto3.resource("s3")

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

