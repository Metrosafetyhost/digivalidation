import importlib
import json
import os
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError


WORK_ORDER_ID = "0WOR5000008zTjdOAE"
OBJECT_KEY = f"WorkOrders/{WORK_ORDER_ID}/inspection_final.pdf"
BUCKET = "metrosafetyprodfiles"
DOWNLOAD_URL = "https://example.com/presigned-download"


@pytest.fixture
def setup_lambda():
    with patch.dict(
        os.environ,
        {
            "FILE_BUCKET": BUCKET,
            "PRESIGNED_URL_SECONDS": "300",
        },
    ):
        with patch("boto3.client") as mock_client:
            mock_s3 = Mock()
            mock_client.return_value = mock_s3

            import lambdas.customer_document_download as module

            module = importlib.reload(module)

            mock_s3.get_object_tagging.return_value = {
                "TagSet": [
                    {"Key": "customer-visible", "Value": "true"},
                    {"Key": "public-visible", "Value": "false"},
                ]
            }

            mock_s3.head_object.return_value = {}

            mock_s3.generate_presigned_url.return_value = DOWNLOAD_URL

            yield module, mock_s3


def make_event(
    object_key=OBJECT_KEY,
    work_order_id=WORK_ORDER_ID,
    method="POST",
    path="/files/customer-documents/download",
):
    return {
        "requestContext": {
            "http": {
                "method": method,
            }
        },
        "rawPath": path,
        "body": json.dumps(
            {
                "objectKey": object_key,
                "workOrderId": work_order_id,
            }
        ),
        "isBase64Encoded": False,
    }


def invoke(module, event):
    result = module.process(event, None)
    return result["statusCode"], json.loads(result["body"])


def test_successful_customer_download(setup_lambda):
    module, mock_s3 = setup_lambda

    status, body = invoke(module, make_event())

    assert status == 200
    assert body["url"] == DOWNLOAD_URL
    assert body["expiresInSeconds"] == 300

    mock_s3.get_object_tagging.assert_called_once_with(
        Bucket=BUCKET,
        Key=OBJECT_KEY,
    )

    mock_s3.head_object.assert_called_once_with(
        Bucket=BUCKET,
        Key=OBJECT_KEY,
    )

    mock_s3.generate_presigned_url.assert_called_once_with(
        ClientMethod="get_object",
        Params={
            "Bucket": BUCKET,
            "Key": OBJECT_KEY,
        },
        ExpiresIn=300,
    )


def test_customer_visible_false_is_rejected(setup_lambda):
    module, mock_s3 = setup_lambda

    mock_s3.get_object_tagging.return_value = {
        "TagSet": [
            {"Key": "customer-visible", "Value": "false"},
        ]
    }

    status, _ = invoke(module, make_event())

    assert status == 404
    mock_s3.generate_presigned_url.assert_not_called()


def test_missing_visibility_tag_is_rejected(setup_lambda):
    module, mock_s3 = setup_lambda

    mock_s3.get_object_tagging.return_value = {
        "TagSet": [],
    }

    status, _ = invoke(module, make_event())

    assert status == 404
    mock_s3.generate_presigned_url.assert_not_called()


def test_wrong_work_order_prefix_is_rejected(setup_lambda):
    module, mock_s3 = setup_lambda

    different_key = (
        "WorkOrders/0WOR5000008zTjdOAF/inspection_final.pdf"
    )

    status, _ = invoke(
        module,
        make_event(object_key=different_key),
    )

    assert status == 403
    mock_s3.get_object_tagging.assert_not_called()
    mock_s3.generate_presigned_url.assert_not_called()


@pytest.mark.parametrize(
    "invalid_key",
    [
        "",
        "Buildings/example.pdf",
        "WorkOrders/../secret.pdf",
        f"WorkOrders/{WORK_ORDER_ID}/../secret.pdf",
        f"WorkOrders/{WORK_ORDER_ID}/",
        f"WorkOrders/{WORK_ORDER_ID}/test\\file.pdf",
        f"WorkOrders/{WORK_ORDER_ID}/test\nfile.pdf",
        f"WorkOrders/{WORK_ORDER_ID}/inspection.pdf ",
    ],
)
def test_invalid_s3_keys_are_rejected(setup_lambda, invalid_key):
    module, mock_s3 = setup_lambda

    status, _ = invoke(
        module,
        make_event(object_key=invalid_key),
    )

    assert status in (400, 403)
    mock_s3.generate_presigned_url.assert_not_called()


@pytest.mark.parametrize(
    "invalid_id",
    [
        "",
        "invalid",
        "0WOR5000008zTjdOAE/",
        "../123456789012345",
        None,
    ],
)
def test_invalid_work_order_ids_are_rejected(setup_lambda, invalid_id):
    module, mock_s3 = setup_lambda

    status, _ = invoke(
        module,
        make_event(work_order_id=invalid_id),
    )

    assert status == 400
    mock_s3.generate_presigned_url.assert_not_called()


def test_get_request_is_rejected(setup_lambda):
    module, mock_s3 = setup_lambda

    status, _ = invoke(
        module,
        make_event(method="GET"),
    )

    assert status == 404
    mock_s3.generate_presigned_url.assert_not_called()


def test_wrong_route_is_rejected(setup_lambda):
    module, mock_s3 = setup_lambda

    status, _ = invoke(
        module,
        make_event(path="/files/workorders/download"),
    )

    assert status == 404
    mock_s3.generate_presigned_url.assert_not_called()


def test_invalid_json_is_rejected(setup_lambda):
    module, mock_s3 = setup_lambda

    event = make_event()
    event["body"] = "{invalid json"

    status, _ = invoke(module, event)

    assert status == 400
    mock_s3.generate_presigned_url.assert_not_called()


def test_missing_object_returns_404(setup_lambda):
    module, mock_s3 = setup_lambda

    mock_s3.head_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "404",
                "Message": "Not Found",
            }
        },
        "HeadObject",
    )

    status, _ = invoke(module, make_event())

    assert status == 404
    mock_s3.generate_presigned_url.assert_not_called()


def test_s3_access_denied_returns_404(setup_lambda):
    module, mock_s3 = setup_lambda

    mock_s3.get_object_tagging.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access Denied",
            }
        },
        "GetObjectTagging",
    )

    status, _ = invoke(module, make_event())

    assert status == 404
    mock_s3.generate_presigned_url.assert_not_called()


def test_unexpected_s3_error_returns_500(setup_lambda):
    module, mock_s3 = setup_lambda

    mock_s3.head_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "InternalError",
                "Message": "S3 internal error",
            }
        },
        "HeadObject",
    )

    status, _ = invoke(module, make_event())

    assert status == 500
    mock_s3.generate_presigned_url.assert_not_called()


def test_presigned_url_generation_failure(setup_lambda):
    module, mock_s3 = setup_lambda

    mock_s3.generate_presigned_url.side_effect = RuntimeError(
        "Presigning failed"
    )

    status, _ = invoke(module, make_event())

    assert status == 500
    assert "url" not in _


def test_production_stage_path_is_supported(setup_lambda):
    module, mock_s3 = setup_lambda

    status, body = invoke(
        module,
        make_event(
            path="/prod/files/customer-documents/download",
        ),
    )

    assert status == 200
    assert body["url"] == DOWNLOAD_URL