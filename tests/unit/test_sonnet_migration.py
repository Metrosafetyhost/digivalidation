"""Response compatibility checks for the Sonnet migration; no live AWS calls."""
import importlib
import io
import json
from unittest.mock import Mock

import pytest
from moto import mock_aws


def reply(text):
    return {"body": io.BytesIO(json.dumps({"content": [{"type": "text", "text": text}]}).encode())}


@pytest.mark.parametrize("name", ["checklist_proofing", "fra_checklist_proofing", "hsa_checklist_proofing"])
@pytest.mark.parametrize("answer", ["PASS", "FAIL: Counts differ."])
def test_checklist_response_and_lifecycle(name, answer, aws_credentials, monkeypatch):
    with mock_aws():
        module = importlib.import_module(f"lambdas.{name}")
        runtime = Mock()
        runtime.invoke_model.return_value = reply(answer)
        control = Mock()
        control.get_foundation_model.return_value = {"modelDetails": {"modelLifecycle": {"status": "ACTIVE"}}}
        monkeypatch.setattr(module, "bedrock", runtime)
        monkeypatch.setattr(module, "_MODEL_LIFECYCLE_CACHE", None)
        if name == "checklist_proofing":
            monkeypatch.setattr(module, "bedrock_control", control)
        original_client = module.boto3.client
        monkeypatch.setattr(module.boto3, "client", lambda service, **kw: control if service == "bedrock" else original_client(service, **kw))
        assert module.send_to_bedrock("Check that the totals match.") == answer
        control.get_foundation_model.assert_called_once_with(modelIdentifier="anthropic.claude-sonnet-5")
        request = runtime.invoke_model.call_args.kwargs
        assert request["modelId"] == "eu.anthropic.claude-sonnet-5"
        payload = json.loads(request["body"])
        assert payload["thinking"] == {"type": "disabled"}
        assert "temperature" not in payload


def test_html_json_and_error_fallback(aws_credentials, monkeypatch):
    with mock_aws():
        module = importlib.import_module("lambdas.salesforce_input")
        runtime = Mock()
        runtime.invoke_model.return_value = reply('["[[P]]The door is damaged.[[/P]]"]')
        monkeypatch.setattr(module, "bedrock_client", runtime)
        html = "<table><tr><td>Condition</td><td><p>The door is damged.</p></td></tr></table>"
        corrected, logs = module.proof_table_content(html, "smoke")
        assert corrected == html.replace("damged", "damaged")
        assert logs[0]["proofed"] == "<p>The door is damaged.</p>"
        runtime.invoke_model.return_value = reply("invalid JSON")
        assert module.proof_table_content(html, "smoke") == (html, [])


def test_classifier_json_and_error(aws_credentials, monkeypatch):
    with mock_aws():
        module = importlib.import_module("lambdas.cannot_complete_classifier")
        runtime = Mock()
        monkeypatch.setattr(module, "bedrock_client", runtime)
        runtime.invoke_model.return_value = reply('{"cleanedDescription": "The door is locked."}')
        assert module.call_bedrock("No access", "Door locked") == {"cleanedDescription": "The door is locked."}
        with pytest.raises(ValueError, match="missing required fields"):
            module.validate_classification({})
        runtime.invoke_model.return_value = reply("invalid JSON")
        with pytest.raises(RuntimeError, match="valid JSON"):
            module.call_bedrock("No access", "Door locked")
