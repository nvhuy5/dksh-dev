import pytest
from unittest.mock import patch, MagicMock
from fastapi_celery.models.class_models import StepOutput, StatusEnum
from fastapi_celery.processors.workflow_processors.parse_file_to_json import parse_file_to_json


class DummyProcessor:
    def parse_file_to_json(self):
        # Giả lập data trả về có items
        return MagicMock(items=[{"a": 1}, {"a": 2}])

class DummySchema:
    def model_copy(self, update=None):
        return {"messages": update.get("messages")}
    
class DummyProcessorBase:
    file_record = {"file_path": "dummy_path"}
    tracking_model = {"tracking": "test"}


def test_parse_file_to_json_success():
    dummy_self = DummyProcessorBase()
    response_api = [{"templateFileParse": {"code": "TEST_CODE"}}]

    with patch(
        "fastapi_celery.processors.workflow_processors.parse_file_to_json.ProcessorRegistry.get_processor_for_file"
    ) as mock_get_proc:
        mock_enum = MagicMock()
        mock_enum.create_instance.return_value = DummyProcessor()
        mock_get_proc.return_value = mock_enum

        result = parse_file_to_json(dummy_self, None, None , response_api)

    assert type(result).__name__ == "StepOutput"
    assert result.step_status == StatusEnum.SUCCESS
    assert result.sub_data["data_output"]["totalRecords"] == 2  # ✅ đúng key
    assert result.step_failure_message is None


def test_parse_file_to_json_failed():
    dummy_self = DummyProcessorBase()
    schema_object = DummySchema()
    response_api = [{"templateFileParse": {"code": "TEST_CODE"}}]

    # Giả lập get_processor_for_file() trả về None -> lỗi khi gọi create_instance
    with patch(
        "fastapi_celery.processors.workflow_processors.parse_file_to_json.ProcessorRegistry.get_processor_for_file",
        return_value=None,
    ):
        result = parse_file_to_json(dummy_self, None, schema_object, response_api)

    assert type(result).__name__ == "StepOutput"
    assert result.step_status == StatusEnum.FAILED
    assert "An error occurred" in result.step_failure_message[0]
