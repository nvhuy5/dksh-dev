import pytest
from unittest.mock import MagicMock, patch
from fastapi_celery.models.class_models import StepOutput, StatusEnum , PODataParsed, DocumentType
from fastapi_celery.processors.workflow_processors.parse_file_to_json import parse_file_to_json

# ===== Dummy self class =====
class DummySelf:
    file_record = "dummy_file"
    tracking_model = MagicMock()


# ===== Test parse_file_to_json success =====
def test_parse_file_to_json_success():
    dummy_self = DummySelf()
    response_api = [{"templateFileParse": {"code": "TEST_CODE"}}]

    # Create a real PODataParsed object
    fake_data = PODataParsed(
        original_file_path="dummy_path",
        document_type= DocumentType.ORDER,
        po_number="PO123",
        items=[{"col1": "1"}, {"col1": "2"}, {"col1": "3"}],
        metadata={},
        capacity="small",
        step_status=None,
        messages=None,
    )

    # Mock processor instance to return real object
    mock_processor_instance = MagicMock()
    mock_processor_instance.parse_file_to_json.return_value = fake_data

    # Patch ProcessorRegistry
    with patch("fastapi_celery.processors.workflow_processors.parse_file_to_json.ProcessorRegistry") as mock_registry:
        mock_registry.get_processor_for_file.return_value.create_instance.return_value = mock_processor_instance

        result = parse_file_to_json(dummy_self, None, response_api)

        # Assertions
        from fastapi_celery.models.class_models import StepOutput
        assert type(result).__name__ == "StepOutput"
        assert result.step_status == StatusEnum.SUCCESS
        assert result.sub_data["data_output"]["totalRecords"] == 3
        assert result.sub_data["data_output"]["storageLocation"] == "dummy_path"


# ===== Test parse_file_to_json raises exception =====
def test_parse_file_to_json_exception():
    dummy_self = DummySelf()
    
    response_api = [{"templateFileParse": {"code": "TEST_CODE"}}]
    
    # Patch ProcessorRegistry to raise an exception
    with patch("fastapi_celery.processors.workflow_processors.parse_file_to_json.ProcessorRegistry") as mock_registry:
        mock_registry.get_processor_for_file.side_effect = Exception("Registry error")
        
        with pytest.raises(Exception) as exc_info:
            parse_file_to_json(dummy_self, None, response_api)
        
        assert "Registry error" in str(exc_info.value)
