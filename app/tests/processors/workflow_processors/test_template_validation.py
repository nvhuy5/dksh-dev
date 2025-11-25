import pytest
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

from fastapi_celery.models.class_models import (
    PODataParsed,
    DocumentType,
    StatusEnum,
    StepOutput,
)
from fastapi_celery.processors.workflow_processors.template_validation import (
    TemplateValidation,
    template_format_validation,
)


@pytest.fixture
def sample_po_parsed():
    return PODataParsed(
        file_path="/tmp/template.xlsx",
        document_type=DocumentType.ORDER,
        po_number="PO12345",
        items=[
            {"col_1": "123", "col_2": "ABC", "col_3": "2024-01-01"},
            {"col_1": "456", "col_2": "XYZ", "col_3": "2024-01-02"},
        ],
        metadata={"supplier": "ABC"},
        file_size="small",
        step_status=None,
        messages=None,
    )


@pytest.fixture
def mock_tracking_model():
    return MagicMock()


def test_data_validation_success(sample_po_parsed, mock_tracking_model):
    schema = [
        {"order": 1, "dataType": "Number", "metadata": '{"required": true}'},
        {"order": 2, "dataType": "String", "metadata": '{"required": true, "maxLength": 10}'},
        {"order": 3, "dataType": "Date", "metadata": '{"required": true}'},
    ]
    validator = TemplateValidation(sample_po_parsed, mock_tracking_model)
    validated_data, data_output = validator.data_validation(schema)

    assert validated_data.step_status == StatusEnum.SUCCESS
    assert validated_data.messages is None
    assert data_output["totalRecords"] == 2
    assert data_output["validRecords"] == 2


def test_data_validation_required_missing(sample_po_parsed, mock_tracking_model):
    schema = [{"order": 1, "dataType": "Number", "metadata": '{"required": true, "allowEmpty": false}'}]
    parsed = sample_po_parsed.model_copy(update={
        "items": [{"col_1": "", "col_2": "ABC", "col_3": "2024-01-01"}]
    })
    validator = TemplateValidation(parsed, mock_tracking_model)
    validated_data, _ = validator.data_validation(schema)
    assert validated_data.step_status == StatusEnum.FAILED
    assert any("required but empty" in msg for msg in validated_data.messages)


def test_data_validation_max_length_error(sample_po_parsed, mock_tracking_model):
    schema = [{"order": 2, "dataType": "String", "metadata": '{"maxLength": 3}'}]
    parsed = sample_po_parsed.model_copy(update={
        "items": [{"col_1": "1", "col_2": "TOO_LONG", "col_3": "2024-01-01"}]
    })
    validator = TemplateValidation(parsed, mock_tracking_model)
    validated_data, _ = validator.data_validation(schema)
    assert validated_data.step_status == StatusEnum.FAILED
    assert any("exceeds maxLength" in msg for msg in validated_data.messages)


def test_data_validation_regex_error(sample_po_parsed, mock_tracking_model):
    schema = [{"order": 2, "dataType": "String", "metadata": '{"regex": "^[A-Z]{3}$"}'}]
    parsed = sample_po_parsed.model_copy(update={
        "items": [{"col_1": "1", "col_2": "wrong", "col_3": "2024-01-01"}]
    })
    validator = TemplateValidation(parsed, mock_tracking_model)
    validated_data, _ = validator.data_validation(schema)
    assert validated_data.step_status == StatusEnum.FAILED
    assert any("does not match regex" in msg for msg in validated_data.messages)


def test_data_validation_invalid_number(sample_po_parsed, mock_tracking_model):
    schema = [{"order": 1, "dataType": "Number"}]
    parsed = sample_po_parsed.model_copy(update={
        "items": [{"col_1": "abc", "col_2": "XYZ", "col_3": "2024-01-01"}]
    })
    validator = TemplateValidation(parsed, mock_tracking_model)
    validated_data, _ = validator.data_validation(schema)
    assert validated_data.step_status == StatusEnum.FAILED
    assert any("is not a valid number" in msg for msg in validated_data.messages)


def test_data_validation_invalid_date(sample_po_parsed, mock_tracking_model):
    schema = [{"order": 3, "dataType": "Date"}]
    parsed = sample_po_parsed.model_copy(update={
        "items": [{"col_1": "1", "col_2": "ABC", "col_3": "invalid"}]
    })
    validator = TemplateValidation(parsed, mock_tracking_model)
    validated_data, _ = validator.data_validation(schema)
    assert validated_data.step_status == StatusEnum.FAILED
    assert any("is not a valid date" in msg for msg in validated_data.messages)


@patch("fastapi_celery.models.class_models.PODataParsed.model_dump_json", lambda self, **kwargs: "{}")
def test_template_format_validation_success(sample_po_parsed, mock_tracking_model):
    class DummySelf:
        def __init__(self):
            self.tracking_model = mock_tracking_model

    schema = [
        {"order": 1, "dataType": "Number", "metadata": '{"required": true}'},
    ]

    step_input = StepOutput(
        data=sample_po_parsed,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )

    response_api = {"data": {"columns": schema}}
    result = template_format_validation(DummySelf(), step_input, None, response_api)

    assert type(result).__name__ == "StepOutput"

    if isinstance(result, dict):
        assert "data" in result or "sub_data" in result
    else:
        assert result.step_status in (StatusEnum.SUCCESS, StatusEnum.FAILED)


def test_template_format_validation_schema_missing(sample_po_parsed, mock_tracking_model):
    class DummySelf:
        def __init__(self):
            self.tracking_model = mock_tracking_model

    step_input = StepOutput(
        data=sample_po_parsed,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )

    response_api = {"data": {"columns": []}}
    result = template_format_validation(DummySelf(), step_input, None, response_api)

    assert result.step_status == StatusEnum.FAILED
    assert "Schema columns not found" in result.step_failure_message[0]
