import pytest
from pathlib import Path
from fastapi_celery.models.class_models import (
    PODataParsed,
    DocumentType,
    StatusEnum,
    StepOutput,
)
from fastapi_celery.processors.workflow_processors.template_mapping import (
    template_data_mapping,
)


@pytest.fixture
def sample_po_parsed():
    return PODataParsed(
        file_path="/tmp/template.xlsx",
        document_type=DocumentType.ORDER,
        po_number="PO12345",
        items=[{"header1": "123", "header2": "456"}],
        metadata={"supplier": "ABC"},
        step_status=StatusEnum.SUCCESS,
        file_size="small",
        messages=[],
    )


class DummySelf:
    def __init__(self):
        self.tracking_model = None

class DummySchema:
    def model_copy(self, update=None):
        return {"messages": update.get("messages")}

# === SUCCESS CASE ===
def test_template_data_mapping_success(sample_po_parsed):
    data_input = StepOutput(data=sample_po_parsed)

    response_api = {
        "templateMappingHeaders": [
            {"header": "renamed_col", "fromHeader": "header1", "order": 1},
            {"header": "header2", "fromHeader": "Unmapping", "order": 2},
        ]
    }

    result = template_data_mapping(DummySelf(), data_input, None, response_api)

    assert result.step_status == StatusEnum.SUCCESS
    assert result.step_failure_message is None

    row = result.data.items[0]
    assert "renamed_col" in row
    assert "header2" in row
    assert row["renamed_col"] == "123"


def test_template_data_mapping_invalid_response(sample_po_parsed):
    data_input = StepOutput(data=sample_po_parsed)
    schema_object = DummySchema()
    response_api = None

    result = template_data_mapping(DummySelf(), data_input, schema_object, response_api)

    assert hasattr(result, "step_status")
    assert getattr(result, "step_status") == StatusEnum.FAILED

    assert hasattr(result, "step_failure_message")
    assert "[template_data_mapping] An error occurred" in result.step_failure_message[0]

def test_template_data_mapping_missing_headers(sample_po_parsed):
    data_input = StepOutput(data=sample_po_parsed)

    response_api = {
        "templateMappingHeaders": [
            {"header": "renamed_col", "fromHeader": "not_exist_col", "order": 1},
        ]
    }

    result = template_data_mapping(DummySelf(), data_input, None, response_api)

    assert result.step_status == StatusEnum.FAILED
    assert "expected headers not found" in result.step_failure_message[0]
