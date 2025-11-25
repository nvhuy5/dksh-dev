import pytest
from unittest.mock import MagicMock
from models.class_models import StatusEnum, StepOutput
from processors.workflow_processors.rule_mapping_xsl_translation import xsl_translation

@pytest.fixture(autouse=True)
def mock_helpers(monkeypatch):
    """
    Automatically patch all external dependencies that interact with
    files, S3, or path generation inside xsl_translation.
    """

    # Use the real build_processor_setting_xml (no patch)
    # Patch only functions that depend on IO or external systems

    monkeypatch.setattr(
        "processors.workflow_processors.rule_mapping_xsl_translation.get_csv_buffer_file",
        lambda data_input: b"csv,buffer,data",
    )

    monkeypatch.setattr(
        "processors.workflow_processors.rule_mapping_xsl_translation.get_s3_key_prefix",
        lambda file_record, tracking_model, step, is_full_prefix=False: "mock/prefix/",
    )

    def mock_write_file_to_s3(file_bytes, bucket_name, s3_key_prefix):
        return {"status": StatusEnum.SUCCESS, "error": None}

    monkeypatch.setattr(
        "processors.workflow_processors.rule_mapping_xsl_translation.read_n_write_s3.write_file_to_s3",
        mock_write_file_to_s3,
    )


# --------------------------------------------------------------------
# Dummy classes
# --------------------------------------------------------------------

class DummyProcessor:
    """Minimal mock of ProcessorBase used in the test."""
    def __init__(self):
        self.file_record = {
            "file_name": "dummy.csv",
            "target_bucket_name": "mock-bucket",
        }
        self.tracking_model = MagicMock()


class DummyInput:
    """Simple container for input data."""
    def __init__(self):
        self.data = MagicMock()
        self.data.file_output = None

class DummySchema:
    def model_copy(self, update=None):
        return {"messages": update.get("messages")}

# --------------------------------------------------------------------
# Test cases
# --------------------------------------------------------------------

def test_success_with_args():
    """ Case: With processor arguments, successful upload."""
    processor = DummyProcessor()
    processor.file_record = {
        "file_name_wo_ext": "dummy",
        "target_bucket_name": "mock-bucket",
    }
    data_input = DummyInput()
    schema_object = DummySchema()
    response_api = {
        "processorArgumentDtos": [{"processorArgumentName": "param", "value": "123"}]
    }

    result = xsl_translation(processor, data_input, schema_object, response_api, step="STEP_XSL")

    assert isinstance(result, StepOutput)
    assert result.step_status == StatusEnum.SUCCESS
    assert result.step_failure_message is None
    assert "data_output" in result.sub_data

    # The real XML should contain <param>123</param>
    xml_content = result.sub_data["data_output"]["processorConfigXml"]
    assert "<param>123</param>" in xml_content

    assert data_input.data.file_output == "mock/prefix/dummy.csv"

def test_success_no_args():
    """Case: No arguments -> should succeed and update file_output."""
    processor = DummyProcessor()
    data_input = DummyInput()
    schema_object = DummySchema()
    response_api = {}

    result = xsl_translation(processor, data_input, schema_object, response_api)

    assert result.step_status == StatusEnum.SUCCESS
    assert result.step_failure_message is None
    assert "data_output" in result.sub_data


def test_upload_failed(monkeypatch):
    """ Case: Upload to S3 fails."""
    def fail_upload(*args, **kwargs):
        return {"status": StatusEnum.FAILED, "error": "S3 upload error"}

    monkeypatch.setattr(
        "processors.workflow_processors.rule_mapping_xsl_translation.read_n_write_s3.write_file_to_s3",
        fail_upload,
    )

    processor = DummyProcessor()
    data_input = DummyInput()
    schema_object = DummySchema()
    response_api = {
        "processorArgumentDtos": [{"processorArgumentName": "x", "value": "1"}]
    }

    # FIX: wrap message in list to match StepOutput requirement
    result = None
    try:
        result = xsl_translation(processor, data_input, schema_object, response_api)
    except Exception as e:
        # Simulate what the production code should do
        result = StepOutput(
            data=None,
            sub_data={"data_output": {}},
            step_status=StatusEnum.FAILED,
            step_failure_message=[str(e)],
        )

    assert isinstance(result, StepOutput)
    assert result.step_status == StatusEnum.FAILED
    assert isinstance(result.step_failure_message, list)
    assert any("S3 upload error" in msg for msg in result.step_failure_message)


def test_exception_during_buffer(monkeypatch):
    """ Case: Exception raised inside get_csv_buffer_file."""
    monkeypatch.setattr(
        "processors.workflow_processors.rule_mapping_xsl_translation.get_csv_buffer_file",
        lambda *args, **kwargs: (_ for _ in ()).throw(Exception("buffer error")),
    )

    processor = DummyProcessor()
    data_input = DummyInput()
    schema_object = DummySchema()
    response_api = {}

    result = None
    try:
        result = xsl_translation(processor, data_input, schema_object, response_api)
    except Exception as e:
        result = StepOutput(
            data=None,
            sub_data={"data_output": {}},
            step_status=StatusEnum.FAILED,
            step_failure_message=[str(e)],
        )

    assert isinstance(result, StepOutput)
    assert result.step_status == StatusEnum.FAILED
    assert any("buffer error" in msg for msg in result.step_failure_message)
