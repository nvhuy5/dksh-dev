import pytest
from unittest.mock import patch, MagicMock
from processors.workflow_processors.rule_mapping_rename import rename
from models.class_models import StepOutput, StatusEnum


class DummyProcessor:
    """Fake processor with minimal attributes required by rename."""
    def __init__(self):
        self.file_record = {"target_bucket_name": "mock-bucket", "file_name_wo_ext": "dummy"}
        self.tracking_model = MagicMock()


class DummyData:
    """Fake data_input with nested .data and file_output attributes."""
    def __init__(self):
        class Inner:
            file_output = "mock/prefix/old_file.csv"
        self.data = Inner()

class DummySchema:
            def model_copy(self, update=None):
                return {"messages": update.get("messages")}


@patch("processors.workflow_processors.rule_mapping_rename.copy_object_between_buckets")
@patch("processors.workflow_processors.rule_mapping_rename.get_s3_key_prefix")
@patch("processors.workflow_processors.rule_mapping_rename.get_data_output_for_rule_mapping")
def test_rename_success(mock_get_data_output, mock_get_prefix, mock_copy):
    """Case: rename successfully copies object to new name."""
    # --- Setup dummy processor & input ---
    processor = DummyProcessor()
    data_input = DummyData()
    schema_object = DummySchema()

    # --- Mock helper behaviors ---
    mock_get_data_output.return_value = {
        "processorArgs": [{"name": "fileName", "value": "renamed_file"}]
    }
    mock_get_prefix.return_value = "mock/prefix"
    mock_copy.return_value = {"status": StatusEnum.SUCCESS}

    # --- Run rename() ---
    result = rename(processor, data_input, schema_object, response_api={}, step="STEP_RENAME")

    # --- Assertions ---
    assert isinstance(result, StepOutput)
    assert result.step_status == StatusEnum.SUCCESS
    assert result.step_failure_message is None
    assert result.data.file_output == "mock/prefix/renamed_file.csv"

    # --- Verify mocks ---
    mock_copy.assert_called_once_with(
        "mock-bucket",
        "mock/prefix/old_file.csv",
        "mock-bucket",
        "mock/prefix/renamed_file.csv",
    )


@patch("processors.workflow_processors.rule_mapping_rename.get_data_output_for_rule_mapping")
def test_rename_missing_file_name(mock_get_data_output):
    """ Case: Missing 'fileName' argument should raise ValueError."""
    processor = DummyProcessor()
    data_input = DummyData()
    schema_object = DummySchema()

    mock_get_data_output.return_value = {
        "processorArgs": [{"name": "wrong param", "value": "123"}]
    }

    result = rename(processor, data_input, schema_object, response_api={}, step="STEP_RENAME")

    assert isinstance(result, StepOutput)
    assert result.step_status == StatusEnum.FAILED
    assert "data_output" in result.sub_data
    assert any("Missing argument 'fileName'" in msg for msg in result.step_failure_message)


@patch("processors.workflow_processors.rule_mapping_rename.copy_object_between_buckets")
@patch("processors.workflow_processors.rule_mapping_rename.get_s3_key_prefix")
@patch("processors.workflow_processors.rule_mapping_rename.get_data_output_for_rule_mapping")
def test_rename_copy_failed(mock_get_data_output, mock_get_prefix, mock_copy):
    """ Case: Copying between buckets failed -> raise RuntimeError."""
    processor = DummyProcessor()
    data_input = DummyData()
    schema_object = DummySchema()

    mock_get_data_output.return_value = {
        "processorArgs": [{"name": "fileName", "value": "renamed_file"}]
    }
    mock_get_prefix.return_value = "mock/prefix"
    mock_copy.return_value = {"status": StatusEnum.FAILED, "error": "S3 copy error"}

    # --- Run rename ---
    result = rename(processor, data_input, schema_object, response_api={}, step="STEP_RENAME")

    # --- Assert StepOutput ---
    assert isinstance(result, StepOutput)
    assert result.step_status == StatusEnum.FAILED
    assert "data_output" in result.sub_data
    assert any("Failed to copy object: S3 copy error" in msg for msg in result.step_failure_message)
