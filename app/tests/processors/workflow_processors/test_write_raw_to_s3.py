import pytest
from unittest.mock import MagicMock, patch
from fastapi_celery.processors.workflow_processors import write_raw_to_s3
from fastapi_celery.models.class_models import StepOutput, StatusEnum


class FakeProcessorBase:
    """Fake ProcessorBase for testing write_raw_to_s3."""
    def __init__(self):
        self.file_record = {
            "file_name": "test.csv",
            "file_name_wo_ext": "test",
            "raw_bucket_name": "raw-bucket",
            "target_bucket_name": "target-bucket",
            "file_path": "raw/test.csv",
        }
        self.tracking_model = MagicMock()


@pytest.fixture
def fake_processor():
    return FakeProcessorBase()


def test_write_raw_to_s3_success_first_version(fake_processor):
    """Should create version 001 when no existing versions."""
    with (
        patch.object(write_raw_to_s3, "read_n_write_s3") as mock_s3,
        patch.object(write_raw_to_s3, "logger") as mock_logger,
    ):
        mock_s3.copy_object_between_buckets.return_value = {"result": "ok"}
        mock_s3.list_objects_with_prefix.return_value = []

        result = write_raw_to_s3.write_raw_to_s3(fake_processor)

        # Validate correct result
        assert hasattr(result, "step_status")
        assert result.step_status == StatusEnum.SUCCESS
        assert result.data == {"result": "ok"}
        assert result.step_failure_message is None

        # Should copy twice: to main and version folder
        assert mock_s3.copy_object_between_buckets.call_count == 2
        mock_logger.info.assert_called_once()


def test_write_raw_to_s3_success_next_version(fake_processor):
    """Should increment version number when previous exist."""
    existing_keys = [
        "versioning/test/001/test.csv",
        "versioning/test/002/test.csv",
    ]

    with (
        patch.object(write_raw_to_s3, "read_n_write_s3") as mock_s3,
        patch.object(write_raw_to_s3, "logger") as mock_logger,
    ):
        mock_s3.copy_object_between_buckets.return_value = {"result": "ok"}
        mock_s3.list_objects_with_prefix.return_value = existing_keys

        result = write_raw_to_s3.write_raw_to_s3(fake_processor)

        # Expect version_number = 3
        calls = mock_s3.copy_object_between_buckets.call_args_list
        dest_keys = [c.kwargs["dest_key"] for c in calls]
        assert any("/003/" in key for key in dest_keys)

        assert result.step_status == StatusEnum.SUCCESS
        assert mock_logger.info.called


def test_write_raw_to_s3_exception(fake_processor):
    """Should log error and raise when exception occurs."""
    with (
        patch.object(write_raw_to_s3, "read_n_write_s3") as mock_s3,
        patch.object(write_raw_to_s3, "logger") as mock_logger,
    ):
        mock_s3.copy_object_between_buckets.side_effect = Exception("S3 copy failed")

        with pytest.raises(Exception) as excinfo:
            write_raw_to_s3.write_raw_to_s3(fake_processor)

        assert "S3 copy failed" in str(excinfo.value)
        mock_logger.error.assert_called_once()
