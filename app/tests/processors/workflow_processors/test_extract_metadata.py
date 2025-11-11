import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from pathlib import Path

from fastapi_celery.processors.workflow_processors.extract_metadata import extract_metadata


class DummyTrackingModel:
    """Mock tracking model used to simulate the real tracking model."""
    def __init__(self):
        self.document_type = None


class DummyClass:
    """A dummy class that mimics the real class where extract_metadata() is defined."""
    def __init__(self):
        self.tracking_model = DummyTrackingModel()
        self.file_record = None


class TestExtractMetadata(unittest.TestCase):
    """Unit tests for the extract_metadata() function."""

    @patch("fastapi_celery.processors.workflow_processors.extract_metadata.ext_extraction.FileExtensionProcessor")
    def test_extract_metadata_success(self, mock_processor_class):
        """Test that metadata is correctly extracted and stored in file_record."""

        # Arrange: prepare mock objects
        dummy_instance = DummyClass()
        mock_processor = MagicMock()

        # Define return values for the mock FileExtensionProcessor
        mock_processor.file_path = Path("/tmp/test.pdf")
        mock_processor.file_path_parent = Path("/tmp")
        mock_processor.source_type = "LOCAL"
        mock_processor.object_buffer = b"fakebytes"
        mock_processor.file_size = 1024
        mock_processor.file_name = "test.pdf"
        mock_processor.file_name_wo_ext = "test"
        mock_processor.file_extension = ".pdf"
        mock_processor.document_type = MagicMock(value="INVOICE")
        mock_processor.raw_bucket_name = "raw-bucket"
        mock_processor.target_bucket_name = "target-bucket"

        # Patch the class constructor to return our mock instance
        mock_processor_class.return_value = mock_processor

        # Act: call the function under test
        extract_metadata(dummy_instance)

        # Assert: verify results and side effects
        file_record = dummy_instance.file_record
        assert isinstance(file_record, dict)
        assert file_record["file_name"] == "test.pdf"
        assert file_record["file_size"] == 1024
        assert file_record["file_extension"] == ".pdf"
        assert file_record["document_type"].value == "INVOICE"
        assert file_record["raw_bucket_name"] == "raw-bucket"
        assert file_record["target_bucket_name"] == "target-bucket"
        assert "proceed_at" in file_record

        # Ensure proceed_at is a valid datetime string
        datetime.strptime(file_record["proceed_at"], "%Y-%m-%d %H:%M:%S")

        # Verify that tracking_model.document_type was updated correctly
        assert dummy_instance.tracking_model.document_type == "INVOICE"

        # Ensure FileExtensionProcessor was called with correct arguments
        mock_processor_class.assert_called_once_with(dummy_instance.tracking_model)

    @patch("fastapi_celery.processors.workflow_processors.extract_metadata.ext_extraction.FileExtensionProcessor", side_effect=Exception("Mock error"))
    def test_extract_metadata_failure(self, mock_processor_class):
        dummy_instance = DummyClass()

        # Act & Assert: verify that an exception is raised
        with self.assertRaises(Exception) as ctx:
            extract_metadata(dummy_instance)

        assert "Mock error" in str(ctx.exception)
