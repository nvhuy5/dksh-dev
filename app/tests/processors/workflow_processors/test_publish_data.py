import unittest
from processors.workflow_processors.publish_data import copy_file, publish_data
import pytest
from unittest.mock import MagicMock, patch
from processors.processor_base import ProcessorBase
from processors.workflow_processors.publish_data import StepOutput, StatusEnum

class DummyClass:
    def publish_data(self, data_input, response_api, *args, **kwargs):
        return StepOutput(
            data=data_input,
            sub_data={},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None
        )
    
class DummySchema:
    def model_copy(self, update=None):
        return {"messages": update.get("messages")}

class DummyProcessor(ProcessorBase):
    def __init__(self):
        super().__init__(tracking_model=MagicMock())
        self.file_record = {"target_bucket_name": "test-bucket"}

class DummyDataModel:
    def __init__(self):
        self.data_field = "test"
        self.file_output = "path/to/source_file.txt"

    def model_copy(self, update: dict):
        new_instance = DummyDataModel()
        for k, v in update.items():
            setattr(new_instance, k, v)
        return new_instance

class TestPublishData(unittest.TestCase):

    def setUp(self):
        self.obj = DummyClass()

    @patch("processors.workflow_processors.publish_data.build_publish_data_output")
    def test_publish_data_success_status(self, mock_build_output):
        mock_build_output.return_value = {"dummy": "value"}
 
        processor = DummyProcessor()
        data_input = MagicMock()
        data_input.data = DummyDataModel()
        response_api = {"success": True, "message": "OK"}
 
        result = publish_data(
            processor,
            data_input,
            None,
            response_api,
            connectionDto={"requiredFields": {"REQUIRED": []}, "connectionType": "SFTP"}
        )
 
        self.assertIsInstance(result, StepOutput)
        self.assertEqual(result.step_status, StatusEnum.SUCCESS)
        self.assertEqual(result.sub_data["data_output"]["sentStatus"], "Sent")
    @patch("processors.workflow_processors.publish_data.build_publish_data_output")
    def test_publish_data_success_status(self, mock_build_output):
        mock_build_output.return_value = {"dummy": "value"}
 
        processor = DummyProcessor()
        data_input = MagicMock()
        data_input.data = DummyDataModel()
        response_api = {"success": True, "message": "OK"}
 
        result = publish_data(
            processor,
            data_input,
            None,
            response_api,
            connectionDto={"requiredFields": {"REQUIRED": []}, "connectionType": "SFTP"}
        )
 
        self.assertIsInstance(result, StepOutput)
        self.assertEqual(result.step_status, StatusEnum.SUCCESS)
        self.assertEqual(result.sub_data["data_output"]["sentStatus"], "Sent")
 
    def test_publish_data_invalid_response_raises_error(self):
        processor = DummyProcessor()
        data_input = MagicMock()
        schema_object = DummySchema()
        data_input.data = DummyDataModel()
 
        response_api = None
 
        result = publish_data(
            processor,
            data_input,
            schema_object,
            response_api,
            connectionDto={"requiredFields": {"REQUIRED": []}, "connectionType": "SFTP"}
        )
 
        self.assertIsInstance(result, StepOutput)
        self.assertEqual(result.step_status, StatusEnum.FAILED)
        self.assertIn("did not return a valid response", result.step_failure_message[0])
 
 
    @patch("processors.workflow_processors.publish_data.read_n_write_s3.copy_object_between_buckets")
    @patch("processors.workflow_processors.publish_data.get_s3_key_prefix")
    def test_copy_file_success(self, mock_get_prefix, mock_copy_s3):
        mock_get_prefix.return_value = "prefix/"
        mock_copy_s3.return_value = {"status": StatusEnum.SUCCESS}
        
        processor = DummyProcessor()
        data_input = MagicMock()
        data_input.data = DummyDataModel()
        
        result = copy_file(processor, data_input, response_api=None, step="dummy_step")
        
        self.assertIn("fileOutputLink", result)
        self.assertEqual(data_input.data.file_output, "prefix/source_file.txt")
        self.assertEqual(result["fileOutputLink"], "test-bucket/prefix/source_file.txt")

    @patch("processors.workflow_processors.publish_data.read_n_write_s3.copy_object_between_buckets")
    @patch("processors.workflow_processors.publish_data.get_s3_key_prefix")
    def test_copy_file_failure(self, mock_get_prefix, mock_copy_s3):
        mock_get_prefix.return_value = "prefix/"
        mock_copy_s3.return_value = {"status": StatusEnum.FAILED, "error": "Copy failed"}
        
        processor = DummyProcessor()
        data_input = MagicMock()
        data_input.data = DummyDataModel()
        
        result = copy_file(processor, data_input, response_api=None, step="dummy_step")
        
        assert result["fileOutputLink"] == ""