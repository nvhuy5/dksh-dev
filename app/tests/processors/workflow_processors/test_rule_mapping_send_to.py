import unittest
from unittest.mock import patch
from fastapi_celery.processors.workflow_processors.rule_mapping_send_to import send_to, StepOutput

class DummyClass:
    metadata_extract = send_to

class TestMetadataExtract(unittest.TestCase):

    def setUp(self):
        self.obj = DummyClass()

    @patch("processors.helpers.xml_helper.build_processor_setting_xml")
    def test_metadata_extract_with_args(self, mock_build_xml):
        mock_build_xml.return_value = "<PROCESSORSETTINGXML>\n  <param>value</param>\n</PROCESSORSETTINGXML>"

        class DataInput:
            data = {"file": "test.csv"}
        
        class DummySchema:
            def model_copy(self, update=None):
                return {"messages": update.get("messages")}

        data_input = DataInput()
        schema_object = DummySchema()
        response_api = {
            "processorArgumentDtos": [
                {"processorArgumentName": "param", "value": "value"}
            ]
        }

        result = self.obj.metadata_extract(data_input, schema_object, response_api)

        self.assertEqual(type(result), StepOutput)
        self.assertEqual(result.data, {"file": "test.csv"})
        self.assertIn("data_output", result.sub_data)
        self.assertEqual(result.step_status, result.step_status.SUCCESS)
        self.assertIsNone(result.step_failure_message)

        xml = result.sub_data["data_output"]["processorConfigXml"]
        self.assertIn("<param>value</param>", xml)
        self.assertTrue(xml.startswith("<PROCESSORSETTINGXML>"))
        self.assertTrue(xml.endswith("</PROCESSORSETTINGXML>"))

    @patch("processors.helpers.xml_helper.build_processor_setting_xml")
    def test_metadata_extract_no_args(self, mock_build_xml):
        mock_build_xml.return_value = "<PROCESSORSETTINGXML></PROCESSORSETTINGXML>"

        class DataInput:
            data = {"file": "empty.csv"}
        
        class DummySchema:
            def model_copy(self, update=None):
                return {"messages": update.get("messages")}

        data_input = DataInput()
        schema_object = DummySchema()
        response_api = {}

        result = self.obj.metadata_extract(data_input, schema_object, response_api)

        self.assertEqual(type(result), StepOutput)
        self.assertEqual(result.data, {"file": "empty.csv"})
        self.assertIn("data_output", result.sub_data)

        self.assertEqual(
            result.sub_data["data_output"]["processorConfigXml"],
            "<PROCESSORSETTINGXML></PROCESSORSETTINGXML>"
        )

        self.assertEqual(result.step_status, result.step_status.SUCCESS)
        self.assertIsNone(result.step_failure_message)

    @patch("processors.helpers.xml_helper.build_processor_setting_xml")
    def test_metadata_extract_exception(self, mock_build_xml):
        mock_build_xml.return_value = "<PROCESSORSETTINGXML></PROCESSORSETTINGXML>"

        class BrokenDataInput:
            @property
            def data(self):
                raise RuntimeError("broken data")
        
        class DummySchema:
            def model_copy(self, update=None):
                return {"messages": update.get("messages")}

        data_input = BrokenDataInput()
        schema_object = DummySchema()
        response_api = {}

        result = self.obj.metadata_extract(data_input, schema_object, response_api)

        self.assertEqual(type(result), StepOutput)
        self.assertEqual(result.step_status, result.step_status.FAILED)
        self.assertIn("broken data", result.step_failure_message[0])
        self.assertIn("data_output", result.sub_data)
