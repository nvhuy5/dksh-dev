import unittest
from unittest.mock import patch
from fastapi_celery.processors.workflow_processors.rule_mapping_metadata_extract import metadata_extract, StepOutput

class DummyClass:
    metadata_extract = metadata_extract

class TestMetadataExtract(unittest.TestCase):

    def setUp(self):
        self.obj = DummyClass()

    @patch("processors.helpers.xml_helper.build_processor_setting_xml")
    def test_metadata_extract_with_args(self, mock_build_xml):
        mock_build_xml.return_value = "<PROCESSORSETTINGXML>\n  <param>value</param>\n</PROCESSORSETTINGXML>"

        class DataInput:
            data = {"file": "test.csv"}

        data_input = DataInput()
        response_api = {
            "processorArgumentDtos": [
                {"processorArgumentName": "param", "value": "value"}
            ]
        }

        result = self.obj.metadata_extract(data_input, response_api)

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
        mock_build_xml.return_value = None

        class DataInput:
            data = {"file": "empty.csv"}

        data_input = DataInput()
        response_api = {}

        result = self.obj.metadata_extract(data_input, response_api)

        self.assertEqual(type(result), StepOutput)
        self.assertEqual(result.data, {"file": "empty.csv"})
        self.assertIn("data_output", result.sub_data)

        self.assertEqual(
            result.sub_data["data_output"]["processorConfigXml"],
            "<PROCESSORSETTINGXML></PROCESSORSETTINGXML>"
        )

        self.assertEqual(result.step_status, result.step_status.SUCCESS)
        self.assertIsNone(result.step_failure_message)

