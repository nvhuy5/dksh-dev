import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
from xml.etree.ElementTree import Element
from fastapi_celery.processors.file_processors.xml_processor import XMLProcessor
from fastapi_celery.models.class_models import PODataParsed


class TestXMLProcessor(unittest.TestCase):
    def setUp(self):
        self.dummy_path = Path("dummy.xml")
        self.xml_content = """
        <Invoice>
            <Header>
                <Number>PO123456</Number>
                <Date>2025-07-10</Date>
            </Header>
            <Items>
                <Item>
                    <Name>Widget A</Name>
                    <Quantity>10</Quantity>
                </Item>
            </Items>
        </Invoice>
        """
        self.file_record = {
            "file_path": str(self.dummy_path),
            "source_type": "local",
            "document_type": "invoice",
            "file_size": "small",
        }

    def test_extract_text_from_s3(self):
        buffer = MagicMock()
        buffer.read.return_value = self.xml_content.encode("utf-8")
        buffer.seek.return_value = None

        file_record = {
            "file_path": str(self.dummy_path),
            "source_type": "s3",
            "object_buffer": buffer,
            "document_type": "invoice",
            "file_size": "small",
        }

        processor = XMLProcessor(file_record=file_record)
        text = processor.extract_text()
        self.assertIn("<Invoice>", text)

    def test_extract_text_local_file(self):
        with patch("builtins.open", unittest.mock.mock_open(read_data=self.xml_content)):
            processor = XMLProcessor(file_record=self.file_record)
            text = processor.extract_text()
            self.assertIn("<Invoice>", text)

    def test_parse_element_and_find_po(self):
        root = Element("Root")
        header = Element("Header")
        number = Element("Number")
        number.text = "PO999888"
        header.append(number)
        root.append(header)

        processor = XMLProcessor(file_record=self.file_record)

        parsed = processor.parse_element(root)
        self.assertEqual(parsed, {"Header": {"Number": "PO999888"}})

        po = processor.find_po_in_xml(root)
        self.assertEqual(po, "PO999888")

    def test_parse_element_with_text_node(self):
        element = Element("Description")
        element.text = "Simple text"
        processor = XMLProcessor(file_record=self.file_record)
        result = processor.parse_element(element)
        self.assertEqual(result, "Simple text")

    def test_find_po_in_attribute(self):
        element = Element("Invoice", attrib={"ref": "PO555666"})
        processor = XMLProcessor(file_record=self.file_record)
        po = processor.find_po_in_xml(element)
        self.assertEqual(po, "PO555666")

    def test_find_po_not_found(self):
        element = Element("Header")
        sub = Element("Number")
        sub.text = "NOPOHERE"
        element.append(sub)

        processor = XMLProcessor(file_record=self.file_record)
        po = processor.find_po_in_xml(element)
        self.assertEqual(po, "")

    def test_parse_file_to_json(self):
        with patch("builtins.open", unittest.mock.mock_open(read_data=self.xml_content)):
            with patch(
                "fastapi_celery.processors.file_processors.xml_processor.PODataParsed"
            ) as MockPODataParsed:
                dummy_parsed = MagicMock()
                MockPODataParsed.return_value = dummy_parsed

                processor = XMLProcessor(file_record=self.file_record)
                result = processor.parse_file_to_json()

                MockPODataParsed.assert_called_once()
                called_args = MockPODataParsed.call_args.kwargs

                self.assertEqual(called_args["po_number"], "PO123456")
                self.assertEqual(called_args["document_type"], "invoice")
                self.assertEqual(
                    called_args["items"]["Header"]["Number"], "PO123456"
                )
                self.assertEqual(result, dummy_parsed)

    def test_parse_file_to_json_invalid_xml(self):
        bad_content = "<Invoice><Header></Invoice"  # invalid XML
        with patch("builtins.open", unittest.mock.mock_open(read_data=bad_content)):
            processor = XMLProcessor(file_record=self.file_record)
            with self.assertRaises(Exception):
                processor.parse_file_to_json()
