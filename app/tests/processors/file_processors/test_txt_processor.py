import io
import builtins
import pytest
from unittest.mock import mock_open, patch

from fastapi_celery.models.class_models import StatusEnum, DocumentType
from fastapi_celery.processors.file_processors.txt_processor import TXTProcessor, PO_MAPPING_KEY


@pytest.fixture
def local_file_record(tmp_path):
    """Fixture for local TXT file"""
    file_path = tmp_path / "dummy.txt"
    file_path.write_text("採購單-PO123\n料品代號\t品名\t數量\nA001\tABC\t10", encoding="utf-8")
    return {
        "source_type": "local",
        "file_path": str(file_path),
        "document_type": DocumentType.ORDER,
        "file_size": "1.23 KB",
    }


@pytest.fixture
def s3_file_record():
    """Fixture for S3-like TXT file"""
    buffer = io.BytesIO("採購單-PO123\n料品代號\t品名\t數量\nA001\tABC\t10".encode("utf-8"))
    return {
        "source_type": "s3",
        "object_buffer": buffer,
        "file_path": "s3://bucket/file.txt",
        "document_type": DocumentType.ORDER,
        "file_size": "2.34 KB",
    }


def test_extract_text_local(local_file_record):
    """Extract text from a local file"""
    txt_content = "採購單-PO123\n料品代號\t品名\t數量\nA001\tABC\t10"
    m_open = mock_open(read_data=txt_content)

    with patch.object(builtins, "open", m_open):
        processor = TXTProcessor(local_file_record)
        result = processor.extract_text()

    assert "PO123" in result
    assert "ABC" in result


def test_extract_text_s3(s3_file_record):
    """Extract text from S3-like buffer"""
    processor = TXTProcessor(s3_file_record)
    result = processor.extract_text()
    assert "PO123" in result
    assert "ABC" in result


def test_parse_file_to_json_basic(s3_file_record):
    """Parse basic S3 file"""
    processor = TXTProcessor(s3_file_record)
    parsed = processor.parse_file_to_json()

    assert parsed.po_number == "PO123"
    assert parsed.items[PO_MAPPING_KEY] == "PO123"
    assert parsed.items["products"][0]["料品代號"] == "A001"
    assert parsed.step_status == StatusEnum.SUCCESS
    assert parsed.file_size == "2.34 KB"


def test_parse_file_with_key_values(s3_file_record):
    """Parse file with key-value and tab mix"""
    text_data = """採購單-PO999
單號：T001
日期：2024-10-10\t公司：TestCorp
料品代號\t名稱\t數量
B001\tXYZ\t5
"""
    s3_file_record["object_buffer"] = io.BytesIO(text_data.encode("utf-8"))
    processor = TXTProcessor(s3_file_record)
    parsed = processor.parse_file_to_json()

    assert parsed.po_number == "PO999"
    assert "單號" in parsed.items
    assert parsed.items["公司"] == "TestCorp"
    assert parsed.items["products"][0]["名稱"] == "XYZ"


def test_parse_file_with_incomplete_product(s3_file_record):
    """Product line missing last column should be padded"""
    text_data = "採購單-PO777\n料品代號\t品名\t數量\nC001\tSample\n"
    s3_file_record["object_buffer"] = io.BytesIO(text_data.encode("utf-8"))
    processor = TXTProcessor(s3_file_record)
    parsed = processor.parse_file_to_json()

    assert parsed.items["products"][0]["數量"] == ""


def test_parse_file_skip_lines(s3_file_record):
    """Skip empty or dashed lines"""
    text_data = """---
採購單-PO555
料品代號\t品名\t數量
D001\tTest\t1
---"""
    s3_file_record["object_buffer"] = io.BytesIO(text_data.encode("utf-8"))
    processor = TXTProcessor(s3_file_record)
    parsed = processor.parse_file_to_json()

    assert parsed.po_number == "PO555"
    assert len(parsed.items["products"]) == 1


def test_parse_file_no_products(s3_file_record):
    """Handle file without product section"""
    text_data = "採購單-PO321\n單號：T002"
    s3_file_record["object_buffer"] = io.BytesIO(text_data.encode("utf-8"))
    processor = TXTProcessor(s3_file_record)
    parsed = processor.parse_file_to_json()

    assert "products" not in parsed.items


def test_logger_called(monkeypatch, s3_file_record):
    """Ensure logging messages are triggered"""
    mock_logger = patch(
        "fastapi_celery.processors.file_processors.txt_processor.logger"
    ).start()

    processor = TXTProcessor(s3_file_record)
    processor.parse_file_to_json()

    mock_logger.info.assert_any_call("File has been proceeded successfully!")
    mock_logger.info.assert_any_call(
        f"Start processing for file: {s3_file_record.get('file_path')}"
    )
    patch.stopall()


def test_file_with_extra_spaces(s3_file_record):
    """Handle extra spaces and tabs in input"""
    text_data = """採購單 - PO111
料品代號\t 品名 \t數量
E001\t   Hello \t 3
"""
    s3_file_record["object_buffer"] = io.BytesIO(text_data.encode("utf-8"))
    processor = TXTProcessor(s3_file_record)
    parsed = processor.parse_file_to_json()
    assert parsed.po_number == "PO111"
    assert parsed.items["products"][0]["品名"] == "Hello"


def test_file_multiple_products(s3_file_record):
    """Handle multiple product rows"""
    text_data = """採購單-PO432
料品代號\t品名\t數量
A1\tX\t1
A2\tY\t2
"""
    s3_file_record["object_buffer"] = io.BytesIO(text_data.encode("utf-8"))
    processor = TXTProcessor(s3_file_record)
    parsed = processor.parse_file_to_json()

    assert len(parsed.items["products"]) == 2
    assert parsed.items["products"][1]["品名"] == "Y"
