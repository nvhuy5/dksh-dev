import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

from fastapi_celery.processors.file_processors.excel_processor import ExcelProcessor
from models.class_models import StatusEnum


@pytest.fixture
def mock_file_record():
    """Mocked file_record dictionary for ExcelProcessor."""
    return {
        "file_path": Path("/fake/path/dummy.xlsx"),
        "document_type": "order",
        "capacity": "small",
    }


@pytest.fixture
def processor(mock_file_record):
    """Initialize ExcelProcessor with mocked ExcelHelper to skip real file I/O."""
    with patch(
        "fastapi_celery.processors.file_processors.excel_processor.excel_helper.ExcelHelper.__init__",
        return_value=None,
    ):
        processor = ExcelProcessor(file_record=mock_file_record)
        processor.file_record = mock_file_record
        processor.rows = []
        processor.po_number = None
        return processor


def test_parse_only_metadata(processor):
    """Test case where rows contain only metadata."""
    processor.rows = [
        ["PO Number：12345"],
        ["Date：2025-10-17"]
    ]

    def fake_extract_metadata(row):
        if "PO Number" in row[0]:
            return {"PO Number": "12345"}
        if "Date" in row[0]:
            return {"Date": "2025-10-17"}
        return {}

    processor.extract_metadata = fake_extract_metadata

    result = processor.parse_file_to_json()

    assert result.metadata == {"PO Number": "12345", "Date": "2025-10-17"}
    assert result.items == []
    assert result.step_status == StatusEnum.SUCCESS
    assert Path(result.original_file_path).name == "dummy.xlsx"


def test_parse_with_table(processor):
    """Test case with table data only."""
    processor.rows = [
        ["Item", "Qty"],
        ["Pen", "10"],
        ["Book", "20"]
    ]

    processor.extract_metadata = lambda row: {}

    result = processor.parse_file_to_json()

    assert len(result.items) == 2
    assert result.items[0]["Item"] == "Pen"
    assert result.items[1]["Qty"] == "20"
    assert result.metadata == {}
    assert result.step_status == StatusEnum.SUCCESS


def test_parse_mixed_metadata_and_table(processor):
    """Test case mixing metadata and table data."""
    processor.rows = [
        ["PO Number：8888"],
        ["Item", "Qty"],
        ["A", "1"],
        ["B", "2"],
        ["Comment：Done"]
    ]

    def fake_extract_metadata(row):
        text = row[0]
        if "PO Number" in text:
            return {"PO Number": "8888"}
        if "Comment" in text:
            return {"Comment": "Done"}
        return {}

    processor.extract_metadata = fake_extract_metadata

    result = processor.parse_file_to_json()

    assert result.metadata == {"PO Number": "8888", "Comment": "Done"}
    assert len(result.items) == 2
    assert result.items[0]["Item"] == "A"
    assert result.items[1]["Qty"] == "2"
    assert result.step_status == StatusEnum.SUCCESS


def test_parse_metadata_between_tables(processor):
    """Test case where metadata appears between table blocks."""
    processor.rows = [
        ["Header1", "Header2"],
        ["x1", "y1"],
        ["Info：mid-table"],
        ["H2-1", "H2-2"],
        ["x2", "y2"]
    ]

    def fake_extract_metadata(row):
        if "Info" in row[0]:
            return {"Info": "mid-table"}
        return {}

    processor.extract_metadata = fake_extract_metadata

    result = processor.parse_file_to_json()

    assert "Info" in result.metadata
    assert any("Header1" in item for item in result.items)
    assert result.step_status == StatusEnum.SUCCESS


def test_parse_empty_rows(processor):
    """Test case with empty rows list."""
    processor.rows = []
    processor.extract_metadata = lambda row: {}

    result = processor.parse_file_to_json()

    assert result.metadata == {}
    assert result.items == []
    assert result.step_status == StatusEnum.SUCCESS
