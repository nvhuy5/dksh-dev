import io
import pytest
from unittest.mock import patch
from pathlib import Path

from fastapi_celery.processors.file_processors.csv_processor import CSVProcessor, METADATA_SEPARATOR
from models.class_models import DocumentType, PODataParsed, SourceType, StatusEnum


@pytest.fixture
def mock_file_record_local(tmp_path):
    file_path = tmp_path / "file.csv"
    file_path.write_text("col1,col2\nval1,val2\n", encoding="utf-8")
    return {
        "file_path": file_path,
        "source_type": "local",
        "document_type": DocumentType.ORDER,
        "file_size": "100 KB",
    }


@pytest.fixture
def mock_file_record_buffer():
    buffer = io.BytesIO(b"col1,col2\nval1,val2\n")
    return {
        "file_path": "dummy/path/file.csv",
        "source_type": "s3",
        "document_type": DocumentType.ORDER,
        "file_size": "100 KB",
        "object_buffer": buffer,
    }


@patch("fastapi_celery.processors.file_processors.csv_processor.chardet.detect", return_value={"encoding": "utf-8"})
def test_load_csv_rows_local(mock_detect, mock_file_record_local):
    processor = CSVProcessor(mock_file_record_local)
    rows = processor.rows
    assert rows == [["col1", "col2"], ["val1", "val2"]]


@patch("fastapi_celery.processors.file_processors.csv_processor.chardet.detect", return_value={"encoding": "utf-8"})
def test_load_csv_rows_buffer(mock_detect, mock_file_record_buffer):
    processor = CSVProcessor(mock_file_record_buffer)
    rows = processor.rows
    assert rows == [["col1", "col2"], ["val1", "val2"]]


def test_extract_metadata_found(mock_file_record_local):
    processor = CSVProcessor(mock_file_record_local)
    row = [f"Key{METADATA_SEPARATOR}Value"]
    result = processor.extract_metadata(row)
    assert result == {"Key": "Value"}


def test_extract_metadata_not_found(mock_file_record_local):
    processor = CSVProcessor(mock_file_record_local)
    row = ["no meta here"]
    result = processor.extract_metadata(row)
    assert result == {}


@pytest.mark.parametrize("row,expected", [
    (["A", "B", "C"], True),
    (["1", "2", "3"], False),
])
def test_is_likely_header(row, expected, mock_file_record_local):
    processor = CSVProcessor(mock_file_record_local)
    assert processor.is_likely_header(row) == expected


def test_parse_metadata_rows(mock_file_record_local):
    processor = CSVProcessor(mock_file_record_local)
    processor.rows = [
        [f"A{METADATA_SEPARATOR}1"],
        [f"B{METADATA_SEPARATOR}2"],
        ["data"]
    ]
    metadata, index = processor._parse_metadata_rows(0)
    assert metadata == {"A": "1", "B": "2"}
    assert index == 2


def test_identify_header_found(mock_file_record_local):
    processor = CSVProcessor(mock_file_record_local)
    processor.rows = [["Header1", "Header2"], ["data1", "data2"]]
    header, next_idx = processor._identify_header(0)
    assert header == ["Header1", "Header2"]
    assert next_idx == 1


def test_identify_header_generate(mock_file_record_local):
    processor = CSVProcessor(mock_file_record_local)
    processor.rows = [["1", "2"], ["a", "b"]]
    header, next_idx = processor._identify_header(0)
    assert header == ["col_1", "col_2"]
    assert next_idx == 1


def test_collect_data_block(mock_file_record_local):
    processor = CSVProcessor(mock_file_record_local)
    processor.rows = [
        ["val1", "val2"],
        ["val3", "val4"],
        [f"meta{METADATA_SEPARATOR}1"]
    ]
    header = ["col1", "col2"]
    items, next_idx = processor._collect_data_block(0, header)
    assert items == [{"col1": "val1", "col2": "val2"}, {"col1": "val3", "col2": "val4"}]
    assert next_idx == 2


def test_parse_file_to_json_full_flow(mock_file_record_local):
    processor = CSVProcessor(mock_file_record_local)
    processor.rows = [
        [f"meta{METADATA_SEPARATOR}value"],
        ["header1", "header2"],
        ["v1", "v2"],
        ["v3", "v4"]
    ]

    result = processor.parse_file_to_json()
    assert isinstance(result, PODataParsed)

    data = result.model_dump()  # convert Pydantic model to dict

    assert data["document_type"] == DocumentType.ORDER
    assert len(data["items"]) == 2
    assert data["items"][0] == {"header1": "v1", "header2": "v2"}
    assert data["metadata"] == {"meta": "value"}
    assert data["step_status"] == StatusEnum.SUCCESS
