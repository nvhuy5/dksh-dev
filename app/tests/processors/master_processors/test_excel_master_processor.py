import pytest
from pathlib import Path
from models.class_models import MasterDataParsed, StatusEnum
from fastapi_celery.processors.master_processors.excel_master_processor import ExcelMasterProcessor


# === Fixtures ===

@pytest.fixture
def fake_file_record():
    return {
        "document_type": "master_data",
        "file_path": "tests/samples/0808fake_xlsx.xlsx",
        "file_size": "100KB",
        "source_type": "local",
        "object_buffer": None,
    }


@pytest.fixture
def processor(fake_file_record):
    """ExcelMasterProcessor using fake file_record."""
    processor = ExcelMasterProcessor(file_record=fake_file_record)
    processor.extract_metadata = lambda row: {}
    return processor


def _ensure_parsed_object(result):
    if isinstance(result, MasterDataParsed):
        return result
    elif isinstance(result, dict):
        return MasterDataParsed(**result)
    else:
        raise AssertionError(f"Unexpected return type: {type(result)}")


# === Tests ===

def test_parse_file_to_json_success(processor):
    processor.rows = [
        ["Customer：DKSH"],
        ["Code", "Name", "Age"],
        ["001", "John", "30"],
        ["002", "Anna", "25"],
    ]

    def mock_extract_metadata(row):
        if "Customer" in row[0]:
            return {"Customer": "DKSH"}
        return {}

    processor.extract_metadata = mock_extract_metadata

    result = _ensure_parsed_object(processor.parse_file_to_json())

    assert isinstance(result, MasterDataParsed)
    assert result.step_status == StatusEnum.SUCCESS
    assert result.headers == ["Code", "Name", "Age"]
    assert len(result.items) == 2
    assert result.items[0]["Code"] == "001"
    assert result.items[1]["Name"] == "Anna"


def test_parse_file_to_json_metadata_only(processor):
    processor.rows = [
        ["DocType：Master Data"],
        ["Version：1.0"],
    ]

    def mock_extract_metadata(row):
        if "DocType" in row[0]:
            return {"DocType": "Master Data"}
        if "Version" in row[0]:
            return {"Version": "1.0"}
        return {}

    processor.extract_metadata = mock_extract_metadata

    result = _ensure_parsed_object(processor.parse_file_to_json())
    assert result.step_status == StatusEnum.SUCCESS
    assert result.headers == []
    assert result.items == []


def test_parse_file_to_json_exception(processor, monkeypatch):
    processor.rows = [["Bad", "Row"]]

    def raise_error(*args, **kwargs):
        raise Exception("mock error")

    # Monkeypatch method extract_metadata to force exception
    monkeypatch.setattr(processor, "extract_metadata", raise_error)

    result = _ensure_parsed_object(processor.parse_file_to_json())
    assert result.step_status == StatusEnum.FAILED
    assert isinstance(result.messages, list)
    assert any("mock error" in msg for msg in result.messages)


def test_extract_table_block_with_metadata(processor):
    processor.rows = [
        ["Code", "Name"],
        ["001", "John"],
        ["002", "Anna"],
        ["Version：1.0"]
    ]

    def mock_extract_metadata(row):
        if "Version" in row[0]:
            return {"Version": "1.0"}
        return {}

    processor.extract_metadata = mock_extract_metadata

    headers = ["Code", "Name"]
    table_block, next_index, metadata = processor._extract_table_block(1, headers)

    assert len(table_block) == 2
    assert next_index > 1
    assert metadata == {"Version": "1.0"}


def test_clean_row_strip(processor):
    row = ["  Code ", " Name  ", " Age "]
    cleaned = processor._clean_row(row)
    assert cleaned == ["Code", "Name", "Age"]


@pytest.mark.parametrize("sample_file", [
    "0808fake_xlsx.xlsx",
    "0808三友WX.xls",
])
def test_real_excel_files_exist(sample_file):
    path = Path("tests/samples") / sample_file
    assert path.exists(), f"Sample file missing: {path}"
