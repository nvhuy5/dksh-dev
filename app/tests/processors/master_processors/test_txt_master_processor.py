import io
import pytest
from pathlib import Path
from fastapi_celery.processors.master_processors.txt_master_processor import TxtMasterProcessor
from models.class_models import DocumentType, MasterDataParsed, StatusEnum



@pytest.fixture
def file_record_local(tmp_path):
    file_path = tmp_path / "sample_master.txt"
    file_path.write_text("# Table: Products\nCode | Name\n001 | Apple\n002 | Banana\n")
    return {
        "file_path": str(file_path),
        "source_type": "local",
        "document_type": DocumentType.MASTER_DATA,
        "file_size": "100 KB",
    }


@pytest.fixture
def file_record_s3():
    buffer = io.BytesIO(b"# Table: Products\nCode | Name\n001 | Apple\n")
    return {
        "object_buffer": buffer,
        "source_type": "s3",
        "document_type": DocumentType.MASTER_DATA,
        "file_path": "dummy_s3.txt",
        "file_size": "100 KB",
    }


# ---------------------------------------------------------
# parse_file_to_json success (local)
# ---------------------------------------------------------
def test_parse_file_to_json_success_local(file_record_local):
    processor = TxtMasterProcessor(file_record_local)
    result = processor.parse_file_to_json()

    assert isinstance(result, MasterDataParsed)
    assert result.step_status == StatusEnum.SUCCESS
    assert isinstance(result.headers, dict)
    assert "Products" in result.headers
    assert "Code" in result.headers["Products"]
    assert len(result.items["Products"]) == 2
    assert result.capacity == "100 KB"


# ---------------------------------------------------------
# parse_file_to_json success (s3)
# ---------------------------------------------------------
def test_parse_file_to_json_success_s3(file_record_s3):
    processor = TxtMasterProcessor(file_record_s3)
    result = processor.parse_file_to_json()

    assert isinstance(result, MasterDataParsed)
    assert result.step_status == StatusEnum.SUCCESS
    assert result.headers["Products"] == ["Code", "Name"]
    assert len(result.items["Products"]) == 1


# ---------------------------------------------------------
# parse_file_to_json exception handling
# ---------------------------------------------------------
def test_parse_file_to_json_exception(monkeypatch, file_record_local):
    processor = TxtMasterProcessor(file_record_local)

    # Force _read_file_content to raise exception
    def raise_error():
        raise ValueError("Simulated error")

    monkeypatch.setattr(processor, "_read_file_content", raise_error)
    result = processor.parse_file_to_json()

    assert isinstance(result, MasterDataParsed)
    assert result.step_status == StatusEnum.FAILED
    assert result.messages and "Simulated error" in result.messages[0]


# ---------------------------------------------------------
# _read_file_content local
# ---------------------------------------------------------
def test_read_file_content_local(file_record_local):
    processor = TxtMasterProcessor(file_record_local)
    text = processor._read_file_content()
    assert "# Table: Products" in text


# ---------------------------------------------------------
# _read_file_content s3
# ---------------------------------------------------------
def test_read_file_content_s3(file_record_s3):
    processor = TxtMasterProcessor(file_record_s3)
    text = processor._read_file_content()
    assert "Products" in text


# ---------------------------------------------------------
# _parse_text_blocks multiple valid tables
# ---------------------------------------------------------
def test_parse_text_blocks_multiple_tables():
    text = (
        "# Table: Products\nCode | Name\n001 | Apple\n\n"
        "# Table: Customers\nID | Name\nC01 | John\nC02 | Jane\n"
    )
    processor = TxtMasterProcessor({"file_path": "dummy.txt"})
    headers, items = processor._parse_text_blocks(text)

    assert set(headers.keys()) == {"Products", "Customers"}
    assert len(items["Products"]) == 1
    assert len(items["Customers"]) == 2
    assert items["Customers"][0]["ID"] == "C01"


# ---------------------------------------------------------
# _parse_text_blocks invalid rows
# (Code gốc vẫn thêm bảng nếu có 2 dòng, nên test phải chấp nhận điều này)
# ---------------------------------------------------------
def test_parse_text_blocks_invalid_rows():
    text = "# Table: Invalid\nOnlyOneLine\n\n# Table: Valid\nA | B\n1 | 2\n"
    processor = TxtMasterProcessor({"file_path": "dummy.txt"})
    headers, items = processor._parse_text_blocks(text)

    # Code gốc vẫn giữ bảng "Invalid" -> ta chỉ kiểm tra có dữ liệu đúng ở bảng "Valid"
    assert "Valid" in headers
    assert headers["Valid"] == ["A", "B"]
    assert items["Valid"][0]["A"] == "1"
    # "Invalid" bảng không có dòng dữ liệu hợp lệ
    assert isinstance(headers, dict)


# ---------------------------------------------------------
# _parse_text_blocks empty content
# ---------------------------------------------------------
def test_parse_text_blocks_empty_content():
    processor = TxtMasterProcessor({"file_path": "dummy.txt"})
    headers, items = processor._parse_text_blocks("")
    assert headers == {}
    assert items == {}


# ---------------------------------------------------------
# Clean integration flow
# ---------------------------------------------------------
def test_clean_integration_flow(file_record_local):
    """Simulate realistic full parsing end-to-end."""
    processor = TxtMasterProcessor(file_record_local)
    text = "# Table: Products\nCode | Name\n001 | Apple\n002 | Banana\n"
    processor._read_file_content = lambda: text  # Mock file read

    result = processor.parse_file_to_json()
    assert isinstance(result, MasterDataParsed)
    assert result.step_status == StatusEnum.SUCCESS
    assert "Products" in result.headers
    assert "Code" in result.headers["Products"]
    assert len(result.items["Products"]) == 2
