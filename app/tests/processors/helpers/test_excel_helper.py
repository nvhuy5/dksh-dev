import pytest
import pandas as pd
from io import BytesIO
from fastapi_celery.processors.helpers.excel_helper import ExcelHelper

# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_read_excel(monkeypatch):
    """Mock pandas.read_excel to avoid real file operations."""
    df = pd.DataFrame({
        0: ["Header(Ver：1.0)", "Owner：Alice", "DocLink：https://example.com"],
        1: ["Note：Check", "", ""],
    })
    monkeypatch.setattr(pd, "read_excel", lambda *a, **kw: {"Sheet1": df})
    return df


@pytest.fixture
def local_file_record():
    """Mock local Excel file info."""
    return {
        "source_type": "local",
        "file_path": "/fake/path.xlsx",
        "file_extension": ".xlsx",
    }


@pytest.fixture
def s3_file_record():
    """Mock S3 Excel file info."""
    return {
        "source_type": "s3",
        "object_buffer": BytesIO(b"fake binary"),
        "file_extension": ".xlsx",
    }


# ============================================================
# Tests: read_rows
# ============================================================

def test_read_rows_local(monkeypatch, mock_read_excel, local_file_record):
    helper = ExcelHelper(local_file_record)
    rows = helper.read_rows()
    assert isinstance(rows, list)
    assert len(rows) > 0
    assert all(isinstance(r, list) for r in rows)


def test_read_rows_s3(monkeypatch, mock_read_excel, s3_file_record):
    called = {}

    def fake_read_excel(file_input, **kwargs):
        called["used"] = True
        return {"Sheet1": pd.DataFrame({0: ["A"], 1: ["B"]})}

    monkeypatch.setattr(pd, "read_excel", fake_read_excel)

    helper = ExcelHelper(s3_file_record)
    rows = helper.read_rows()
    assert called["used"]
    assert isinstance(rows, list)
    assert len(rows) > 0


def test_read_rows_xls(monkeypatch, local_file_record):
    """Ensure .xls uses xlrd engine."""
    called = {}

    def fake_read_excel(*args, **kwargs):
        called["engine"] = kwargs.get("engine")
        return {"Sheet1": pd.DataFrame({0: ["X"], 1: ["Y"]})}

    monkeypatch.setattr(pd, "read_excel", fake_read_excel)

    local_file_record["file_extension"] = ".xls"
    helper = ExcelHelper(local_file_record)
    helper.read_rows()
    assert called["engine"] == "xlrd"


# ============================================================
# Tests: metadata extraction
# ============================================================

@pytest.fixture
def excel_helper(local_file_record, mock_read_excel):
    return ExcelHelper(local_file_record)


def test_has_inner_metadata(excel_helper):
    assert excel_helper._has_inner_metadata("Title(Ver：1.0)")
    assert not excel_helper._has_inner_metadata("Title no version")


def test_extract_inner_metadata(excel_helper):
    metadata = {}
    excel_helper._extract_inner_metadata("Header(Version：2.1)", metadata)
    assert metadata["header"] == "Header(Version：2.1)"
    assert metadata["Version"] == "2.1"


def test_extract_inner_metadata_no_match(excel_helper):
    metadata = {}
    excel_helper._extract_inner_metadata("Invalid text", metadata)
    assert metadata == {}


def test_is_url(excel_helper):
    assert excel_helper._is_url("https://example.com")
    assert excel_helper._is_url("http://abc.com")
    assert not excel_helper._is_url("ftp://example.com")
    assert not excel_helper._is_url(123)


def test_extract_standard_metadata(excel_helper):
    metadata = {}
    excel_helper._extract_standard_metadata("Key：Value", 0, ["Key：Value"], metadata)
    assert metadata["Key"] == "Value"

    metadata2 = {}
    excel_helper._extract_standard_metadata("Key：", 0, ["Key：", "NextVal"], metadata2)
    assert metadata2["Key"] == "NextVal"

    metadata3 = {}
    excel_helper._extract_standard_metadata("NoSepCell", 0, ["NoSepCell"], metadata3)
    assert metadata3 == {}


def test_extract_metadata_full(excel_helper):
    row = [
        "Header(Ver：1.0)",
        "Owner：Alice",
        "DocLink：https://example.com",
        "Note：Check",
    ]
    metadata = excel_helper.extract_metadata(row)
    assert metadata["header"] == "Header(Ver：1.0)"
    assert metadata["Ver"] == "1.0"
    assert metadata["Owner"] == "Alice"
    assert metadata["DocLink"] == "https://example.com"
    assert metadata["Note"] == "Check"


def test_extract_metadata_empty(excel_helper):
    result = excel_helper.extract_metadata(["", "   "])
    assert result == {}


# ============================================================
# Tests: parse_file_to_json
# ============================================================

def test_parse_file_to_json_returns_none(excel_helper):
    """Since the method is not implemented, expect None."""
    assert excel_helper.parse_file_to_json() is None
