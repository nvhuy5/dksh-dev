import io
import pytest
from unittest.mock import patch, MagicMock

import os
import tempfile
from fastapi_celery.processors.helpers import txt_helper


@pytest.fixture
def dummy_file_record_local():
    temp_dir = tempfile.gettempdir()
    dummy_path = os.path.join(temp_dir, "file.csv")
    with open(dummy_path, "w", encoding="utf-8") as f:
        f.write("col1,col2\nval1,val2\n")
        
    return {
        "file_path": dummy_path,
        "source_type": "local",
        "document_type": "order",
        "file_size": "20KB",
    }


@pytest.fixture
def dummy_file_record_buffer():
    buffer = io.BytesIO(b"rowA\nrowB\nrowC")
    return {
        "file_path": "buffer_file.txt",
        "source_type": "s3",
        "object_buffer": buffer,
        "document_type": "invoice",
        "file_size": "10KB",
    }


def test_init_sets_attributes(dummy_file_record_local):
    helper = txt_helper.TxtHelper(dummy_file_record_local, encoding="latin-1")
    assert helper.file_record == dummy_file_record_local
    assert helper.encoding == "latin-1"


def test_extract_text_local_file(monkeypatch, dummy_file_record_local):
    mock_open = patch("builtins.open", create=True).start()
    mock_file = MagicMock()
    mock_file.read.return_value = "abc\nxyz"
    mock_open.return_value.__enter__.return_value = mock_file

    helper = txt_helper.TxtHelper(dummy_file_record_local)
    result = helper.extract_text()

    mock_file.read.assert_called_once()
    assert "abc" in result

    patch.stopall()


def test_extract_text_from_buffer(dummy_file_record_buffer):
    helper = txt_helper.TxtHelper(dummy_file_record_buffer)
    result = helper.extract_text()
    assert "rowA" in result
    assert isinstance(result, str)


@patch("fastapi_celery.processors.helpers.txt_helper.PODataParsed")
@patch("fastapi_celery.processors.helpers.txt_helper.StatusEnum")
def test_parse_file_to_json_local(mock_status_enum, mock_podata_parsed, dummy_file_record_local):
    mock_status_enum.SUCCESS = "SUCCESS"

    # Mock return object of PODataParsed
    mock_result = MagicMock()
    mock_podata_parsed.return_value = mock_result

    # Patch extract_text to return fixed text
    helper = txt_helper.TxtHelper(dummy_file_record_local)
    with patch.object(helper, "extract_text", return_value="item1\nitem2\nitem3"):
        def fake_parse_func(lines):
            return [{"line": l} for l in lines]

        result = helper.parse_file_to_json(fake_parse_func)

    mock_podata_parsed.assert_called_once()
    called_kwargs = mock_podata_parsed.call_args.kwargs

    assert called_kwargs["document_type"] == "order"
    assert called_kwargs["po_number"] == "3"  # 3 lines parsed
    assert isinstance(result, MagicMock)


@patch("fastapi_celery.processors.helpers.txt_helper.PODataParsed")
@patch("fastapi_celery.processors.helpers.txt_helper.StatusEnum")
def test_parse_file_to_json_from_buffer(mock_status_enum, mock_podata_parsed, dummy_file_record_buffer):
    mock_status_enum.SUCCESS = "SUCCESS"
    mock_result = MagicMock()
    mock_podata_parsed.return_value = mock_result

    helper = txt_helper.TxtHelper(dummy_file_record_buffer)
    with patch.object(helper, "extract_text", return_value="A\nB\nC\nD"):
        def mock_parser(lines):
            return [l.lower() for l in lines]

        result = helper.parse_file_to_json(mock_parser)

    called_args = mock_podata_parsed.call_args.kwargs
    assert called_args["po_number"] == "4"
    assert called_args["items"] == ["a", "b", "c", "d"]
    assert result == mock_result


def test_extract_text_buffer_empty(monkeypatch):
    buffer = io.BytesIO(b"")
    record = {
        "file_path": "x.txt",
        "source_type": "s3",
        "object_buffer": buffer,
        "document_type": "data",
        "file_size": "0KB",
    }

    helper = txt_helper.TxtHelper(record)
    result = helper.extract_text()
    assert result == ""


def test_parse_file_to_json_with_empty_parse_func(monkeypatch, dummy_file_record_local):
    """Ensure parse_func returning [] still handled cleanly."""
    with patch.object(txt_helper.TxtHelper, "extract_text", return_value="a\nb\nc"):
        mock_po_data = MagicMock()
        with patch("fastapi_celery.processors.helpers.txt_helper.PODataParsed", return_value=mock_po_data):
            mock_status = MagicMock()
            monkeypatch.setattr(txt_helper, "StatusEnum", mock_status)
            helper = txt_helper.TxtHelper(dummy_file_record_local)
            result = helper.parse_file_to_json(lambda lines: [])

    assert result == mock_po_data
