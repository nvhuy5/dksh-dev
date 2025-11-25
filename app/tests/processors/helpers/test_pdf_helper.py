import pytest
from unittest.mock import MagicMock
from fastapi_celery.processors.helpers import pdf_helper
from fastapi_celery.models.class_models import StatusEnum


def test_build_success_response_basic():
    file_path = "dummy.pdf"
    document_type = "order"
    po_number = "PO123"
    items = [{"item": "A"}]
    metadata = {"meta": "ok"}
    file_size = "100KB"

    result = pdf_helper.build_success_response(
        file_path=file_path,
        document_type=document_type,
        po_number=po_number,
        items=items,
        metadata=metadata,
        file_size=file_size,
    )

    assert type(result).__name__ == "PODataParsed"
    assert result.file_path == file_path
    assert result.document_type == document_type
    assert result.po_number == po_number
    assert result.items == items
    assert result.metadata == metadata
    assert result.file_size  == file_size 
    assert result.step_status == StatusEnum.SUCCESS
    assert result.messages is None


def test_build_failed_response_default(monkeypatch):
    mock_logger = MagicMock()
    monkeypatch.setattr(pdf_helper, "logger", mock_logger)

    file_path = "failed.pdf"
    exc = ValueError("broken PDF")

    result = pdf_helper.build_failed_response(file_path, exc=exc)

    assert type(result).__name__ == "PODataParsed"
    assert result.file_path == file_path
    assert result.document_type == "order"
    assert result.items == []
    assert result.metadata == {}
    assert result.file_size == ""
    assert result.step_status == StatusEnum.FAILED
    assert isinstance(result.messages, list)
    assert result.messages  # not empty
    mock_logger.error.assert_called_once()


def test_build_failed_response_master_data_type(monkeypatch):
    mock_logger = MagicMock()
    monkeypatch.setattr(pdf_helper, "logger", mock_logger)

    file_path = "sample.pdf"
    exc = RuntimeError("failure")

    result = pdf_helper.build_failed_response(
        file_path=file_path,
        document_type="master_data",
        file_size="5MB",
        exc=exc,
    )

    assert type(result).__name__ == "PODataParsed"
    assert result.file_path == file_path
    assert result.document_type == "master_data"
    assert result.file_size == "5MB"
    assert result.step_status == StatusEnum.FAILED
    mock_logger.error.assert_called_once()


def test_build_failed_response_invalid_document_type(monkeypatch):
    mock_logger = MagicMock()
    monkeypatch.setattr(pdf_helper, "logger", mock_logger)

    file_path = "weird.pdf"
    exc = Exception("invalid type")

    result = pdf_helper.build_failed_response(
        file_path=file_path,
        document_type="unsupported_type",
        exc=exc,
    )

    assert type(result).__name__ == "PODataParsed"
    assert result.file_path == file_path
    assert result.document_type == "order"
    assert result.step_status == StatusEnum.FAILED
    mock_logger.error.assert_called_once()


def test_build_failed_response_no_exception(monkeypatch):
    mock_logger = MagicMock()
    monkeypatch.setattr(pdf_helper, "logger", mock_logger)

    file_path = "noexc.pdf"

    result = pdf_helper.build_failed_response(file_path)

    assert type(result).__name__ == "PODataParsed"
    assert result.file_path == file_path
    assert result.step_status == StatusEnum.FAILED
    assert isinstance(result.messages, list)
    # Should contain either traceback info or "NoneType"
    assert any(
        "Traceback" in msg or "NoneType" in msg for msg in result.messages
    )
    mock_logger.error.assert_called_once()
