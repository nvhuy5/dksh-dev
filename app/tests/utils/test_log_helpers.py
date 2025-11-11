import logging
import dataclasses
import pytest
from pydantic import BaseModel

from fastapi_celery.models.tracking_models import LogType, ServiceLog
import fastapi_celery.utils.log_helpers as log_helpers



# === Dummy classes for normalization tests ===
@dataclasses.dataclass
class DummyData:
    a: int
    b: str


class DummyModel(BaseModel):
    x: int
    y: str


# === logging_config ===
def test_logging_config_creates_logger(mocker):
    """Should call logging.config.dictConfig with correct structure."""
    mock_dict_config = mocker.patch("logging.config.dictConfig")
    log_helpers.logging_config("my_logger")
    mock_dict_config.assert_called_once()
    config_arg = mock_dict_config.call_args[0][0]
    assert "version" in config_arg
    assert "loggers" in config_arg


# === validate_log_fields ===
def test_validate_log_fields_valid_enum_instances():
    """Should accept valid Enum members and return their string values."""
    adapter = log_helpers.ValidatingLoggerAdapter(logging.getLogger("x"), {})
    extra = {"service": ServiceLog.FILE_STORAGE, "log_type": LogType.ERROR}
    result = adapter.validate_log_fields(extra)
    assert result["service"] == "file-storage"
    assert result["log_type"] == "error"


def test_validate_log_fields_string_enum_values():
    """Should convert string values to enum automatically."""
    adapter = log_helpers.ValidatingLoggerAdapter(logging.getLogger("x"), {})
    extra = {"service": "file-storage", "log_type": "error"}
    validated = adapter.validate_log_fields(extra.copy())
    assert validated["service"] == "file-storage"
    assert validated["log_type"] == "error"


def test_validate_log_fields_invalid_service_raises():
    """Should raise ValueError when invalid service name is used."""
    adapter = log_helpers.ValidatingLoggerAdapter(logging.getLogger("x"), {})
    with pytest.raises(ValueError):
        adapter.validate_log_fields({"service": "INVALID"})


def test_validate_log_fields_invalid_logtype_raises():
    """Should raise ValueError when invalid log_type name is used."""
    adapter = log_helpers.ValidatingLoggerAdapter(logging.getLogger("x"), {})
    with pytest.raises(ValueError):
        adapter.validate_log_fields({"log_type": "UNKNOWN"})


# === normalize_extra ===
def test_normalize_extra_with_various_types():
    """Should handle dict, dataclass, pydantic model, and Enum properly."""
    adapter = log_helpers.ValidatingLoggerAdapter(logging.getLogger("x"), {})
    model = DummyModel(x=1, y="z")
    dc = DummyData(a=2, b="b")
    enum = ServiceLog.FILE_STORAGE

    extra = {
        "dict_val": {"x": 1},
        "dataclass_val": dc,
        "model_val": model,
        "enum_val": enum,
        "primitive": 42,
        "unknown": object(),
    }

    result = adapter.normalize_extra(extra)
    assert result["dict_val"] == {"x": 1}
    assert isinstance(result["dataclass_val"], dict)
    assert isinstance(result["model_val"], dict)
    assert result["enum_val"] == "file-storage"
    assert result["primitive"] == 42
    assert isinstance(result["unknown"], str)


def test_normalize_extra_handles_exception():
    """Should replace unserializable object with fallback string."""

    adapter = log_helpers.ValidatingLoggerAdapter(logging.getLogger("x"), {})

    # Use a real class whose __str__ raises exception
    class BadObject:
        def __str__(self):
            raise Exception("boom")

    bad_obj = BadObject()
    result = adapter.normalize_extra({"x": bad_obj})

    # The fallback should be applied
    assert "<Unserializable" in result["x"]



# === process ===
def test_process_validates_and_normalizes():
    """Should validate, normalize and add environment key."""
    adapter = log_helpers.ValidatingLoggerAdapter(logging.getLogger("x"), {})
    extra = {"service": "file-storage", "log_type": "error", "data": {"k": 1}}
    msg, kwargs = adapter.process("hello", {"extra": extra})
    assert msg == "hello"
    assert "environment" in kwargs["extra"]
    assert kwargs["extra"]["service"] == "file-storage"


def test_process_handles_invalid_extra(mocker):
    """Should catch validation error and still return safe kwargs."""

    adapter = log_helpers.ValidatingLoggerAdapter(logging.getLogger("x"), {})

    # Patch validate_log_fields to raise ValueError
    mocker.patch.object(adapter, "validate_log_fields", side_effect=ValueError("fail"))

    msg, kwargs = adapter.process("msg", {"extra": {"bad": "data"}})

    # The message should be unchanged
    assert msg == "msg"

    # Even if validation fails, the original data remains
    assert "bad" in kwargs["extra"]

    # Always include environment key if you updated process to do so
    # Uncomment this if you applied the ENV fallback in process()
    # assert "environment" in kwargs["extra"]


# === get_logger ===
def test_get_logger_returns_valid_adapter(mocker):
    """Should configure logger and return ValidatingLoggerAdapter."""
    mock_conf = mocker.patch("fastapi_celery.utils.log_helpers.logging_config")
    logger = log_helpers.get_logger("abc")
    mock_conf.assert_called_once_with("abc")
    assert isinstance(logger, log_helpers.ValidatingLoggerAdapter)
