import pytest
from unittest.mock import MagicMock
import fastapi_celery.processors.helpers.xml_helper as xml_helper


@pytest.fixture(autouse=True)
def mock_logger(monkeypatch):
    """Automatically mock the logger for all tests."""
    mock_log = MagicMock()
    monkeypatch.setattr(xml_helper, "logger", mock_log)
    return mock_log


def test_build_processor_setting_xml_basic(mock_logger):
    args = [
        {"name": "Param1", "value": "Value1"},
        {"name": "Param2", "value": "Value2"},
    ]

    result = xml_helper.build_processor_setting_xml(args)

    expected = (
        "<PROCESSORSETTINGXML>\n"
        "  <Param1>Value1</Param1>\n"
        "  <Param2>Value2</Param2>\n"
        "</PROCESSORSETTINGXML>"
    )
    assert result == expected
    mock_logger.info.assert_called_once()
    mock_logger.warning.assert_not_called()


def test_build_processor_setting_xml_empty_list(mock_logger):
    result = xml_helper.build_processor_setting_xml([])
    assert result is None
    mock_logger.warning.assert_called_once_with(
        "[build_processor_setting_xml] No processor arguments provided."
    )
    mock_logger.info.assert_not_called()


def test_build_processor_setting_xml_missing_name(mock_logger):
    args = [{"value": "Something"}]

    result = xml_helper.build_processor_setting_xml(args)

    assert result.startswith("<PROCESSORSETTINGXML>")
    mock_logger.warning.assert_any_call(
        "[build_processor_setting_xml] Missing processorArgumentName in {'value': 'Something'}"
    )
    mock_logger.info.assert_called_once()


def test_build_processor_setting_xml_escape_special_chars(mock_logger):
    args = [
        {"name": "Note", "value": "5 < 10 & 20 > 15 ' \" "},
    ]
    result = xml_helper.build_processor_setting_xml(args)

    assert "&lt;" in result
    assert "&gt;" in result
    assert "&amp;" in result
    assert "&quot;" in result
    assert "&apos;" in result
    mock_logger.info.assert_called_once()


def test_build_processor_setting_xml_invalid_input_type(mock_logger):
    """Handles case when input is None or invalid type."""
    result = xml_helper.build_processor_setting_xml(None)
    assert result is None
    mock_logger.warning.assert_called_once_with(
        "[build_processor_setting_xml] No processor arguments provided."
    )
