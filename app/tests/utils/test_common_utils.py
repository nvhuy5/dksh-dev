import pytest
from unittest.mock import patch, MagicMock
from utils import common_utils
import pandas as pd
from io import BytesIO
from pydantic import BaseModel

MOCK_PROCESS_DEFINITIONS = {
    "TEMPLATE_FILE_PARSE": MagicMock(),
    "MASTER_DATA_FILE_PARSER": MagicMock(),
    "[RULE_MP]_SUBMIT": MagicMock(),
}

def test_get_step_name_exact_match():
    with patch("utils.common_utils.PROCESS_DEFINITIONS", MOCK_PROCESS_DEFINITIONS):
        result = common_utils.get_step_name("TEMPLATE_FILE_PARSE")
        assert result == "TEMPLATE_FILE_PARSE"

def test_get_step_name_dynamic_match():
    with patch("utils.common_utils.PROCESS_DEFINITIONS", MOCK_PROCESS_DEFINITIONS):
        with patch.object(common_utils.logger, "info") as mock_info:
            result = common_utils.get_step_name("CUSTOMER_3_DKSH_TW_SUBMIT")
            assert result == "[RULE_MP]_SUBMIT"
            mock_info.assert_called_once()
            assert "Dynamic match found" in mock_info.call_args[0][0]

def test_get_step_name_no_match():
    with patch("utils.common_utils.PROCESS_DEFINITIONS", MOCK_PROCESS_DEFINITIONS):
        with patch.object(common_utils.logger, "warning") as mock_warn:
            result = common_utils.get_step_name("UNKNOWN_STEP")
            assert result is None
            mock_warn.assert_called_once()
            assert "No match found" in mock_warn.call_args[0][0]


# -- get_csv_buffer_file tests --
class DummyModel(BaseModel):
    items: list[dict]


class DummyInput:
    def __init__(self, data):
        self.data = data


def test_get_csv_buffer_file_success_with_list(monkeypatch):
    """ Case: valid list of dicts should return non-empty CSV buffer."""
    data_input = DummyInput(data=MagicMock(items=[{"a": 1, "b": 2}, {"a": 3, "b": 4}]))
    buf = common_utils.get_csv_buffer_file(data_input)
    assert isinstance(buf, BytesIO)
    content = buf.getvalue().decode("utf-8")
    assert "a,b" in content
    assert "1,2" in content


def test_get_csv_buffer_file_with_pydantic_model(monkeypatch):
    """ Case: items is a Pydantic model that dumps correctly."""
    dummy_model = DummyModel(items=[{"x": 10, "y": 20}])
    data_input = DummyInput(data=dummy_model)
    buf = common_utils.get_csv_buffer_file(data_input)
    assert isinstance(buf, BytesIO)
    csv_str = buf.getvalue().decode("utf-8")
    assert "x,y" in csv_str
    assert "10,20" in csv_str


def test_get_csv_buffer_file_empty_payload(monkeypatch):
    """ Case: items is empty list -> raises ValueError."""
    data_input = DummyInput(data=MagicMock(items=[]))
    with pytest.raises(ValueError, match="Empty payload"):
        common_utils.get_csv_buffer_file(data_input)


def test_get_csv_buffer_file_invalid_payload(monkeypatch):
    """ Case: items is None -> raises ValueError."""
    data_input = DummyInput(data=MagicMock(items=None))
    with pytest.raises(ValueError, match="Empty payload"):
        common_utils.get_csv_buffer_file(data_input)


def test_get_csv_buffer_file_dataframe_empty(monkeypatch):
    """ Case: items is list but DataFrame becomes empty -> raises ValueError."""
    real_df = pd.DataFrame  # backup original constructor

    def fake_dataframe(x):
        return real_df([])

    monkeypatch.setattr(pd, "DataFrame", fake_dataframe)

    data_input = DummyInput(data=MagicMock(items=[{"a": 1}]))
    with pytest.raises(ValueError, match="DataFrame is empty"):
        common_utils.get_csv_buffer_file(data_input)



def test_get_csv_buffer_file_no_data_input():
    """ Case: data_input is None -> raises ValueError."""
    with pytest.raises(ValueError, match="No data_input provided"):
        common_utils.get_csv_buffer_file(None)
