import unittest
from unittest.mock import MagicMock

from fastapi_celery.models.class_models import StatusEnum, StepOutput
from fastapi_celery.processors.workflow_processors.master_sync_data import master_sync_data


class DummySelf:
    """Dummy class to simulate the actual class where master_sync_data() is defined."""
    pass


class TestMasterSyncData(unittest.TestCase):
    """Unit tests for the master_sync_data() function."""

    def test_master_sync_data_success(self):
        """Should return a StepOutput with SUCCESS status and the same input data."""
        dummy_self = DummySelf()
        mock_data = {"id": 1, "name": "test record"}

        data_input = StepOutput(
            data=mock_data,
            sub_data={"old": "info"},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

        response_api = {"result": "ok"}

        result = master_sync_data(dummy_self, data_input, response_api)

        # Assertion
        assert type(result).__name__ == "StepOutput"
        assert result.data == mock_data
        assert result.step_status == StatusEnum.SUCCESS
        assert result.step_failure_message is None
        assert result.sub_data == {}

    def test_master_sync_data_with_extra_args(self):
        """Should ignore *args and **kwargs gracefully."""
        dummy_self = DummySelf()
        data_input = StepOutput(
            data={"test": True},
            step_status=StatusEnum.SUCCESS,
        )
        response_api = {"meta": "ok"}

        result = master_sync_data(dummy_self, data_input, response_api, "extra_arg", key="extra_value")

        assert type(result).__name__ == "StepOutput"
        assert result.step_status == StatusEnum.SUCCESS
        assert result.step_failure_message is None
