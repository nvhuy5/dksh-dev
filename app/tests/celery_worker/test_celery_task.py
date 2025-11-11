import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi_celery.celery_worker.step_handler import execute_step
from fastapi_celery.models.class_models import ContextData, WorkflowStep, StepDetail, StepOutput, StatusEnum
from fastapi_celery.models.tracking_models import TrackingModel

@pytest.mark.asyncio
@pytest.mark.parametrize("step_status", [StatusEnum.SUCCESS, StatusEnum.FAILED])
async def test_execute_step_full_coverage(step_status):
    # --- Prepare TrackingModel ---
    tracking_model = TrackingModel(
        request_id="REQ-TEST",
        project_name="DKSH_TW",
        file_path="dummy_path",
        source_name="LOCAL",
        sap_masterdata=False,
    )

    # --- Mock FileExtensionProcessor ---
    with patch("fastapi_celery.utils.ext_extraction.FileExtensionProcessor") as MockProcessor:
        processor_instance = MockProcessor.return_value
        processor_instance.file_record = {"file_name": "dummy.txt"}
        processor_instance.tracking_model = tracking_model

        # --- Mock BEConnector ---
        with patch("fastapi_celery.celery_worker.step_handler.BEConnector", autospec=True) as MockConnector:
            mock_connector_instance = MockConnector.return_value
            mock_connector_instance.get = AsyncMock(return_value={"data": 123})
            mock_connector_instance.post = AsyncMock(return_value={"data": 123})

            # --- Mock PROCESS_DEFINITIONS ---
            with patch("fastapi_celery.celery_worker.step_handler.PROCESS_DEFINITIONS", {
                "FILE_PARSE": MagicMock(function_name="dummy_func", data_input=None, data_output=None, kwargs={})
            }):

                # --- Add dummy method to processor ---
                async def dummy_func(data_input, response, **kwargs):
                    return StepOutput(data={"ok": True}, step_status=step_status)

                processor_instance.dummy_func = dummy_func

                # --- Prepare WorkflowStep ---
                workflow_step = WorkflowStep(
                    workflowStepId="STEP-1",
                    stepName="FILE_PARSE",
                    stepOrder=0  # stepOrder bắt đầu từ 0
                )

                # --- Prepare full_sorted_steps ---
                full_sorted_steps = [workflow_step]

                # --- Prepare ContextData with enough StepDetail slots ---
                context_data = ContextData(
                    request_id=tracking_model.request_id,
                    step_detail=[StepDetail() for _ in range(len(full_sorted_steps))],
                    processing_steps={}
                )

                # --- Patch get_step_name to return the mocked step ---
                with patch("fastapi_celery.celery_worker.step_handler.get_step_name", return_value="FILE_PARSE"):
                    # --- Execute step ---
                    result = await execute_step(processor_instance, context_data, full_sorted_steps, workflow_step)

                    # --- Assertions ---
                    assert isinstance(result, StepOutput)
                    assert result.step_status == step_status
                    assert result.data == {"ok": True}
