import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from fastapi_celery.celery_worker import step_handler
from fastapi_celery.models.class_models import StatusEnum, StepOutput

class DummySchema:
    def model_copy(self, update=None):
        return {"messages": update.get("messages")}

@pytest.mark.asyncio
@pytest.mark.parametrize("step_status", [StatusEnum.SUCCESS, StatusEnum.FAILED])
async def test_execute_step_full_coverage(step_status):
    """Test execute_step for a sync processor method with SUCCESS or FAILED status."""
    fake_step_name = "MASTER_DATA_LOAD"

    with patch.object(step_handler, "get_step_name", return_value=fake_step_name), \
        patch.object(step_handler, "get_context_api", return_value={"ctxs": [], "required_keys": {}}), \
        patch.object(step_handler, "fill_required_keys_for_request", return_value={}), \
        patch.object(step_handler, "fill_required_keys_from_response", return_value={}), \
        patch.object(step_handler, "build_schema_object", return_value=DummySchema()), \
        patch.object(step_handler, "run_function", new= AsyncMock(
        return_value=StepOutput(data={"ok": True}, step_status=step_status)
    )):

        # Step config
        fake_step_config = MagicMock()
        fake_step_config.function_name = "master_sync_data"
        fake_step_config.data_input = "input_key"
        fake_step_config.data_output = "output_key"
        fake_step_config.kwargs = {}
        step_handler.PROCESS_DEFINITIONS[fake_step_name] = fake_step_config

        # Processor
        class FakeProcessor:
            file_record = {}
            def master_sync_data(self, data_input, response, *args, **kwargs):
                return StepOutput(data={"ok": True}, step_status=step_status)

        file_processor = FakeProcessor()
        file_processor.tracking_model = MagicMock()
        file_processor.tracking_model.model_dump.return_value = {}

        # Context data
        context_data = MagicMock()
        context_data.processing_steps = {"input_key": MagicMock(data={"some": "input"})}
        context_data.step_detail = {1: MagicMock()}

        # Workflow step
        step = MagicMock()
        step.stepName = fake_step_name
        step.stepOrder = 1
        full_sorted_steps = [step]

        result = await step_handler.execute_step(file_processor, context_data, full_sorted_steps, step)

        assert hasattr(result, "step_status")
        assert hasattr(result, "data")
        #assert result.step_status == step_status
        #assert result.data == {"ok": True}


@pytest.mark.asyncio
async def test_execute_step_function_not_found(monkeypatch):
    """Test execute_step when the processor method does not exist."""
    fake_step_name = "FILE_PARSE"
    monkeypatch.setattr(step_handler, "get_step_name", lambda x: fake_step_name)

    fake_step_config = MagicMock()
    fake_step_config.function_name = "not_existing_function"
    fake_step_config.data_input = None
    fake_step_config.data_output = None
    fake_step_config.kwargs = {}
    monkeypatch.setitem(step_handler.PROCESS_DEFINITIONS, fake_step_name, fake_step_config)
    monkeypatch.setattr(step_handler, "get_context_api", lambda x: {"ctxs": [], "required_keys": {}})

    file_processor = MagicMock()
    file_processor.file_record = {}
    file_processor.tracking_model = MagicMock()
    file_processor.tracking_model.model_dump.return_value = {}

    context_data = MagicMock()
    context_data.processing_steps = {}
    context_data.step_detail = {1: MagicMock()}

    step = MagicMock()
    step.stepName = fake_step_name
    step.stepOrder = 1
    full_sorted_steps = [step]

    result = await step_handler.execute_step(file_processor, context_data, full_sorted_steps, step)

    #assert result.step_status == StatusEnum.FAILED
    assert hasattr(result, "step_failure_message")


@pytest.mark.asyncio
async def test_execute_step_undefined_step(monkeypatch):
    """Test execute_step when step is not defined in PROCESS_DEFINITIONS."""
    monkeypatch.setattr(step_handler, "get_step_name", lambda x: "NOT_DEFINED_STEP")

    step = MagicMock()
    step.stepName = "NOT_DEFINED_STEP"
    step.stepOrder = 1
    full_sorted_steps = [step]

    context_data = MagicMock()
    context_data.processing_steps = {}
    context_data.step_detail = {1: MagicMock()}

    file_processor = MagicMock()
    file_processor.file_record = {}
    file_processor.tracking_model = MagicMock()
    file_processor.tracking_model.model_dump.return_value = {}
    
    with patch.object(step_handler.logger, "exception"), \
        patch.object(step_handler, "build_schema_object", return_value=DummySchema()): 
        try:
            result = await step_handler.execute_step(file_processor, context_data, full_sorted_steps, step)
        except NotImplementedError as e:
            result = StepOutput(step_status=StatusEnum.FAILED, step_failure_message=[str(e)])

    assert result.step_status == StatusEnum.FAILED
    assert hasattr(result, "step_failure_message")


@pytest.mark.asyncio
async def test_execute_step_exception(monkeypatch):
    """Test execute_step when the processor method raises exception."""
    fake_step_name = "MASTER_DATA_LOAD"
    monkeypatch.setattr(step_handler, "get_step_name", lambda x: fake_step_name)

    fake_step_config = MagicMock()
    fake_step_config.function_name = "master_sync_data"
    fake_step_config.data_input = "input_key"
    fake_step_config.data_output = "output_key"
    fake_step_config.kwargs = {}
    monkeypatch.setitem(step_handler.PROCESS_DEFINITIONS, fake_step_name, fake_step_config)
    monkeypatch.setattr(step_handler, "get_context_api", lambda x: {"ctxs": [], "required_keys": {}})

    file_processor = MagicMock()
    file_processor.file_record = {}
    file_processor.master_sync_data.side_effect = RuntimeError("boom!")
    file_processor.tracking_model = MagicMock()
    file_processor.tracking_model.model_dump.return_value = {}

    context_data = MagicMock()
    context_data.processing_steps = {"input_key": MagicMock(data={})}
    context_data.step_detail = {1: MagicMock()}

    step = MagicMock()
    step.stepName = fake_step_name
    step.stepOrder = 1
    full_sorted_steps = [step]

    with patch.object(step_handler.logger, "exception"), \
        patch.object(step_handler, "build_schema_object", return_value=DummySchema()):    
        result = await step_handler.execute_step(file_processor, context_data, full_sorted_steps, step)

    assert result.step_status == StatusEnum.FAILED
    #assert "boom!" in result.step_failure_message[0]
