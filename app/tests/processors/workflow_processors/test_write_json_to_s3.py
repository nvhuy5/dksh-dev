import pytest
import types
from fastapi_celery.models.class_models import StepOutput, StatusEnum, WorkflowStep
import fastapi_celery.processors.workflow_processors.write_json_to_s3 as wj


class FakeProcessor:
    def __init__(self):
        self.file_record = {
            "target_bucket_name": "bucket-test",
            "file_name_wo_ext": "file_abc",
            "document_type": "DOC",
        }
        self.tracking_model = types.SimpleNamespace(rerun_attempt=1)


# ---------- write_json_to_s3 tests ----------

def test_write_json_to_s3_success(monkeypatch):
    """Test successful JSON writing to S3 returns StepOutput with SUCCESS status."""
    mock_result = {"path": "s3://bucket-test/prefix/result.json"}

    monkeypatch.setattr(wj.read_n_write_s3, "write_json_to_s3", lambda **_: mock_result)
    logs = {"info": []}
    monkeypatch.setattr(wj, "logger", types.SimpleNamespace(info=lambda msg, **_: logs["info"].append(msg)))

    processor = FakeProcessor()
    result = wj.write_json_to_s3(processor, {"foo": "bar"}, "prefix")

    # Compare by class name to avoid import reference mismatch
    assert result.__class__.__name__ == "StepOutput"
    assert result.data == mock_result
    assert result.step_status == StatusEnum.SUCCESS
    assert any("Successfully wrote" in m for m in logs["info"])


def test_write_json_to_s3_failure(monkeypatch):
    """Test exception path logs error and raises exception."""
    def fake_write_json_to_s3(**_): raise RuntimeError("S3 failed")
    monkeypatch.setattr(wj.read_n_write_s3, "write_json_to_s3", fake_write_json_to_s3)

    logs = {"error": []}
    monkeypatch.setattr(wj, "logger", types.SimpleNamespace(
        info=lambda *_: None,
        error=lambda msg, **_: logs["error"].append(msg),
    ))

    processor = FakeProcessor()

    with pytest.raises(RuntimeError):
        wj.write_json_to_s3(processor, {"foo": "bar"}, "prefix")

    assert any("Failed to write" in m for m in logs["error"])


# ---------- get_step_result_from_s3 tests ----------

@pytest.fixture
def fake_read_n_write(monkeypatch):
    """Fixture to fake S3 read/write helpers."""
    fake = types.SimpleNamespace(
        list_objects_with_prefix=lambda **_: ["k1"],
        select_latest_rerun=lambda **_: "k1",
        # FIXED: use correct StatusEnum value ("1" for SUCCESS)
        read_json_from_s3=lambda **_: {"step_status": StatusEnum.SUCCESS.value},
    )
    monkeypatch.setattr(wj, "read_n_write_s3", fake)
    return fake


def test_get_step_result_from_s3_success(monkeypatch, fake_read_n_write):
    """Test normal successful flow when JSON data is found."""
    processor = FakeProcessor()

    monkeypatch.setattr(wj, "get_s3_key_prefix", lambda **_: "prefix")
    logs = {"info": []}
    monkeypatch.setattr(wj, "logger", types.SimpleNamespace(info=lambda msg, **_: logs["info"].append(msg)))

    monkeypatch.setattr(
        wj.template_helper, "parse_data",
        lambda document_type, data: {"parsed": True, "doc": document_type, "data": data}
    )

    step = WorkflowStep(workflowStepId="S1", stepName="DUMMY_STEP", stepOrder=1)

    result = wj.get_step_result_from_s3(processor, step)
    assert result["parsed"] is True
    assert any("already completed" in m for m in logs["info"])


def test_get_step_result_from_s3_no_data(monkeypatch):
    """Test when S3 returns no data (None)."""
    processor = FakeProcessor()
    monkeypatch.setattr(wj, "get_s3_key_prefix", lambda **_: "prefix")

    mock = types.SimpleNamespace(
        list_objects_with_prefix=lambda **_: ["k1"],
        select_latest_rerun=lambda **_: "k1",
        read_json_from_s3=lambda **_: None
    )
    monkeypatch.setattr(wj, "read_n_write_s3", mock)

    logs = {"info": []}
    monkeypatch.setattr(wj, "logger", types.SimpleNamespace(info=lambda msg, **_: logs["info"].append(msg)))

    step = WorkflowStep(workflowStepId="S2", stepName="TEST_STEP", stepOrder=2)

    result = wj.get_step_result_from_s3(processor, step)
    assert result is None
    assert any("No S3 file found" in m for m in logs["info"])


def test_get_step_result_from_s3_failed_status(monkeypatch, fake_read_n_write):
    """Test when step_status != SUCCESS triggers rerun log."""
    processor = FakeProcessor()

    monkeypatch.setattr(wj, "get_s3_key_prefix", lambda **_: "prefix")
    # FIXED: use "2" (FAILED) instead of "FAILED"
    fake_read_n_write.read_json_from_s3 = lambda **_: {"step_status": StatusEnum.FAILED.value}

    logs = {"info": []}
    monkeypatch.setattr(wj, "logger", types.SimpleNamespace(info=lambda msg, **_: logs["info"].append(msg)))

    monkeypatch.setattr(
        wj.template_helper,
        "parse_data",
        lambda **_: {"parsed": True, "rerun_required": True}
    )

    step = WorkflowStep(workflowStepId="S3", stepName="STEP_FAILED", stepOrder=3)

    result = wj.get_step_result_from_s3(processor, step)
    assert result["parsed"] is True
    assert any("Rerun required" in m for m in logs["info"])
