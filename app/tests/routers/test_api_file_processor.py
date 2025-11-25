import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from fastapi import FastAPI
from fastapi_celery.routers.api_file_processor import router
from fastapi_celery.models.class_models import StatusEnum

# Create a FastAPI app instance for testing
app = FastAPI()
app.include_router(router)
client = TestClient(app)


# ------------------ /file/process Tests ------------------

@patch("fastapi_celery.routers.api_file_processor.celery_task.task_execute.apply_async")
@patch("celery.app.task.Task.apply_async")
@patch("kombu.connection.Connection")
def test_process_file(mock_connection, mock_apply_async, mock_apply_async_task):
    mock_apply_async.return_value = None
    mock_apply_async_task.return_value = None
    mock_connection.return_value = MagicMock()

    payload = {"file_path": "/some/path/to/file.csv", "project": "test_project", "source": "SFTP"}
    response = client.post("/file/process", json=payload)

    assert response.status_code == 200
    res_json = response.json()
    assert "file_path" in res_json and res_json["file_path"] == payload["file_path"]
    assert "celery_id" in res_json
    assert mock_apply_async.called
    args, kwargs = mock_apply_async.call_args
    task_data = kwargs["kwargs"]["data"]
    assert task_data["file_path"] == payload["file_path"]


@patch("fastapi_celery.routers.api_file_processor.celery_task.task_execute.apply_async")
@patch("celery.app.task.Task.apply_async")
@patch("kombu.connection.Connection")
def test_process_file_failure(mock_connection, mock_apply_async, mock_apply_async_task):
    # When Celery apply_async throws an error
    mock_apply_async.side_effect = Exception("Task submission failed")
    mock_apply_async_task.return_value = None
    mock_connection.return_value = MagicMock()

    payload = {"file_path": "/some/path/to/file.csv", "project": "test_project", "source": "SFTP"}
    response = client.post("/file/process", json=payload)

    # The root router will pay 500 if Celery raises
    assert response.status_code == 500
    res_json = response.json()
    assert "Task submission failed" in res_json.get("detail", "") or "Internal Server Error" in res_json.get("detail", "")


# ------------------ /tasks/stop Tests ------------------

@patch("fastapi_celery.routers.api_file_processor.DISABLE_STOP_TASK_ENDPOINT", False)
@patch("fastapi_celery.routers.api_file_processor.celery_app.control.revoke")
@patch("fastapi_celery.routers.api_file_processor.read_n_write_s3.write_json_to_s3", return_value={"status": "Success", "error": None})
@patch("fastapi_celery.routers.api_file_processor.get_s3_key_prefix", return_value="mock/prefix.json")
@patch("fastapi_celery.routers.api_file_processor.BEConnector")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_all_steps_for_task")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_celery_task")
def test_stop_task_success(mock_get_celery_task, mock_get_all_steps_for_task, mock_BEConnector, mock_get_all_steps, mock_get_task, mock_revoke, mock_disable=None, mock_write_json=None):
    # --- Mock redis task ---
    mock_get_celery_task.return_value = {
        "status": StatusEnum.PROCESSING.name,
        "file_record": {
            "target_bucket_name": "mock-bucket",
            "file_path": "/tmp/order.csv",
            "document_type": "order",
            "file_size": "111",
        },
        "tracking_model": {"request_id": "req_001"},
        "context_data": {
            "request_id": "req_001",
            "step_detail": [{}, {}],
            "workflow_detail": {"metadata_api": {"session_finish_api": {}}},
        },
        "start_session_model": {"id": "session_1"},
    }

    # --- Mock steps data ---
    mock_get_all_steps_for_task.return_value = {
        "step_1": {
            "status": "PROCESSING",
            "step": {
                "workflowStepId": "1",
                "stepName": "Step 1",
                "stepOrder": 1,
            },
            "start_step_model": {"workflowHistoryId": "hist_1"},
        }
    }
    async def _post():
        return {"status_code": 200}
    mock_BEConnector.return_value.post = _post

    payload = {"task_id": "task_123", "reason": "Manual stop"}
    response = client.post("/tasks/stop", json=payload)

    assert response.status_code == 200
    res_json = response.json()
    assert res_json["status"] == "Task stopped successfully"
    mock_revoke.assert_called_once_with("task_123", terminate=True, signal="SIGKILL")


@patch("fastapi_celery.routers.api_file_processor.DISABLE_STOP_TASK_ENDPOINT", False)
@patch("fastapi_celery.routers.api_file_processor.celery_app.control.revoke")
@patch("fastapi_celery.routers.api_file_processor.BEConnector") 
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_all_steps_for_task")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_celery_task")
def test_stop_task_failure_workflow_not_found(mock_get_celery_task, mock_get_all_steps_for_task, mock_BEConnector, mock_revoke):
    mock_get_all_steps_for_task.return_value = None
    mock_get_celery_task.return_value = {}

    payload = {"task_id": "task_123", "reason": "Manual stop"}
    response = client.post("/tasks/stop", json=payload)

    assert response.status_code == 404
    res_json = response.json()
    assert res_json["error"] == "Workflow ID not found for task"
    mock_revoke.assert_not_called()
    mock_BEConnector.assert_not_called()


@patch("fastapi_celery.routers.api_file_processor.DISABLE_STOP_TASK_ENDPOINT", False)
@patch("fastapi_celery.routers.api_file_processor.celery_app.control.revoke")
@patch("fastapi_celery.routers.api_file_processor.BEConnector")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_all_steps_for_task")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_celery_task")
@patch("fastapi_celery.routers.api_file_processor.logger")
def test_stop_task_exception_handling_be_failure(mock_logger, mock_get_celery_task, mock_get_all_steps_for_task, mock_BEConnector, mock_revoke):
    mock_get_celery_task.return_value = {
        "status": StatusEnum.PROCESSING.name,
        "file_record": {
            "target_bucket_name": "mock-bucket",
            "file_path": "/tmp/order.csv",
            "document_type": "order",
            "file_size": "111",
        },
        "tracking_model": {"request_id": "req_001"},
        "context_data": {
            "request_id": "req_001",
            "step_detail": [{}, {}],
            "workflow_detail": {"metadata_api": {"session_finish_api": {}}},
        },
        "start_session_model": {"id": "session_1"},
    }

    mock_get_all_steps_for_task.return_value = {
        "step_1": {
            "status": "PROCESSING",
            "step": {
                "workflowStepId": "1", 
                "stepName": "Step 1",
                 "stepOrder": 1
            },
            "start_step_model": {"workflowHistoryId": "hist_1"},
        }
    }

    async def _boom():
        raise Exception("Simulated BEConnector Exception")
    mock_BEConnector.return_value.post = _boom

    payload = {"task_id": "task_123", "reason": "Manual stop"}
    response = client.post("/tasks/stop", json=payload)

    assert response.status_code == 500
    res_json = response.json()
    assert "Simulated BEConnector Exception" in res_json["error"]
    mock_logger.error.assert_called()


@patch("fastapi_celery.routers.api_file_processor.DISABLE_STOP_TASK_ENDPOINT", False)
@patch("fastapi_celery.routers.api_file_processor.celery_app.control.revoke")
@patch("fastapi_celery.routers.api_file_processor.read_n_write_s3.write_json_to_s3", return_value={"status": "Failed", "error": "S3 error"})
@patch("fastapi_celery.routers.api_file_processor.get_s3_key_prefix", return_value="mock/prefix.json")
@patch("fastapi_celery.routers.api_file_processor.BEConnector")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_all_steps_for_task")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_celery_task")
def test_stop_task_inprogress_flow_s3_fail(mock_get_celery_task, mock_get_all_steps_for_task, mock_BEConnector, mock_get_all_steps, mock_get_task, mock_revoke, mock_disable=None, mock_write_json=None):
    mock_get_celery_task.return_value = {
        "status": StatusEnum.PROCESSING.name,
        "file_record": {
            "target_bucket_name": "mock-bucket",
            "file_path": "/tmp/order.csv",
            "document_type": "order",
            "file_size": "111",
        },
        "tracking_model": {"request_id": "req_001"},
        "context_data": {
            "request_id": "req_001",
            "step_detail": [{}, {}],
            "workflow_detail": {"metadata_api": {"session_finish_api": {}}},
        },
        "start_session_model": {"id": "session_1"},
    }
    
    mock_get_all_steps_for_task.return_value = {
        "step_1": {
            "status": "PROCESSING",
            "step": {
                "workflowStepId": "1",
                "stepName": "Step 1",
                "stepOrder": 1,
            },
            "start_step_model": {"workflowHistoryId": "hist_1"},
        }
    }
    async def _post():
        return {"status_code": 200}
    mock_BEConnector.return_value.post = _post

    payload = {"task_id": "task_456", "reason": "Test InProgress"}
    response = client.post("/tasks/stop", json=payload)

    assert response.status_code == 200
    # Even with S3 failure response still success for stop operation
    res_json = response.json()
    assert res_json["status"] == "Task stopped successfully"
    mock_revoke.assert_called_once_with("task_456", terminate=True, signal="SIGKILL")


@patch("fastapi_celery.routers.api_file_processor.DISABLE_STOP_TASK_ENDPOINT", False)
@patch("fastapi_celery.routers.api_file_processor.celery_app.control.revoke")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_all_steps_for_task", return_value={})
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_celery_task")
def test_stop_task_non_processing_status(mock_get_celery_task, mock_get_all_steps_for_task, mock_revoke):
    mock_get_celery_task.return_value = {
        "status": StatusEnum.SUCCESS.name,
        "file_record": {},
    }
    resp = client.post("/tasks/stop", json={"task_id": "finished_task"})
    assert resp.status_code == 500
    assert resp.json()["error"] == "Workflow has been done or stopped! Cannot stop the task"
    mock_revoke.assert_not_called()


@patch("fastapi_celery.routers.api_file_processor.DISABLE_STOP_TASK_ENDPOINT", False)
@patch("fastapi_celery.routers.api_file_processor.celery_app.control.revoke")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_all_steps_for_task")
@patch("fastapi_celery.routers.api_file_processor.RedisConnector.get_celery_task")
def test_stop_task_step_index_error(mock_get_celery_task, mock_get_all_steps_for_task, mock_revoke):
    # Context step_detail too short for stepOrder=1 (only one element at index 0)
    mock_get_celery_task.return_value = {
        "status": StatusEnum.PROCESSING.name,
        "file_record": {
            "target_bucket_name": "mock-bucket",
            "file_path": "/tmp/order.csv",
            "document_type": "order",
            "file_size": "111",
        },
        "tracking_model": {"request_id": "req_001"},
        "context_data": {
            "request_id": "req_001",
            "step_detail": [{}],
            "workflow_detail": {"metadata_api": {"session_finish_api": {}}},
        },
        "start_session_model": {"id": "session_1"},
    }
    mock_get_all_steps_for_task.return_value = {
        "step_1": {
            "status": "PROCESSING",
            "step": {
                "workflowStepId": "1",
                "stepName": "Step 1",
                "stepOrder": 1,
            },
            "start_step_model": {"workflowHistoryId": "hist_1"},
        }
    }
    resp = client.post("/tasks/stop", json={"task_id": "task_idx_err"})
    assert resp.status_code == 500
    assert "list index out of range" in resp.json()["error"]
    mock_revoke.assert_called_once()
