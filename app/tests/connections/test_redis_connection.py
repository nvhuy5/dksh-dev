# tests/test_redis_connection.py
import json
import pytest
from unittest.mock import patch, MagicMock
from redis.exceptions import RedisError
from fastapi_celery.connections.redis_connection import RedisConnector

# === RedisConnector Tests ===

TASK_ID = "task123"
STEP_NAME = "stepA"
STATUS = "completed"
STEP_ID = "step-id-001"
WORKFLOW_ID = "workflow-xyz"

# -------------------------
# store_step_processing
# -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_step_processing_success(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hset.return_value = True
    mock_redis.expire.return_value = True
    data = {"step": {"workflowStepId": "step1"}, "status": "PROCESSING"}

    redis_conn = RedisConnector()
    result = redis_conn.store_step_processing(TASK_ID, data)
    assert result is True
    assert mock_redis.hset.call_count == 1
    assert mock_redis.expire.call_count == 1


@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_step_processing_redis_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hset.side_effect = RedisError("Connection error")
    data = {"step": {"workflowStepId": "step1"}}

    redis_conn = RedisConnector()
    result = redis_conn.store_step_processing(TASK_ID, data)
    assert result is False

# # -------------------------
# # get_step_processing
# # -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_get_step_processing_success(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hgetall.return_value = {"status": json.dumps("done")}

    redis_conn = RedisConnector()
    result = redis_conn.get_step_processing(TASK_ID, "123")
    assert result == {"status": "done"}

@patch("fastapi_celery.connections.redis_connection.redis.Redis")  
def test_get_step_processing_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hgetall.side_effect = RedisError("Connection error")

    redis_conn = RedisConnector()
    result = redis_conn.get_step_processing(TASK_ID, "123")
    assert result is None

# # -------------------------
# # update_step_fields
# # -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")  
def test_update_step_fields_success(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.exists.return_value = True
    fields = {"status": "Success"}

    redis_conn = RedisConnector()
    result = redis_conn.update_step_fields(TASK_ID, "123", fields)
    assert result is True

@patch("fastapi_celery.connections.redis_connection.redis.Redis")  
def test_update_step_fields_missing_key(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.exists.return_value = False

    redis_conn = RedisConnector()
    result = redis_conn.update_step_fields(TASK_ID, "123", {"a": "b"})
    assert result is False

@patch("fastapi_celery.connections.redis_connection.redis.Redis")  
def test_update_step_fields_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hset.side_effect = RedisError("Redis fail")

    redis_conn = RedisConnector()
    result = redis_conn.update_step_fields(TASK_ID, "123", {"a": "b"})
    assert result is False

# # -------------------------
# # get_all_steps_for_task
# # -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")  
def test_get_all_steps_for_task_success(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.scan_iter.return_value = [b"celery_task:task123:step_id:step1"]

    redis_conn = RedisConnector()
    with patch.object(redis_conn, "get_step_processing", return_value={"ok": True}) as mock_get:
        result = redis_conn.get_all_steps_for_task("task123")

    assert "step1" in result
    assert result["step1"] == {"ok": True}

# # -------------------------
# # store_celery_task
# # -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_celery_task_success(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    data = {"status": "running"}

    redis_conn = RedisConnector()
    result = redis_conn.store_celery_task(TASK_ID, data)
    assert result is True
    mock_redis.hset.assert_called_once()
    mock_redis.expire.assert_called_once()

@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_celery_task_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hset.side_effect = RedisError("Redis fail")

    redis_conn = RedisConnector()
    result = redis_conn.store_celery_task(TASK_ID, {"x": 1})
    assert result is False

# # -------------------------
# # get_celery_task
# # -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_get_celery_task_success(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hgetall.return_value = {"status": json.dumps("done")}

    redis_conn = RedisConnector()
    result = redis_conn.get_celery_task(TASK_ID)
    assert result == {"status": "done"}

@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_get_celery_task_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.hgetall.side_effect = RedisError("Connection error")

    redis_conn = RedisConnector()
    result = redis_conn.get_celery_task(TASK_ID)
    assert result is None

# -------------------------
# JWT token store & get
# -------------------------
@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_and_get_jwt_token(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.set.return_value = True
    mock_redis.get.return_value = "jwt-token"

    redis_conn = RedisConnector()
    store_result = redis_conn.store_jwt_token("jwt-token", 3600)
    get_result = redis_conn.get_jwt_token()

    assert store_result is True
    assert get_result == "jwt-token"


@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_store_jwt_token_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.set.side_effect = RedisError("Connection error")

    redis_conn = RedisConnector()
    result = redis_conn.store_jwt_token("jwt-token", 3600)
    assert result is False

@patch("fastapi_celery.connections.redis_connection.redis.Redis")
def test_get_jwt_token_failure(mock_redis_class):
    mock_redis = mock_redis_class.return_value
    mock_redis.get.side_effect = RedisError("Connection error")

    redis_conn = RedisConnector()
    result = redis_conn.get_jwt_token()
    assert result is None
