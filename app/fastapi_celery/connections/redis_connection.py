import json
from typing import Any
import redis
from redis.exceptions import RedisError
import config_loader
from utils import log_helpers
from models.tracking_models import ServiceLog, LogType
from models.class_models import WorkflowStep
from pathlib import Path

# === Set up logging ===
logger = log_helpers.get_logger("Redis Connection")


class RedisConnector:
    """Redis connector for storing and retrieving workflow data."""

    def __init__(self):
        self.redis_client = redis.Redis(
            host=config_loader.get_env_variable("REDIS_HOST", "localhost"),
            port=config_loader.get_env_variable("REDIS_PORT", 6379),
            password=None,
            db=0,
            decode_responses=True,
        )

    # === Store step_processing ===
    def store_step_processing(self, celery_id: str, data: dict, ttl: int = 3600) -> bool:
        """Store step_processing data for a specific Celery task step."""
        try:
            step = data.get("step")
            workflow_step_id = (
                step.workflowStepId if hasattr(step, "workflowStepId") else step.get("workflowStepId")
            )
            name = f"celery_task:{celery_id}:step_id:{workflow_step_id}"

            # Serialize complex types
            serialized_data = {
                k: json.dumps(v, default=lambda o: o.model_dump() if hasattr(o, "model_dump") else str(o))
                for k, v in data.items()
            }

            self.redis_client.hset(name=name, mapping=serialized_data)
            self.redis_client.expire(name, ttl)

            logger.info(
                "[Redis] Stored step_processing successfully",
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ACCESS,
                    "celery_id": celery_id,
                    "workflow_step_id": workflow_step_id,
                    "key": name,
                    "data": serialized_data,
                    "ttl_seconds": ttl,
                },
            )
            return True

        except RedisError as e:
            logger.error(
                "[Redis] Failed to store step_processing",
                exc_info=True,
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ERROR,
                    "celery_id": celery_id,
                    "data": data,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return False

    # === Get step_processing ===
    def get_step_processing(self, celery_id: str, workflow_step_id: str) -> dict[str, any] | None:
        """Retrieve step_processing data for a specific step of a Celery task."""
        name = f"celery_task:{celery_id}:step_id:{workflow_step_id}"
        try:
            data = self.redis_client.hgetall(name)
            if not data:
                logger.info(
                    "[Redis] No step_processing data found",
                    extra={
                        "service": ServiceLog.REDIS_SERVICE,
                        "log_type": LogType.ACCESS,
                        "celery_id": celery_id,
                        "workflow_step_id": workflow_step_id,
                        "key": name,
                    },
                )
                return None

            deserialized = {}
            for k, v in data.items():
                try:
                    deserialized[k] = json.loads(v)
                except (TypeError, json.JSONDecodeError):
                    deserialized[k] = v  # fallback

            logger.info(
                "[Redis] Retrieved step_processing successfully",
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ACCESS,
                    "celery_id": celery_id,
                    "workflow_step_id": workflow_step_id,
                    "key": name,
                    "data": deserialized,
                },
            )
            return deserialized

        except RedisError as e:
            logger.error(
                "[Redis] Failed to fetch step_processing",
                exc_info=True,
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ERROR,
                    "celery_id": celery_id,
                    "workflow_step_id": workflow_step_id,
                    "key": name,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return None

    # === Update step fields ===
    def update_step_fields(self, celery_id: str, workflow_step_id: str, fields: dict) -> bool:
        """Update specific fields for a step_processing record."""
        name = f"celery_task:{celery_id}:step_id:{workflow_step_id}"
        try:
            if not self.redis_client.exists(name):
                logger.warning(
                    "[Redis] Step key not found for update",
                    extra={
                        "service": ServiceLog.REDIS_SERVICE,
                        "log_type": LogType.WARNING,
                        "celery_id": celery_id,
                        "workflow_step_id": workflow_step_id,
                        "fields": fields,
                        "key": name,
                    },
                )
                return False

            serialized_fields = {
                k: json.dumps(v, default=lambda o: o.model_dump() if hasattr(o, "model_dump") else str(o))
                for k, v in fields.items()
            }

            self.redis_client.hset(name=name, mapping=serialized_fields)

            logger.info(
                "[Redis] Updated step_processing fields successfully",
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ACCESS,
                    "celery_id": celery_id,
                    "workflow_step_id": workflow_step_id,
                    "updated_fields": serialized_fields,
                    "key": name,
                },
            )
            return True

        except RedisError as e:
            logger.error(
                "[Redis] Failed to update step_processing fields",
                exc_info=True,
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ERROR,
                    "celery_id": celery_id,
                    "workflow_step_id": workflow_step_id,
                    "fields": fields,
                    "key": name,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return False

    # === Get all steps for task ===
    def get_all_steps_for_task(self, celery_id: str) -> dict[str, dict]:
        """Retrieve all step_processing data for a given celery_id."""
        pattern = f"celery_task:{celery_id}:step_id:*"
        result = {}
        for key in self.redis_client.scan_iter(pattern):
            step_id = (key.decode() if isinstance(key, bytes) else key).split(":")[-1]
            result[step_id] = self.get_step_processing(celery_id, step_id)
        return result

    # === Store celery_task ===
    def store_celery_task(self, celery_id: str, data: dict, ttl: int = 3600) -> bool:
        """Store celery_task in Redis (values serialized as JSON)."""
        name = f"celery_task:{celery_id}"
        try:
            # Ensure all values are strings (serialize complex types)
            serialized_data = {
                k: json.dumps(v, default=lambda o: o.model_dump() if hasattr(o, "model_dump") else str(o))
                for k, v in data.items()
            }

            self.redis_client.hset(name=name, mapping=serialized_data)
            self.redis_client.expire(name, ttl)

            logger.info(
                "[Redis] Stored celery_task in Redis successfully",
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ACCESS,
                    "celery_id": celery_id,
                    "key": name,
                    #"data": serialized_data,
                    "ttl_seconds": ttl,
                },
            )
            return True

        except RedisError as e:
            logger.error(
                "[Redis] Failed to store celery_task in Redis",
                exc_info=True,
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ERROR,
                    "celery_id": celery_id,
                    "key": name,
                    "data": data,
                    "ttl_seconds": ttl,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return False
    
    # === Get celery_task ===
    def get_celery_task(self, celery_id: str) -> dict[str, Any] | None:
        """Get celery_task from Redis (auto-deserialize JSON values)."""
        name = f"celery_task:{celery_id}"
        try:
            data = self.redis_client.hgetall(name)
            if not data:
                logger.info(
                    "[Redis] No celery_task found",
                    extra={
                        "service": ServiceLog.REDIS_SERVICE,
                        "log_type": LogType.ACCESS,
                        "celery_id": celery_id,
                        "key": name,
                    },
                )
                return None

            # Deserialize JSON values if possible
            for k, v in data.items():
                try:
                    data[k] = json.loads(v)
                except (json.JSONDecodeError, TypeError):
                    pass

            logger.info(
                "[Redis] Retrieved celery_task successfully",
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ACCESS,
                    "celery_id": celery_id,
                    "key": name,
                    "data": data,
                },
            )
            return data
        except RedisError as e:
            logger.error(
                "[Redis] Failed to fetch celery_task",
                exc_info=True,
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ERROR,
                    "celery_id": celery_id,
                    "key": name,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return None
    
    # === Update celery_task fields ===
    def update_celery_task_fields(self, celery_id: str, fields: dict[str, any]) -> bool:
        """Update specific fields in Redis for a Celery task."""
        name = f"celery_task:{celery_id}"

        def default_encoder(obj):
            return str(obj)
        
        try:
            # Serialize values to JSON
            serialized = {k: json.dumps(v, default=default_encoder) for k, v in fields.items()}

            # Update Redis hash
            self.redis_client.hset(name, mapping=serialized)

            logger.info(
                "[Redis] Updated celery_task fields successfully",
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ACCESS,
                    "celery_id": celery_id,
                    "key": name,
                    #"fields": serialized,
                },
            )
            return True

        except RedisError as e:
            logger.error(
                "[Redis] Failed to update celery_task fields",
                exc_info=True,
                extra={
                    "service": ServiceLog.REDIS_SERVICE,
                    "log_type": LogType.ERROR,
                    "celery_id": celery_id,
                    "key": name,
                    "fields": fields,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            return False
    
    def store_jwt_token(self, token: str, ttl: int) -> bool:
        """Store JWT token with TTL."""
        try:
            self.redis_client.set("jwt_token", token, ex=ttl)
            logger.info("[Redis] Stored JWT token successfully")
            return True
        except RedisError as e:
            logger.error(f"[Redis] Failed to store JWT token: {e}")
            return False

    def get_jwt_token(self) -> str | None:
        """Retrieve JWT token from Redis."""
        try:
            token = self.redis_client.get("jwt_token")
            if token:
                logger.info("[Redis] Retrieved JWT token successfully")
            return token
        except RedisError as e:
            logger.error(f"[Redis] Failed to retrieve JWT token: {e}")
            return None
