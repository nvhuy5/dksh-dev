import traceback
from typing import Dict, Any
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from models.class_models import HealthResponse
from utils import log_helpers
from models.tracking_models import ServiceLog, LogType

# === Set up logging ===
logger = log_helpers.get_logger("Health-check Router")

router = APIRouter()


def _internal_health_check() -> Dict[str, str]:
    """Internal health check logic."""
    return {"status": "ok"}


@router.get("/api_health", response_model=HealthResponse)
async def api_health() -> Dict[str, Any]:
    try:
        return _internal_health_check()
    except Exception as e:
        logger.error(
            "Health check failed",
            extra={
                "error_message": str(e),
                "trace": traceback.format_exc(),
                "service": ServiceLog.API_GATEWAY,
                "log_type": LogType.ERROR,
            }
        )

        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "error",
                "message": "Health check failed"
            },
        )
