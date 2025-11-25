from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from typing import Callable, Awaitable
import uuid
import logging


class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    Middleware to assign a unique UUID as a request ID for each incoming HTTP request.

    The request ID is added to the response headers as `X-Request-ID` to facilitate
    tracing and correlation of logs and requests across distributed systems.

    Usage:
        Add this middleware to your FastAPI or Starlette application to enable
        consistent request tracing.

    Example:
        app.add_middleware(RequestIDMiddleware)
    """

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response


class HealthCheckFilter(logging.Filter):
    """Filter to exclude health check endpoints from uvicorn access logs."""
    
    def __init__(self, exclude_paths: list):
        super().__init__()
        self.exclude_paths = exclude_paths
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Return False to filter out (not log) the record."""
        message = record.getMessage()
        # Check if any excluded path is in the log message
        return not any(path in message for path in self.exclude_paths)


class AccessLogFilterMiddleware(BaseHTTPMiddleware):
    """
    Middleware to filter out access logs for specific endpoints (e.g., health check).

    This prevents excessive logging for endpoints that are called frequently by
    monitoring systems or load balancers, reducing log clutter on production systems.

    Usage:
        Add this middleware to your FastAPI application:
        app.add_middleware(AccessLogFilterMiddleware, exclude_paths=["/fastapi/api_health"])

    Args:
        exclude_paths (list): List of paths to exclude from access logging.
    """

    def __init__(self, app, exclude_paths: list = None):
        super().__init__(app)
        self.exclude_paths = exclude_paths or []
        
        # Add filter to uvicorn.access logger
        if self.exclude_paths:
            uvicorn_logger = logging.getLogger("uvicorn.access")
            health_filter = HealthCheckFilter(self.exclude_paths)
            uvicorn_logger.addFilter(health_filter)

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        # Just pass through - filtering is done by logging.Filter
        return await call_next(request)
