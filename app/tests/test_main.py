from fastapi.testclient import TestClient
from fastapi_celery.main import app


client = TestClient(app)


def test_app_startup_and_routes():
    """Ensure app starts and routers are registered."""
    response = client.get("/fastapi/api_health")
    assert response.status_code in (200, 503)


def test_lifespan_startup_flag():
    """Ensure lifespan startup flag is set."""
    with TestClient(app) as test_client:
        assert app.state.startup_triggered is True


def test_global_exception_handler():
    """Ensure custom exception handler catches unhandled errors."""

    @app.get("/fastapi/raise_error")
    async def raise_error():
        raise Exception("Not Found")

    response = client.get("/fastapi/raise_error")
    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}


def test_request_id_middleware_generates_id():
    """Ensure RequestIDMiddleware adds an X-Request-ID header."""
    response = client.get("/fastapi/api_health")
    assert "X-Request-ID" in response.headers
    assert len(response.headers["X-Request-ID"]) > 0
