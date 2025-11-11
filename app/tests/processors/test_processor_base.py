import types
import importlib
import pytest
import inspect

from processors import processor_base


class FakeTrackingModel:
    def __init__(self):
        self.request_id = "REQ-1"
        self.file_path = "file.txt"
        self.project_name = "PROJ"


@pytest.fixture
def fake_tracking_model():
    return FakeTrackingModel()


def test_register_workflow_processors_success(monkeypatch, fake_tracking_model):
    """Ensure all modules in WORKFLOW_PROCESSORS are imported successfully."""
    imported = []

    def fake_import_module(path):
        imported.append(path)
        return types.SimpleNamespace(fake_func=lambda self: "done")

    monkeypatch.setattr(importlib, "import_module", fake_import_module)
    monkeypatch.setattr("processors.processor_base.WORKFLOW_PROCESSORS", ["extract_metadata", "template_mapping"])

    p = processor_base.ProcessorBase(fake_tracking_model)

    assert len(imported) == 2
    assert hasattr(p, "fake_func")
    assert callable(p.fake_func)


def test_register_workflow_processors_missing_module(monkeypatch, fake_tracking_model):
    """If module not found, it logs warning but does not crash."""
    calls = {"warn": [], "debug": []}

    class DummyLogger:
        def warning(self, msg):
            calls["warn"].append(msg)

        def debug(self, msg):
            calls["debug"].append(msg)

    monkeypatch.setattr("processors.processor_base.logger", DummyLogger())

    def fake_import_module(path):
        if "extract_metadata" in path:
            raise ModuleNotFoundError("missing")
        return types.SimpleNamespace(func=lambda self: "done")

    monkeypatch.setattr(importlib, "import_module", fake_import_module)
    monkeypatch.setattr("processors.processor_base.WORKFLOW_PROCESSORS", ["extract_metadata", "template_mapping"])

    processor_base.ProcessorBase(fake_tracking_model)
    assert any("extract_metadata" in msg for msg in calls["warn"])


def test_run_calls_extract_metadata(monkeypatch, fake_tracking_model):
    """Ensure `run` calls extract_metadata method safely."""
    called = {"flag": False}
    processor = processor_base.ProcessorBase(fake_tracking_model)

    def fake_extract_metadata():
        called["flag"] = True

    processor.extract_metadata = fake_extract_metadata
    processor.run()
    assert called["flag"] is True




def test_register_workflow_processors_binds_functions(monkeypatch, fake_tracking_model):
    """Ensure functions from imported module are bound as methods."""
    def fake_import_module(path):
        def func_a(self): return "A"
        def func_b(self): return "B"
        return types.SimpleNamespace(func_a=func_a, func_b=func_b)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)
    monkeypatch.setattr("processors.processor_base.WORKFLOW_PROCESSORS", ["extract_metadata"])

    p = processor_base.ProcessorBase(fake_tracking_model)
    assert p.func_a() == "A"
    assert p.func_b() == "B"


def test_register_workflow_processors_ignores_non_functions(monkeypatch, fake_tracking_model):
    """Non-function attributes should be ignored."""
    def fake_import_module(path):
        return types.SimpleNamespace(non_func="string_value")

    monkeypatch.setattr(importlib, "import_module", fake_import_module)
    monkeypatch.setattr("processors.processor_base.WORKFLOW_PROCESSORS", ["template_mapping"])

    p = processor_base.ProcessorBase(fake_tracking_model)
    assert not hasattr(p, "non_func")


def test_register_workflow_processors_logs(monkeypatch, fake_tracking_model):
    """Ensure logger.warning and logger.debug are called properly."""
    logs = {"warn": [], "debug": []}

    # Mock logger directly
    class DummyLogger:
        def warning(self, msg):
            logs["warn"].append(msg)
        def debug(self, msg):
            logs["debug"].append(msg)

    monkeypatch.setattr(processor_base, "logger", DummyLogger())

    # Mock import to simulate one missing and one successful module
    def fake_import_module(path):
        if "template_mapping" in path:
            raise ModuleNotFoundError("template_mapping missing")
        # Return module with a valid function
        def func_ok(self): return "ok"
        return types.SimpleNamespace(func_ok=func_ok)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)
    monkeypatch.setattr(processor_base, "WORKFLOW_PROCESSORS", ["extract_metadata", "template_mapping"])

    p = processor_base.ProcessorBase(fake_tracking_model)

    # Verify warning logged for missing module
    assert any("template_mapping" in w for w in logs["warn"])
    # Verify debug log for successful function registration
    assert any("func_ok" in d for d in logs["debug"])
    # And method is bound
    assert hasattr(p, "func_ok")
    assert p.func_ok() == "ok"
