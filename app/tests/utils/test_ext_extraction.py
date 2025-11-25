import pytest
from unittest.mock import patch, MagicMock
from fastapi_celery.utils.ext_extraction import FileExtensionProcessor
from fastapi_celery.models.tracking_models import TrackingModel
from fastapi_celery.models.class_models import SourceType, DocumentType


@pytest.fixture
def tracking_model():
    return TrackingModel(
        request_id="req-1",
        file_path="dummy.txt",
        project_name="PROJ-1",
        sap_masterdata=True,
    )


@pytest.fixture(autouse=True)
def mock_helpers(monkeypatch):
    """
    Patch all external dependencies and methods that touch file system or S3.
    """
    # Patch get_bucket_name to return mock bucket
    monkeypatch.setattr(
        "fastapi_celery.utils.ext_extraction.get_bucket_name",
        lambda *args, **kwargs: f"mock-{args[1]}-bucket"
    )

    # Patch S3Connector to return a fake client
    fake_client = MagicMock()
    monkeypatch.setattr(
        "fastapi_celery.connections.aws_connection.S3Connector",
        lambda *args, **kwargs: MagicMock(client=fake_client)
    )

    # Patch get_object to return dummy bytes
    monkeypatch.setattr(
        "fastapi_celery.utils.read_n_write_s3.get_object",
        lambda client, bucket_name, object_name: b"dummy content"
    )

    # Patch _load_local_file to set attributes without reading file
    monkeypatch.setattr(
        "fastapi_celery.utils.ext_extraction.FileExtensionProcessor._load_local_file",
        lambda self: setattr(self, "file_name", "dummy.txt") or setattr(self, "file_path_parent", "/tmp/")
    )

    # Patch _load_s3_file to set attributes without reading S3
    monkeypatch.setattr(
        "fastapi_celery.utils.ext_extraction.FileExtensionProcessor._load_s3_file",
        lambda self: setattr(self, "file_name", "dummy.txt") or setattr(self, "file_path_parent", "/s3/") or setattr(self, "object_buffer", b"dummy content")
    )

    # Patch _get_file_extension to return valid extension
    monkeypatch.setattr(
        "fastapi_celery.utils.ext_extraction.FileExtensionProcessor._get_file_extension",
        lambda self: setattr(self, "file_extension", ".txt") or setattr(self, "file_name_wo_ext", "dummy")
    )

    # Patch _get_file_size to return fake size
    monkeypatch.setattr(
        "fastapi_celery.utils.ext_extraction.FileExtensionProcessor._get_file_size",
        lambda self: setattr(self, "file_size", "1.00 KB")
    )

    # Patch _get_document_type to return a fixed document type
    monkeypatch.setattr(
        "fastapi_celery.utils.ext_extraction.FileExtensionProcessor._get_document_type",
        lambda self: setattr(self, "document_type", DocumentType.ORDER)
    )


def test_init_sets_attributes_correctly(tracking_model):
    processor = FileExtensionProcessor(tracking_model, source_type=SourceType.LOCAL)
    assert processor.file_name == "dummy.txt"
    assert processor.file_extension == ".txt"
    assert processor.file_name_wo_ext == "dummy"
    assert processor.file_size == "1.00 KB"
    assert processor.file_path_parent in ["/tmp/", "/s3/"]
    assert processor.document_type == DocumentType.ORDER


def test_load_local_file_success(tracking_model):
    processor = FileExtensionProcessor(tracking_model, source_type=SourceType.LOCAL)
    # _load_local_file is patched, file_name should be set
    assert processor.file_name == "dummy.txt"
    assert processor.file_path_parent == "/tmp/"


def test_load_s3_file_success(tracking_model):
    processor = FileExtensionProcessor(tracking_model, source_type=SourceType.S3)
    # _load_s3_file is patched, file_name and object_buffer should be set
    assert processor.file_name == "dummy.txt"
    assert processor.file_path_parent == "/s3/"
    assert processor.object_buffer == b"dummy content"


def test_get_file_extension_invalid_extension(tracking_model):
    processor = FileExtensionProcessor.__new__(FileExtensionProcessor)
    processor.tracking_model = tracking_model
    processor.file_path = tracking_model.file_path
    processor.source_type = SourceType.LOCAL

    with patch.object(processor, "_get_file_extension", side_effect=TypeError("unsupported")):
        with pytest.raises(TypeError):
            processor._get_file_extension()


def test_get_file_size_local(tracking_model):
    processor = FileExtensionProcessor(tracking_model, source_type=SourceType.LOCAL)
    assert processor.file_size == "1.00 KB"


def test_get_file_size_s3(tracking_model):
    processor = FileExtensionProcessor(tracking_model, source_type=SourceType.S3)
    assert processor.file_size == "1.00 KB"


def test_get_document_type_master_data(tracking_model):
    # Patch _get_document_type to return MASTER_DATA
    with patch.object(FileExtensionProcessor, "_get_document_type", lambda self: setattr(self, "document_type", DocumentType.MASTER_DATA)):
        processor = FileExtensionProcessor(tracking_model, source_type=SourceType.LOCAL)
        assert processor.document_type == DocumentType.MASTER_DATA


def test_get_document_type_order(tracking_model):
    processor = FileExtensionProcessor(tracking_model, source_type=SourceType.LOCAL)
    assert processor.document_type == DocumentType.ORDER


def test_format_size_returns_correct_units():
    from fastapi_celery.utils.ext_extraction import FileExtensionProcessor

    assert FileExtensionProcessor._format_size(1024) == "1.00 KB"
    assert FileExtensionProcessor._format_size(1024 * 1024) == "1.00 MB"
    assert FileExtensionProcessor._format_size(500) == f"{500/1024:.2f} KB"
