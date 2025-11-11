import pytest
from unittest.mock import patch, MagicMock
from utils import bucket_helper
from fastapi_celery.models.class_models import DocumentType
from fastapi_celery.models.tracking_models import TrackingModel

# -------------------------------
# Fixtures / mocks
# -------------------------------
@pytest.fixture
def tracking_model():
    return TrackingModel(
        request_id="REQ-1",
        file_path="dummy.txt",
        project_name="PROJ-1",
        sap_masterdata=True,
        rerun_attempt=2
    )

MOCK_BUCKET_MAP = {
    "raw_bucket": {
        "PROJ-1": "raw_proj1_bucket"
    },
    "target_bucket": {
        "master_data": {"sap_masterdata": "master_data_bucket"},
        "order": {"PROJ-1": "order_proj1_bucket"}
    }
}

MOCK_PROCESS_DEFINITIONS = {
    "STEP_X": MagicMock(target_store_data="process_data")
}

# -------------------------------
# Tests for get_bucket_name
# -------------------------------
def test_get_bucket_name_raw_bucket():
    with patch("utils.bucket_helper.BUCKET_MAP", MOCK_BUCKET_MAP), \
         patch("utils.bucket_helper.config_loader.get_config_value", return_value="RAW_BUCKET_NAME"):
        bucket = bucket_helper.get_bucket_name(DocumentType.MASTER_DATA, "raw_bucket", "PROJ-1")
        assert bucket == "RAW_BUCKET_NAME"

def test_get_bucket_name_target_bucket_master_data():
    with patch("utils.bucket_helper.BUCKET_MAP", MOCK_BUCKET_MAP), \
         patch("utils.bucket_helper.config_loader.get_config_value", return_value="MASTER_BUCKET"):
        bucket = bucket_helper.get_bucket_name(DocumentType.MASTER_DATA, "target_bucket", "PROJ-1", sap_masterdata=True)
        assert bucket == "MASTER_BUCKET"

def test_get_bucket_name_target_bucket_order():
    with patch("utils.bucket_helper.BUCKET_MAP", MOCK_BUCKET_MAP), \
         patch("utils.bucket_helper.config_loader.get_config_value", return_value="ORDER_BUCKET"):
        bucket = bucket_helper.get_bucket_name(DocumentType.ORDER, "target_bucket", "PROJ-1")
        assert bucket == "ORDER_BUCKET"

def test_get_bucket_name_invalid_project():
    with patch("utils.bucket_helper.BUCKET_MAP", MOCK_BUCKET_MAP):
        with pytest.raises(ValueError):
            bucket_helper.get_bucket_name(DocumentType.ORDER, "raw_bucket", "INVALID_PROJ")

def test_get_bucket_name_invalid_document_type():
    with patch("utils.bucket_helper.BUCKET_MAP", MOCK_BUCKET_MAP):
        with pytest.raises(ValueError):
            bucket_helper.get_bucket_name(DocumentType.MASTER_DATA, "target_bucket", "PROJ-1", sap_masterdata=False)

# -------------------------------
# Tests for get_s3_key_prefix
# -------------------------------
@pytest.fixture
def file_record_master():
    return {
        "file_name": "file.txt",
        "file_name_wo_ext": "file",
        "document_type": DocumentType.MASTER_DATA,
        "proceed_at": "20231030"
    }

@pytest.fixture
def file_record_order():
    return {
        "file_name": "file.txt",
        "file_name_wo_ext": "file",
        "document_type": DocumentType.ORDER,
        "folder_name": "order_folder",
        "customer_foldername": "customerA"
    }

@pytest.fixture
def workflow_step():
    step = MagicMock()
    step.stepName = "STEP_X"
    step.stepOrder = 1
    return step

def test_get_s3_key_prefix_master_data(tracking_model, file_record_master, workflow_step):
    with patch("utils.bucket_helper.get_step_name", return_value="STEP_X"), \
         patch("utils.bucket_helper.PROCESS_DEFINITIONS", MOCK_PROCESS_DEFINITIONS):
        prefix = bucket_helper.get_s3_key_prefix(file_record_master, tracking_model, workflow_step, target_folder="master_data")
        assert prefix.startswith("master_data/file/")

def test_get_s3_key_prefix_order(tracking_model, file_record_order, workflow_step):
    with patch("utils.bucket_helper.get_step_name", return_value="STEP_X"), \
         patch("utils.bucket_helper.PROCESS_DEFINITIONS", MOCK_PROCESS_DEFINITIONS):
        prefix = bucket_helper.get_s3_key_prefix(file_record_order, tracking_model, workflow_step)
        assert "process_data/order_folder/customerA/" in prefix

def test_get_s3_key_prefix_no_step_name(tracking_model, file_record_order, workflow_step):
    with patch("utils.bucket_helper.get_step_name", return_value=None):
        prefix = bucket_helper.get_s3_key_prefix(file_record_order, tracking_model, workflow_step)
        assert prefix.startswith("workflow-node-materialized/order_folder/customerA/")

def test_get_s3_key_prefix_rerun(tracking_model, file_record_master, workflow_step):
    tracking_model.rerun_attempt = 3
    with patch("utils.bucket_helper.get_step_name", return_value="STEP_X"), \
         patch("utils.bucket_helper.PROCESS_DEFINITIONS", MOCK_PROCESS_DEFINITIONS):
        # use target_folder=None to let function choose default folder
        prefix = bucket_helper.get_s3_key_prefix(file_record_master, tracking_model, workflow_step, target_folder=None)
        assert "_rerun_3.json" in prefix
