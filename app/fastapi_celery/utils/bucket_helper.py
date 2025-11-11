import config_loader
from datetime import datetime, timezone
from processors.processor_nodes import BUCKET_MAP, PROCESS_DEFINITIONS
from utils.common_utils import get_step_name
from models.class_models import DocumentType, WorkflowStep
from models.tracking_models import TrackingModel


def get_bucket_name(
    document_type: DocumentType,
    bucket_type: str,
    project_name: str,
    sap_masterdata: bool | None = None,
):
    """
    Retrieve the corresponding S3 bucket name based on document type, bucket type,
    project name, and SAP master data flag.

    Raises:
        ValueError: If the bucket name cannot be resolved due to invalid inputs or missing mappings.
    """
    try:
        project_name = project_name.upper()
        buckets = BUCKET_MAP.get(bucket_type)
        if not buckets:
            raise KeyError(f"Unknown bucket_type '{bucket_type}'")

        # raw_bucket
        if bucket_type == "raw_bucket":
            bucket = buckets.get(project_name)
            if not bucket:
                raise KeyError(f"Project '{project_name}' not found in '{bucket_type}'")
            return config_loader.get_config_value("s3_buckets", bucket)

        # target_bucket
        doc_data = buckets.get(document_type.value)
        if not doc_data:
            raise KeyError(f"Unsupported document_type '{document_type.value}' for '{bucket_type}'")

        key = (
            "sap_masterdata"
            if document_type == DocumentType.MASTER_DATA and sap_masterdata
            else project_name
        )
        bucket = doc_data.get(key)
        if not bucket:
            raise KeyError(f"No bucket found for key '{key}' in '{document_type.value}'")

        return config_loader.get_config_value("s3_buckets", bucket)

    except Exception as e:
        raise ValueError(f"Failed to resolve bucket name: {e}")


def get_s3_key_prefix(
    file_record: dict,
    tracking_model: TrackingModel,
    step: WorkflowStep | None = None,
    target_folder: str | None = None,
    is_full_prefix: bool | None = True,
    version_folder: str | None = None,
) -> str:

    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    step_order = f"{int(step.stepOrder):02}" if step else ""

    file_name = file_record.get("file_name")
    file_name_wo_ext = file_record.get("file_name_wo_ext")
    is_master = False

    if file_record.get("document_type") == DocumentType.MASTER_DATA:
        prefix_part = file_name_wo_ext
        is_master = True
    else:
        folder = file_record.get("folder_name")
        customer = file_record.get("customer_foldername")
        prefix_part = f"{folder}/{customer}"

    if tracking_model.rerun_attempt:
        object_name = f"{file_name_wo_ext}_rerun_{tracking_model.rerun_attempt}.json"
    else:
        object_name = f"{file_name_wo_ext}.json"
        
    step_name = get_step_name(step.stepName)
    
    if step_name:
        step_config = PROCESS_DEFINITIONS.get(step_name)
    else:
        return (
            f"workflow-node-materialized/"
            f"{prefix_part}/{date_str}/"
            f"{tracking_model.request_id}/"
            f"{step_order}_{step.stepName}/"
            f"{object_name}"
        )

    if not target_folder and is_full_prefix:
        target_folder = step_config.target_store_data
        return (
            f"{target_folder}/"
            f"{prefix_part}/{date_str}/"
            f"{tracking_model.request_id}/"
            f"{step_order}_{step.stepName}/"
            f"{object_name}"
        )
    
    elif not target_folder and not is_full_prefix:
        target_folder = step_config.target_store_data
        return (
            f"{target_folder}/"
            f"{prefix_part}/{date_str}/"
            f"{tracking_model.request_id}/"
            f"{step_order}_{step.stepName}/"
        )
    
    elif is_master and target_folder == "master_data":
        return (
            f"{target_folder}/"
            f"{file_name_wo_ext}/"
            f"{file_name}"
        )
    
    elif is_master and target_folder == "process_data":
        return (
            f"{target_folder}/"
            f"{file_name_wo_ext}/"
            f"{file_name_wo_ext}_{file_record['proceed_at']}.json"
        )
    
    elif is_master and target_folder == "versioning":
        return (
            f"{target_folder}/"
            f"{file_name_wo_ext}/"
            f"{version_folder}/"
            f"{file_name}"
        )
