import traceback
from models.class_models import (
    StatusEnum,
    StepOutput,
    WorkflowStep,
)
from utils.bucket_helper import get_s3_key_prefix
from models.tracking_models import ServiceLog, LogType
from utils import read_n_write_s3
from processors.helpers import template_helper
from processors.processor_base import ProcessorBase, logger


def write_json_to_s3(self: ProcessorBase, step_result, s3_key_prefix: str) -> StepOutput:
    """
    Write JSON data to S3 and return step output with SUCCESS status.
    """
    try:
        result = read_n_write_s3.write_json_to_s3(
            json_data=step_result,
            bucket_name=self.file_record["target_bucket_name"],
            s3_key_prefix=s3_key_prefix,
        )

        logger.info(
            f"[write_json_to_s3] Successfully wrote JSON file to S3 "
            f"(bucket='{self.file_record['target_bucket_name']}', prefix='{s3_key_prefix}').",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "data": self.tracking_model,
            },
        )

        return StepOutput(
            data=result,
            sub_data={},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

    except Exception as e:
        full_tb = traceback.format_exception(type(e), e, e.__traceback__)
        logger.error(
            "[write_json_to_s3] Failed to write JSON to S3.",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                "data": self.tracking_model,
                "traceback": full_tb,
            },
            exc_info=True,
        )
        raise


def get_step_result_from_s3(self: ProcessorBase, step: WorkflowStep):
    """
    Load step result from S3 and parse it into a BaseModel if the file exists and status is SUCCESS.
    """

    s3_key_prefix = get_s3_key_prefix(
        file_record=self.file_record,
        tracking_model=self.tracking_model,
        step=step,
        target_folder=None,
        is_full_prefix=False,
        version_folder=None,
    )

    bucket_name = self.file_record.get("target_bucket_name")
    keys = read_n_write_s3.list_objects_with_prefix(bucket_name=bucket_name, prefix=s3_key_prefix)
    key = read_n_write_s3.select_latest_rerun(keys=keys, base_filename=self.file_record.get("file_name_wo_ext"))
    data = read_n_write_s3.read_json_from_s3(bucket_name=bucket_name, object_name=key)

    if data is None:
        logger.info(
            f"[get_step_result_from_s3] No S3 file found for step '{step.stepName}' "
            f"(attempt {self.tracking_model.rerun_attempt}). Rerun required.",
            extra={"service": ServiceLog.FILE_STORAGE, "data": self.tracking_model},
        )
        return None

    step_status = data.get("step_status")
    data.update(json_output=key)

    if StatusEnum(step_status) == StatusEnum.SUCCESS:
        logger.info(
            f"[get_step_result_from_s3] Step [{step.stepName}]"
            f"already completed successfully. Skipping rerun.",
            extra={"service": ServiceLog.FILE_STORAGE, "data": self.tracking_model},
        )
    else:
        logger.info(
            f"[get_step_result_from_s3] Step [{step.stepName}]"
            f"has status={step_status}. Rerun required.",
            extra={"service": ServiceLog.FILE_STORAGE, "data": self.tracking_model},
        )

    return template_helper.parse_data(
        document_type=self.file_record.get("document_type"),
        data=data,
    )
