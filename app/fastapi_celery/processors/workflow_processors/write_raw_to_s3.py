import re
import traceback
from utils.bucket_helper import get_s3_key_prefix
from processors.processor_base import ProcessorBase, logger
from models.class_models import StepOutput, StatusEnum
from models.tracking_models import ServiceLog, LogType
from utils import read_n_write_s3


def write_raw_to_s3(self: ProcessorBase) -> StepOutput:
    """
    Copy the raw input file from the raw S3 bucket to the target bucket,
    and create a versioned backup under the 'versioning/' folder.
    """
    try:
        # === Generate target key for master_data ===
        file_name = self.file_record.get("file_name")
        file_name_wo_ext = self.file_record.get("file_name_wo_ext")
        s3_key_prefix = f"master_data/{file_name_wo_ext}/{file_name}"

        # === Copy file from raw → target bucket ===
        result = read_n_write_s3.copy_object_between_buckets(
            source_bucket=self.file_record.get("raw_bucket_name"),
            source_key=self.file_record.get("file_path"),
            dest_bucket=self.file_record.get("target_bucket_name"),
            dest_key=s3_key_prefix,
        )

        # === Determine next version number ===
        version_prefix = f"versioning/{file_name_wo_ext}/"
        existing_keys = read_n_write_s3.list_objects_with_prefix(
            bucket_name=self.file_record.get("target_bucket_name"),
            prefix=version_prefix,
        )

        version_number = 1
        if existing_keys:
            numbers = []
            for key in existing_keys:
                match = re.search(
                    rf"versioning/{re.escape(file_name_wo_ext)}/(\d{{3}})/",
                    key,
                )
                if match:
                    numbers.append(int(match.group(1)))
            if numbers:
                version_number = max(numbers) + 1

        version_folder = f"{version_number:03d}"
        version_key = f"{version_prefix}{version_folder}/{file_name}"

        # === Copy file to versioning folder ===
        read_n_write_s3.copy_object_between_buckets(
            source_bucket=self.file_record.get("raw_bucket_name"),
            source_key=self.file_record.get("file_path"),
            dest_bucket=self.file_record.get("target_bucket_name"),
            dest_key=version_key,
        )

        # === Log success ===
        logger.info(
            f"[write_raw_to_s3] Successfully copied raw file to target bucket "
            f"(bucket='{self.file_record.get('target_bucket_name')}', key='{s3_key_prefix}'). "
            f"Created version backup: '{version_key}'.",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
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
            f"[write_raw_to_s3] Failed to copy raw file to S3: {e}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                "data": self.tracking_model,
                "traceback": full_tb,
            },
            exc_info=True,
        )
        raise
