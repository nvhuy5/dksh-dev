from utils import read_n_write_s3
from utils.bucket_helper import get_s3_key_prefix
from utils.common_utils import get_csv_buffer_file
from processors.processor_base import ProcessorBase, logger
from models.class_models import StatusEnum, StepOutput
from processors.helpers.xml_helper import get_data_output_for_rule_mapping
from config_loader import ALLOW_TEST_SLEEP, SLEEP_DURATION
import time


def xsl_translation(self: ProcessorBase, data_input, response_api, *args, **kwargs) -> StepOutput: # NOSONAR
    if ALLOW_TEST_SLEEP and SLEEP_DURATION >0: # NOSONAR
        time.sleep(SLEEP_DURATION)
        
    data_output = get_data_output_for_rule_mapping(response_api)

    try:
        # --- Get buffer data ---
        csv_buffer = get_csv_buffer_file(data_input=data_input)

        # --- Build S3 key ---
        s3_key_prefix = get_s3_key_prefix(self.file_record, self.tracking_model, kwargs.get("step"), is_full_prefix=False)
        file_name_wo_ext = self.file_record.get("file_name_wo_ext")
        s3_key_csv = f"{s3_key_prefix.rstrip('/')}/{file_name_wo_ext}.csv"

        # --- Upload to S3 ---
        upload_result = read_n_write_s3.write_file_to_s3(
            file_bytes=csv_buffer,
            bucket_name=self.file_record["target_bucket_name"],
            s3_key_prefix=s3_key_csv,
        )

        if upload_result.get("status") != StatusEnum.SUCCESS:
            raise RuntimeError(f"{upload_result.get('error')}")

        logger.info(f"[xsl_translation] Upload successful: {s3_key_csv}")
        data_input.data.file_output = s3_key_csv

        return StepOutput(
            data=data_input.data,
            sub_data={"data_output": data_output},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

    except Exception as e:
        logger.exception(f"[xsl_translation] An error occurred: {e}", exc_info=True)
        return StepOutput(
            data=None,
            sub_data={"data_output": data_output},
            step_status=StatusEnum.FAILED,
            step_failure_message=[f"[xsl_translation] An error occurred: {e}"],
        )
