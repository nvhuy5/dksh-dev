from processors.processor_base import ProcessorBase
from models.class_models import StatusEnum, StepOutput
from processors.helpers.xml_helper import get_data_output_for_rule_mapping
from config_loader import ALLOW_TEST_SLEEP, SLEEP_DURATION
from utils.bucket_helper import get_s3_key_prefix
from utils.read_n_write_s3 import copy_object_between_buckets
import time

def rename(self: ProcessorBase, data_input, response_api, *args, **kwargs) -> StepOutput: # NOSONAR
    if ALLOW_TEST_SLEEP and SLEEP_DURATION >0: # NOSONAR
        time.sleep(SLEEP_DURATION)
        
    data_output = get_data_output_for_rule_mapping(response_api)

    # --- Extract new file name from processor arguments ---
    rename_args = data_output.get("processorArgs", [])
    new_file_name = next(
        (arg["value"] for arg in rename_args if arg["name"] == "fileName"),
        None,
    )
    if not new_file_name:
        raise ValueError("[rename] Missing argument 'fileName' for new file name")

    # --- Build S3 key prefix ---
    s3_key_prefix = get_s3_key_prefix(
        self.file_record, self.tracking_model, kwargs.get("step"), is_full_prefix=False
    )

    # --- Copy object to new key with new name ---
    source_bucket = self.file_record["target_bucket_name"]
    source_key = data_input.data.file_output
    dest_bucket = source_bucket
    dest_key = f"{s3_key_prefix.rstrip('/')}/{new_file_name}.csv"

    # --- Perform copy operation and upload csv file to s3 with new name---
    result = copy_object_between_buckets(source_bucket, source_key, dest_bucket, dest_key)
    if result["status"] != StatusEnum.SUCCESS:
        raise RuntimeError(f"Failed to copy object: {result['error']}")

    data_input.data.file_output = dest_key

    return StepOutput(
        data=data_input.data,
        sub_data={"data_output": data_output},
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )
