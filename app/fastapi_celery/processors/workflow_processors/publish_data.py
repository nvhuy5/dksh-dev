from utils.bucket_helper import get_s3_key_prefix
from utils import read_n_write_s3
from processors.processor_base import ProcessorBase,logger
from models.class_models import StatusEnum, StepOutput
from config_loader import ALLOW_TEST_SLEEP, SLEEP_DURATION
import time

def publish_data(self: ProcessorBase, data_input, response_api, *args, **kwargs) -> StepOutput: # pragma: no cover  # NOSONAR
    """
    Handles the final publishing step of a data processing workflow.
    This method evaluates the response from an external publish API call, constructs
    a data output payload, and returns a StepOutput indicating success or failure.
    """
    # This method is currently empty. It will be implemented in the future
    if ALLOW_TEST_SLEEP and SLEEP_DURATION >0: # NOSONAR
        time.sleep(SLEEP_DURATION)

    if not response_api:
        raise RuntimeError(f"Publish API did not return a valid response: {response_api}")
    
    data_output = build_publish_data_ouput(kwargs.get("connectionDto"))

    if bool(response_api.get("success")):
        data_output["sentStatus"] = "Success"

        return StepOutput(
            data=data_input.data,
            sub_data={"data_output": data_output},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None
        )
    
    data_output["sentStatus"] = "Unsend"
    failed_output = data_input.data.model_copy(
        update={"step_status": StatusEnum.FAILED, "messages": [response_api.get("message")]}
    )

    return StepOutput(
        data=failed_output,
        sub_data={"data_output": data_output},
        step_status=StatusEnum.FAILED,
         step_failure_message=[response_api.get("message")],
    )
    
def copy_file(self: ProcessorBase, data_input, response_api, *args, **kwargs) -> dict: # NOSONAR
    """
    Copy a file within or across S3 buckets and update its reference.
    This method:
        - Reads the source S3 key from `data_input.data.file_output`
        - Constructs a destination key using workflow metadata
        - Copies the file via `read_n_write_s3.copy_object_between_buckets`
        - Returns the new S3 path
    """
    
    target_bucket_name = self.file_record.get("target_bucket_name")
    s3_key_prefix = get_s3_key_prefix(self.file_record, self.tracking_model, kwargs.get("step"), is_full_prefix=False)

    # --- get file name from file_ouput ---
    file_output = data_input.data.file_output
    file_name = file_output.strip("/").split("/")[-1]
    dest_key = f"{s3_key_prefix}{file_name}"

    # --- copy file---
    result_copy_file = result_copy_file = read_n_write_s3.copy_object_between_buckets(
        source_bucket=target_bucket_name,
        source_key=file_output,
        dest_bucket=target_bucket_name,
        dest_key=dest_key,
    )

    if result_copy_file.get("status") == StatusEnum.FAILED:
        raise ValueError(f"[copy_file] error: {result_copy_file.get("error")}")
    
    data_input.data.file_output = dest_key
    return {"fileOutputLink": f"{target_bucket_name}/{dest_key}"}

def build_publish_data_ouput(conection: dict) -> dict:
    """
    Builds the data output for publishing data based on the 
    provided connection configuration.
    """
    fields = conection.get("requiredFields")["REQUIRED"]
    field_map = {item["name"]: item["value"] for item in fields}

    if conection["connectionType"] == "SFTP":
        host = field_map.get("HOST", "")
        user = field_map.get("USER_NAME", "")
        port = field_map.get("PORT", "")
        return {
            "sftp":f"{user}@{host}:{port}",
            "sentStatus": "",
            "fileLogLink": ""
        }
    else:
        return {
            "email":  field_map.get("EMAIL_ADDRESS", ""),
            "sentStatus": "",
            "fileLogLink": ""
       }