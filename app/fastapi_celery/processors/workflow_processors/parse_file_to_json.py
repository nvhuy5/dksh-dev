from models.class_models import StatusEnum, StepOutput
from models.tracking_models import ServiceLog, LogType
from processors.processor_registry import ProcessorRegistry
from processors.processor_base import ProcessorBase, logger
from config_loader import ALLOW_TEST_SLEEP, SLEEP_DURATION
import time


def parse_file_to_json(self: ProcessorBase, data_input, schema_object, response_api, *args, **kwargs) -> StepOutput:  # NOSONAR
    """
    Parses a file to JSON using the appropriate processor based on template code
    """
    if ALLOW_TEST_SLEEP and SLEEP_DURATION > 0:  # NOSONAR
        time.sleep(SLEEP_DURATION)

    data_output = {
        "totalRecords": 0,
        "storageLocation": self.file_record["file_path"],
        "fileLogLink": "",
    }

    try:
        template_info = response_api[0].get("templateFileParse", {})
        template_code = template_info.get("code")

        processor_enum = ProcessorRegistry.get_processor_for_file(template_code)
        processor_instance = processor_enum.create_instance(self.file_record)
        data = processor_instance.parse_file_to_json()

        data_output["totalRecords"] = len(data.items)

        return StepOutput(
            data=data,
            sub_data={"data_output": data_output},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

    except Exception as e:
        logger.error(
            f"[parse_file_to_json] An error occurred: {e}",
            extra={
                "service": ServiceLog.DOCUMENT_PARSER,
                "log_type": LogType.ERROR,
                "data": self.tracking_model,
            },
            exc_info=True,
        )
        return StepOutput(
            data=schema_object.model_copy(
                update={
                    "messages" : [f"[parse_file_to_json] An error occurred: {e}"]
                }
            ),
            sub_data={"data_output": data_output},
            step_status=StatusEnum.FAILED,
            step_failure_message=[f"[parse_file_to_json] An error occurred: {e}"],
        )
