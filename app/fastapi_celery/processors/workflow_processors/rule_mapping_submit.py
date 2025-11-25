from processors.processor_base import ProcessorBase, logger
from models.class_models import StatusEnum, StepOutput
from processors.helpers.xml_helper import get_data_output_for_rule_mapping
from config_loader import ALLOW_TEST_SLEEP, SLEEP_DURATION
import time

def submit(self: ProcessorBase, data_input, schema_object, response_api, *args, **kwargs) -> StepOutput: # NOSONAR
    if ALLOW_TEST_SLEEP and SLEEP_DURATION >0: # NOSONAR
        time.sleep(SLEEP_DURATION)
        
    data_output = get_data_output_for_rule_mapping(response_api)
    try:
        return StepOutput(
            data=data_input.data,
            sub_data={"data_output": data_output},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )
    except Exception as e:
        logger.exception(f"[submit] An error occurred: {e}", exc_info=True)
        error_msg = (
            "[submit] missing data_input from previous step"
            if data_input is None
            else f"[submit] An error occurred: {e}"
        )
        return StepOutput(
            data=schema_object.model_copy(
                update={
                    "messages" : [error_msg]
                }
            ),
            sub_data={"data_output": data_output},
            step_status=StatusEnum.FAILED,
            step_failure_message=[error_msg],
        )
