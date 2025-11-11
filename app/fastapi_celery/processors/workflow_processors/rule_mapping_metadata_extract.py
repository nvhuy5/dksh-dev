from processors.processor_base import ProcessorBase
from models.class_models import StatusEnum, StepOutput
from processors.helpers.xml_helper import get_data_output_for_rule_mapping
from config_loader import ALLOW_TEST_SLEEP, SLEEP_DURATION
import time

def metadata_extract(self: ProcessorBase, data_input, response_api, *args, **kwargs) -> StepOutput: # NOSONAR
    if ALLOW_TEST_SLEEP and SLEEP_DURATION >0: # NOSONAR
        time.sleep(SLEEP_DURATION)
        
    data_output = get_data_output_for_rule_mapping(response_api)

    return StepOutput(
        data=data_input.data,
        sub_data={"data_output": data_output},
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )
