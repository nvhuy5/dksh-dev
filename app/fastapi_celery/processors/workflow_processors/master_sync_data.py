from models.class_models import StatusEnum, StepOutput


def master_sync_data(self, data_input, response_api, *args, **kwargs) -> StepOutput: # NOSONAR

    return StepOutput(
        data=data_input.data,
        sub_data={},
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )