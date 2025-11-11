import traceback
import asyncio
from urllib.parse import urlparse
from pydantic import BaseModel
from utils.common_utils import get_step_name
from connections.be_connection import BEConnector
from processors.processor_nodes import PROCESS_DEFINITIONS
from processors.processor_base import ProcessorBase
from models.class_models import (
    ApiUrl,
    ContextData,
    StepDetail,
    WorkflowStep,
    StepDefinition,
    StatusEnum,
    StepOutput,
)
from models.tracking_models import ServiceLog, LogType, TrackingModel
from typing import Dict, Any, Callable, List, Optional, Union
from utils import log_helpers


# === Set up logging ===
logger = log_helpers.get_logger("Step Handler")


# Suppress Cognitive Complexity warning due to step-specific business logic  # NOSONAR
async def execute_step(file_processor: ProcessorBase, context_data: ContextData, full_sorted_steps: list[WorkflowStep], step: WorkflowStep) -> StepOutput:  # NOSONAR
    try:
        logger.info(f"Starting execute step: [{step.stepName}]")
        step_name = get_step_name(step.stepName)
        step_config = PROCESS_DEFINITIONS.get(step_name)
        
        if not step_config:
            raise NotImplementedError(f"The step [{step_name or step.stepName}] is not yet defined")
        
        prev_order = 0
        prev_step = None
        if step.stepOrder > 0:
            prev_order = step.stepOrder - 1
            prev_step = next((s for s in full_sorted_steps if s.stepOrder == prev_order), None)
        
        if getattr(file_processor.tracking_model, "rerun_step_id", None):
            if str(step.workflowStepId) == str(file_processor.tracking_model.rerun_step_id):
                if prev_step:
                    prev_result = file_processor.get_step_result_from_s3(step=prev_step)
                    if prev_result:
                        key_name = step_config.data_input
                        step_output = StepOutput(
                            data=prev_result,
                            sub_data={}, 
                            step_status=StatusEnum.SUCCESS,
                            step_failure_message=None,
                        )
                        if key_name:
                            context_data.processing_steps[key_name] = step_output
                    else:
                        logger.info(f"Not found data for previous step [{prev_step.stepName}]")
                else:
                    logger.info("First step - No need to load previous step")

        result_ctx_api = get_context_api(step_name)
        ctxs = result_ctx_api["ctxs"]
        required_keys = result_ctx_api["required_keys"]
        required_keys = fill_required_keys_for_request(
            required=required_keys,
            file_record=file_processor.file_record,
            step=step,
            processing_steps=context_data.processing_steps,
        )

        ctx_api_records = []
        response = None
        if not result_ctx_api:
            logger.warning(f"There is no API context for this step: {step_name}")
        else:
            for ctx in ctxs:
                url = ctx["url"](required_keys) if callable(ctx["url"]) else ctx["url"]
                method = ctx["method"]
                params = (
                    ctx["params"](required_keys)
                    if callable(ctx["params"])
                    else ctx["params"]
                )
                body = ctx["body"](required_keys) if callable(ctx["body"]) else ctx["body"]
                
                if not url:
                    result = await run_function(file_processor=file_processor, 
                                    context_data=context_data, 
                                    response=response, 
                                    step=step,
                                    step_config=step_config, 
                                    func_name=method)
                    if "extract" in ctx:
                        ctx["extract"](result, required_keys)
                    
                else:
                    connector = BEConnector(api_url=url, body_data=body, params=params)
                    response = await (connector.get() if method == "get" else connector.post())

                    if "extract" in ctx:
                        ctx["extract"](response, required_keys)

                    parsed_url = urlparse(url)
                    short_url = parsed_url.path

                    ctx_api_records.append(
                        {
                            "url": short_url,
                            "method": method.upper(),
                            "request": {"params": params, "body": body},
                            "response": response,
                        }
                    )

        context_data.step_detail[step.stepOrder].config_api = ctx_api_records

        result = await run_function(file_processor=file_processor, 
                                    context_data=context_data, 
                                    response=response, 
                                    step=step,
                                    step_config=step_config, 
                                    func_name=None)

        return result

    except Exception as e:
        full_tb = traceback.format_exception(type(e), e, e.__traceback__)
        logger.exception(
            f"Error occurred while executing step '{step_name}': {e}",
            extra={
                "service": ServiceLog.STEP_EXECUTION,
                "log_type": LogType.ERROR,
                "data": file_processor.tracking_model,
                "traceback": full_tb,
            },
        )
        return StepOutput(
            data=None,
            sub_data={},
            step_status=StatusEnum.FAILED,
            step_failure_message=[str(e)],
        )



def get_context_api(step_name: str) -> dict[str, Any] | None:

    step_name_upper = step_name.upper()
    step_map = {
        "FILE_PARSE": [
            {
                "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE.full_url(),
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
                "body": None,
            }
        ],
        "VALIDATE_HEADER": [
            {
                "url": ApiUrl.MASTERDATA_HEADER_VALIDATION.full_url(),
                "method": "get",
                "required_context": ["file_name"],
                "params": lambda ctx: {"fileName": ctx["file_name"]},
                "body": None,
            }
        ],
        "VALIDATE_DATA": [
            {
                "url": ApiUrl.MASTERDATA_COLUMN_VALIDATION.full_url(),
                "method": "get",
                "required_context": ["file_name"],
                "params": lambda ctx: {"fileName": ctx["file_name"]},
                "body": None,
            }
        ],
        "MASTER_DATA_LOAD": [
            {
                "url": ApiUrl.MASTER_DATA_LOAD_DATA.full_url(),
                "method": "post",
                "required_context": ["file_name_wo_ext", "items"],
                "params": None,
                "body": lambda ctx: {
                    "fileName": ctx["file_name_wo_ext"],
                    "data": ctx["items"],
                },
            }
        ],
        "TEMPLATE_FORMAT_VALIDATION": [
            {
                "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE.full_url(),
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
                "body": None,
                "extract": lambda resp, ctx: ctx.update(
                    {"templateFileParseId": resp[0]["templateFileParse"]["id"]}
                ),
            },
            {
                "url": lambda ctx: f"{ApiUrl.TEMPLATE_FORMAT_VALIDATION.full_url()}/{ctx['templateFileParseId']}",
                "method": "get",
                "required_context": ["templateFileParseId"],
                "params": lambda _: {},
                "body": None,
            },
        ],
        "TEMPLATE_DATA_MAPPING": [
            {
                "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE.full_url(),
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
                "body": None,
                "extract": lambda resp, ctx: ctx.update(
                    {"templateFileParseId": resp[0]["templateFileParse"]["id"]}
                ),
            },
            {
                "url": lambda ctx: f"{ApiUrl.DATA_MAPPING.full_url()}?templateFileParseId={ctx['templateFileParseId']}",
                "method": "get",
                "required_context": ["templateFileParseId"],
                "params": lambda ctx: {
                    "templateFileParseId": ctx["templateFileParseId"]
                },
                "body": None,
            },
        ],
        "TEMPLATE_PUBLISH_DATA": [
            {
                "url": lambda ctx: f"{ApiUrl.WORKFLOW_STEP.full_url()}/{ctx['workflowStepId']}",
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda _: {},
                "body": None,
                "extract": lambda resp, ctx: ctx.update(
                    {"connectionId": resp["connectionDto"]["id"]}
                ),
            },
            {
                "url": None,
                "method": "copy_file",
                "required_context": ["fileOutputLink"],
                "params": None,
                "body": None,
                "extract": lambda result, ctx: ctx.update(
                    {"fileOutputLink": result["fileOutputLink"]}
                ),
            },
            {
                "url": ApiUrl.TEMPLATE_PUBLISH_DATA.full_url(),
                "method": "post",
                "required_context": ["connectionId", "fileOutputLink"],
                "params": None,
                "body": lambda ctx: {
                    "connectionId": ctx["connectionId"],
                    "fileOutputLink": ctx["fileOutputLink"],
                }
            }
        ],
        "METADATA_EXTRACT": [
            {
                "url": lambda ctx: f"{ApiUrl.WORKFLOW_STEP.full_url()}/{ctx['workflowStepId']}",
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda _: {},
                "body": None,
            }
        ],
        "XSL_TRANSLATION": [
            {
                "url": lambda ctx: f"{ApiUrl.WORKFLOW_STEP.full_url()}/{ctx['workflowStepId']}",
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda _: {},
                "body": None,
            }
        ],
        "SUBMIT": [
            {
                "url": lambda ctx: f"{ApiUrl.WORKFLOW_STEP.full_url()}/{ctx['workflowStepId']}",
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda _: {},
                "body": None,
            }
        ],
        "SEND_TO": [
            {
                "url": lambda ctx: f"{ApiUrl.WORKFLOW_STEP.full_url()}/{ctx['workflowStepId']}",
                "method": "get",
                "required_context": ["workflowStepId"],
                "params": lambda _: {},
                "body": None,
            }
        ],
        "RENAME": [
            {
                "url": lambda ctx: f"{ApiUrl.WORKFLOW_STEP.full_url()}/{ctx['workflowStepId']}",
                "method": "get",
                "required_context": ["workflowStepId"],
                "params":  lambda _: {},
                "body": None,
            }
        ],
    }

    for key, ctxs in step_map.items():
        if key in step_name_upper:
            required_keys = {
                key: None for ctx in ctxs for key in ctx.get("required_context", [])
            }
            return {
                "ctxs": ctxs,
                "required_keys": required_keys,
            }

    return None


def fill_required_keys_for_request(
    required: Dict[str, Any],
    file_record: Dict[str, Any],
    step: BaseModel,
    processing_steps: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Fill required fields with values from file_record, step, and processing_steps.
    Priority: file_record > step > processing_steps.
    """
    # Convert Pydantic model to dict
    step_data = step.model_dump()
    
    for key in required.keys():
        if key in file_record:
            required[key] = file_record[key]
        elif key in step_data:
            required[key] = step_data[key]
        else:
            for _, step_info in processing_steps.items():
                if key in step_info:
                    required[key] = step_info[key]
                    break
                elif hasattr(step_info, "data"):
                    data = step_info.data
                    if hasattr(data,key):
                        required[key] = getattr(data,key)
                        break 
    return required


def fill_required_keys_from_response(
    response: Union[Dict[str, Any], List[Dict[str, Any]]], required: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Fill values from response into required

    Args:
        response: API response (dict or list of dicts).
        required: Dict with keys to fill (values may be None).

    Returns:
        Updated dict with matched keys filled from response.
    """
    if isinstance(response, dict):
        # Directly map keys from dict response
        for key, value in response.items():
            if key in required and not required[key]:
                    required[key] = value
    elif isinstance(response, list):
        # Iterate all dicts in list, overwrite if key repeats
        for item in response:
            if isinstance(item, dict):
                for key, value in item.items():
                    if key in required and not required[key]:
                            required[key] = value

    return required


async def run_function(
    file_processor: ProcessorBase, 
    context_data: ContextData, 
    response: Any, 
    step: WorkflowStep,
    step_config: StepDefinition | None, 
    func_name: str | None) -> Any:
    """
    Execute a target function from the given file processor (sync or async).

    The function name is resolved from `func_name` or `step_config.function_name`.
    Input data is fetched from `context_data.processing_steps` based on 
    `step_config.data_input`, and the result is stored back using `data_output`.

    Raises:
        AttributeError: If the target function is not found or not callable.

    Returns:
        Any: The result produced by the executed function.
    """
    if not func_name:
        func_name = step_config.function_name
    function = getattr(file_processor, func_name, None)

    if function is None or not callable(function):
        raise AttributeError(f"Function '{func_name}' not found in FileProcessor.")

    args = []
    kwargs = fill_required_keys_from_response(response, step_config.kwargs)
    data_input = (
        context_data.processing_steps.get(step_config.data_input)
        if getattr(step_config, "data_input", None)
        else None
    )

    kwargs["step"] = step
    result = (
        await function(data_input, response, *args, **kwargs)
        if asyncio.iscoroutinefunction(function)
        else function(data_input, response, *args, **kwargs)
    )

    key_name = step_config.data_output
    if key_name:
        context_data.processing_steps[key_name] = result

    return result