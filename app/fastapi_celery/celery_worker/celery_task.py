import sys
import json
import asyncio
import traceback
import contextvars
from pathlib import Path
from typing import Any
from dataclasses import asdict

from celery import shared_task
from pydantic import BaseModel

from utils import log_helpers
from utils.bucket_helper import get_bucket_name, get_s3_key_prefix
from celery_worker.step_handler import execute_step

from connections.be_connection import BEConnector
from connections.redis_connection import RedisConnector

from processors.processor_base import ProcessorBase
from processors.helpers import template_helper

from models.body_models import (
    WorkflowFilterBody,
    WorkflowSessionFinishBody,
    WorkflowSessionStartBody,
    WorkflowStepFinishBody,
    WorkflowStepStartBody,
)
from models.class_models import (
    ContextData,
    FilePathRequest,
    StepDetail,
    StepDetailConfig,
    WorkflowDetailConfig,
    WorkflowModel,
    ApiUrl,
    StatusEnum,
    WorkflowSession,
    StartStep,
    DocumentType,
    StepOutput,
    WorkflowStep,
)
from models.tracking_models import ServiceLog, LogType, TrackingModel

import config_loader


# === Logging & Config ===
logger = log_helpers.get_logger("Celery Task Execution")
sys.path.append(str(Path(__file__).resolve().parent.parent))
types_list = json.loads(config_loader.get_config_value("support_types", "types"))


@shared_task(bind=True)
def task_execute(self, data: dict) -> str: # pragma: no cover  # NOSONAR
    """
    Celery task entry point (sync wrapper).
    Runs the main async file-processing workflow via `asyncio.run`.
    Args:
        data (dict): File path and metadata payload.
    Returns:
        str: Task status message.
    """
    try:
        file_request = FilePathRequest(**data)
        tracking_model = TrackingModel.from_data_request(file_request)
        logger.info(
            f"[{tracking_model.request_id}] Starting task execution",
            extra={
                "service": ServiceLog.TASK_EXECUTION,
                "log_type": LogType.TASK,
                "data": tracking_model,
            },
        )
        ctx = contextvars.copy_context()
        ctx.run(lambda: asyncio.run(handle_task(tracking_model)))
        return "Task completed"
    
    except Exception as e:
        # Capture full traceback as structured string list
        full_tb = traceback.format_exception(type(e), e, e.__traceback__)
        logger.error(
            f"[{tracking_model.request_id}] Task execution failed: {e}",
            extra={
                "service": ServiceLog.TASK_EXECUTION,
                "log_type": LogType.ERROR,
                "data": tracking_model,
                "traceback": full_tb,
            },
        )


async def handle_task(tracking_model: TrackingModel) -> dict[str, Any]: # pragma: no cover  # NOSONAR
    """
    Run the asynchronous file processing workflow.
    Args:
        tracking_model (TrackingModel): Contains file info, Celery task ID, and tracking metadata.
    Returns:
        dict[str, Any]: Extracted or processed data results.
    """

    # === Pre-Processing ===
    logger.info("Start processing file")
    
    redis_connector = RedisConnector()
    file_processor = ProcessorBase(tracking_model)
    file_processor.run()

    logger.info("File_extraction result",
        extra={
            "service": ServiceLog.FILE_EXTRACTION,
            "log_type": LogType.TASK,
            "data": file_processor.file_record,
        },
    )
    tracking_model.document_type = DocumentType(file_processor.file_record["document_type"]).name
    context_data = ContextData(request_id=tracking_model.request_id)

    # === Fetch workflow ===
    workflow_model = await get_workflow_filter(
        context_data=context_data,
        file_processor=file_processor,
        tracking_model=tracking_model,
    )

    # === Update target_bucket_name ===
    file_processor.file_record["target_bucket_name"] = get_bucket_name(
        file_processor.file_record["document_type"],
        "target_bucket",
        tracking_model.project_name,
        tracking_model.sap_masterdata,
    )

    logger.info("Workflow_filter result",
        extra={
            "service": ServiceLog.CALL_BE_API,
            "log_type": LogType.ACCESS,
            "data": workflow_model,
        },
    )
    
    logger.info("Updated tracking_model object",
        extra={
            "service": ServiceLog.TASK_EXECUTION,
            "log_type": LogType.ACCESS,
            "data": tracking_model,
        },
    )

    # === Start session ===
    start_session_model = await call_workflow_session_start(
        context_data=context_data,
        tracking_model=tracking_model,
    )

    logger.info("Session_start result",
        extra={
            "service": ServiceLog.CALL_BE_API,
            "log_type": LogType.ACCESS,
            "data": start_session_model,
        },
    )

    # === Update Redis ===
    redis_connector.store_workflow_id(
        task_id=tracking_model.request_id,
        workflow_id=workflow_model.id,
        status=StatusEnum.PROCESSING,
    )

    try:
        # === Process steps ===
        # Sort steps in ascending order by stepOrder
        full_sorted_steps = sorted(workflow_model.workflowSteps, key=lambda step: step.stepOrder)
        if tracking_model.rerun_step_id:
            rerun_step = next(
                (s for s in full_sorted_steps if s.workflowStepId == tracking_model.rerun_step_id),
                None
            )
            rerun_index = rerun_step.stepOrder
            sorted_steps = full_sorted_steps[rerun_index:]
        else:
            sorted_steps = full_sorted_steps
            
        status_step_result = True
        for step in sorted_steps:
            # === Start step ===
            _ = await call_workflow_step_start(
                context_data=context_data,
                step=step,
            )

            # === Update Redis ===
            redis_connector.store_step_status(
                task_id=tracking_model.request_id,
                step_name=step.stepName,
                status=StatusEnum.PROCESSING,
                step_id=step.workflowStepId,
            )

            # === Execute step ===
            step_result = await execute_step(file_processor, context_data, full_sorted_steps, step)
            
            logger.info("Result of execute_step",
                extra={
                    "service": ServiceLog.TASK_EXECUTION,
                    "log_type": LogType.ACCESS,
                    "data": step_result.step_status,
                },
            )
            
            s3_key_prefix = get_s3_key_prefix(file_processor.file_record, tracking_model, step)
            
            # === Update Redis ===
            redis_connector.store_step_status(
                task_id=tracking_model.request_id,
                step_name=step.stepName,
                status=step_result.step_status,
                step_id=step.workflowStepId,
            )

            # === Finish step ===
            _ = await call_workflow_step_finish(
                context_data=context_data,
                target_bucket_name=file_processor.file_record["target_bucket_name"],
                s3_key_prefix=s3_key_prefix,
                step=step,
                step_result=step_result
            )
            
            if step_result.data:
                # inject_metadata_into_step_result
                inject_metadata_into_step_result(
                    step=step, 
                    step_result=step_result, 
                    context_data=context_data,
                    s3_key_prefix=s3_key_prefix,
                    document_type=file_processor.file_record["document_type"]
                )
                
            status_step_result = step_result.step_status == StatusEnum.SUCCESS
            
            # === Store data in AWS S3 ===
            store_data_in_s3(file_processor=file_processor, step_result=step_result, s3_key_prefix=s3_key_prefix)

            if not status_step_result:
                break


        # === Update Redis ===
        redis_connector.store_workflow_id(
            task_id=tracking_model.request_id,
            workflow_id=workflow_model.id,
            status=step_result.step_status,
        )

        # === Finish session ===
        _ = await call_workflow_session_finish(
            context_data=context_data, 
            status_step_result=status_step_result
        )
                
        # === Store final processed data in AWS S3 ===
        save_raw = file_processor.file_record.get("document_type") == DocumentType.MASTER_DATA
        store_data_in_s3(
            file_processor=file_processor, 
            step_result=step_result, 
            s3_key_prefix=s3_key_prefix,
            save_raw=save_raw
        )

        return context_data

    except Exception:
        # Update Redis
        redis_connector.store_workflow_id(
            task_id=tracking_model.request_id,
            workflow_id=workflow_model.id,
            status=StatusEnum.FAILED,
        )

        # Reraise the original exception to keep full traceback
        raise


async def get_workflow_filter(
    context_data: ContextData,
    file_processor: ProcessorBase,
    tracking_model: TrackingModel,
):
    logger.info("Running workflow_filter")
    body_data = asdict(WorkflowFilterBody(
        filePath=file_processor.file_record["file_path_parent"],
        fileName=file_processor.file_record["file_name"],
        fileExtension=file_processor.file_record["file_extension"],
        project=tracking_model.project_name,
        source=tracking_model.source_name,
    ))
    workflow_connector = BEConnector(ApiUrl.WORKFLOW_FILTER.full_url(), body_data=body_data)
    workflow_response = await workflow_connector.post()
    if not workflow_response:
        raise RuntimeError("Failed to fetch workflow")

    workflow_model = WorkflowModel(**workflow_response)
    if not workflow_model:
        raise RuntimeError("Failed to initialize WorkflowModel from response")

    context_data.workflow_detail = WorkflowDetailConfig()
    context_data.workflow_detail.filter_api.url = ApiUrl.WORKFLOW_FILTER.full_url()
    context_data.workflow_detail.filter_api.method = "POST"
    context_data.workflow_detail.filter_api.request = body_data
    context_data.workflow_detail.filter_api.response = workflow_model

    tracking_model.workflow_id = workflow_model.id
    tracking_model.workflow_name = workflow_model.name
    tracking_model.sap_masterdata = bool(workflow_model.sapMasterData)

    file_processor.file_record["folder_name"] = workflow_model.folderName
    file_processor.file_record["customer_foldername"] = workflow_model.customerFolderName

    return workflow_model


async def call_workflow_session_start(
    context_data: ContextData,
    tracking_model: TrackingModel,
):
    logger.info("Running session_start")
    body_data = asdict(WorkflowSessionStartBody(
        workflowId=tracking_model.workflow_id,
        celeryId=tracking_model.request_id,
        filePath=tracking_model.file_path,
    ))
    session_connector = BEConnector(ApiUrl.WORKFLOW_SESSION_START.full_url(), body_data=body_data)
    session_response = await session_connector.post()
    if not session_response:
        raise RuntimeError("Failed to fetch workflow_session_start")

    start_session_model = WorkflowSession(**session_response)
    if not start_session_model:
        raise RuntimeError("Failed to initialize WorkflowSession from response")

    context_data.workflow_detail.metadata_api.session_start_api.url = ApiUrl.WORKFLOW_SESSION_START.full_url()
    context_data.workflow_detail.metadata_api.session_start_api.method = "POST"
    context_data.workflow_detail.metadata_api.session_start_api.request = body_data
    context_data.workflow_detail.metadata_api.session_start_api.response = start_session_model

    return start_session_model


async def call_workflow_session_finish(context_data: ContextData, status_step_result: bool):

    logger.info("Running session_finish")

    body_data = asdict(WorkflowSessionFinishBody(
        id=context_data.workflow_detail.metadata_api.session_start_api.response.id,
        code=StatusEnum.SUCCESS.value if status_step_result else StatusEnum.FAILED.value,
        message=""
    ))
    session_connector = BEConnector(ApiUrl.WORKFLOW_SESSION_FINISH.full_url(), body_data=body_data)
    session_response = await session_connector.post()
    if not session_response:
        raise RuntimeError("Failed to fetch workflow_session_finish")

    context_data.workflow_detail.metadata_api.session_finish_api.url = ApiUrl.WORKFLOW_SESSION_FINISH.full_url()
    context_data.workflow_detail.metadata_api.session_finish_api.method = "POST"
    context_data.workflow_detail.metadata_api.session_finish_api.request = body_data
    context_data.workflow_detail.metadata_api.session_finish_api.response = session_response

    return session_response


async def call_workflow_step_start(
    context_data: ContextData,
    step: WorkflowStep,
):

    logger.info(f"Running step_start [{step.stepName}]")
    body_data = asdict(WorkflowStepStartBody(
        sessionId=context_data.workflow_detail.metadata_api.session_start_api.response.id,
        stepId=step.workflowStepId,
        dataInput=""
    ))
    start_step_connector = BEConnector(ApiUrl.WORKFLOW_STEP_START.full_url(), body_data)
    start_step_response = await start_step_connector.post()
    if not start_step_response:
        raise RuntimeError("Failed to fetch workflow_step_start")

    start_step_model = StartStep(**start_step_response)
    if not start_step_model:
        raise RuntimeError("Failed to initialize StartStep from response")

    if not context_data.step_detail:
        context_data.step_detail = []

    while len(context_data.step_detail) <= step.stepOrder:
        context_data.step_detail.append(StepDetail())

    context_data.step_detail[step.stepOrder].step = step
    context_data.step_detail[step.stepOrder].metadata_api = StepDetailConfig()
    context_data.step_detail[step.stepOrder].metadata_api.Step_start_api.url = ApiUrl.WORKFLOW_STEP_START.full_url()
    context_data.step_detail[step.stepOrder].metadata_api.Step_start_api.method = "POST"
    context_data.step_detail[step.stepOrder].metadata_api.Step_start_api.request = body_data
    context_data.step_detail[step.stepOrder].metadata_api.Step_start_api.response = start_step_model
    return start_step_model


async def call_workflow_step_finish(
    context_data: ContextData,
    target_bucket_name:str,
    s3_key_prefix: str,
    step: WorkflowStep,
    step_result: StepOutput
):

    logger.info(f"Running step_finish [{step.stepName}]")

    index = max(0, step.stepOrder)
    while len(context_data.step_detail) <= index:
        context_data.step_detail.append(StepDetail())

    err_msg = "; ".join(step_result.step_failure_message or ["Unknown error"])

    data_output = step_result.sub_data.get("data_output") or {} # pragma: no cover  # NOSONAR
    tmp_data_output = ""

    data_output["fileLogLink"] = f"{target_bucket_name}/{s3_key_prefix}"
    tmp_data_output = json.dumps(data_output)
    context_data.step_detail[index].data_output = data_output

    body_data = asdict(WorkflowStepFinishBody(
        workflowHistoryId=context_data.step_detail[step.stepOrder].metadata_api.Step_start_api.response.workflowHistoryId,
        code=step_result.step_status.value,
        message=err_msg if step_result.step_status == StatusEnum.FAILED else "",
        dataOutput=tmp_data_output,
    ))
        
    finish_step_connector = BEConnector(ApiUrl.WORKFLOW_STEP_FINISH.full_url(), body_data=body_data)
    finish_step_response = await finish_step_connector.post()

    context_data.step_detail[index].metadata_api.Step_finish_api.url = ApiUrl.WORKFLOW_STEP_FINISH.full_url()
    context_data.step_detail[index].metadata_api.Step_finish_api.method = "POST"
    context_data.step_detail[index].metadata_api.Step_finish_api.request = body_data
    context_data.step_detail[index].metadata_api.Step_finish_api.response = finish_step_response

    return finish_step_response


def inject_metadata_into_step_result(
    step: WorkflowStep,
    step_result: StepOutput,
    context_data: ContextData,
    s3_key_prefix: str,
    document_type: DocumentType,
) -> None:
    """
    Injects workflow, step metadata, and S3 output information into the `step_result.data` object.

    This function enriches the `step_result.data` field with `step_detail`, 
    `workflow_detail`, and the S3 key prefix (`json_output`) where the processed 
    data is stored. The injection behavior depends on the data type of `step_result.data`:

    - If `data` is a Pydantic model: creates a copy and adds metadata fields.
    - If `data` is a dictionary: attempts to parse its "json_data" → "data" field, 
      convert it into a Pydantic model, then inject metadata.
    - If `data` is not supported: logs an error and raises an exception.

    Args:
        step (WorkflowStep): Current workflow step information.
        step_result (StepOutput): The result object that holds the `data` to update.
        context_data (ContextData): Context containing workflow and step metadata.
        s3_key_prefix (str): S3 key or prefix path where the output file is stored.
        document_type (DocumentType): Type of the document used for parsing raw data.

    Raises:
        ValueError: If `step_result.data` is missing, invalid, or unsupported.
    """

    step_detail = context_data.step_detail
    workflow_detail = context_data.workflow_detail
    data = getattr(step_result, "data", None)

    if not data:
        logger.error(f"Step [{step.stepName}] - 'step_result.data' is missing or invalid")
        raise ValueError(f"Step [{step.stepName}] - 'step_result.data' is missing or invalid")

    # Case 1: data is a Pydantic model
    if isinstance(data, BaseModel):
        logger.debug(f"Step [{step.stepName}] - Injecting metadata into Pydantic model")
        step_result.data = data.model_copy(
            update={
                "step_detail": step_detail,
                "workflow_detail": workflow_detail,
                "json_output": s3_key_prefix,
            }
        )
        return

    # Case 2: data is a dict
    if isinstance(data, dict):
        logger.debug(f"Step [{step.stepName}] - Processing dict data for metadata injection")

        json_data = data.get("json_data", {})
        raw_output = getattr(json_data, "data", None) or json_data.get("data")

        if raw_output is None:
            logger.error(f"Step [{step.stepName}] - Missing 'data' field inside json_data")
            raise ValueError(f"Step [{step.stepName}] - 'json_data.data' field is missing")

        parsed_output = template_helper.parse_data(
            document_type=document_type,
            data=raw_output,
        )
        logger.debug(f"Step [{step.stepName}] - Successfully parsed data, injecting metadata")
        step_result.data = parsed_output.model_copy(
            update={
                "step_detail": step_detail,
                "workflow_detail": workflow_detail,
                "json_output": s3_key_prefix,
            }
        )
        return

    # Case 3: unsupported type
    logger.error(f"Step [{step.stepName}] - Unsupported data type '{type(data).__name__}', cannot inject metadata")
    raise ValueError(f"Step [{step.stepName}] - Unsupported data type '{type(data).__name__}' for metadata injection")


def store_data_in_s3(
    file_processor: ProcessorBase, 
    step_result: StepOutput, 
    s3_key_prefix: str, 
    save_raw: bool | None = False
) -> None:
    """
    Store processed data to S3, optionally saving raw data.

    Args:
        file_processor: Processor instance handling S3 operations.
        step_result: Processed step output to store.
        s3_key_prefix: S3 key prefix for storing JSON data.
        save_raw: If True, also save raw data to S3.
    """
    file_processor.write_json_to_s3(step_result, s3_key_prefix)        
    if save_raw:
        file_processor.write_raw_to_s3()

