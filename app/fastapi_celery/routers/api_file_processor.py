import json
import traceback
from dataclasses import asdict
from fastapi import APIRouter, HTTPException, status, Request
from fastapi.responses import JSONResponse
from utils import read_n_write_s3
from utils.bucket_helper import get_s3_key_prefix
from models.body_models import WorkflowSessionFinishBody, WorkflowStepFinishBody
from models.class_models import (
    ContextData,
    FilePathRequest,
    PODataParsed,
    StepOutput,
    StopTaskRequest,
    ApiUrl,
    StatusEnum,
    WorkflowStep,
)
from models.tracking_models import ServiceLog, LogType, TrackingModel
from celery_worker import celery_task
from utils import log_helpers
from uuid import uuid4
from connections.redis_connection import RedisConnector
from celery_worker.celery_config import celery_app
from connections.be_connection import BEConnector
from typing import Dict, Any


# === Set up logging ===
logger = log_helpers.get_logger("File Processing Routers")

DISABLE_STOP_TASK_ENDPOINT = False  # Currently, disable stop_task endpoint
router = APIRouter()


@router.post("/file/process", summary="Process file and log task result")
async def process_file(data: FilePathRequest, http_request: Request) -> Dict[str, str]:
    """
    Submit a task to the Celery worker and update the workflow.

    This endpoint is used to start file processing asynchronously.
    It supports both initial runs and reruns:
    - If `celery_id` is not provided, a new UUID will be generated.
    - If `celery_id` and `rerun_attempt` are provided, the task is treated as a rerun.

    Args:
        data (FilePathRequest): Request payload including the file path and optional celery_id and rerun_attempt.
        http_request (Request): FastAPI Request object for context extraction.
        project: Name of the project this file belongs to. 
              This is used to determine the workflow configuration, storage location, 
              and possibly the business logic that applies.

    Returns:
        Dict[str, str]: Dictionary with 'celery_id' and 'file_path' if successful.

    Raises:
        HTTPException: If task submission fails due to an internal error.
    """
    try:
        # If run for the first time, it will create request_id (celery_id)
        # If run again, it will reuse request_id (celery_id)
        is_cancel = False
        if data.is_cancel:
            is_cancel = str(data.is_cancel).strip().lower() in ("True", "true", "1")
            new_celery_id = getattr(http_request.state, "request_id", str(uuid4()))

        if not (data.celery_id and data.celery_id.strip()):
            data.celery_id = getattr(http_request.state, "request_id", str(uuid4()))

        celery_task.task_execute.apply_async(
            kwargs={"data": data.model_dump()},
            task_id=new_celery_id if is_cancel else data.celery_id,
        )
        
        logger.info(
            f"Submitted Celery task: {data.celery_id}",
            extra={
                "service": ServiceLog.API_GATEWAY,
                "log_type": LogType.ACCESS,
                "data": data.model_dump(),
            },
        )
        return {
            "celery_id": data.celery_id,
            "file_path": data.file_path,
        }

    except Exception as e:
        traceback.print_exc()
        full_tb = traceback.format_exception(type(e), e, e.__traceback__)
        logger.exception(
            "Submitted Celery task failed.",
            extra={
                "service": ServiceLog.API_GATEWAY,
                "log_type": LogType.ERROR,
                "data": data,
                "traceback": full_tb,
            },
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Submitted Celery task failed, exception: {str(e)}",
        )


# stop a stask
@router.post("/tasks/stop", summary="Stop a running task by providing the task_id")
async def stop(data: StopTaskRequest) -> Dict[str, Any]:
    """
    Stop a running task by revoking its Celery task and updating the workflow.

    Retrieves workflow and step details from Redis, revokes the Celery task if in progress,
    and notifies the backend API. Returns success status or error response.

    Args:
        request (StopTaskRequest): Pydantic model containing task_id and optional reason.

    Returns:
        Dict[str, Any]: Dictionary with 'status', 'task_id', and 'message' if successful.
        JSONResponse: Error response with status code 500 if the operation fails.

    Raises:
        HTTPException: If an unexpected error occurs during the process.
    """
    if DISABLE_STOP_TASK_ENDPOINT:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="This endpoint is temporarily disabled.",
        )
    redis_connector = RedisConnector()
    celery_id = data.task_id
    reason = data.reason or "Stopped manually by user"

    celery_task = redis_connector.get_celery_task(celery_id)
    steps = redis_connector.get_all_steps_for_task(celery_id)
    
    if not celery_task:
        logger.warning(
            f"Workflow not found for task_id: {celery_id}",
            extra={
                "service": ServiceLog.API_GATEWAY,
                "log_type": LogType.ERROR,
                "traceability": celery_id,
            },
        )
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "status_code": status.HTTP_404_NOT_FOUND,
                "error": "Workflow ID not found for task",
                "task_id": celery_id,
            },
        )
    
    if celery_task["status"] != StatusEnum.PROCESSING.name:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "error": "Workflow has been done or stopped! Cannot stop the task",
                "task_id": celery_id,
            },
        )
        
    # Kill the running process
    celery_app.control.revoke(celery_id, terminate=True, signal="SIGKILL")
    logger.info(
        f"Revoked Celery task {celery_id} with reason: {reason}",
        extra={
            "service": ServiceLog.API_GATEWAY,
            "log_type": LogType.TASK,
            "traceability": celery_id,
        },
    )

    try:
        file_record = celery_task["file_record"]
        tracking_model_raw = celery_task.get("tracking_model")
        tracking_model = TrackingModel(**tracking_model_raw) if tracking_model_raw else TrackingModel(
            request_id=celery_id
        )

        context_data_raw = celery_task.get("context_data")
        if context_data_raw:
            try:
                context_data = ContextData(**context_data_raw)
            except Exception:
                # Fallback: build minimal context
                context_data = ContextData(request_id=celery_id, step_detail=[], workflow_detail=None)
        else:
            context_data = ContextData(request_id=celery_id, step_detail=[], workflow_detail=None)

        start_session_model = celery_task.get("start_session_model", {})
        target_bucket_name = file_record["target_bucket_name"]
        s3_key_prefix = None
        
        for step_id, step_data in steps.items():
            if step_data["status"] == "PROCESSING":
                step = WorkflowStep(**step_data["step"])
                step_name = step.stepName
                data_output = {}
                tmp_data_output = ""
                s3_key_prefix = get_s3_key_prefix(file_record, tracking_model, step)
                data_output["fileLogLink"] = f"{target_bucket_name}/{s3_key_prefix}"
                tmp_data_output = json.dumps(data_output)
                
                start_step_model = step_data["start_step_model"]

                context_data.step_detail[step.stepOrder].data_output = data_output
                
                # Update step status to CANCEL
                body_data = asdict(WorkflowStepFinishBody(
                    workflowHistoryId=start_step_model["workflowHistoryId"],
                    code=StatusEnum.CANCEL.value,
                    message=f"Step [{step_name}] was manually canceled by the user",
                    dataOutput=tmp_data_output,
                ))
                    
                finish_step_connector = BEConnector(ApiUrl.WORKFLOW_STEP_FINISH.full_url(), body_data=body_data)
                finish_step_response = await finish_step_connector.post()
                logger.info(f"finish_step_response_log: [{finish_step_response}]")

                context_data.step_detail[step.stepOrder].metadata_api.Step_finish_api.url = ApiUrl.WORKFLOW_STEP_FINISH.full_url()
                context_data.step_detail[step.stepOrder].metadata_api.Step_finish_api.method = "POST"
                context_data.step_detail[step.stepOrder].metadata_api.Step_finish_api.request = body_data
                context_data.step_detail[step.stepOrder].metadata_api.Step_finish_api.response = finish_step_response

        # Update session status to CANCEL
        body_data = asdict(WorkflowSessionFinishBody(
            id=start_session_model["id"],
            code=StatusEnum.CANCEL.value,
            message="The session has been stopped by the user",
        ))
        session_connector = BEConnector(ApiUrl.WORKFLOW_SESSION_FINISH.full_url(), body_data=body_data)
        session_response = await session_connector.post()

        logger.info(f"session_response_log: [{session_response}]")

        context_data.workflow_detail.metadata_api.session_finish_api.url = ApiUrl.WORKFLOW_SESSION_FINISH.full_url()
        context_data.workflow_detail.metadata_api.session_finish_api.method = "POST"
        context_data.workflow_detail.metadata_api.session_finish_api.request = body_data
        context_data.workflow_detail.metadata_api.session_finish_api.response = session_response

        # Convert context objects to plain dicts for schema compatibility
        step_detail_dump = None
        if context_data.step_detail is not None:
            step_detail_dump = [
                (sd.model_dump() if hasattr(sd, "model_dump") else sd)
                for sd in context_data.step_detail
            ]

        workflow_detail_dump = None
        if context_data.workflow_detail is not None:
            workflow_detail_dump = (
                context_data.workflow_detail.model_dump()
                if hasattr(context_data.workflow_detail, "model_dump")
                else context_data.workflow_detail
            )

        schema_object = PODataParsed(
            file_path=file_record["file_path"],
            document_type=file_record["document_type"],
            po_number=None,
            items=[],
            metadata={},
            step_status=StatusEnum.CANCEL,
            messages=[f"Revoked Celery task {celery_id} with reason: {reason}"],
            file_size=file_record["file_size"],
            step_detail=step_detail_dump,
            workflow_detail=workflow_detail_dump,
            json_output=s3_key_prefix
        )

        step_output = StepOutput(
            data=schema_object,
            sub_data={},
            step_status=StatusEnum.CANCEL,
            step_failure_message=[f"Revoked Celery task {celery_id} with reason: {reason}"],
        )

        result = read_n_write_s3.write_json_to_s3(
            json_data=step_output,
            bucket_name=target_bucket_name,
            s3_key_prefix=s3_key_prefix,
        )
        if result.get("status") == "Success":
            logger.info(
                f"[write_json_to_s3] Successfully wrote JSON file to S3 "
                f"(bucket='{target_bucket_name}', prefix='{s3_key_prefix}').",
            )
        else:
            logger.error(
                f"[write_json_to_s3] Failed to write JSON to S3 "
                f"(bucket='{target_bucket_name}', prefix='{s3_key_prefix}'). "
                f"error={result.get('error')}"
            )

  
        return {
            "status": "Task stopped successfully",
            "task_id": celery_id,
            "message": reason,
        }

    except Exception as e:
        full_tb = traceback.format_exception(type(e), e, e.__traceback__)
        logger.error(
            f"Failed to stop task {celery_id}!\n",
            extra={
                "service": ServiceLog.API_GATEWAY,
                "log_type": LogType.ERROR,
                "traceability": celery_id,
                "traceback": full_tb,
            },
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "error": str(e),
                "traceback": traceback.format_exc(),
            },
        )
