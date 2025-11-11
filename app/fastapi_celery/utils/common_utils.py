from utils import log_helpers
from processors.processor_nodes import PROCESS_DEFINITIONS
from io import BytesIO
import pandas as pd
from pydantic import BaseModel

# === Set up logging ===
logger = log_helpers.get_logger("common_utils")

def get_step_name(step_name: str) -> str | None:
    """Return the matching step name from PROCESS_DEFINITIONS."""
    
    list_step = list(PROCESS_DEFINITIONS.keys())

    # Case 1: exact match
    if step_name in list_step:
        return step_name

    # Case 2: match dynamic prefix (e.g., [RULE_MP]_SUBMIT)
    for step_key in list_step:
        if step_key.startswith("[") and "]_" in step_key:
            suffix = step_key.split("]_")[-1]
            if step_name.endswith(suffix):
                logger.info(f"[get_step_name] Dynamic match found for '{step_name}' → '{step_key}'")
                return step_key

    logger.warning(f"[get_step_name] No match found for '{step_name}'")
    return None

def get_csv_buffer_file(data_input) -> BytesIO:
    """
    Build CSV from data_input

    Args:
        data_input: Object with .data (list[dict] or DataFrame-like).

    Returns:
        BytesIO: In-memory CSV file buffer.
    """
    if data_input is None:
        raise ValueError("No data_input provided — expected object or iterable data.")

    payload = getattr(data_input, "data", None)
    payload = getattr(payload, "items", None)

    # --- Pydantic model case ---
    if isinstance(payload, BaseModel):
        payload = payload.model_dump()

    # Ensure payload exists and is in a valid format.
    # 1. payload is not None
    # 2. payload must be a list or dict
    # 3. payload must not be empty ([] or {})
    if payload is None or not isinstance(payload, (list, dict)) or not payload:
        raise ValueError("Empty payload — no data to process for CSV.")

    # --- Convert to DataFrame ---
    df = pd.DataFrame(payload)
    if df.empty:
        raise ValueError("DataFrame is empty — no data to write to CSV.")

    # --- Build CSV buffer ---
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    return csv_buffer
