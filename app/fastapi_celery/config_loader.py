import os
from pathlib import Path
from typing import Any, Optional
from dotenv import load_dotenv
from configparser import ConfigParser

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env")

# Load configs.ini
config = ConfigParser()
config.read(Path(__file__).resolve().parent / "configs.ini")


def get_config_value(section: str, key: str, fallback: Optional[Any] = None) -> Any:
    """
    Retrieve a value from the configs.ini file.

    Args:
        section (str): The section in the INI file to look under.
        key (str): The key to retrieve from the section.
        fallback (Optional[Any]): The value to return if the key is not found. Defaults to None.

    Returns:
        Any: The value associated with the key in the specified section, or the fallback value if not found.
    """
    return config.get(section, key, fallback=fallback)


def get_env_variable(key: str, fallback: Optional[Any] = None) -> Any:
    """
    Get an environment variable.

    Args:
        key (str): The name of the environment variable to retrieve.
        fallback (Optional[Any]): The value to return if the environment variable is not found. Defaults to None.

    Returns:
        Any: The value of the environment variable, or the fallback value if not found.
    """
    return os.getenv(key, fallback)


# === Load environment variables (once) ===
ALLOW_TEST_SLEEP = str(get_env_variable("ALLOW_TEST_SLEEP", "False")).strip().lower() in ("True", "true", "1")
SLEEP_DURATION = int(get_env_variable("SLEEP_DURATION", 0))

# --- Celery dynamic config ---
CELERY_RESULT_EXPIRES = int(get_env_variable("CELERY_RESULT_EXPIRES", 10800))
CELERY_TASK_SOFT_TIME_LIMIT = int(get_env_variable("CELERY_TASK_SOFT_TIME_LIMIT", 7200))
CELERY_TASK_TIME_LIMIT = int(get_env_variable("CELERY_TASK_TIME_LIMIT", 10800))
