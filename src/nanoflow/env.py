"""Detect where we're running so defaults (sinks, run_id) can adapt automatically."""
from __future__ import annotations

import os
import platform
import socket
import sys
from typing import Any


def detect_runtime() -> str:
    env = os.environ
    if "AWS_LAMBDA_FUNCTION_NAME" in env:
        return "lambda"
    if "GLUE_VERSION" in env or "GLUE_PYTHON_VERSION" in env or "--JOB_NAME" in " ".join(sys.argv):
        return "glue"
    if "SM_TRAINING_ENV" in env or "SAGEMAKER_PROGRAM" in env or "SM_CURRENT_HOST" in env:
        return "sagemaker"
    if "ECS_CONTAINER_METADATA_URI" in env or "ECS_CONTAINER_METADATA_URI_V4" in env:
        return "ecs"
    if "AIRFLOW_CTX_DAG_ID" in env:
        return "airflow"
    return "local"


def infer_run_id() -> str | None:
    """Pull a run id from the surrounding environment when one exists.

    Priority: explicit NANOFLOW_RUN_ID env var, then Lambda request id (set by
    handler helper), then Glue JOB_RUN_ID from argv, then Airflow run id.
    """
    env = os.environ
    for key in ("NANOFLOW_RUN_ID", "_NANOFLOW_LAMBDA_REQUEST_ID", "AIRFLOW_CTX_DAG_RUN_ID"):
        if env.get(key):
            return env[key]
    argv = sys.argv
    for i, a in enumerate(argv):
        if a == "--JOB_RUN_ID" and i + 1 < len(argv):
            return argv[i + 1]
        if a.startswith("--JOB_RUN_ID="):
            return a.split("=", 1)[1]
    return None


def environment_info() -> dict[str, Any]:
    env = os.environ
    info: dict[str, Any] = {
        "runtime": detect_runtime(),
        "python": platform.python_version(),
        "host": socket.gethostname(),
        "region": env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION"),
    }
    if info["runtime"] == "lambda":
        info["function"] = env.get("AWS_LAMBDA_FUNCTION_NAME")
        info["memory_mb"] = env.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
    if info["runtime"] == "glue":
        argv = sys.argv
        for i, a in enumerate(argv):
            if a == "--JOB_NAME" and i + 1 < len(argv):
                info["job_name"] = argv[i + 1]
    return {k: v for k, v in info.items() if v is not None}
