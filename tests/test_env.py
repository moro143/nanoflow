import sys

from nanoflow.env import detect_runtime, environment_info, infer_run_id

_RUNTIME_ENV_VARS = (
    "AWS_LAMBDA_FUNCTION_NAME",
    "GLUE_VERSION",
    "GLUE_PYTHON_VERSION",
    "SM_TRAINING_ENV",
    "SAGEMAKER_PROGRAM",
    "SM_CURRENT_HOST",
    "ECS_CONTAINER_METADATA_URI",
    "ECS_CONTAINER_METADATA_URI_V4",
    "AIRFLOW_CTX_DAG_ID",
)


def _clear_runtime_env(monkeypatch, *skip):
    for k in _RUNTIME_ENV_VARS:
        if k not in skip:
            monkeypatch.delenv(k, raising=False)


def test_detect_runtime_local_by_default(monkeypatch):
    _clear_runtime_env(monkeypatch)
    assert detect_runtime() == "local"


def test_detect_runtime_lambda(monkeypatch):
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "fn")
    assert detect_runtime() == "lambda"


def test_detect_runtime_glue(monkeypatch):
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("GLUE_VERSION", "4.0")
    assert detect_runtime() == "glue"


def test_detect_runtime_sagemaker(monkeypatch):
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("SM_CURRENT_HOST", "host")
    assert detect_runtime() == "sagemaker"


def test_detect_runtime_ecs(monkeypatch):
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("ECS_CONTAINER_METADATA_URI", "http://x")
    assert detect_runtime() == "ecs"


def test_detect_runtime_airflow(monkeypatch):
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag")
    assert detect_runtime() == "airflow"


def test_infer_run_id_from_glue_argv(monkeypatch):
    monkeypatch.delenv("NANOFLOW_RUN_ID", raising=False)
    monkeypatch.delenv("_NANOFLOW_LAMBDA_REQUEST_ID", raising=False)
    monkeypatch.delenv("AIRFLOW_CTX_DAG_RUN_ID", raising=False)
    monkeypatch.setattr(sys, "argv", ["job.py", "--JOB_RUN_ID", "jr_123"])
    assert infer_run_id() == "jr_123"


def test_infer_run_id_prefers_env_var(monkeypatch):
    monkeypatch.setenv("NANOFLOW_RUN_ID", "explicit")
    assert infer_run_id() == "explicit"


def test_environment_info_includes_lambda_fields(monkeypatch):
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "fn")
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "256")
    info = environment_info()
    assert info["runtime"] == "lambda"
    assert info["function"] == "fn"
    assert info["memory_mb"] == "256"


def test_environment_info_includes_glue_job_name(monkeypatch):
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("GLUE_VERSION", "4.0")
    monkeypatch.setattr(sys, "argv", ["job.py", "--JOB_NAME", "my-job"])
    info = environment_info()
    assert info["runtime"] == "glue"
    assert info["job_name"] == "my-job"
