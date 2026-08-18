import os

from nanoflow import Flow, task
from nanoflow.aws import glue_args, glue_run_id, lambda_flow, run_id_from_event


@task
def one():
    return 1


@task
def add(a, b):
    return a + b


@task
def boom():
    raise ValueError("nope")


def test_lambda_flow_decorator():
    @lambda_flow
    def handler(event, context):
        with Flow("h", sinks=[]) as f:
            add(one(), 1)
        return f.run()

    out = handler({"nanoflow_run_id": "exec-123"}, None)
    assert out["status"] == "success" and out["run_id"] == "exec-123"


def test_lambda_flow_reraise_false_returns_status_failed():
    @lambda_flow(reraise=False)
    def handler(event, context):
        with Flow("h", sinks=[]) as f:
            boom()
        return f.run()

    out = handler({}, None)
    assert out == {"status": "failed"}


def test_lambda_flow_return_summary_false_returns_raw_record():
    @lambda_flow(return_summary=False)
    def handler(event, context):
        with Flow("h", sinks=[]) as f:
            add(one(), 1)
        return f.run()

    out = handler({}, None)
    assert out.ok  # raw RunRecord, not a summary dict


def test_lambda_flow_restores_previous_env_var(monkeypatch):
    monkeypatch.setenv("_NANOFLOW_LAMBDA_REQUEST_ID", "outer")

    @lambda_flow
    def handler(event, context):
        return os.environ.get("_NANOFLOW_LAMBDA_REQUEST_ID")

    out = handler({"nanoflow_run_id": "inner"}, None)
    assert out == "inner"
    assert os.environ["_NANOFLOW_LAMBDA_REQUEST_ID"] == "outer"


def test_run_id_from_event():
    assert run_id_from_event({"nanoflow": {"run_id": "x"}}) == "x"
    assert run_id_from_event({"runId": "y"}) == "y"
    assert run_id_from_event("nope") is None


def test_glue_args():
    argv = ["--JOB_NAME", "j", "--JOB_RUN_ID=jr_1", "--flag", "--extra", "v"]
    assert glue_args(argv=argv) == {"JOB_NAME": "j", "JOB_RUN_ID": "jr_1", "flag": "", "extra": "v"}
    assert glue_args("JOB_NAME", argv=argv) == {"JOB_NAME": "j"}


def test_glue_run_id_prefers_explicit_over_job_run_id():
    argv = ["--nanoflow_run_id", "explicit", "--JOB_RUN_ID", "jr_1"]
    assert glue_run_id(argv=argv) == "explicit"


def test_glue_run_id_falls_back_to_job_run_id():
    argv = ["--JOB_RUN_ID", "jr_1"]
    assert glue_run_id(argv=argv) == "jr_1"
