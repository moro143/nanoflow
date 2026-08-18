import logging

from nanoflow import Flow, task
from nanoflow.sinks import FileSink, LogSink, S3Sink, default_sinks


@task
def one():
    return 1


@task
def boom():
    raise ValueError("nope")


def test_logsink_emits_lifecycle_lines(caplog):
    caplog.set_level(logging.INFO, logger="nanoflow")
    with Flow("t", sinks=[LogSink()]) as f:
        one()
    f.run()
    messages = caplog.messages
    assert any("START (1 tasks)" in m for m in messages)
    assert any("task=one attempt=1 START" in m for m in messages)
    assert any("task=one SUCCESS" in m for m in messages)
    assert any(m.startswith("flow=t run=") and "SUCCESS" in m for m in messages)


def test_logsink_logs_failure_as_error(caplog):
    caplog.set_level(logging.INFO, logger="nanoflow")
    with Flow("t", sinks=[LogSink()], raise_on_failure=False) as f:
        boom()
    f.run()
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("FAILED after 1 attempt(s): ValueError: nope" in r.getMessage() for r in errors)


def test_default_sinks_local_uses_log_and_file():
    sinks = default_sinks("local")
    assert any(isinstance(s, LogSink) for s in sinks)
    assert any(isinstance(s, FileSink) for s in sinks)


def test_default_sinks_lambda_without_bucket_has_no_s3sink(monkeypatch):
    monkeypatch.delenv("NANOFLOW_S3_BUCKET", raising=False)
    sinks = default_sinks("lambda")
    assert not any(isinstance(s, S3Sink) for s in sinks)


def test_default_sinks_lambda_with_bucket_adds_s3sink(monkeypatch):
    monkeypatch.setenv("NANOFLOW_S3_BUCKET", "my-bucket")
    sinks = default_sinks("lambda")
    s3_sinks = [s for s in sinks if isinstance(s, S3Sink)]
    assert len(s3_sinks) == 1
    assert s3_sinks[0].bucket == "my-bucket"


class _FakeS3Client:
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)


def test_s3sink_uploads_run_json_via_injected_client():
    client = _FakeS3Client()
    sink = S3Sink("my-bucket", prefix="runs", client=client)
    with Flow("t", sinks=[sink]) as f:
        one()
    f.run()
    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["Bucket"] == "my-bucket"
    assert call["Key"].startswith("runs/t/")
    assert call["ContentType"] == "application/json"
