"""Sinks receive run/task lifecycle events and persist them somewhere.

All sinks are best-effort: a failing sink never fails the flow.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional

from nanoflow.records import RunRecord, TaskRecord

log = logging.getLogger("nanoflow")


class Sink:
    """Base class. Override any subset of hooks."""

    def on_run_start(self, run: RunRecord) -> None: ...
    def on_task_start(self, run: RunRecord, task: TaskRecord) -> None: ...
    def on_task_end(self, run: RunRecord, task: TaskRecord) -> None: ...
    def on_run_end(self, run: RunRecord) -> None: ...


class LogSink(Sink):
    """Human-readable lines via the `nanoflow` logger (works in CloudWatch)."""

    def __init__(self, level: int = logging.INFO) -> None:
        self.level = level

    def on_run_start(self, run: RunRecord) -> None:
        log.log(self.level, "flow=%s run=%s START (%d tasks)", run.flow, run.run_id, len(run.tasks))

    def on_task_start(self, run: RunRecord, task: TaskRecord) -> None:
        log.log(self.level, "flow=%s task=%s attempt=%d START", run.flow, task.task, task.attempts)

    def on_task_end(self, run: RunRecord, task: TaskRecord) -> None:
        if task.status == "failed":
            log.error(
                "flow=%s task=%s FAILED after %d attempt(s): %s: %s",
                run.flow, task.task, task.attempts, task.error_type, task.error,
            )
        else:
            log.log(
                self.level, "flow=%s task=%s %s %.1fms",
                run.flow, task.task, task.status.upper(), task.duration_ms or 0.0,
            )

    def on_run_end(self, run: RunRecord) -> None:
        log.log(self.level, "flow=%s run=%s %s %.1fms",
                run.flow, run.run_id, run.status.upper(), run.duration_ms or 0.0)


class JsonLinesSink(Sink):
    """One JSON object per event to a stream (default stdout). Ideal for CloudWatch Logs Insights."""

    def __init__(self, stream: Any = None) -> None:
        self.stream = stream or sys.stdout

    def _emit(self, event: str, payload: dict[str, Any]) -> None:
        self.stream.write(json.dumps({"nanoflow_event": event, **payload}, default=str) + "\n")
        self.stream.flush()

    def on_run_start(self, run: RunRecord) -> None:
        self._emit("run_start", {"flow": run.flow, "run_id": run.run_id, "environment": run.environment})

    def on_task_end(self, run: RunRecord, task: TaskRecord) -> None:
        d = task.to_dict()
        d.pop("traceback", None)
        self._emit("task_end", {"flow": run.flow, "run_id": run.run_id, **d})

    def on_run_end(self, run: RunRecord) -> None:
        self._emit("run_end", run.summary())


class FileSink(Sink):
    """Writes the full run.json to a directory (default .nanoflow/runs/<run_id>.json)."""

    def __init__(self, directory: str | os.PathLike = ".nanoflow/runs") -> None:
        self.directory = Path(directory)

    def on_run_end(self, run: RunRecord) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / f"{_safe(run.flow)}__{_safe(run.run_id)}.json"
        path.write_text(run.to_json())


class S3Sink(Sink):
    """Uploads run.json to s3://bucket/prefix/<flow>/<run_id>.json. Requires boto3."""

    def __init__(self, bucket: str, prefix: str = "nanoflow/runs", client: Any = None) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._client = client

    def _s3(self) -> Any:
        if self._client is None:
            import boto3  # optional dependency

            self._client = boto3.client("s3")
        return self._client

    def on_run_end(self, run: RunRecord) -> None:
        key = f"{self.prefix}/{_safe(run.flow)}/{_safe(run.run_id)}.json"
        self._s3().put_object(Bucket=self.bucket, Key=key, Body=run.to_json().encode(),
                              ContentType="application/json")


class CallbackSink(Sink):
    """Wrap plain functions as a sink, e.g. to push metrics or Slack messages."""

    def __init__(self, on_run_end: Optional[Any] = None, on_task_end: Optional[Any] = None) -> None:
        self._run_end = on_run_end
        self._task_end = on_task_end

    def on_task_end(self, run: RunRecord, task: TaskRecord) -> None:
        if self._task_end:
            self._task_end(run, task)

    def on_run_end(self, run: RunRecord) -> None:
        if self._run_end:
            self._run_end(run)


def default_sinks(runtime: str) -> list[Sink]:
    """Sensible defaults per environment. Override by passing sinks=[...] to Flow."""
    if runtime == "local":
        return [LogSink(), FileSink()]
    # In AWS runtimes stdout goes to CloudWatch, so structured JSON lines are best.
    sinks: list[Sink] = [JsonLinesSink()]
    bucket = os.environ.get("NANOFLOW_S3_BUCKET")
    if bucket:
        sinks.append(S3Sink(bucket, os.environ.get("NANOFLOW_S3_PREFIX", "nanoflow/runs")))
    return sinks


def _safe(s: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in s)
