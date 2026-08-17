"""Serializable records describing a flow run. These are what sinks emit."""
from __future__ import annotations

import json
import time
import traceback
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

MAX_REPR = 200


def _short_repr(value: Any) -> str:
    """Compact, safe representation of task inputs/outputs for logs.

    We never store the actual object (could be a DataFrame or huge list),
    just a truncated repr plus a type name.
    """
    try:
        r = repr(value)
    except Exception:  # pragma: no cover - defensive
        r = f"<unrepr-able {type(value).__name__}>"
    if len(r) > MAX_REPR:
        r = r[:MAX_REPR] + "..."
    return r


@dataclass
class TaskRecord:
    node_id: str
    task: str
    status: str = "pending"  # pending | running | success | failed | skipped
    attempts: int = 0
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    upstream: list[str] = field(default_factory=list)
    inputs: dict[str, str] = field(default_factory=dict)
    output_type: Optional[str] = None
    output_repr: Optional[str] = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    traceback: Optional[str] = None
    tags: dict[str, str] = field(default_factory=dict)

    @property
    def duration_ms(self) -> Optional[float]:
        if self.started_at is None or self.ended_at is None:
            return None
        return round((self.ended_at - self.started_at) * 1000, 3)

    def mark_running(self) -> None:
        self.status = "running"
        self.started_at = time.time()

    def mark_success(self, output: Any) -> None:
        self.status = "success"
        self.ended_at = time.time()
        self.output_type = type(output).__name__
        self.output_repr = _short_repr(output)

    def mark_failed(self, exc: BaseException) -> None:
        self.status = "failed"
        self.ended_at = time.time()
        self.error = str(exc)
        self.error_type = type(exc).__name__
        self.traceback = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))

    def mark_skipped(self, reason: str) -> None:
        self.status = "skipped"
        self.error = reason

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["duration_ms"] = self.duration_ms
        return d


@dataclass
class RunRecord:
    flow: str
    run_id: str
    status: str = "pending"  # pending | running | success | failed
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    environment: dict[str, Any] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    tasks: dict[str, TaskRecord] = field(default_factory=dict)
    graph: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> Optional[float]:
        if self.started_at is None or self.ended_at is None:
            return None
        return round((self.ended_at - self.started_at) * 1000, 3)

    @property
    def ok(self) -> bool:
        return self.status == "success"

    def to_dict(self) -> dict[str, Any]:
        return {
            "flow": self.flow,
            "run_id": self.run_id,
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_ms": self.duration_ms,
            "environment": self.environment,
            "params": self.params,
            "graph": self.graph,
            "tasks": {k: v.to_dict() for k, v in self.tasks.items()},
        }

    def to_json(self, indent: Optional[int] = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def summary(self) -> dict[str, Any]:
        """Small dict suitable for returning from a Lambda handler."""
        counts: dict[str, int] = {}
        for t in self.tasks.values():
            counts[t.status] = counts.get(t.status, 0) + 1
        return {
            "flow": self.flow,
            "run_id": self.run_id,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "tasks": counts,
            "failed": [t.task for t in self.tasks.values() if t.status == "failed"],
        }
