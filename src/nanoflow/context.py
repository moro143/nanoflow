"""Tracks which Flow is currently being built, so @task calls can register nodes."""
from __future__ import annotations

import contextvars
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nanoflow.flow import Flow

_current: contextvars.ContextVar[Flow | None] = contextvars.ContextVar(
    "nanoflow_current_flow", default=None
)


def current_flow() -> Flow | None:
    return _current.get()


def push_flow(flow: Flow) -> contextvars.Token:
    return _current.set(flow)


def pop_flow(token: contextvars.Token) -> None:
    _current.reset(token)
