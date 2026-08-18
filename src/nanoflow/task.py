"""Task definitions and the lazy references returned when tasks are called inside a Flow."""
from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable

from nanoflow.context import current_flow


@dataclass(frozen=True)
class TaskRef:
    """A lazy handle to the future output of a task node in a Flow.

    Returned when a @task is called inside a `with Flow(...)` block. Passing a
    TaskRef as an argument to another task creates a dependency edge.
    """

    node_id: str
    flow: Any = field(repr=False, compare=False)

    def result(self) -> Any:
        """Return the materialized value. Only valid after `flow.run()`."""
        return self.flow.result_of(self.node_id)


class Task:
    """A unit of work. Create with the @task decorator."""

    def __init__(
        self,
        fn: Callable[..., Any],
        *,
        name: str | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        retry_backoff: float = 2.0,
        timeout: float | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        self.fn = fn
        self.name = name or fn.__name__
        self.retries = retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff
        self.timeout = timeout
        self.tags = dict(tags or {})
        self.signature = inspect.signature(fn)
        functools.update_wrapper(self, fn)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        flow = current_flow()
        if flow is None:
            # Outside a Flow: behave like a plain function. This keeps tasks
            # unit-testable and lets you call them from anywhere.
            return self.fn(*args, **kwargs)
        return flow.add_node(self, args, kwargs)

    def run_direct(self, *args: Any, **kwargs: Any) -> Any:
        """Call the wrapped function directly, bypassing any active Flow."""
        return self.fn(*args, **kwargs)

    def __repr__(self) -> str:
        return f"Task({self.name!r})"


def task(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    retries: int = 0,
    retry_delay: float = 0.0,
    retry_backoff: float = 2.0,
    timeout: float | None = None,
    tags: dict[str, str] | None = None,
) -> Any:
    """Decorator turning a function into a Task.

    Usage::

        @task
        def a(): ...

        @task(retries=3, retry_delay=1.0)
        def b(x): ...
    """

    def wrap(f: Callable[..., Any]) -> Task:
        return Task(
            f,
            name=name,
            retries=retries,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
            timeout=timeout,
            tags=tags,
        )

    return wrap(fn) if fn is not None else wrap
