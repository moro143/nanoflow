"""Flow: collect tasks into a DAG, then run it."""
from __future__ import annotations

import concurrent.futures as cf
import contextvars
import logging
import threading
import time
import uuid
from collections.abc import Iterable
from typing import Any

from nanoflow import env as _env
from nanoflow.context import pop_flow, push_flow
from nanoflow.errors import FlowFailed, TaskTimeout, UpstreamFailed
from nanoflow.graph import Graph, Node
from nanoflow.records import RunRecord, TaskRecord, _short_repr
from nanoflow.sinks import Sink, default_sinks
from nanoflow.task import Task, TaskRef

log = logging.getLogger("nanoflow")

_MISSING = object()


class Flow:
    """A DAG of tasks.

    Usage::

        with Flow("etl") as f:
            raw = extract()
            clean = transform(raw)
            load(clean)
        run = f.run()

    Nodes are created by calling @task functions inside the `with` block.
    Passing the result of one task into another creates a dependency.
    """

    def __init__(
        self,
        name: str,
        *,
        run_id: str | None = None,
        params: dict[str, Any] | None = None,
        sinks: Iterable[Sink] | None = None,
        max_workers: int = 1,
        fail_fast: bool = True,
        raise_on_failure: bool = True,
    ) -> None:
        self.name = name
        self.run_id = run_id or _env.infer_run_id() or uuid.uuid4().hex[:12]
        self.params = dict(params or {})
        self.max_workers = max(1, max_workers)
        self.fail_fast = fail_fast
        self.raise_on_failure = raise_on_failure
        self.graph = Graph()
        self._results: dict[str, Any] = {}
        self._counter: dict[str, int] = {}
        self._token: contextvars.Token | None = None
        runtime = _env.detect_runtime()
        self.sinks: list[Sink] = list(sinks) if sinks is not None else default_sinks(runtime)
        self.record = RunRecord(
            flow=name, run_id=self.run_id, environment=_env.environment_info(), params=self.params
        )

    # ------------------------------------------------------------------ building
    def __enter__(self) -> Flow:  # noqa: PYI034 - Self needs typing_extensions on py39
        self._token = push_flow(self)
        return self

    def __exit__(self, *exc: object) -> None:
        assert self._token is not None, "__exit__ called without a matching __enter__"
        pop_flow(self._token)
        self._token = None

    def add_node(self, task: Task, args: tuple, kwargs: dict) -> TaskRef:
        n = self._counter.get(task.name, 0)
        self._counter[task.name] = n + 1
        node_id = task.name if n == 0 else f"{task.name}_{n}"
        upstream = {r.node_id for r in _iter_refs(args, kwargs)}
        node = Node(id=node_id, task=task, args=args, kwargs=kwargs, upstream=upstream)
        self.graph.add(node)
        return TaskRef(node_id=node_id, flow=self)

    def result_of(self, node_id: str) -> Any:
        if node_id not in self._results:
            raise KeyError(f"no result for {node_id!r}; has the flow run?")
        return self._results[node_id]

    # ------------------------------------------------------------------ running
    def run(self) -> RunRecord:
        """Execute the graph. Returns the RunRecord (also available as flow.record)."""
        rec = self.record
        rec.graph = self.graph.to_dict()
        for nid in self.graph.topological_order():
            node = self.graph.nodes[nid]
            rec.tasks[nid] = TaskRecord(
                node_id=nid, task=node.task.name, upstream=sorted(node.upstream), tags=node.task.tags
            )
        rec.status = "running"
        rec.started_at = time.time()
        self._emit("on_run_start", rec)

        try:
            if self.max_workers == 1:
                self._run_sequential()
            else:
                self._run_threaded()
        finally:
            rec.ended_at = time.time()
            failed = any(t.status == "failed" for t in rec.tasks.values())
            rec.status = "failed" if failed else "success"
            self._emit("on_run_end", rec)

        if failed and self.raise_on_failure:
            raise FlowFailed(rec)
        return rec

    def _run_sequential(self) -> None:
        for nid in self.graph.topological_order():
            if not self._run_node(nid) and self.fail_fast:
                self._skip_remaining()
                return

    def _run_threaded(self) -> None:
        graph = self.graph
        pending = {nid: set(graph.nodes[nid].upstream) for nid in graph.nodes}
        done: set[str] = set()
        lock = threading.Lock()
        stop = threading.Event()

        with cf.ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures: dict[cf.Future, str] = {}

            def submit_ready() -> None:
                for nid, ups in list(pending.items()):
                    if not ups and not stop.is_set():
                        pending.pop(nid)
                        futures[pool.submit(self._run_node, nid)] = nid

            submit_ready()
            while futures:
                finished, _ = cf.wait(list(futures), return_when=cf.FIRST_COMPLETED)
                for fut in finished:
                    nid = futures.pop(fut)
                    ok = fut.result()
                    with lock:
                        done.add(nid)
                        if not ok and self.fail_fast:
                            stop.set()
                        for other in pending.values():
                            other.discard(nid)
                    submit_ready()
        self._skip_remaining()

    def _skip_remaining(self) -> None:
        for t in self.record.tasks.values():
            if t.status == "pending":
                t.mark_skipped("upstream failed or flow stopped")
                self._emit("on_task_end", self.record, t)

    def _run_node(self, nid: str) -> bool:
        """Run one node with retries. Returns True on success."""
        node = self.graph.nodes[nid]
        task = node.task
        trec = self.record.tasks[nid]

        # Skip if any upstream did not succeed.
        bad = [u for u in node.upstream if self.record.tasks[u].status != "success"]
        if bad:
            trec.mark_skipped(f"upstream failed: {bad}")
            self._emit("on_task_end", self.record, trec)
            return False

        args = tuple(self._resolve(a) for a in node.args)
        kwargs = {k: self._resolve(v) for k, v in node.kwargs.items()}
        trec.inputs = _describe_inputs(task, args, kwargs)

        attempts_allowed = task.retries + 1
        delay = task.retry_delay
        last_exc: BaseException | None = None
        for attempt in range(1, attempts_allowed + 1):
            trec.attempts = attempt
            trec.mark_running()
            self._emit("on_task_start", self.record, trec)
            try:
                result = _call_with_timeout(task, args, kwargs, task.timeout)
                self._results[nid] = result
                trec.mark_success(result)
                self._emit("on_task_end", self.record, trec)
                return True
            except UpstreamFailed:
                raise
            except BaseException as exc:  # noqa: BLE001 - we record and decide
                last_exc = exc
                if attempt < attempts_allowed:
                    log.warning("task %s attempt %d/%d failed: %r; retrying in %.1fs",
                                task.name, attempt, attempts_allowed, exc, delay)
                    if delay:
                        time.sleep(delay)
                        delay *= task.retry_backoff
        trec.mark_failed(last_exc)  # type: ignore[arg-type]
        self._emit("on_task_end", self.record, trec)
        return False

    def _resolve(self, value: Any) -> Any:
        if isinstance(value, TaskRef):
            return self._results[value.node_id]
        if isinstance(value, (list, tuple)):
            return type(value)(self._resolve(v) for v in value)
        if isinstance(value, dict):
            return {k: self._resolve(v) for k, v in value.items()}
        return value

    def _emit(self, hook: str, *args: Any) -> None:
        for s in self.sinks:
            try:
                getattr(s, hook)(*args)
            except Exception as exc:  # noqa: BLE001 - sinks must never break the flow
                log.warning("sink %s.%s failed: %r", type(s).__name__, hook, exc)

    # ------------------------------------------------------------------ inspection
    def to_mermaid(self) -> str:
        return self.graph.to_mermaid()

    def __repr__(self) -> str:
        return f"Flow({self.name!r}, run_id={self.run_id!r}, nodes={len(self.graph.nodes)})"


# ---------------------------------------------------------------------- helpers
def _iter_refs(args: tuple, kwargs: dict) -> Iterable[TaskRef]:
    def walk(v: Any) -> Iterable[TaskRef]:
        if isinstance(v, TaskRef):
            yield v
        elif isinstance(v, (list, tuple, set)):
            for x in v:
                yield from walk(x)
        elif isinstance(v, dict):
            for x in v.values():
                yield from walk(x)

    for a in args:
        yield from walk(a)
    for a in kwargs.values():
        yield from walk(a)


def _describe_inputs(task: Task, args: tuple, kwargs: dict) -> dict[str, str]:
    try:
        bound = task.signature.bind_partial(*args, **kwargs)
        return {k: _short_repr(v) for k, v in bound.arguments.items()}
    except TypeError:
        return {f"arg{i}": _short_repr(a) for i, a in enumerate(args)} | {
            k: _short_repr(v) for k, v in kwargs.items()
        }


def _call_with_timeout(task: Task, args: tuple, kwargs: dict, timeout: float | None) -> Any:
    if timeout is None:
        return task.fn(*args, **kwargs)
    # Thread-based timeout: portable (works in Lambda/Glue where signals may not).
    # Note: the worker thread is not killed on timeout; it is abandoned.
    ex = cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"nanoflow-{task.name}")
    fut = ex.submit(task.fn, *args, **kwargs)
    try:
        return fut.result(timeout=timeout)
    except cf.TimeoutError as exc:
        raise TaskTimeout(f"task {task.name!r} exceeded {timeout}s") from exc
    finally:
        ex.shutdown(wait=False)
