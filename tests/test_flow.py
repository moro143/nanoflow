import io
import json
import time

import pytest

from nanoflow import (
    CallbackSink,
    CycleError,
    FileSink,
    Flow,
    FlowFailed,
    JsonLinesSink,
    TaskTimeout,
    task,
)
from nanoflow.aws import glue_args, lambda_flow, run_id_from_event


@task
def one():
    return 1


@task
def add(a, b):
    return a + b


@task
def boom(x):
    raise ValueError("nope")


def test_linear_flow_and_results():
    with Flow("t", sinks=[]) as f:
        a = one()
        b = add(a, 2)
        c = add(b, a)
    rec = f.run()
    assert rec.ok
    assert c.result() == 4
    assert [t.status for t in rec.tasks.values()] == ["success"] * 3
    assert rec.tasks["add_1"].upstream == ["add", "one"]


def test_task_outside_flow_is_plain_function():
    assert add(2, 3) == 5


def test_refs_inside_containers_create_edges():
    @task
    def total(xs):
        return sum(xs)

    with Flow("t", sinks=[]) as f:
        a, b = one(), one()
        t = total([a, b, 5])
    f.run()
    assert t.result() == 7
    assert f.graph.nodes["total"].upstream == {"one", "one_1"}


def test_failure_skips_downstream_and_raises():
    with Flow("t", sinks=[]) as f:
        a = one()
        b = boom(a)
        c = add(b, 1)
    with pytest.raises(FlowFailed):
        f.run()
    assert f.record.tasks["boom"].status == "failed"
    assert f.record.tasks["boom"].error_type == "ValueError"
    assert f.record.tasks["add"].status == "skipped"
    assert f.record.status == "failed"


def test_raise_on_failure_false_returns_record():
    with Flow("t", sinks=[], raise_on_failure=False) as f:
        boom(1)
    rec = f.run()
    assert rec.status == "failed"
    assert rec.summary()["failed"] == ["boom"]


def test_retries():
    calls = {"n": 0}

    @task(retries=2)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("try again")
        return "ok"

    with Flow("t", sinks=[]) as f:
        r = flaky()
    f.run()
    assert r.result() == "ok"
    assert f.record.tasks["flaky"].attempts == 3


def test_timeout():
    @task(timeout=0.05)
    def slow():
        time.sleep(1)

    with Flow("t", sinks=[], raise_on_failure=False) as f:
        slow()
    rec = f.run()
    assert rec.tasks["slow"].error_type == "TaskTimeout"


def test_parallel_execution_is_faster():
    @task
    def nap():
        time.sleep(0.2)
        return 1

    with Flow("t", sinks=[], max_workers=4) as f:
        xs = [nap() for _ in range(4)]
        add(xs[0], xs[1])
    t0 = time.time()
    f.run()
    assert time.time() - t0 < 0.6


def test_parallel_failure_skips():
    @task
    def nap():
        time.sleep(0.05)
        return 1

    with Flow("t", sinks=[], max_workers=2, raise_on_failure=False) as f:
        a = nap()
        b = boom(a)
        add(b, 1)
        nap()
    rec = f.run()
    assert rec.tasks["add"].status == "skipped"


def test_cycle_detection():
    from nanoflow.graph import Graph, Node

    g = Graph()
    g.add(Node("a", one, (), {}))
    g.add(Node("b", one, (), {}, upstream={"a"}))
    g.nodes["a"].upstream.add("b")
    with pytest.raises(CycleError):
        g.topological_order()


def test_jsonlines_and_file_sinks(tmp_path):
    buf = io.StringIO()
    with Flow("t", sinks=[JsonLinesSink(buf), FileSink(tmp_path)]) as f:
        add(one(), 1)
    f.run()
    events = [json.loads(l) for l in buf.getvalue().splitlines()]
    assert events[0]["nanoflow_event"] == "run_start"
    assert events[-1]["nanoflow_event"] == "run_end"
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
    data = json.loads(files[0].read_text())
    assert data["status"] == "success" and set(data["tasks"]) == {"one", "add"}


def test_sink_errors_do_not_break_flow():
    def bad(*a):
        raise RuntimeError("sink broke")

    with Flow("t", sinks=[CallbackSink(on_run_end=bad, on_task_end=bad)]) as f:
        one()
    assert f.run().ok


def test_mermaid():
    with Flow("t", sinks=[]) as f:
        add(one(), 1)
    m = f.to_mermaid()
    assert "one --> add" in m


def test_lambda_flow_decorator():
    @lambda_flow
    def handler(event, context):
        with Flow("h", sinks=[]) as f:
            add(one(), 1)
        return f.run()

    out = handler({"nanoflow_run_id": "exec-123"}, None)
    assert out["status"] == "success" and out["run_id"] == "exec-123"


def test_run_id_from_event():
    assert run_id_from_event({"nanoflow": {"run_id": "x"}}) == "x"
    assert run_id_from_event({"runId": "y"}) == "y"
    assert run_id_from_event("nope") is None


def test_glue_args():
    argv = ["--JOB_NAME", "j", "--JOB_RUN_ID=jr_1", "--flag", "--extra", "v"]
    assert glue_args(argv=argv) == {"JOB_NAME": "j", "JOB_RUN_ID": "jr_1", "flag": "", "extra": "v"}
    assert glue_args("JOB_NAME", argv=argv) == {"JOB_NAME": "j"}
