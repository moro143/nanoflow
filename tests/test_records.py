import json

from nanoflow.records import MAX_REPR, RunRecord, TaskRecord, _short_repr


def test_short_repr_truncates_long_values():
    r = _short_repr("x" * 500)
    assert len(r) == MAX_REPR + 3
    assert r.endswith("...")


def test_short_repr_handles_unrepresentable_object():
    class Bad:
        def __repr__(self):
            raise RuntimeError("boom")

    assert "unrepr-able" in _short_repr(Bad())


def test_taskrecord_duration_none_before_completion():
    t = TaskRecord(node_id="x", task="x")
    assert t.duration_ms is None


def test_taskrecord_lifecycle_transitions():
    t = TaskRecord(node_id="x", task="x")
    t.mark_running()
    assert t.status == "running"
    t.mark_success(42)
    assert t.status == "success"
    assert t.output_type == "int"
    assert t.output_repr == "42"
    assert t.duration_ms is not None

    failed = TaskRecord(node_id="y", task="y")
    failed.mark_running()
    failed.mark_failed(ValueError("nope"))
    assert failed.status == "failed"
    assert failed.error == "nope"
    assert failed.error_type == "ValueError"
    assert failed.traceback is not None

    skipped = TaskRecord(node_id="z", task="z")
    skipped.mark_skipped("upstream failed")
    assert skipped.status == "skipped"
    assert skipped.error == "upstream failed"


def test_runrecord_duration_none_before_completion():
    r = RunRecord(flow="f", run_id="r")
    assert r.duration_ms is None
    assert r.ok is False


def test_runrecord_summary_counts_by_status():
    r = RunRecord(flow="f", run_id="r", status="failed")
    r.tasks["a"] = TaskRecord(node_id="a", task="a", status="success")
    r.tasks["b"] = TaskRecord(node_id="b", task="b", status="failed")
    r.tasks["b"].error = "boom"
    s = r.summary()
    assert s["tasks"] == {"success": 1, "failed": 1}
    assert s["failed"] == ["b"]


def test_runrecord_to_json_round_trips():
    r = RunRecord(flow="f", run_id="r", status="success")
    data = json.loads(r.to_json())
    assert data["flow"] == "f" and data["run_id"] == "r"
