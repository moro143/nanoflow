import json

from nanoflow import Flow, task
from nanoflow.cli import main


@task
def extract():
    return [1, 2, 3]


@task
def double(xs):
    return [x * 2 for x in xs]


@task
def boom(xs):
    raise ValueError("nope")


def _run_json(tmp_path, fail=False):
    with Flow("t", sinks=[], raise_on_failure=not fail) as f:
        xs = extract()
        boom(xs) if fail else double(xs)
    run = f.run()
    path = tmp_path / "run.json"
    path.write_text(run.to_json())
    return path


def test_show_success(tmp_path, capsys):
    path = _run_json(tmp_path)
    code = main(["show", str(path)])
    out = capsys.readouterr().out
    assert code == 0
    assert "flow:     t" in out
    assert "status:   success" in out
    assert "extract" in out and "double" in out


def test_show_failed(tmp_path, capsys):
    path = _run_json(tmp_path, fail=True)
    code = main(["show", str(path)])
    out = capsys.readouterr().out
    assert code == 1
    assert "1 failed: boom" in out
    assert "nope" in out


def test_graph_mermaid(tmp_path, capsys):
    path = _run_json(tmp_path)
    code = main(["graph", str(path)])
    out = capsys.readouterr().out
    assert code == 0
    assert out.startswith("graph TD")
    assert "extract --> double" in out


def test_graph_dot(tmp_path, capsys):
    path = _run_json(tmp_path)
    main(["graph", str(path), "--format", "dot"])
    out = capsys.readouterr().out
    assert out.startswith("digraph nanoflow {")
    assert '"extract" -> "double";' in out


def test_show_missing_file(tmp_path, capsys):
    code = main(["show", str(tmp_path / "nope.json")])
    err = capsys.readouterr().err
    assert code == 2
    assert "error:" in err


def test_show_invalid_json(tmp_path, capsys):
    path = tmp_path / "bad.json"
    path.write_text("{not json")
    code = main(["show", str(path)])
    err = capsys.readouterr().err
    assert code == 2
    assert "invalid JSON" in err


def _write_run(directory, flow_name, run_id, started_at, fail=False):
    with Flow(flow_name, run_id=run_id, sinks=[], raise_on_failure=not fail) as f:
        boom(1) if fail else extract()
    rec = f.run()
    data = json.loads(rec.to_json())
    data["started_at"] = started_at
    run_dir = directory / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / f"{flow_name}.json"
    path.write_text(json.dumps(data))
    return path


def test_pipeline_lists_steps_in_temporal_order(tmp_path, capsys):
    _write_run(tmp_path, "stats-step", "demo-1", started_at=200)
    _write_run(tmp_path, "filter-step", "demo-1", started_at=100)
    code = main(["pipeline", "demo-1", "--dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert code == 0
    filter_line = out.index("filter-step")
    stats_line = out.index("stats-step")
    assert filter_line < stats_line


def test_pipeline_reports_failed_steps(tmp_path, capsys):
    _write_run(tmp_path, "filter-step", "demo-1", started_at=100)
    _write_run(tmp_path, "stats-step", "demo-1", started_at=200, fail=True)
    code = main(["pipeline", "demo-1", "--dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert code == 1
    assert "1 step(s) failed: stats-step" in out


def test_pipeline_graph_includes_subgraphs_and_connector(tmp_path, capsys):
    _write_run(tmp_path, "filter-step", "demo-1", started_at=100)
    _write_run(tmp_path, "stats-step", "demo-1", started_at=200)
    main(["pipeline", "demo-1", "--dir", str(tmp_path), "--graph"])
    out = capsys.readouterr().out
    assert 'subgraph filter_step ["filter-step"]' in out
    assert 'subgraph stats_step ["stats-step"]' in out
    assert "-.->" in out  # connector between the two steps' run.json


def test_pipeline_unknown_run_id_errors(tmp_path, capsys):
    code = main(["pipeline", "nope", "--dir", str(tmp_path)])
    err = capsys.readouterr().err
    assert code == 2
    assert "no run.json files found" in err
