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
