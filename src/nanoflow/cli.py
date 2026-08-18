"""nanoflow CLI: inspect run.json files produced by FileSink/S3Sink."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def _fmt_ms(ms: float | None) -> str:
    return f"{ms:.1f}ms" if ms is not None else "-"


def cmd_show(args: argparse.Namespace) -> int:
    run = _load(args.run_json)
    print(f"flow:     {run['flow']}")
    print(f"run_id:   {run['run_id']}")
    print(f"status:   {run['status']}")
    print(f"duration: {_fmt_ms(run.get('duration_ms'))}")
    if run.get("params"):
        print(f"params:   {run['params']}")
    print()

    tasks = run.get("tasks", {})
    rows = [("TASK", "STATUS", "ATTEMPTS", "DURATION", "ERROR")]
    for nid, t in tasks.items():
        err = t.get("error") or ""
        if len(err) > 40:
            err = err[:37] + "..."
        rows.append((nid, t["status"], str(t["attempts"]), _fmt_ms(t.get("duration_ms")), err))
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    for row in rows:
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths)))

    failed = [t["task"] for t in tasks.values() if t["status"] == "failed"]
    if failed:
        print(f"\n{len(failed)} failed: {', '.join(failed)}")
    return 1 if run["status"] == "failed" else 0


def _graph_nodes(run: dict[str, Any]) -> list[dict[str, Any]]:
    graph = run.get("graph")
    if not graph or "nodes" not in graph:
        raise SystemExit("run.json has no graph data")
    return graph["nodes"]


def _to_mermaid(nodes: list[dict[str, Any]]) -> str:
    lines = ["graph TD"]
    for n in nodes:
        lines.append(f'    {n["id"]}["{n["task"]}"]')
    for n in nodes:
        for up in n["upstream"]:
            lines.append(f'    {up} --> {n["id"]}')
    return "\n".join(lines)


def _to_dot(nodes: list[dict[str, Any]]) -> str:
    lines = ["digraph nanoflow {"]
    for n in nodes:
        lines.append(f'    "{n["id"]}" [label="{n["task"]}"];')
    for n in nodes:
        for up in n["upstream"]:
            lines.append(f'    "{up}" -> "{n["id"]}";')
    lines.append("}")
    return "\n".join(lines)


def cmd_graph(args: argparse.Namespace) -> int:
    run = _load(args.run_json)
    nodes = _graph_nodes(run)
    text = _to_mermaid(nodes) if args.format == "mermaid" else _to_dot(nodes)
    print(text)
    return 0


def _find_pipeline_runs(directory: Path, run_id: str) -> list[dict[str, Any]]:
    run_dir = directory / run_id
    files = sorted(run_dir.glob("*.json")) if run_dir.is_dir() else []
    runs = [_load(str(f)) for f in files]
    runs.sort(key=lambda r: r.get("started_at") or 0)
    return runs


def cmd_pipeline(args: argparse.Namespace) -> int:
    runs = _find_pipeline_runs(Path(args.dir), args.run_id)
    if not runs:
        run_dir = Path(args.dir) / args.run_id
        print(f"error: no run.json files found in {run_dir}", file=sys.stderr)
        return 2

    print(f"pipeline run_id: {args.run_id}")
    print(f"steps:           {len(runs)}")
    print()

    rows = [("STEP", "STATUS", "DURATION", "TASKS")]
    for r in runs:
        tasks = ",".join(sorted(r.get("tasks", {})))
        rows.append((r["flow"], r["status"], _fmt_ms(r.get("duration_ms")), tasks))
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    for row in rows:
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths)))

    failed = [r["flow"] for r in runs if r["status"] == "failed"]
    if failed:
        print(f"\n{len(failed)} step(s) failed: {', '.join(failed)}")

    if args.graph:
        print()
        print(_pipeline_mermaid(runs))

    return 1 if failed else 0


def _mermaid_id(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name)


def _pipeline_mermaid(runs: list[dict[str, Any]]) -> str:
    lines = ["graph TD"]
    prev_step: str | None = None
    prev_leaves: list[str] = []

    for r in runs:
        step = _mermaid_id(r["flow"])
        nodes = r.get("graph", {}).get("nodes", [])
        lines.append(f'    subgraph {step} ["{r["flow"]}"]')
        for n in nodes:
            lines.append(f'        {step}_{n["id"]}["{n["task"]}"]')
        for n in nodes:
            for up in n["upstream"]:
                lines.append(f"        {step}_{up} --> {step}_{n['id']}")
        lines.append("    end")

        depended_on = {up for n in nodes for up in n["upstream"]}
        roots = [n["id"] for n in nodes if not n["upstream"]]
        leaves = [n["id"] for n in nodes if n["id"] not in depended_on]

        if prev_step and roots:
            for leaf in prev_leaves:
                for root in roots:
                    lines.append(f"    {prev_step}_{leaf} -.-> {step}_{root}")

        prev_step, prev_leaves = step, leaves

    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="nanoflow", description="Inspect nanoflow run.json files.")
    sub = parser.add_subparsers(dest="command", required=True)

    show = sub.add_parser("show", help="pretty-print a run.json summary")
    show.add_argument("run_json")
    show.set_defaults(func=cmd_show)

    graph = sub.add_parser("graph", help="render the DAG from a run.json")
    graph.add_argument("run_json")
    graph.add_argument("--format", choices=["mermaid", "dot"], default="mermaid")
    graph.set_defaults(func=cmd_graph)

    pipeline = sub.add_parser("pipeline", help="show all steps sharing a run_id as one pipeline")
    pipeline.add_argument("run_id")
    pipeline.add_argument("--dir", default=".nanoflow/runs", help="directory of run.json files")
    pipeline.add_argument("--graph", action="store_true", help="also print a combined Mermaid diagram")
    pipeline.set_defaults(func=cmd_pipeline)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except json.JSONDecodeError as exc:
        target = getattr(args, "run_json", None) or getattr(args, "dir", "run.json")
        print(f"error: invalid JSON in {target}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
