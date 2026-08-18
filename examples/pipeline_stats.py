"""Step 2 of a two-step pipeline. Run pipeline_filter.py first with the same
NANOFLOW_RUN_ID, then:

    NANOFLOW_RUN_ID=demo-1 python examples/pipeline_stats.py

Reads the file pipeline_filter.py wrote — plain file I/O, same as your own
code would use with S3 in Glue/Lambda. Afterwards, check .nanoflow/runs/:
both steps' run.json share the same run_id, so their execution history
(status, timings, task graph) can be correlated even though they were two
separate Python processes.
"""
import json
import logging
from pathlib import Path

from nanoflow import Flow, task

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

INPUT_PATH = Path(".nanoflow/data/filtered_rows.json")


@task
def fetch_filtered() -> list[dict]:
    if not INPUT_PATH.exists():
        raise RuntimeError(f"{INPUT_PATH} not found — run examples/pipeline_filter.py first")
    return json.loads(INPUT_PATH.read_text())


@task
def stats(rows: list[dict]) -> dict:
    amounts = [r["amount"] for r in rows]
    return {
        "count": len(amounts),
        "sum": sum(amounts),
        "avg": round(sum(amounts) / len(amounts), 2) if amounts else 0,
        "min": min(amounts) if amounts else None,
        "max": max(amounts) if amounts else None,
    }


if __name__ == "__main__":
    with Flow("stats-step") as f:
        result = stats(fetch_filtered())

    run = f.run()
    print("stats-step done, run_id:", run.run_id)
    print(run.summary())
