"""Step 1 of a two-step pipeline. Run with a shared run id so both steps'
run.json files can be correlated afterwards (the way two Step Functions
states would share one execution id):

    NANOFLOW_RUN_ID=demo-1 python examples/pipeline_filter.py
    NANOFLOW_RUN_ID=demo-1 python examples/pipeline_stats.py

The filtered rows themselves cross the process boundary as a plain file this
task writes — the same way your own code would hand data between a Glue job
and a Lambda via S3. Nanoflow never touches the file's contents; it only
records that this step ran, via FileSink's run.json.
"""
import json
import logging
import random
from pathlib import Path

from nanoflow import Flow, task

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

OUTPUT_PATH = Path(".nanoflow/data/filtered_rows.json")


@task
def extract(n: int) -> list[dict]:
    return [{"id": i, "amount": random.randint(-20, 100)} for i in range(n)]


@task
def clean(rows: list[dict]) -> list[dict]:
    return [r for r in rows if r["amount"] > 0]


@task
def publish(rows: list[dict]) -> str:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(rows))
    print(f"wrote {len(rows)} filtered rows to {OUTPUT_PATH}")
    return str(OUTPUT_PATH)


if __name__ == "__main__":
    with Flow("filter-step") as f:
        path = publish(clean(extract(50)))

    run = f.run()
    print("filter-step done, run_id:", run.run_id)
    print(run.summary())
