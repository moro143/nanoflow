"""Run with: python examples/local_etl.py

Writes .nanoflow/runs/<flow>__<run_id>.json and logs to stdout.
"""
import logging
import random

from nanoflow import Flow, task

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


@task
def extract(n: int) -> list[dict]:
    return [{"id": i, "amount": random.randint(-5, 100)} for i in range(n)]


@task
def clean(rows: list[dict]) -> list[dict]:
    return [r for r in rows if r["amount"] > 0]


@task(retries=2, retry_delay=0.1)
def enrich(rows: list[dict]) -> list[dict]:
    if random.random() < 0.5:  # simulate flaky API
        raise ConnectionError("upstream API blip")
    return [{**r, "amount_eur": r["amount"] * 0.92} for r in rows]


@task
def load(rows: list[dict], target: str) -> int:
    print(f"would write {len(rows)} rows to {target}")
    return len(rows)


if __name__ == "__main__":
    with Flow("local-etl", params={"n": 20}, max_workers=2) as f:
        raw = extract(20)
        cleaned = clean(raw)
        enriched = enrich(cleaned)
        n = load(enriched, "s3://curated/orders/")

    run = f.run()
    print("loaded", n.result())
    print(run.summary())
    print(f.to_mermaid())
