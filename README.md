# nanoflow-core

A lightweight, zero-dependency DAG task runner that lives *inside* your existing Python code.

It does not schedule, deploy or provision anything. You add it to a Lambda, a Glue job, a SageMaker
script or a plain `python main.py`, and it gives you:

- a DAG built from ordinary function calls (dependencies inferred from data flow)
- retries with backoff, timeouts, optional thread parallelism
- a structured `run.json` per run: task order, timings, inputs/outputs (truncated), errors
- pluggable sinks (log lines, JSON lines for CloudWatch, local file, S3, callbacks)
- automatic runtime detection (local / Lambda / Glue / SageMaker) with sensible defaults
- one shared `run_id` across Step Functions steps so runs can be stitched together later

## Install

```bash
pip install nanoflow-core          # zero deps
pip install "nanoflow-core[aws]"   # adds boto3 for the S3 sink
```

## 60-second example

```python
from nanoflow import Flow, task

@task
def extract(n): return list(range(n))

@task
def transform(xs): return [x * 2 for x in xs]

@task(retries=3, retry_delay=1.0)
def load(xs, target): print(f"{len(xs)} rows -> {target}"); return len(xs)

with Flow("etl") as f:
    raw = extract(10)
    out = transform(raw)
    n = load(out, "s3://bucket/path")

run = f.run()          # executes in dependency order
print(n.result())      # 10
print(run.summary())   # {'flow': 'etl', 'status': 'success', 'tasks': {'success': 3}, ...}
```

Locally this logs to stdout and writes `.nanoflow/runs/etl__<run_id>.json`.
Inside Lambda/Glue it emits JSON lines to CloudWatch, and to S3 if `NANOFLOW_S3_BUCKET` is set.

Calling a `@task` outside a `Flow` just calls the function, so tasks stay unit-testable.

## Inside AWS Lambda

```python
from nanoflow import Flow, task
from nanoflow.aws import lambda_flow

@lambda_flow
def handler(event, context):
    with Flow("ingest") as f:
        push(validate(fetch(event["source"])))
    return f.run()   # converted to a JSON summary for Step Functions
```

`@lambda_flow` seeds the run id from `event["nanoflow_run_id"]` (or the Lambda request id).
In Step Functions pass `"nanoflow_run_id.$": "$$.Execution.Name"` to every step to correlate them.

## Inside AWS Glue

```python
from nanoflow import Flow, task
from nanoflow.aws import glue_args, glue_run_id

args = glue_args("JOB_NAME", "input", "output")   # no awsglue import needed, works locally too

with Flow("orders-transform", run_id=glue_run_id(), params=args) as f:
    write(clean(read(args["input"])), args["output"])
f.run()
```

## Options

```python
Flow(
    name,
    run_id=None,          # default: from env/argv, else random
    params={},            # recorded in run.json
    sinks=None,           # default: chosen per runtime; pass [] to disable
    max_workers=1,        # >1 runs independent tasks in threads
    fail_fast=True,       # stop scheduling new tasks after a failure
    raise_on_failure=True # raise FlowFailed at the end if anything failed
)

@task(name=None, retries=0, retry_delay=0.0, retry_backoff=2.0, timeout=None, tags={})
```

Custom sinks: subclass `nanoflow.Sink` and override any of `on_run_start`, `on_task_start`,
`on_task_end`, `on_run_end`. Sinks are best-effort and never fail the flow.

## CLI

```bash
nanoflow show .nanoflow/runs/etl__3e78029f4f0d.json     # summary + per-task table
nanoflow graph .nanoflow/runs/etl__3e78029f4f0d.json     # DAG as Mermaid (default)
nanoflow graph .nanoflow/runs/etl__3e78029f4f0d.json --format dot
```

`show` exits `1` if the run failed, `0` otherwise — usable in scripts/CI. Both commands read
a `run.json` produced by `FileSink`/`S3Sink`; no live Flow object needed.

## Design notes

- Data is passed between tasks **in memory** as Python objects (DataFrames are fine). Nanoflow runs
  inside one process; cross-process handoff is left to your Step Function / S3 as today.
- `run.json` never stores real payloads, only type + truncated repr.
- Timeouts use a worker thread (portable to Lambda/Glue); the thread is abandoned, not killed.
- The whole library is a few hundred lines with no dependencies on purpose.

## Roadmap

- CloudWatch metrics sink, Slack/SNS callback helpers
- Hosted run explorer (cross-job timeline, lineage, cost) — the paid tier
- Shared config file (e.g. `nanoflow.toml`) so a multi-job pipeline (several Glue jobs +
  Lambdas) can agree on bucket/prefix/tags without repeating them per job. Format still
  undecided (yaml/json/toml).
- `Store` abstraction for passing actual result data between separate job invocations —
  today only `run_id` is shared across steps (via Step Functions / env), the real data
  stays in-memory within one `Flow.run()` call. Needs deciding: local-disk backend before
  S3, and whether tasks opt in per-task (`store.put(...)`) vs. automatic.

## Development

```bash
pip install -e ".[dev]"
pytest
```
