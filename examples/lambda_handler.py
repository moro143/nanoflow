"""Drop this into an existing Lambda. No infra changes needed.

Step Functions state to share one run id across every step:

    "Ingest": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "my-ingest-fn",
        "Payload": {
          "source.$": "$.source",
          "nanoflow_run_id.$": "$$.Execution.Name"
        }
      },
      "Next": "Transform"
    }

Set NANOFLOW_S3_BUCKET on the function to also persist run.json to S3.
"""
from nanoflow import Flow, task
from nanoflow.aws import lambda_flow


@task
def fetch(source: str) -> list[dict]:
    return [{"id": 1, "src": source}]


@task
def validate(rows: list[dict]) -> list[dict]:
    if not rows:
        raise ValueError("no rows")
    return rows


@task(retries=3, retry_delay=0.5)
def push(rows: list[dict]) -> int:
    return len(rows)


@lambda_flow
def handler(event, context):
    with Flow("ingest") as f:
        push(validate(fetch(event["source"])))
    return f.run()  # returned as a JSON summary to Step Functions


if __name__ == "__main__":
    print(handler({"source": "local-test", "nanoflow_run_id": "manual-1"}, None))
