"""Small helpers for running flows inside AWS Lambda, Glue and Step Functions.

Nothing here provisions resources. These are conveniences for code you already
have running in AWS. boto3 is only imported where strictly needed.
"""
from __future__ import annotations

import functools
import os
import sys
from typing import Any, Callable

from nanoflow.errors import FlowFailed
from nanoflow.records import RunRecord

RUN_ID_KEYS = ("nanoflow_run_id", "run_id", "runId")


def run_id_from_event(event: Any) -> str | None:
    """Extract a run id from a Lambda/Step Functions event.

    Looks at top-level keys and at `$.nanoflow`. Step Functions users typically
    pass `"nanoflow_run_id.$": "$$.Execution.Name"` in Task parameters so every
    step in the state machine shares one run id.
    """
    if not isinstance(event, dict):
        return None
    for k in RUN_ID_KEYS:
        v = event.get(k)
        if isinstance(v, str) and v:
            return v
    nested = event.get("nanoflow")
    if isinstance(nested, dict):
        for k in RUN_ID_KEYS:
            v = nested.get(k)
            if isinstance(v, str) and v:
                return v
    return None


def lambda_flow(
    fn: Callable[..., Any] | None = None,
    *,
    return_summary: bool = True,
    reraise: bool = True,
) -> Any:
    """Decorator for Lambda handlers that build and run a Flow.

    - Seeds the run id from the event (or the Lambda request id) so nested
      Flows pick it up automatically via `infer_run_id()`.
    - If the handler returns a RunRecord (i.e. `return f.run()`), converts it
      to a JSON-safe summary for Step Functions.

    Usage::

        @lambda_flow
        def handler(event, context):
            with Flow("ingest") as f:
                push(validate(fetch(event["source"])))
            return f.run()
    """

    def wrap(handler: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(handler)
        def inner(event: Any, context: Any = None) -> Any:
            rid = run_id_from_event(event) or getattr(context, "aws_request_id", None)
            prev = os.environ.get("_NANOFLOW_LAMBDA_REQUEST_ID")
            if rid:
                os.environ["_NANOFLOW_LAMBDA_REQUEST_ID"] = str(rid)
            try:
                result = handler(event, context)
            except FlowFailed:
                if reraise:
                    raise
                return {"status": "failed"}
            finally:
                if prev is None:
                    os.environ.pop("_NANOFLOW_LAMBDA_REQUEST_ID", None)
                else:
                    os.environ["_NANOFLOW_LAMBDA_REQUEST_ID"] = prev
            if isinstance(result, RunRecord) and return_summary:
                return result.summary()
            return result

        return inner

    return wrap(fn) if fn is not None else wrap


def glue_args(*names: str, argv: list[str] | None = None) -> dict[str, str]:
    """Parse `--KEY value` / `--KEY=value` style Glue job arguments without awsglue.

    Works locally too, so the same script can run outside Glue.
    Requested names that are absent are simply omitted from the result.
    """
    argv = list(sys.argv[1:] if argv is None else argv)
    out: dict[str, str] = {}
    i = 0
    while i < len(argv):
        a = argv[i]
        if a.startswith("--"):
            if "=" in a:
                k, v = a[2:].split("=", 1)
                out[k] = v
            elif i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                out[a[2:]] = argv[i + 1]
                i += 1
            else:
                out[a[2:]] = ""
        i += 1
    if names:
        out = {k: v for k, v in out.items() if k in names}
    return out


def glue_run_id(argv: list[str] | None = None) -> str | None:
    """Return a stable run id inside Glue: explicit --nanoflow_run_id, else JOB_RUN_ID."""
    a = glue_args("nanoflow_run_id", "JOB_RUN_ID", argv=argv)
    return a.get("nanoflow_run_id") or a.get("JOB_RUN_ID")
