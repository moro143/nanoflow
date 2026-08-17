class NanoflowError(Exception):
    """Base class for nanoflow errors."""


class CycleError(NanoflowError):
    """Raised when the task graph contains a cycle."""


class TaskFailed(NanoflowError):
    """Raised when a task fails after exhausting retries."""

    def __init__(self, node_id: str, task_name: str, cause: BaseException) -> None:
        self.node_id = node_id
        self.task_name = task_name
        self.cause = cause
        super().__init__(f"task {task_name!r} ({node_id}) failed: {cause!r}")


class TaskTimeout(NanoflowError):
    """Raised when a task exceeds its timeout."""


class UpstreamFailed(NanoflowError):
    """Raised for tasks skipped because an upstream dependency failed."""


class FlowFailed(NanoflowError):
    """Raised by Flow.run() when one or more tasks failed."""

    def __init__(self, run: "object") -> None:  # RunRecord
        self.run = run
        failed = [t.task for t in getattr(run, "tasks", {}).values() if t.status == "failed"]
        super().__init__(f"flow failed; failed tasks: {failed}")
