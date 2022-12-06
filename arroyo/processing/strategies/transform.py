from arroyo.processing.strategies.run_task import (
    MessageBatch,
    RunTask,
    RunTaskWithMultiprocessing,
    TResult,
    ValueTooLarge,
)
from arroyo.processing.strategies.run_task import (
    parallel_run_task_worker_apply as parallel_transform_worker_apply,
)
from arroyo.types import TPayload


class TransformStep(RunTask[TPayload, TResult]):
    """
    Transforms a message and submits the transformed value to the next
    processing step.

    This is now an alias for RunTask, which explicitly allows side effects.
    Kept for backwards compatibility.
    """

    pass


class ParallelTransformStep(RunTaskWithMultiprocessing[TPayload, TResult]):
    """
    Transforms a message in parallel and submits the transformed value to
    the next processing step.

    This is now an alias for RunTaskWithMultiprocessing , which explicitly
    allows side effects. Kept for backwards compatibility.
    """


__all__ = [
    "TransformStep",
    "ParallelTransformStep",
    "MessageBatch",
    "ValueTooLarge",
    "parallel_transform_worker_apply",
]
