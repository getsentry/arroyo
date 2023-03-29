from arroyo.processing.strategies.run_task import RunTask, TResult
from arroyo.processing.strategies.run_task_with_multiprocessing import (
    MessageBatch,
    RunTaskWithMultiprocessing,
)
from arroyo.processing.strategies.run_task_with_multiprocessing import (
    parallel_run_task_worker_apply as parallel_transform_worker_apply,
)
from arroyo.types import TStrategyPayload


class TransformStep(RunTask[TStrategyPayload, TResult]):
    """
    Transforms a message and submits the transformed value to the next
    processing step.

    This is now an alias for RunTask, which explicitly allows side effects.
    Kept for backwards compatibility.
    """

    pass


class ParallelTransformStep(RunTaskWithMultiprocessing[TStrategyPayload, TResult]):
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
    "parallel_transform_worker_apply",
]
