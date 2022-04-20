from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidBatchedMessages,
)


class RaiseInvalidMessagePolicy(DeadLetterQueuePolicy):
    def handle_invalid_messages(self, e: InvalidBatchedMessages) -> None:
        raise e
