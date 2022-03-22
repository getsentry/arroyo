from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    DeadLetterQueuePolicy,
    InvalidMessage,
)


class RaiseInvalidMessagePolicy(DeadLetterQueuePolicy):
    def handle_invalid_message(self, e: InvalidMessage) -> None:
        raise e
