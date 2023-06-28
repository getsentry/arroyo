==================
Dead letter queues
==================

.. warning::
    Dead letter queues should be used with caution as they break some of the ordering guarantees
    otherwise offered by Arroyo and Kafka consumer code. In particular, it must be safe for the
    consumer to drop a message. If replaying or later re-processing of the DLQ'ed messages is done,
    it is critical that ordering is not a requirement in the relevant downstream code.

Arroyo provides support for routing invalid messages to dead letter queues in consumers.
Dead letter queues are critical in some applications because messages are ordered in Kafka
and a single invalid message can cause a consumer to crash and every subsequent message to
not be processed.

The dead letter queue configuration is passed to the `StreamProcessor` and, if provided, any
`InvalidMessage` raise by a strategy will be produced to the dead letter queue.


.. automodule:: arroyo.dlq
   :members: InvalidMessage, DlqLimit, DlqPolicy, DlqProducer, KafkaDlqProducer, NoopDlqProducer
