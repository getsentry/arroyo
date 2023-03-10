Processing Strategies
=====================

The processing strategies are the components to be wired together to
build a consumer.

Processing Strategy Interface
-----------------------------

The abstract interface to be implemented when creating a new Processing Strategy

.. automodule:: arroyo.processing.strategies.abstract
   :members:
   :undoc-members:
   :show-inheritance:

Transformers
-----------------------------

Transformation steps. They transform the messages one by one provided a processing
function.

.. automodule:: arroyo.processing.strategies.transform
   :members:
   :undoc-members:

Filters
-----------------------------

.. automodule:: arroyo.processing.strategies.filter
   :members:
   :undoc-members:

Batch and Unbatch
-----------------------------

Accumulate messages into a batch and pass to the next step.

.. automodule:: arroyo.processing.strategies.batching
   :members:
   :undoc-members:

Reduce (Fold)
-----------------------------

Accumulate messages based on a custom accumulator function

.. automodule:: arroyo.processing.strategies.reduce
   :members:
   :undoc-members:

Unfold
-----------------------------

Generates a sequence of messages from a single message based on a custom generator function

.. automodule:: arroyo.processing.strategies.unfold
   :members:
   :undoc-members:

Task Runners
-----------------------------

.. automodule:: arroyo.processing.strategies.run_task
   :members:
   :undoc-members:

Producers
-----------------------------

.. automodule:: arroyo.processing.strategies.produce
   :members:
   :undoc-members:

Decoders
-----------------------------

Provides decoders and schema validation for messages

.. automodule:: arroyo.processing.strategies.decoder
   :members:
   :undoc-members:


Dead Letter Queue
-----------------------------

Arroyo's DLQ is subject to change, and will likely be redesigned from the
ground up in an upcoming release.

.. automodule:: arroyo.processing.strategies.dead_letter_queue
   :members:
   :undoc-members:
   :show-inheritance:
