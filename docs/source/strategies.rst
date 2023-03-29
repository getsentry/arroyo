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


Filter
-----------------------------

.. automodule:: arroyo.processing.strategies.filter
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


Batch and Unbatch
-----------------------------

Accumulate messages into a batch and pass to the next step.
The batch and unbatch strategies are based on reduce and unfold.
Use reduce/unfold instead if you want to provide custom
accumulator/generator functions.

.. automodule:: arroyo.processing.strategies.batching
   :members:
   :undoc-members:


Run Task
-----------------------------

.. automodule:: arroyo.processing.strategies.run_task
   :members:
   :undoc-members:


Run Task in Threads
-----------------------------

.. automodule:: arroyo.processing.strategies.run_task_in_threads
   :members:
   :undoc-members:


Run Task with Multiprocessing
-----------------------------

.. automodule:: arroyo.processing.strategies.run_task_with_multiprocessing
   :members:
   :undoc-members:


Transformers
-----------------------------

Transformation steps. They transform the messages one by one provided a processing
function. Alias for RunTask strategies.

.. automodule:: arroyo.processing.strategies.transform
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

Commit offsets
-----------------------------

Should be used as the last strategy in the chain, to ensure
that offsets are only committed once all processing is complete.

.. automodule:: arroyo.processing.strategies.commit
   :members:
   :undoc-members:
