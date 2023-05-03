Processing Strategies
=====================

The processing strategies are the components to be wired together to
build a consumer.

Filter
-----------------------------

.. automodule:: arroyo.processing.strategies.filter
   :members:


Reduce (Fold)
-----------------------------

Accumulate messages based on a custom accumulator function

.. automodule:: arroyo.processing.strategies.reduce
   :members:

Unfold
-----------------------------

Generates a sequence of messages from a single message based on a custom generator function

.. automodule:: arroyo.processing.strategies.unfold
   :members:


Batch and Unbatch
-----------------------------

Accumulate messages into a batch and pass to the next step.
The batch and unbatch strategies are based on reduce and unfold.
Use reduce/unfold instead if you want to provide custom
accumulator/generator functions.

.. automodule:: arroyo.processing.strategies.batching
   :members:


Run Task
-----------------------------

.. automodule:: arroyo.processing.strategies.run_task
   :members:


Run Task in Threads
-----------------------------

.. automodule:: arroyo.processing.strategies.run_task_in_threads
   :members:


Run Task with Multiprocessing
-----------------------------

.. automodule:: arroyo.processing.strategies.run_task_with_multiprocessing
   :members:


Producers
-----------------------------

.. automodule:: arroyo.processing.strategies.produce
   :members:


Commit offsets
-----------------------------

Should be used as the last strategy in the chain, to ensure
that offsets are only committed once all processing is complete.

.. automodule:: arroyo.processing.strategies.commit
   :members:


Writing your own strategy
-------------------------

We normally don't recommend writing your own strategy, and encourage you to use
built-in ones such as "reduce" or "run task" to plug in your application logic.
Nevertheless, all arroyo strategies are written against the following interface:

.. automodule:: arroyo.processing.strategies.abstract
   :members:
   :undoc-members:
   :show-inheritance:
