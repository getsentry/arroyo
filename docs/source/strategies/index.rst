Processing Strategies
=====================

The processing strategies are the components to be wired together to
build a consumer.

Strategy interface
-------------------------

We normally don't recommend writing your own strategy, and encourage you to use
built-in ones such as "reduce" or "run task" to plug in your application logic.
Nevertheless, all arroyo strategies are written against the following interface:

.. automodule:: arroyo.processing.strategies.abstract
   :members:
   :undoc-members:
   :show-inheritance:

Messages
------------

.. automodule:: arroyo.types
   :members:
   :undoc-members:

.. toctree::
    :hidden:

    filter
    reduce
    unfold
    batching
    run_task
    run_task_in_threads
    run_task_with_multiprocessing
    produce
    commit_offsets
    healthcheck
