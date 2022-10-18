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

Collectors
-----------------------------

Accumulate messages in  abuffer and execute an operation on the batch when full.

.. automodule:: arroyo.processing.strategies.collect
   :members:
   :undoc-members:

Task Runners
-----------------------------

.. automodule:: arroyo.processing.strategies.run_task
   :members:
   :undoc-members:

Producers
--------------------------

.. automodule:: arroyo.processing.strategies.produce
   :members:
   :undoc-members:
