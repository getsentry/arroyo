Batch and Unbatch
-----------------------------

Accumulate messages into a batch and pass to the next step.
The batch and unbatch strategies are based on reduce and unfold.
Use reduce/unfold instead if you want to provide custom
accumulator/generator functions.

.. automodule:: arroyo.processing.strategies.batching
   :members:
