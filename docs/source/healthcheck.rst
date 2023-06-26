Healthchecks
============

If your code blocks for too long in the main thread, the consumer can turn
unhealthy.

Kafka has a setting called ``max.poll.interval.ms`` for this that tells Kafka
to kick the consumer out of the broker after this many milliseconds of not polling.

You can pass this option into :py:class:`arroyo.backends.kafka.consumer.KafkaConsumer` like so:

.. code-block:: Python

   consumer = KafkaConsumer(
       {
           "max.poll.interval.ms": 300000, # default 5 minutes
       }
   )

However, this will not shut down the consumer, it will just keep running doing
nothing (because it is blocked in the main thread). You want a pod-level
healthcheck as well.

Arroyo supports touching a file repeatedly from the main thread to indicate health.

.. code-block:: Python

    processor = StreamProcessor(
        consumer=consumer,
        # Arroyo will touch/create this file at least every second.
        healtcheck_file="/tmp/health.txt"
    )

The Kubernetes `liveness
<https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/>`_
command would look like:

.. code-block:: YAML

    apiVersion: v1
    kind: Pod
    metadata:
      labels:
        test: liveness
      name: liveness-exec
    spec:
      containers:
      - name: liveness
        image: registry.k8s.io/busybox
        args:
          - bin/my_arroyo_consumer
        livenessProbe:
          exec:
            command:
            - rm
            - /tmp/health.txt
          initialDelaySeconds: 5
          periodSeconds: 320  # should be higher than max.poll.interval.ms
