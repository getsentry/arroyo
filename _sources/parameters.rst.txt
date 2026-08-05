==========================
Common consumer parameters
==========================

Many consumers expose a CLI interface on top of arroyo, but in the case of
Sentry's own consumers they all look very similar. This page is there to
explain some of them.

* ``--no-strict-offset-reset``: Arroyo, by default, overwrites rdkafka's offset
  reset behavior to always error when the underlying offset is out of range.
  That means ``auto-offset-reset: latest`` will reset to latest when the offset
  does not yet exist, but still error if the offset is out of range. This is to
  prevent accidental data loss during incident response, when a consumer has
  been backlogged for a very long time.

  In arroyo the parameter can be passed as kafka configuration parameter
  ``arroyo.strict.offset.reset``.
