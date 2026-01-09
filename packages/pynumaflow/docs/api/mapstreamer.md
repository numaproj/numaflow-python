# Map Streamer

The Map Streamer module provides classes and functions for implementing MapStream UDFs that stream results as they're produced.
Unlike regular Map which returns all messages at once, Map Stream yields messages one at a time as they're ready, reducing latency for downstream consumers.

## Classes

::: pynumaflow.mapstreamer
    options:
      show_root_heading: false
      show_root_full_path: false
      members_order: source
