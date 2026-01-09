# Mapper

The Mapper module provides classes and functions for implementing Map UDFs that transform messages one at a time.
Map is the most common UDF type. It receives one message at a time and can return:

- One message (1:1 transformation)
- Multiple messages (fan-out)
- No messages (filter/drop)

## Classes

::: pynumaflow.mapper
    options:
      show_root_heading: false
      show_root_full_path: false
      members_order: source
