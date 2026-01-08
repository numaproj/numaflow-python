# Sourcer

The Sourcer module provides classes and functions for implementing User Defined Sources that produce messages for Numaflow pipelines.

## Overview

A Source must implement three handlers:

- **read_handler** - Read messages from the source
- **ack_handler** - Acknowledge successfully processed messages
- **pending_handler** - Report number of pending messages

## Classes

::: pynumaflow.sourcer
    options:
      show_root_heading: false
      show_root_full_path: false
      members_order: source
