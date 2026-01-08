# Reduce Streamer

The Reduce Streamer module provides classes and functions for implementing ReduceStream UDFs that emit results incrementally during reduction.

## Overview

Unlike regular Reduce which outputs only when the window closes, Reduce Stream emits results as they're computed. This is useful for early alerts or real-time dashboards.

## Classes

::: pynumaflow.reducestreamer
    options:
      show_root_heading: false
      show_root_full_path: false
      members_order: source
