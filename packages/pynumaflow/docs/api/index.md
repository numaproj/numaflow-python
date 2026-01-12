# API Reference

This section provides detailed API documentation for all pynumaflow modules.

## Modules

| Module | Description |
|--------|-------------|
| [Sourcer](sourcer.md) | User Defined Source for custom data sources |
| [Source Transformer](sourcetransformer.md) | Transform data at ingestion |
| [Mapper](mapper.md) | Map UDF for transforming messages one at a time |
| [Map Streamer](mapstreamer.md) | MapStream UDF for streaming results as they're produced |
| [Batch Mapper](batchmapper.md) | BatchMap UDF for processing messages in batches |
| [Sinker](sinker.md) | User Defined Sink for custom data destinations |
| [Reducer](reducer.md) | Reduce UDF for aggregating messages by key and time window |
| [Reduce Streamer](reducestreamer.md) | Stream reduce results incrementally |
| [Accumulator](accumulator.md) | Accumulate and process data with state |
| [Side Input](sideinput.md) | Inject external data into UDFs |
