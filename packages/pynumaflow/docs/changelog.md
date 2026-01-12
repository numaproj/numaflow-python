# Changelog

All notable changes to pynumaflow will be documented in this page.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.11.0] - Latest

### Added

- Accumulator functionality for stateful data accumulation
- ReduceStream for streaming reduce results
- Improved type hints throughout the codebase

### Changed

- Updated dependencies to latest versions
- Enhanced error handling in gRPC servers

### Fixed

- Various bug fixes and performance improvements

---

## [0.10.0]

### Added

- BatchMap support for processing messages in batches
- MultiProcess server support for Map and SourceTransform
- Improved async server implementations

### Changed

- Refactored server architecture for better performance
- Updated protobuf definitions

---

## [0.9.0]

### Added

- MapStream functionality
- Side Input support
- Enhanced metadata in Datum objects

### Changed

- Improved connection handling
- Better error messages

---

## [0.8.0]

### Added

- User Defined Source support
- Source Transform functionality
- Headers support in messages

### Changed

- Updated gRPC communication protocol
- Enhanced logging

---

## [0.7.0]

### Added

- Async server support for Map and Reduce
- User Defined Sink functionality
- Tagging support for message routing

### Changed

- Improved memory management
- Better type annotations

---

## [0.6.0]

### Added

- Basic Map and Reduce UDF support
- gRPC server implementation
- Initial documentation

---

## Upgrade Guide

### Upgrading to 0.11.0

No breaking changes. New features are additive.

### Upgrading to 0.10.0

If using custom server configurations, review the new server options.

---

## Release Notes

For detailed release notes, see the [GitHub Releases](https://github.com/numaproj/numaflow-python/releases) page.
