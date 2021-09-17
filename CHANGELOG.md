# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- Added versioning code from operator-rs for up and downgrades ([#152]).
- Added `ProductVersion` to status ([#152]).

### Removed
- Code for version handling ([#152]).
- Removed `current_version` and `target_version` from cluster status ([#152]).

[#152]: https://github.com/stackabletech/spark-operator/pull/152

## [0.2.0] - 2021-09-14

### Changed
- **Breaking:** Repository structure was changed and the -server crate renamed to -binary. As part of this change the -server suffix was removed from both the package name for os packages and the name of the executable ([#138]).

[#138]: https://github.com/stackabletech/spark-operator/pull/138

## [0.1.0] - 2021.09.07

### Added

- Initial release
