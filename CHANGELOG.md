# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Reconciliation errors are now reported as Kubernetes events ([#289]).
- Use cli argument `watch-namespace` / env var `WATCH_NAMESPACE` to specify
  a single namespace to watch ([#297]).
- Moved tests from integration tests repo to operator repo ([#344]).

### Changed

- `operator-rs` `0.10.0` -> `0.21.1` ([#289], [#291], [#297], [#326], [#344]).
- BREAKING: Specifying the product version has been changed to adhere to [ADR018](https://docs.stackable.tech/home/contributor/adr/ADR018-product_image_versioning.html) instead of just specifying the product version you will now have to add the Stackable image version as well, so version: 3.1.1 becomes (for example) version: 3.1.1-stackable0 ([#344])

### Removed

- BREAKING: Removed `enableMonitoring` flag. Metrics / Monitoring always enabled ([#326]).

[#289]: https://github.com/stackabletech/spark-operator/pull/289
[#291]: https://github.com/stackabletech/spark-operator/pull/291
[#297]: https://github.com/stackabletech/spark-operator/pull/297
[#326]: https://github.com/stackabletech/spark-operator/pull/326
[#344]: https://github.com/stackabletech/spark-operator/pull/344

## [0.5.0] - 2022-02-14

### Changed

- Complete rewrite to use `StatefulSet`, `Service` and the Kubernetes overlay network. ([#222])
- Shut down gracefully ([#237]).
- `operator-rs` `0.6.0` → `0.8.0` ([#243]).
- `snafu` `0.6.0` → `0.7.0` ([#243]).
- Fixed ports for master, worker and history server ([#243]).
- `operator-rs` `0.8.0` → `0.10.0` ([#271]).
- Monitoring scraping label `prometheus.io/scrape: true` ([#271]).

### Removed

- Port configuration from CRD and product config ([#243]).
- Obsolete code to extract these ports ([#243]).

[#222]: https://github.com/stackabletech/spark-operator/pull/222
[#237]: https://github.com/stackabletech/spark-operator/pull/237
[#243]: https://github.com/stackabletech/spark-operator/pull/243
[#271]: https://github.com/stackabletech/spark-operator/pull/271

## [0.4.0] - 2021-12-06

### Changed

- `operator-rs` `0.3.0` → `0.4.0` ([#187]).
- Adapted pod image and container command to docker image ([#187]).
- Adapted documentation to represent new workflow with docker images ([#187]).

[#187]: https://github.com/stackabletech/spark-operator/pull/187

## [0.3.0] - 2021-10-27

### Changed
- `operator-rs`: `0.3.0` ([#179])
- `kube-rs`: `0.59` → `0.60` ([#157]).
- `k8s-openapi`: `version: 0.12, feature: v1_21` → `version: 0.13, feature: v1_22` ([#157]).
- Use sticky pod scheduling ([#156])

### Fixed
- Fixed a bug where `wait_until_crds_present` only reacted to the main CRD, not the commands ([#179]).

[#179]: https://github.com/stackabletech/spark-operator/pull/179
[#156]: https://github.com/stackabletech/spark-operator/pull/156
[#157]: https://github.com/stackabletech/spark-operator/pull/157

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
