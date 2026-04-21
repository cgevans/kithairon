# Changelog

All notable changes to `kithairon` will be documented in this file.

## [0.3.0] - 2026-04-21

### Changed

- Build system migrated to `uv` (replacing previous setup).
- Removed the `kithairon_extra` wildcard import; its functionality now lives in the separate `kithairon-link` package.
- `requires-python` declared in pyproject metadata.
- GitHub Actions CI refactored.

### Added

- Survey-data convenience helpers on `SurveyData`.

### Fixed

- Minor fixes in survey data handling.
