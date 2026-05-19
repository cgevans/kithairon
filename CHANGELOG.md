# Changelog

## [0.4.1] - 2026-05-19

### Fixed

- `SurveyData.read_xml` / `from_xml`: stop relying on Polars schema
  inference.

### Added

- Out-of-package test (`tests/test_real_surveys.py`) that loads every
  `survey-raw-*.xml(.gz)` it finds under `$KITHAIRON_LOCAL_STORE_DIR`
  and asserts schema/row-count integrity plus a parquet round-trip.
  Skips silently when the env var is unset. Verified against 323
  real instrument surveys from `~/.local/share/kithairon/local_store`.

## [0.4.0] - 2026-05-15

### Changed

- Converted to a mixed Rust+Python maturin package. The heavy data types
  — Labware/PlateInfo, PlateSurvey + SurveyReport XML, PickList — are
  now implemented in Rust and exposed to Python via the `_native`
  module. The public Python API is preserved.
- Dropped `lxml` in favour of `quick-xml` on the Rust side. No
  user-visible API change; `lxml` is no longer a runtime dependency.
- Plate-shape lookups now go through the labware (`PlateInfo.shape`)
  instead of regex-parsing leading digits from the plate-type name.
  Non-standard names like `EnduraF96_cge_v1` work now.
- CI: dropped macOS from the test matrix (Linux + Windows remain).

### Added

- `PlateSurvey.to_platesurvey_xml` serializer (round-trip with the
  XML parser).
- `SurveyData` parquet IO + CSV export — both path-based
  (`read_survey_parquet`, `write_survey_parquet`, `write_survey_csv`,
  `read_validation_volumes_parquet`) and `Read`/`Write` adapter
  variants (`*_from_reader`, `*_to_writer`) for in-memory and
  socket-backed callers.

### Removed

- Internal `plate_shape_from_name` / `PLATE_SHAPE_FROM_SIZE` helpers
  in `_util.py` — replaced by `plate_shape_from_labware`. Private
  surface; no external impact expected.

## [0.3.0] - 2026-04-21

### Changed

- Build system migrated to `uv` (replacing previous setup).
- `requires-python` declared in pyproject metadata.
- GitHub Actions CI refactored.

### Added

- Survey-data convenience helpers on `SurveyData`.

### Fixed

- Minor fixes in survey data handling.
