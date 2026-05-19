"""Out-of-package smoke tests against real Echo survey output.

These fixtures aren't shipped with the package (they're large and
instrument-specific). Point ``KITHAIRON_LOCAL_STORE_DIR`` at a kithairon-link
``local_store`` directory containing ``survey-raw-*.xml.gz`` (and/or unzipped
``*.xml``) files emitted by the firmware to run them; otherwise the module
skips.
"""

from __future__ import annotations

import gzip
import os
from pathlib import Path

import polars as pl
import pytest

from kithairon import SurveyData
from kithairon.surveys.surveydata import _RECORDS_SCHEMA

_LOCAL_STORE_DIR_ENV = "KITHAIRON_LOCAL_STORE_DIR"

_dir_env = os.environ.get(_LOCAL_STORE_DIR_ENV)
if not _dir_env:
    pytest.skip(
        f"{_LOCAL_STORE_DIR_ENV} not set; skipping real-survey tests",
        allow_module_level=True,
    )

_LOCAL_STORE_DIR = Path(_dir_env)
if not _LOCAL_STORE_DIR.is_dir():
    pytest.skip(
        f"{_LOCAL_STORE_DIR_ENV}={_dir_env!r} is not a directory; "
        "skipping real-survey tests",
        allow_module_level=True,
    )


def _discover_surveys() -> list[Path]:
    paths = sorted(
        [
            *_LOCAL_STORE_DIR.glob("survey-raw-*.xml.gz"),
            *_LOCAL_STORE_DIR.glob("survey-raw-*.xml"),
        ]
    )
    return paths


_SURVEY_PATHS = _discover_surveys()

if not _SURVEY_PATHS:
    pytest.skip(
        f"No survey-raw-*.xml(.gz) files found in {_LOCAL_STORE_DIR}",
        allow_module_level=True,
    )


def _read_xml_text(path: Path) -> str:
    raw = path.read_bytes()
    if path.suffix == ".gz":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8")


@pytest.mark.parametrize("path", _SURVEY_PATHS, ids=lambda p: p.name)
def test_real_survey_loads(path: Path) -> None:
    xml = _read_xml_text(path)
    s = SurveyData.from_xml(xml)

    # Every required column is present.
    missing = set(_RECORDS_SCHEMA) - set(s.data.columns)
    assert not missing, f"missing columns after load: {missing}"

    # Row count matches the per-survey declared well count.
    total_wells = int(s.data["survey_total_wells"][0])
    assert len(s.data) == total_wells

    # Per-column dtypes match the explicit schema (no Polars `Null`/`Object`
    # fallback). Equality on the full DataType captures nested struct fields
    # too.
    for col, expected in _RECORDS_SCHEMA.items():
        actual = s.data.schema[col]
        assert (
            actual == expected
        ), f"column {col!r}: expected dtype {expected!r}, got {actual!r}"


def test_real_survey_parquet_round_trip(tmp_path: Path) -> None:
    path = _SURVEY_PATHS[0]
    s = SurveyData.from_xml(_read_xml_text(path))

    out = tmp_path / "round_trip.parquet"
    s.write_parquet(out)
    reloaded = SurveyData.read_parquet(out)

    assert len(reloaded.data) == len(s.data)
    assert reloaded.data["volume"].to_list() == s.data["volume"].to_list()
    assert reloaded.data["well"].to_list() == s.data["well"].to_list()
    assert reloaded.data.schema["timestamp"] == pl.Datetime("us")
