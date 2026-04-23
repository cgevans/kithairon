"""Tests for the Rust-backed Labware bindings."""

from pathlib import Path

import polars as pl
import pytest

from kithairon import Labware
from kithairon.labware import PlateInfo

TEST_DATA = Path(__file__).parent / "test_data"


def test_from_elwx_file():
    lw = Labware.from_file(TEST_DATA / "Labware.elwx")
    assert len(lw) > 0
    assert "384LDV_Plus_AQ_GP" in lw
    p = lw["384LDV_Plus_AQ_GP"]
    assert isinstance(p, PlateInfo)
    assert p.plate_format == "384LDV"
    assert p.usage == "SRC"
    assert p.shape == (16, 24)
    assert p.drop_volume == 25.0
    assert p.fluid == "Glycerol"


def test_from_elw_file_populates_canonical_fields():
    lw = Labware.from_file(TEST_DATA / "Labware.elw")
    assert len(lw) > 0
    p = lw["Corning_1536COC_HiBase"]
    assert p.plate_format == "UNKNOWN"
    assert p.usage == "SRC"
    # ELW has no welllength — Rust derives it from wellwidth
    assert p.well_length == p.well_width


def test_missing_key_raises_keyerror():
    lw = Labware.from_file(TEST_DATA / "Labware.elwx")
    with pytest.raises(KeyError):
        _ = lw["not-a-plate-type"]


def test_to_polars_roundtrip_preserves_types():
    lw = Labware.from_file(TEST_DATA / "Labware.elwx")
    frame = lw.to_polars()
    assert frame.shape[0] == len(lw)
    assert frame.schema["plate_type"] == pl.String
    assert frame.schema["rows"] == pl.Int64
    assert frame.schema["drop_volume"] == pl.Float64


def test_xml_roundtrip_preserves_plates():
    lw = Labware.from_file(TEST_DATA / "Labware.elwx")
    xml = lw.to_elwx_string()
    lw2 = Labware.from_xml_str(xml)
    assert len(lw2) == len(lw)
    for key in lw.keys():  # noqa: SIM118 — .keys() returns list, not the dict-view protocol
        assert key in lw2
        assert lw[key].drop_volume == lw2[key].drop_volume
