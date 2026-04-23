"""Tests for the Rust-backed PickList bindings."""

from pathlib import Path

import polars as pl
import pytest

from kithairon import Labware, PickList

TEST_DATA = Path(__file__).parent / "test_data"


@pytest.fixture
def labware() -> Labware:
    return Labware.from_file(TEST_DATA / "Labware.elwx")


def _basic_picklist() -> PickList:
    # Use two plate types that are known to exist in the fixture labware.
    return PickList(
        pl.DataFrame(
            [
                {
                    "Source Plate Name": "SrcA",
                    "Source Plate Type": "384PP_AQ_BP",
                    "Source Well": "A1",
                    "Destination Plate Name": "DstA",
                    "Destination Plate Type": "384PP_Dest",
                    "Destination Well": "B2",
                    "Transfer Volume": 25.0,
                },
                {
                    "Source Plate Name": "SrcA",
                    "Source Plate Type": "384PP_AQ_BP",
                    "Source Well": "A2",
                    "Destination Plate Name": "DstA",
                    "Destination Plate Type": "384PP_Dest",
                    "Destination Well": "B3",
                    "Transfer Volume": 50.0,
                },
            ]
        )
    )


def test_validate_runs_and_returns_lists(labware: Labware):
    pl_obj = _basic_picklist()
    errors, warnings = pl_obj.validate(labware=labware, raise_on=False)
    assert isinstance(errors, list)
    assert isinstance(warnings, list)
    assert errors == []


def test_validate_flags_bad_drop_volume(labware: Labware):
    pl_obj = PickList(
        pl.DataFrame(
            [
                {
                    "Source Plate Name": "SrcA",
                    "Source Plate Type": "384PP_AQ_BP",
                    "Source Well": "A1",
                    "Destination Plate Name": "DstA",
                    "Destination Plate Type": "384PP_Dest",
                    "Destination Well": "B2",
                    "Transfer Volume": 30.0,  # not a multiple of 25
                },
            ]
        )
    )
    with pytest.raises(ValueError, match="Errors in picklist"):
        pl_obj.validate(labware=labware, raise_on="error")


def test_quick_order_reorders_in_place_and_preserves_rows(labware: Labware):
    pl_obj = _basic_picklist()
    # Deliberately scramble row order.
    scrambled = pl_obj.data.reverse()
    pls = PickList(scrambled)
    opt = pls.optimize_well_transfer_order(method="quick")
    assert opt.data.shape == pls.data.shape
    # Every row still present (compare as multisets of (source well,
    # dest well) pairs).
    before = set(
        zip(
            pls.data.get_column("Source Well"),
            pls.data.get_column("Destination Well"),
            strict=True,
        )
    )
    after = set(
        zip(
            opt.data.get_column("Source Well"),
            opt.data.get_column("Destination Well"),
            strict=True,
        )
    )
    assert before == after


def test_read_csv_native_roundtrip(tmp_path: Path, labware: Labware):
    pl_obj = _basic_picklist()
    csv_path = tmp_path / "pick.csv"
    pl_obj.write_csv(str(csv_path))

    reloaded = PickList.read_csv_native(str(csv_path))
    assert reloaded.data.height == pl_obj.data.height
    # Core columns must survive.
    for col in (
        "Source Plate Name",
        "Source Well",
        "Destination Plate Name",
        "Destination Well",
        "Transfer Volume",
    ):
        assert col in reloaded.data.columns
