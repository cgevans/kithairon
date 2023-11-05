import numpy as np
import polars as pl
import pytest

from kithairon._util import _well_and_value_to_array


def test_well_and_value_to_array():
    # Test with a 2x2 plate, completely specified
    wells = pl.Series(["A1", "A2", "B1", "B2"])
    values = pl.Series([1, 2, 3, 4])
    expected = np.array([[1, 2], [3, 4]])
    assert np.array_equal(_well_and_value_to_array(wells, values, (2, 2)), expected)

    # Test with a 96-well plate, with 5 random wells specified, float values, nan fill
    wells = pl.Series(["A1", "B2", "H12", "G4", "E5"])
    values = pl.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    expected = np.full((8, 12), np.nan)
    expected[0, 0] = 1.0
    expected[1, 1] = 2.0
    expected[7, 11] = 3.0
    expected[6, 3] = 4.0
    expected[4, 4] = 5.0
    assert np.array_equal(
        _well_and_value_to_array(wells, values, (8, 12), fill=np.nan),
        expected,
        equal_nan=True,
    )

    # Try assigning an out-of-bounds well
    with pytest.raises(IndexError):
        _well_and_value_to_array(
            pl.Series(["A1", "C2", "B1", "B2"]), pl.Series([1, 2, 3, 4]), (2, 2)
        )
