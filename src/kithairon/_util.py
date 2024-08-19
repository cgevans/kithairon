import io
import json
import re
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl

if TYPE_CHECKING:  # pragma: no cover
    from matplotlib.axes import Axes


_WELL_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


# def _well_and_value_to_array(
#     wells: pl.Series, values: pl.Series, shape: tuple[int, int], fill: Any = 0.0
# ) -> np.ndarray:
#     """With a Series of well names and a Series of values, return a 2D array of values.

#     Parameters
#     ----------
#     wells : pl.Series
#         List of well names, in standard format ("C7" or "C07" will work).
#     values : pl.Series
#         Values for the wells.  Must be the same length as wells.
#     shape : tuple[int, int]
#         Shape of the plate, in (rows, columns)
#     fill : Any, optional
#         Initial fill value for the array, by default 0.0

#     Returns
#     -------
#     np.ndarray
#     """
#     v = np.full(shape, fill)
#     v[
#         [ord(x[0]) - 65 for x in wells], [int(x[1:]) - 1 for x in wells]
#     ] = values.to_list()
#     return v


def plot_plate_array(
    array: np.ndarray,
    *,
    annot: bool = True,
    annot_fmt: str = ".0f",
    cbar: bool = False,
    ax: "Axes | None" = None,
    topleft_offset: tuple[int, int] = (0, 0),
    vmin: float | None = None,
    vmax: float | None = None,
    cmap: str | None = "viridis",
) -> "Axes":
    import seaborn as sns
    from matplotlib import pyplot as plt

    if ax is None:
        _, ax = plt.subplots(figsize=(6 + int(cbar), 4))

    sns.heatmap(
        array,
        annot=annot,
        fmt=annot_fmt,
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
        ax=ax,
        cbar=cbar,
        cbar_kws={"label": "well volume (µL)"},
        annot_kws={"fontsize": 6},
    )

    assert ax is not None
    # put x tick labels on top
    ax.xaxis.tick_top()
    ax.set_aspect("equal")
    # set y tick labels by alphabet
    ax.set_yticklabels(
        _WELL_ALPHABET[topleft_offset[0] : topleft_offset[0] + array.shape[0]]
    )
    ax.set_xticklabels(
        [
            str(i + 1)
            for i in range(topleft_offset[1], topleft_offset[1] + array.shape[1])
        ]
    )

    return ax


def well_to_tuple(well: str) -> tuple[int, int]:
    """Convert a well name (e.g. "A1") to a tuple (row, column)."""
    return ord(well[0]) - 65, int(well[1:]) - 1


def tuple_to_well(row: int, col: int) -> str:
    """Convert a tuple (row, column) to a well name (e.g. "A1")."""
    return chr(row + 65) + str(col + 1)


def wells_to_start_and_shape(
    wells: list[str],
) -> tuple[tuple[int, int], tuple[int, int]]:
    """Given a list of well names, return the start and shape of the plate."""
    row_start = ord(wells[0][0]) - 65
    col_start = int(wells[0][1:]) - 1
    row_end = ord(wells[-1][0]) - 65
    col_end = int(wells[-1][1:]) - 1

    if (row_start > row_end) or (col_start > col_end):
        raise ValueError("Wells must be in order from top left to bottom right.")

    return (row_start, col_start), (row_end - row_start + 1, col_end - col_start + 1)


PLATE_SHAPE_FROM_SIZE: dict[int, tuple[int, int]] = {
    384: (16, 24),
    1536: (32, 48),
    6: (2, 3),
    96: (8, 12),
}


def plate_shape_from_name(plate_type: str) -> tuple[int, int]:
    """Return the shape of a plate given its name."""
    total_wells_match = re.match(r"^(\d+)", plate_type)
    if total_wells_match is None:
        raise ValueError(f"Could not parse plate type {plate_type}")
    return PLATE_SHAPE_FROM_SIZE[int(total_wells_match.group(1))]


def _polars_df_to_json_dict(df: pl.DataFrame) -> dict[str, Any]:
    buffer = io.StringIO()
    df.write_json(buffer)
    return json.loads(buffer.getvalue())


def _polars_df_from_json_dict(d: dict[str, Any]) -> pl.DataFrame:
    buffer = io.StringIO()
    json.dump(d, buffer)
    buffer.seek(0)
    return pl.read_json(buffer)
