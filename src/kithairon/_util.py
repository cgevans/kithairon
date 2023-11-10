from typing import TYPE_CHECKING

import numpy as np

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


def plot_plate_array(  # noqa: PLR0913
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
