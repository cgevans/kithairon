"""Labware definition file support.

Thin Python wrapper over the Rust implementation in ``kithairon._native``.
The XML parsing, round-tripping, and in-memory model live in Rust; this
module adds the Python-side niceties (`to_polars`, XDG default-labware
discovery) that would otherwise pull Python-specific deps into the Rust
crate.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Self

import polars as pl
import xdg_base_dirs

from kithairon._native import Labware as _NativeLabware
from kithairon._native import PlateInfo

if TYPE_CHECKING:
    import os

__all__ = ["Labware", "PlateInfo", "get_default_labware"]

logger = logging.getLogger(__name__)

DEFAULT_LABWARE: Labware | None = None

_POLARS_SCHEMA = {
    "plate_type": pl.String,
    "plate_format": pl.String,
    "usage": pl.String,
    "fluid": pl.String,
    "manufacturer": pl.String,
    "lot_number": pl.String,
    "part_number": pl.String,
    "rows": pl.Int64,
    "cols": pl.Int64,
    "a1_offset_y": pl.Int64,
    "center_spacing_x": pl.Int64,
    "center_spacing_y": pl.Int64,
    "plate_height": pl.Int64,
    "skirt_height": pl.Int64,
    "well_width": pl.Int64,
    "well_length": pl.Int64,
    "well_capacity": pl.Int64,
    "bottom_inset": pl.Float64,
    "center_well_pos_x": pl.Float64,
    "center_well_pos_y": pl.Float64,
    "min_well_vol": pl.Float64,
    "max_well_vol": pl.Float64,
    "max_vol_total": pl.Float64,
    "min_volume": pl.Float64,
    "drop_volume": pl.Float64,
}


def _plate_record(p: PlateInfo) -> dict[str, Any]:
    return {k: getattr(p, k) for k in _POLARS_SCHEMA}


class Labware:
    """A collection of plate-type information."""

    _inner: _NativeLabware

    def __init__(self, plates: list[PlateInfo] | None = None) -> None:
        self._inner = _NativeLabware(plates or [])

    @classmethod
    def _from_native(cls, native: _NativeLabware) -> Self:
        obj = cls.__new__(cls)
        obj._inner = native
        return obj

    @classmethod
    def from_file(cls, path: str | os.PathLike[str]) -> Self:
        return cls._from_native(_NativeLabware.from_file(str(path)))

    @classmethod
    def from_xml_str(cls, xml: str) -> Self:
        return cls._from_native(_NativeLabware.from_xml_str(xml))

    def to_file(self, path: str | os.PathLike[str]) -> None:
        self._inner.to_file(str(path))

    def to_xml(self) -> str:
        return self._inner.to_elwx_string()

    def to_elwx_string(self) -> str:
        return self._inner.to_elwx_string()

    def to_polars(self) -> pl.DataFrame:
        records = [_plate_record(p) for p in self._inner.plates()]
        return pl.DataFrame(records, schema=_POLARS_SCHEMA)

    def plates(self) -> list[PlateInfo]:
        return self._inner.plates()

    def keys(self) -> list[str]:
        return self._inner.keys()

    def add(self, plate: PlateInfo) -> None:
        self._inner.add(plate)

    def __getitem__(self, plate_type: str) -> PlateInfo:
        try:
            return self._inner[plate_type]
        except IndexError as e:
            raise KeyError(plate_type) from e

    def __contains__(self, plate_type: str) -> bool:
        return plate_type in self._inner

    def __len__(self) -> int:
        return len(self._inner)

    def __repr__(self) -> str:
        return repr(self._inner)

    def make_default(self) -> None:
        global DEFAULT_LABWARE  # noqa: PLW0603
        DEFAULT_LABWARE = self
        p = _DEFAULT_LABWARE_PATH.parent
        if not p.exists():
            p.mkdir(parents=True)
        self.to_file(_DEFAULT_LABWARE_PATH)


_DEFAULT_LABWARE_PATH = xdg_base_dirs.xdg_data_home() / "kithairon" / "labware.elwx"

if _DEFAULT_LABWARE_PATH.exists():
    try:
        DEFAULT_LABWARE = Labware.from_file(_DEFAULT_LABWARE_PATH)
    except Exception:
        logger.exception("Error loading default labware")


def get_default_labware() -> Labware:
    if DEFAULT_LABWARE is None:
        raise ValueError("No default labware defined.")
    return DEFAULT_LABWARE
