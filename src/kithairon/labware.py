"""Labware definition file support."""

import logging
import os
import typing
from pathlib import Path
from typing import Any, Self, cast

import polars as pl
import xdg_base_dirs
from pydantic_xml import BaseXmlModel, attr

logger = logging.getLogger(__name__)

DEFAULT_LABWARE = None

_CONSISTENT_COLS = [
    "plate_format",
    "rows",
    "cols",
    "a1_offset_y",
    "center_spacing_x",
    "center_spacing_y",
    "plate_height",
    "skirt_height",
    "well_width",
    "well_length",
    "well_capacity",
    "bottom_inset",
    "center_well_pos_x",
    "center_well_pos_y",
    "min_well_vol",
    "max_well_vol",
    "max_vol_total",
    "min_volume",
    "drop_volume",
]


class PlateInfo(BaseXmlModel, tag="plateinfo"):
    """Plate type information."""

    plate_type: str = attr(
        name="platetype",
    )
    plate_format: str = attr(
        name="plateformat",
    )
    usage: str = attr(
        name="usage",
    )
    fluid: str | None = attr(name="fluid", default=None)
    manufacturer: str = attr(
        name="manufacturer",
    )
    lot_number: str = attr(
        name="lotnumber",
    )
    part_number: str = attr(
        name="partnumber",
    )
    rows: int = attr(
        name="rows",
    )
    cols: int = attr(name="cols")  # FIXME
    a1_offset_y: int = attr(
        name="a1offsety",
    )
    center_spacing_x: int = attr(
        name="centerspacingx",
    )
    center_spacing_y: int = attr(
        name="centerspacingy",
    )
    plate_height: int = attr(
        name="plateheight",
    )
    skirt_height: int = attr(
        name="skirtheight",
    )
    well_width: int = attr(
        name="wellwidth",
    )
    well_length: int = attr(
        name="welllength",
    )
    well_capacity: int = attr(
        name="wellcapacity",
    )
    bottom_inset: float = attr(
        name="bottominset",
    )
    center_well_pos_x: float = attr(
        name="centerwellposx",
    )
    center_well_pos_y: float = attr(
        name="centerwellposy",
    )
    min_well_vol: float | None = attr(name="minwellvol", default=None)
    max_well_vol: float | None = attr(name="maxwellvol", default=None)
    max_vol_total: float | None = attr(name="maxvoltotal", default=None)
    min_volume: float | None = attr(name="minvolume", default=None)
    drop_volume: float | None = attr(name="dropvolume", default=None)

    @property
    def shape(self) -> tuple[int, int]:
        return (self.rows, self.cols)


_PLATE_INFO_SCHEMA = {
    k: cast(
        type,
        v.annotation
        if not (type_union := typing.get_args(v.annotation))
        else type_union[0],
    )
    for k, v in PlateInfo.model_fields.items()
}


class _PlateInfoELWDest(PlateInfo):
    @property
    def usage(self) -> str:
        return "DEST"

    @usage.setter
    def usage(self, value: str) -> None:
        raise ValueError("Cannot set usage in ELW (ELWX-specific)")

    @property
    def well_length(self) -> int:
        return self.well_width

    @well_length.setter
    def well_length(self, value: str) -> None:
        raise ValueError("Cannot set well_length in ELW (ELWX-specific)")

    @property
    def plate_format(self) -> str:
        return "UNKNOWN"

    @plate_format.setter
    def plate_format(self, value: str) -> None:
        raise ValueError("Cannot set plate_format in ELW (ELWX-specific)")


class _PlateInfoELWSrc(PlateInfo):
    @property
    def usage(self) -> str:
        return "SRC"

    @usage.setter
    def usage(self, value: str) -> None:
        raise ValueError("Cannot set usage in ELW (ELWX-specific)")

    @property
    def well_length(self) -> int:
        return self.well_width

    @well_length.setter
    def well_length(self, value: str) -> None:
        raise ValueError("Cannot set well_length in ELW (ELWX-specific)")

    @property
    def plate_format(self) -> str:
        return "UNKNOWN"

    @plate_format.setter
    def plate_format(self, value: str) -> None:
        raise ValueError("Cannot set plate_format in ELW (ELWX-specific)")


class _SourcePlateListELWX(BaseXmlModel, tag="sourceplates"):
    plates: list[PlateInfo]


class _DestinationPlateListELWX(BaseXmlModel, tag="destinationplates"):
    plates: list[PlateInfo]


class _SourcePlateListELW(BaseXmlModel, tag="sourceplates"):
    plates: list[_PlateInfoELWSrc]


class _DestinationPlateListELW(BaseXmlModel, tag="destinationplates"):
    plates: list[_PlateInfoELWDest]


class EchoLabwareELWX(BaseXmlModel, tag="EchoLabware"):
    source_plates: _SourcePlateListELWX
    destination_plates: _DestinationPlateListELWX


class EchoLabwareELW(BaseXmlModel, tag="EchoLabware"):
    source_plates: _SourcePlateListELW
    destination_plates: _DestinationPlateListELW


class Labware:
    """A collection of plate type information."""

    _plates: list[PlateInfo]

    def __init__(self, plates: list[PlateInfo]):
        self._plates = plates

    @classmethod
    def from_raw(cls, raw: EchoLabwareELWX | EchoLabwareELW) -> Self:
        return cls(
            cast(list[PlateInfo], raw.source_plates.plates)
            + cast(list[PlateInfo], raw.destination_plates.plates)
        )

    @classmethod
    def from_file(cls, path: str | os.PathLike[str]) -> Self:
        with Path(path).open("rb") as p:
            xml_string = p.read()
        try:
            return cls.from_raw(EchoLabwareELWX.from_xml(xml_string))
        except Exception:
            return cls.from_raw(EchoLabwareELW.from_xml(xml_string))

    def to_file(self, path: str | os.PathLike[str], **kwargs: dict[str, Any]) -> None:
        """Write an ELWX labware file.

        Parameters
        ----------
        path : str | os.PathLike[str]
            path to write to
        """
        xml_string = self.to_xml(**kwargs)
        path = Path(path)
        match xml_string:
            case str():
                with path.open("w") as f:
                    f.write(xml_string)
            case bytes():
                with path.open("wb") as f:
                    f.write(xml_string)

    def to_xml(self, **kwargs: dict[str, Any]) -> str | bytes:
        """Generate an ELWX XML string.

        Parameters
        ----------
        **kwargs
            passed to pydantic_xml.BaseXmlModel.to_xml

        Returns
        -------
        str | bytes
            XML string
        """
        return self.to_elwx().to_xml(**({"skip_empty": True} | kwargs))

    def to_polars(self) -> pl.DataFrame:
        return pl.from_records(self._plates, schema=_PLATE_INFO_SCHEMA)

    def to_elwx(self) -> EchoLabwareELWX:
        return EchoLabwareELWX(
            source_plates=_SourcePlateListELWX(
                plates=[plate for plate in self._plates if plate.usage == "SRC"]
            ),
            destination_plates=_DestinationPlateListELWX(
                plates=[plate for plate in self._plates if plate.usage == "DEST"]
            ),
        )

    def __getitem__(self, plate_type: str) -> PlateInfo:
        for plate in self._plates:
            if plate.plate_type == plate_type:
                return plate
        raise KeyError(plate_type)

    def keys(self) -> list[str]:
        return [plate.plate_type for plate in self._plates]

    def add(self, plate: PlateInfo) -> None:
        if plate.plate_type in self.keys():
            raise KeyError(f"Plate of type {plate.plate_type} already exists.")
        self._plates.append(plate)

    def make_default(self) -> None:
        global DEFAULT_LABWARE  # noqa
        DEFAULT_LABWARE = self
        p = _DEFAULT_LABWARE_PATH.parent
        if not p.exists():
            p.mkdir(parents=True)
        x = self.to_xml()
        with _DEFAULT_LABWARE_PATH.open(
            "wb",
        ) as f:
            f.write(x)


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
