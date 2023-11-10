import itertools
import os
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, BinaryIO, Self, cast

import numpy as np
import polars as pl
from loguru import logger
from pydantic_xml import ParsingError

from kithairon.surveys.surveyreport import EchoSurveyReport

from .._util import plot_plate_array
from .platesurvey import EchoPlateSurveyXML

if TYPE_CHECKING:
    from io import BytesIO
    from pathlib import Path

    from matplotlib.axes import Axes

PLATE_SHAPE_FROM_SIZE = {
    384: (16, 24),
    1536: (32, 48),
    6: (2, 3),
    96: (8, 12),
}


PER_SURVEY_COLUMNS = [
    "timestamp",
    "plate_name",
    "plate_type",
    "plate_barcode",
    "survey_rows",
    "survey_columns",
    "survey_total_wells",
    "comment",
    "instrument_serial_number",
    # "vtl",
    # "original",
    "data_format_version",
]


@dataclass(frozen=True)
class SurveyData:
    """A container for Echo survey data, potentially from many plates.

    `SurveyData` holds Echo survey data, potentially from many individual surveys and sources,
    in a Polars :ref:`DataFrame <polars:polars.DataFrame>`.  It is intended to allow for easy
    access and use of individual surveys, while allowing for extensive analysis when required.
    It is primarily intended to ingest PlateSurvey XML files from `Echo Liquid Handler`_ software
    (accessible directly via :ref:`EchoPlateSurveyXML`).  The format is Kithairon-specific, but
    can export back to EchoPlateSurveyXML format.  It can be easily and compactly written to
    and read from Parquet files, with compression making them smaller than the originals despite
    increased verbosity.

    All data is held in a single DataFrame, :ref:`SurveyData.data`, and every row is self-contained,
    with all survey metadata duplicated for each well, and all well, signal, and feature data included.
    This allows for easy multi-survey analyses and selections of data.  Like a DataFramee, SurveyData
    is immutable: manipulation and selection operations efficiently return new SurveyData objects,
    only copying data when required.

    .. _Echo Liquid Handler: echo/echo-liquid-handler
    """

    data: pl.DataFrame

    @cached_property
    def timestamp(self) -> datetime:
        v = self.data.get_column("timestamp").unique()
        if len(v) != 1:
            raise ValueError(f"Expected exactly one timestamp, got {len(v)}: {v}")
        return v[0]

    @cached_property
    def survey_rows(self) -> int:
        v = self.data.get_column("survey_rows").unique()
        if len(v) != 1:
            raise ValueError(f"Expected exactly one rows, got {len(v)}: {v}")
        return v[0]

    @cached_property
    def survey_columns(self) -> int:
        v = self.data.get_column("survey_columns").unique()
        if len(v) != 1:
            raise ValueError(f"Expected exactly one columns, got {len(v)}: {v}")
        return v[0]

    @cached_property
    def survey_shape(self) -> tuple[int, int]:
        return self.survey_rows, self.survey_columns

    @cached_property
    def survey_offset(self) -> tuple[int, int]:
        vals = self.data.select(
            pl.col("row").over("timestamp").min(),
            pl.col("column").over("timestamp").min(),
        )
        if len(vals) != 1:
            raise ValueError(f"Expected exactly one offset, got {len(vals)}: {vals}")
        return (vals["row"][0], vals["column"][0])

    @cached_property
    def plate_shape(self) -> tuple[int, int]:
        size = self.plate_size
        return PLATE_SHAPE_FROM_SIZE[size[0, 0]]

    @cached_property
    def plate_size(self):
        size = self.data.select(
            pl.col("plate_type").str.extract(r"(\d+)").unique().cast(int)
        )
        if len(size) != 1:
            raise ValueError(
                f"Expected exactly one plate type, got {len(size)}: {size}"
            )
        return size

    @cached_property
    def surveys(self) -> pl.DataFrame:
        # fixme: checks
        return self.data.unique("timestamp", maintain_order=True).select(
            *PER_SURVEY_COLUMNS
        )

    def volumes_array(
        self,
        *,
        full_plate: bool = False,
        fill_value: Any = np.nan,
    ):
        return self._value_array_of_survey(
            "volume", full_plate=full_plate, fill_value=fill_value
        )

    def _value_array_of_survey(
        self,
        value_selector: str | pl.Expr = "volume",
        timestamp: datetime | None = None,
        *,
        full_plate: bool = True,
        fill_value: Any = np.nan,
    ) -> np.ndarray:
        if timestamp is None:
            timestamp = self.timestamp
        survey = self._get_single_survey(timestamp)
        if full_plate:
            array = np.full(survey.plate_shape, fill_value)
            ro, co = 0, 0
        else:
            array = np.full(survey.survey_shape, fill_value)
            ro, co = survey.survey_offset
        if isinstance(value_selector, str):
            value_selector = pl.col(value_selector)
        v = survey.data.select(value_selector.alias("value"), "row", "column").to_dict()
        array[v["row"] - ro, v["column"] - co] = v["value"].to_numpy()
        return array

    def _plot_single_survey(
        self,
        value_selector: str | pl.Expr = "volume",
        timestamp: datetime | None = None,
        *,
        fill_value: Any = np.nan,
        **kwargs,
    ) -> None:
        array = self._value_array_of_survey(
            value_selector, timestamp, fill_value=fill_value
        )
        plot_plate_array(array, **kwargs)

    def heatmap(
        self,
        value_selector: str | pl.Expr = "volume",
        sel: pl.Expr | None = None,
        axs: "Axes | Iterable[Axes | None] | None" = None,
        title: str | Callable | None = None,
        *,
        fill_value: Any = np.nan,
        **kwargs,
    ) -> "list[Axes]":
        surveys = self.surveys
        if sel is not None:
            surveys = surveys.filter(sel)
        timestamps = surveys.get_column("timestamp")

        used_axes: "list[Axes]" = []
        if axs is None:
            axs = [None] * len(timestamps)
        elif not isinstance(axs, Iterable):
            assert len(timestamps) == 1  # FIXME: explain and raise
            axs = [axs]

        for i, (ax, timestamp) in enumerate(
            itertools.zip_longest(axs, timestamps, fillvalue=-1)
        ):
            if isinstance(ax, int):
                raise ValueError(f"Ran out of axes at plot {i}, for survey {timestamp}")
            if isinstance(timestamp, int):
                break
            array = self._value_array_of_survey(
                value_selector, timestamp, fill_value=fill_value
            )
            ax = plot_plate_array(array, ax=ax, **kwargs)
            used_axes.append(ax)
            if title is None:
                te = [str(value_selector)]
                # if self.plate_barcode:
                # te.append(f"of {self.plate_barcode}")
                te.append(f"on {timestamp}")
                title = " ".join(te)
            elif isinstance(title, str):
                title = title.format(self)
            else:
                title = title(self)

            assert isinstance(title, str)

            ax.set_title(title)

        return used_axes

    @classmethod
    def read_parquet(cls, path: "str | Path | BinaryIO | BytesIO | bytes") -> Self:
        dat = pl.read_parquet(path)
        return cls(
            dat.rename(
                {
                    k: v
                    for k, v in {
                        "rows": "survey_rows",
                        "columns": "survey_columns",
                        "total_wells": "survey_total_wells",
                        "machine_serial_number": "instrument_serial_number",
                        "note": "comment",
                        "s_value_fixme": "fluid_composition",
                    }.items()
                    if k in dat.columns
                }
            )
        )

    def write_parquet(self, path: "str | Path | BytesIO", **kwargs) -> None:
        self.data.write_parquet(path, **kwargs)

    @classmethod
    def read_xml(cls, path: str | os.PathLike) -> Self:
        try:
            return cls(EchoPlateSurveyXML.read_xml(path).to_polars())
        except ParsingError:
            return EchoSurveyReport.read_xml(path).to_surveydata()

    @classmethod
    def from_xml(cls, xml_str: str | bytes) -> Self:
        try:
            return cls(EchoPlateSurveyXML.from_xml(xml_str).to_polars())
        except ParsingError:
            return EchoSurveyReport.from_xml(xml_str).to_surveydata()

    @classmethod
    def from_platesurvey(cls, ps: EchoPlateSurveyXML) -> Self:
        return cls(ps.to_polars())

    def to_platesurveys(self) -> list[EchoPlateSurveyXML]:
        eps = []

        per_well_columns = [k for k in self.data.columns if k not in PER_SURVEY_COLUMNS]

        for _, survey in self.data.group_by("timestamp"):
            survey_dict = survey.select(
                *[pl.col(k).first() for k in PER_SURVEY_COLUMNS]
            ).to_dicts()[0]
            survey_dict["wells"] = survey.select(*per_well_columns).to_dicts()
            eps.append(EchoPlateSurveyXML(**survey_dict))

        return eps

    def write_platesurveys(
        self,
        paths: str
        | os.PathLike[str]
        | Iterable[str | os.PathLike[str]]
        | Callable[[EchoPlateSurveyXML], str],
        path_str_format=True,
    ) -> None:
        # We need to check the names here, not in EchoPlateSurveyXML.write_xml, because
        # we need to avoid duplicates.

        usedpaths = []

        if isinstance(paths, Iterable) and not isinstance(paths, str):
            pathiter = iter(paths)
        else:
            pathiter = None

        for ps in self.to_platesurveys():
            if pathiter:
                path = next(pathiter)
            elif isinstance(paths, Callable):
                path = paths(ps)
            elif path_str_format and hasattr(paths, "format"):
                path = paths.format(ps.model_dump(exclude=["wells"]))  # type: ignore
            else:
                path = cast(str, paths)

            if path in usedpaths:
                raise ValueError(f"Duplicate path {path}")
            ps.write_xml(path, path_str_format=False)
            usedpaths = []

    def extend_read_xml(self, path: str | os.PathLike) -> Self:
        # todo: check duplicates
        return self.extend(self.__class__.read_xml(path))

    def extend_read_parquet(
        self, path: "str | Path | BinaryIO | BytesIO | bytes"
    ) -> Self:
        return self.extend(self.__class__.read_parquet(path))

    def extend(self, other: Self | Iterable[Self]) -> Self:
        match other:
            case self.__class__():
                datas = [self.data, other.data]
            case Iterable():
                datas = itertools.chain([self.data], (o.data for o in other))
            case _:
                raise TypeError(
                    f"Expected {self.__class__.__name__} or Iterable[{self.__class__.__name__}], got {other.__class__.__name__}"
                )
        try:
            return self.__class__(pl.concat(datas))
        except pl.ShapeError as e:
            logger.warning(f"Shape mismatch: {e}")
            return self.__class__(pl.concat(datas, how="diagonal"))

    def find_survey_timestamps(
        self,
        *,
        plate_name: str | None = None,
        plate_type: str | None = None,
        plate_barcode: str | None = None,
    ) -> pl.Series:
        expr = True
        if plate_name is not None:
            expr &= pl.col("plate_name") == plate_name
        if plate_type is not None:
            expr &= pl.col("plate_type") == plate_type
        if plate_barcode is not None:
            expr &= pl.col("plate_barcode") == plate_barcode

        return self.surveys.filter(expr).get_column("timestamp")

    def find_survey_timestamp(self, **kwargs) -> datetime:
        v = self.find_survey_timestamps(**kwargs)
        if len(v) != 1:
            raise ValueError(f"Expected exactly one timestamp, got {len(v)}: {v}")
        return v[0]

    def find_survey(self, **kwargs) -> Self:
        ts = self.find_survey_timestamp(**kwargs)
        return self.__class__(self.data.filter(pl.col("timestamp") == ts))

    def _get_single_survey(self, timestamp: datetime) -> Self:
        return self.__class__(self.data.filter(pl.col("timestamp") == timestamp))
        # fixme: check uniqueness?
