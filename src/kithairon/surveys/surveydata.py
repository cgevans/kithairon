import itertools
import os
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, BinaryIO, Iterable, Self

import numpy as np
import polars as pl
from loguru import logger

from .._util import plot_plate_array
from .platesurveyxml import PlateSurveyXML

if TYPE_CHECKING:
    from io import BytesIO
    from pathlib import Path

PER_SURVEY_COLUMNS = [
    "timestamp",
    "plate_name",
    "plate_type",
    "plate_barcode",
    "survey_rows",
    "survey_columns",
    "survey_total_wells",
    "note",
    "machine_serial_number",
    "vtl",
    "original",
    "data_format_version",
]


@dataclass(frozen=True)
class SurveyData:
    data: pl.DataFrame

    @cached_property
    def timestamp(self) -> datetime:
        v = self.data.get_column("timestamp").unique()
        if len(v) != 1:
            raise ValueError(f"Expected exactly one timestamp, got {len(v)}: {v}")
        return v[0]

    @cached_property
    def rows(self) -> int:
        v = self.data.get_column("rows").unique()
        if len(v) != 1:
            raise ValueError(f"Expected exactly one rows, got {len(v)}: {v}")
        return v[0]

    @cached_property
    def columns(self) -> int:
        v = self.data.get_column("columns").unique()
        if len(v) != 1:
            raise ValueError(f"Expected exactly one columns, got {len(v)}: {v}")
        return v[0]

    @property
    def shape(self) -> tuple[int, int]:
        return self.rows, self.columns

    @cached_property
    def surveys(self) -> pl.DataFrame:
        # fixme: checks
        return self.data.unique("timestamp", maintain_order=True).select(
            *PER_SURVEY_COLUMNS
        )

    def _full_value_array_of_survey(
        self,
        value_selector: str | pl.Expr = "volume",
        timestamp: datetime | None = None,
        *,
        fill_value: Any = np.nan,
    ) -> np.ndarray:
        if timestamp is None:
            timestamp = self.timestamp
        survey = self._get_single_survey(timestamp)
        array = np.full(self.shape, fill_value)
        array[survey["row"], survey["column"]] = survey[value_selector].to_numpy()
        return array

    def _plot_single_survey(
        self,
        value_selector: str | pl.Expr = "volume",
        timestamp: datetime | None = None,
        *,
        fill_value: Any = np.nan,
        **kwargs,
    ) -> None:
        array = self._full_value_array_of_survey(
            value_selector, timestamp, fill_value=fill_value
        )
        plot_plate_array(array, **kwargs)

    def heatmap(
        self,
        value_selector: str | pl.Expr = "volume",
        sel: pl.Expr | None = None,
        *,
        fill_value: Any = np.nan,
        **kwargs,
    ) -> None:
        surveys = self.surveys
        if sel is not None:
            surveys = surveys.filter(sel)
        timestamps = surveys.get_column("timestamp")
        for timestamp in timestamps:
            array = self._full_value_array_of_survey(
                value_selector, timestamp, fill_value=fill_value
            )
            plot_plate_array(array, **kwargs)

    @classmethod
    def read_parquet(cls, path: "str | Path | BinaryIO | BytesIO | bytes") -> Self:
        return cls(pl.read_parquet(path))

    def write_parquet(self, path: "str | Path | BytesIO", **kwargs) -> None:
        self.data.write_parquet(path, **kwargs)

    @classmethod
    def read_xml(cls, path: str | os.PathLike) -> Self:
        return cls(PlateSurveyXML.read_xml(path).to_polars())

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

    def _get_single_survey(self, timestamp: datetime) -> pl.DataFrame:
        return self.data.filter(pl.col("timestamp") == timestamp)
        # fixme: check uniqueness?
