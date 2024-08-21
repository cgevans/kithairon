"""A container for Echo survey data, potentially from many plates."""

import itertools
import os
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, BinaryIO, TypedDict, cast

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

import logging

import numpy as np
import polars as pl
from pydantic_xml import ParsingError

from kithairon._util import (
    PLATE_SHAPE_FROM_SIZE,
    _polars_df_from_json_dict,
    _polars_df_to_json_dict,
    plot_plate_array,
)

from .platesurvey import EchoPlateSurveyXML
from .surveyreport import EchoSurveyReport

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from io import BytesIO
    from pathlib import Path

    from lxml import etree
    from matplotlib.axes import Axes


class _SurveySelectorArgs(TypedDict, total=False):
    plate_name: str
    plate_type: str
    plate_barcode: str
    expr: pl.Expr


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

SURVEY_SCHEMA = {
    "timestamp": pl.Datetime,
    "plate_name": str,
    "plate_type": str,
    "plate_barcode": str,
    "survey_rows": int,
    "survey_columns": int,
    "survey_total_wells": int,
    "instrument_serial_number": str,
    "data_format_version": int,
    "volume": float,
    "comment": str,
}


def _empty_df() -> pl.DataFrame:
    return pl.DataFrame(schema=SURVEY_SCHEMA)


@dataclass(frozen=True)
class SurveyData:
    """A container for Echo survey data, potentially from many plates.

    `SurveyData` holds Echo survey data, potentially from many individual surveys and sources,
    in a Polars :py:class:`DataFrame <polars:polars.DataFrame>`.  It is intended to allow for easy
    access and use of individual surveys, while allowing for extensive analysis when required.
    It is primarily intended to ingest PlateSurvey XML files from `Echo Liquid Handler`_ software
    (accessible directly via :py:class:`platesurvey.EchoPlateSurveyXML`).  The format is Kithairon-specific, but
    can export back to EchoPlateSurveyXML format.  It can be easily and compactly written to
    and read from Parquet files, with compression making them smaller than the originals despite
    increased verbosity.

    All data is held in a single DataFrame, :py:attr:`data`, and every row is self-contained,
    with all survey metadata duplicated for each well, and all well, signal, and feature data included.
    This allows for easy multi-survey analyses and selections of data.  Like a DataFramee, SurveyData
    is immutable: manipulation and selection operations efficiently return new SurveyData objects,
    only copying data when required.

    .. _Echo Liquid Handler: echo/echo-liquid-handler
    """

    data: pl.DataFrame = field(default_factory=_empty_df)

    def to_json_dict(self) -> dict[str, Any]:
        return _polars_df_to_json_dict(self.data)

    @classmethod
    def from_json_dict(cls, d: dict[str, Any]) -> Self:
        return cls(_polars_df_from_json_dict(d))

    @cached_property
    def lazy_data(self) -> pl.LazyFrame:
        return self.data.lazy()

    @cached_property
    def timestamp(self) -> datetime:
        """Timestamp of the survey.  Single survey only."""
        v = self.data.get_column("timestamp").unique()
        if len(v) != 1:
            raise ValueError(f"Expected exactly one timestamp, got {len(v)}: {v}")
        return v[0]

    @cached_property
    def survey_rows(self) -> int:
        """Number of rows in the survey.  Single survey only."""
        v = self.data.get_column("survey_rows").unique()
        if len(v) != 1:
            raise ValueError(f"Expected exactly one rows, got {len(v)}: {v}")
        return v[0]

    @cached_property
    def plate_name(self) -> int:
        """Number of rows in the survey.  Single survey only."""
        v = self.data.get_column("plate_name").unique()
        if len(v) != 1:
            raise ValueError(f"Expected exactly one plate name, got {len(v)}: {v}")
        return v[0]

    @cached_property
    def survey_columns(self) -> int:
        """Number of columns in the survey.  Single survey only."""
        v = self.data.get_column("survey_columns").unique()
        if len(v) != 1:
            raise ValueError(f"Expected exactly one columns, got {len(v)}: {v}")
        return v[0]

    @cached_property
    def survey_shape(self) -> tuple[int, int]:
        """Shape of the *survey*, which may not be the full plate, in (rows, columns).  Single survey only.

        Returns
        -------
        tuple[int, int]
        """
        return self.survey_rows, self.survey_columns

    @cached_property
    def survey_offset(self) -> tuple[int, int]:
        """Top left (row, column) offset from (0, 0) ("A1") of the survey data.  Single survey only.

        Returns
        -------
        tuple[int, int]

        Raises
        ------
        ValueError
            Multiple offsets were returned.
        """
        vals = self.data.select(
            pl.col("row").over("timestamp").min(),
            pl.col("column").over("timestamp").min(),
        )
        if len(vals) != 1:
            raise ValueError(f"Expected exactly one offset, got {len(vals)}: {vals}")
        return (vals["row"][0], vals["column"][0])

    @cached_property
    def plate_shape(self) -> tuple[int, int]:
        """Shape of the full plate, in (rows, columns).

        Calculated based on the standard Echo source
        plate name format, which includes the number of wells at the beginning.  May fail for unusual
        plates. Single survey only.

        Returns
        -------
        tuple[int, int]

        See Also
        --------
        :any:`plate_size`
        """
        size = self.plate_total_wells
        return PLATE_SHAPE_FROM_SIZE[size[0, 0]]

    @cached_property
    def plate_total_wells(self):
        """Total number of wells in the plate (not survey). Single survey only."""
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
        """A DataFrame listing the surveys in the SurveyData.

        Returns
        -------
        pl.DataFrame
        """
        # fixme: checks
        return self.data.unique(
            ["timestamp", "plate_name"], maintain_order=True
        ).select(*PER_SURVEY_COLUMNS)

    @cached_property
    def is_single_survey(self) -> bool:
        """True if the SurveyData contains a single survey, False otherwise."""
        return len(self.surveys) == 1

    def volumes_array(
        self,
        *,
        full_plate: bool = False,
        fill_value: Any = np.nan,
    ) -> np.ndarray:
        """Generate a 2D array of the volumes in each well.  Single survey only.

        Parameters
        ----------
        full_plate : bool, optional
            Return the full plate if true (filling un-surveyed wells with `fill_value`), by default False
        fill_value : Any, optional
            Value to fill unsurveyed wells and wells with no value in the survey, by default np.nan

        Returns
        -------
        np.ndarray
        """
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
        value: str | pl.Expr = "volume",
        sel: pl.Expr | None = None,
        axs: "Axes | Iterable[Axes | None] | None" = None,
        title: str | Callable | None = None,
        *,
        fill_value: Any = np.nan,
        **kwargs,
    ) -> "list[Axes]":
        """Generate a heatmap for each survey in the SurveyData.

        Parameters
        ----------
        value : str | pl.Expr, optional
            Value to use.  May be a string (for a column), or a polars expression.  By default "volume"
        sel : pl.Expr | None, optional
            Selector for surveys, by default None (use all surveys)
        axs : Axes | Iterable[Axes | None] | None, optional
            Axes to use, by default None (generate new axes)
        title : str | Callable | None, optional
            Title for the heatmap.  If a callable, called with the survey-specific SurveyData
            for each survey heatmap.  By default None (generate a default)
        fill_value : Any, optional
            Fill value for missing values, by default np.nan

        Returns
        -------
        list[Axes]
            Each Axes used: one per survey.

        Raises
        ------
        ValueError
            Ran out of provided axes to use.
        """
        surveys = self.surveys
        if sel is not None:
            surveys = surveys.filter(sel)
        timestamps = surveys.get_column("timestamp")

        used_axes: list[Axes] = []
        if axs is None:
            axs = [None] * len(timestamps)
        elif not isinstance(axs, Iterable):
            assert len(timestamps) == 1  # FIXME: explain and raise
            axs = [axs]

        for i, (ax, timestamp) in enumerate(
            itertools.zip_longest(axs, timestamps, fillvalue=-1)
        ):
            if isinstance(ax, int):
                raise ValueError(f"Ran out of axes at plot {i}, for survey {timestamp}")  # noqa: TRY004
            if isinstance(timestamp, int):
                break
            array = self._value_array_of_survey(value, timestamp, fill_value=fill_value)
            ax = plot_plate_array(array, ax=ax, **kwargs)  # noqa: PLW2901
            used_axes.append(ax)
            if title is None:
                te = [str(value)]
                if self.plate_name:
                    te.append(f"of {self.plate_name}")
                te.append(f"on {timestamp}")
                title = " ".join(te)
            elif isinstance(title, str):
                pass  # title = title.format(self) # FIXME
            else:
                title = title(self)

            assert isinstance(title, str)

            ax.set_title(title)

        return used_axes

    @classmethod
    def read_parquet(
        cls,
        path: "str | Path | BinaryIO | BytesIO | bytes",
        polars_options: dict[str, Any] | None = None,
    ) -> Self:
        """Read SurveyData from a Parquet file.

        Parameters
        ----------
        path : str | Path | BinaryIO | BytesIO | bytes
            Path to read from, or file-like object.
        polars_kw : dict[str, Any] | None, optional
            Options to pass to :ref:`polars:polars.read_parquet`, by default None

        Returns
        -------
        SurveyData
        """
        dat = pl.read_parquet(path, **(polars_options or {}))
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

    def write_parquet(
        self, path: "str | Path | BytesIO", polars_options: dict[str, Any] | None = None
    ) -> None:
        """
        Write the survey data to a Parquet file.

        Parameters
        ----------
        path : str or Path or BytesIO
            The path to write the Parquet file to, or a BytesIO object to write to.
        polars_options : dict[str, Any] or None, optional
            Options to pass to the Polars DataFrame.write_parquet method.

        Examples
        --------
        >>> survey_data = SurveyData(...)
        >>> survey_data.write_parquet("survey_data.parquet")
        """
        self.data.write_parquet(path, **(polars_options or {}))

    @classmethod
    def read_xml(cls, path: str | os.PathLike) -> Self:
        """
        Read survey data from an Echo-produced XML file.

        Parameters
        ----------
        path : str or os.PathLike
            The path to the XML file.

        Returns
        -------
        SurveyData
            The survey data read from the XML file.

        Raises
        ------
        ParsingError
            If the XML file cannot be parsed.
        """
        try:
            d = EchoPlateSurveyXML.read_xml(path)._to_polars()
            d = d.cast({k: v for k, v in SURVEY_SCHEMA.items() if k in d.columns})
            return cls(d)
        except ParsingError:
            return EchoSurveyReport.read_xml(path).to_surveydata()

    @classmethod
    def from_xml(cls, xml_str: str | bytes) -> Self:
        """
        Create a new instance of `SurveyData` from an XML string.

        Parameters
        ----------
        xml_str : str or bytes
            The XML string to parse.

        Returns
        -------
        SurveyData
            A new instance of `SurveyData` created from the parsed XML.

        Raises
        ------
        ParsingError
            If the XML string cannot be parsed.

        """
        try:
            d = EchoPlateSurveyXML.from_xml(xml_str)._to_polars()
            d = d.cast({k: v for k, v in SURVEY_SCHEMA.items() if k in d.columns})
            return cls(d)
        except ParsingError:
            return EchoSurveyReport.from_xml(xml_str).to_surveydata()

    @classmethod
    def from_xml_tree(cls, xml_tree: "etree._Element") -> Self:
        """
        Create a new instance of `SurveyData` from an XML string.

        Parameters
        ----------
        xml_tree : lxml.etree._Element
            The XML tree to parse.

        Returns
        -------
        SurveyData
            A new instance of `SurveyData` created from the parsed XML.

        Raises
        ------
        ParsingError
            If the XML tree cannot be parsed.

        """
        try:
            d = EchoPlateSurveyXML.from_xml_tree(xml_tree)._to_polars()
            d = d.cast({k: v for k, v in SURVEY_SCHEMA.items() if k in d.columns})
            return cls(d)
        except ParsingError:
            return EchoSurveyReport.from_xml_tree(xml_tree).to_surveydata()

    @classmethod
    def from_platesurvey(cls, ps: EchoPlateSurveyXML) -> Self:
        """
        Create a new instance of `SurveyData` from an `EchoPlateSurveyXML` object.

        Parameters
        ----------
        ps : EchoPlateSurveyXML
            The `EchoPlateSurveyXML` object to create the new instance from.

        Returns
        -------
        SurveyData
            A new instance of `SurveyData` created from the `EchoPlateSurveyXML` object.
        """
        return cls(ps._to_polars())

    def to_platesurveys(self) -> list[EchoPlateSurveyXML]:
        """
        Convert survey data to a list of EchoPlateSurveyXML objects.

        Returns
        -------
        list[EchoPlateSurveyXML]
            A list of EchoPlateSurveyXML objects representing the survey data.
        """
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
        """
        Write plate surveys to disk as Echo PlateSurvey format.

        Parameters
        ----------
        paths : str or os.PathLike or iterable of str or os.PathLike or callable
            The path(s) to write the plate surveys to. If a callable is provided, it should
            take an `EchoPlateSurveyXML` object as input and return a string path.
        path_str_format : bool, optional
            Whether to format the path(s) using the `format` method of the `paths` argument,
            by default True.

        Raises
        ------
        ValueError
            If a duplicate path is encountered.

        Returns
        -------
        None
        """
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
        """
        Extend the current SurveyData object with the data from an XML file located at the given path.

        Parameters
        ----------
        path : str or os.PathLike
            The path to the XML file to read.

        Returns
        -------
        SurveyData
            A new SurveyData object that contains the data from the current object as well as the data from the XML file.
        """
        # todo: check duplicates
        return self.extend(self.__class__.read_xml(path))

    def extend_read_parquet(
        self, path: "str | Path | BinaryIO | BytesIO | bytes"
    ) -> Self:
        """
        Extend the current SurveyData object with data from a Parquet file.

        Parameters
        ----------
        path : str or Path or BinaryIO or BytesIO or bytes
            The path to the Parquet file or a file-like object containing the data.

        Returns
        -------
        SurveyData
            A new SurveyData object that includes the data from the Parquet file.
        """
        return self.extend(self.__class__.read_parquet(path))

    def extend(self, other: Self | Iterable[Self]) -> Self:
        """
        Extend this survey data with another survey data object or an iterable of survey data objects.

        Parameters
        ----------
        other : Self | Iterable[Self]
            The survey data object or iterable of survey data objects to extend with.

        Returns
        -------
        Self
            The extended survey data object.

        Raises
        ------
        TypeError
            If `other` is not an instance of `Self` or an iterable of `Self`.

        """
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
        except pl.ShapeError as _:
            return self.__class__(pl.concat(datas, how="diagonal"))

    def __add__(self, other: Self | Iterable[Self]) -> Self:
        # todo: check duplicates
        return self.extend(other)

    def find_survey_timestamps(
        self,
        *args,
        **kwargs: _SurveySelectorArgs,
    ) -> pl.Series:
        """
        Find survey timestamps based on the given criteria.

        Parameters
        ----------
        plate_name : str or None, optional
            Name of the plate.
        plate_type : str or None, optional
            Type of the plate.
        plate_barcode : str or None, optional
            Barcode of the plate.
        expr : pl.Expr or None, optional
            Additional expression to filter the surveys.

        Returns
        -------
        pl.Series
            A series of timestamps that match the given criteria.
        """
        combined_expr = True
        for arg in args:
            combined_expr &= arg
        for k, v in kwargs.items():
            combined_expr &= pl.col(k) == v

        return self.surveys.filter(combined_expr).get_column("timestamp")

    def find_survey_timestamp(self, **kwargs: _SurveySelectorArgs) -> datetime:
        """
        Find a single survey timestamp based on the given criteria.

        Parameters
        ----------
        plate_name : str or None, optional
            Name of the plate.
        plate_type : str or None, optional
            Type of the plate.
        plate_barcode : str or None, optional
            Barcode of the plate.
        expr : pl.Expr or None, optional
            Additional expression to filter the surveys.

        Raises
        ------
        ValueError
            If no or multiple timestamps are found.

        Returns
        -------
        datetime
            The timestamps that matches the given criteria.
        """
        v = self.find_survey_timestamps(**kwargs)
        if len(v) != 1:
            raise ValueError(f"Expected exactly one timestamp, got {len(v)}: {v}")
        return v[0]

    def find_survey(self, **kwargs: _SurveySelectorArgs) -> Self:
        """
        Find a single survey timestamp based on the given criteria, returning a SurveyData.

        Parameters
        ----------
        plate_name : str or None, optional
            Name of the plate.
        plate_type : str or None, optional
            Type of the plate.
        plate_barcode : str or None, optional
            Barcode of the plate.
        expr : pl.Expr or None, optional
            Additional expression to filter the surveys.

        Raises
        ------
        ValueError
            If no or multiple timestamps are found.

        Returns
        -------
        SurveyData
            The data for survey that matches the given criteria.
        """
        ts = self.find_survey_timestamp(**kwargs)
        return self.__class__(self.data.filter(pl.col("timestamp") == ts))

    def find_latest_survey(self, *args, **kwargs: _SurveySelectorArgs) -> Self:
        """
        Find the latest survey based on the given criteria, returning a SurveyData.

        Parameters
        ----------
        plate_name : str or None, optional
            Name of the plate.
        plate_type : str or None, optional
            Type of the plate.
        plate_barcode : str or None, optional
            Barcode of the plate.
        expr : pl.Expr or None, optional
            Additional expression to filter the surveys.

        Returns
        -------
        SurveyData
            The data for the latest survey that matches the given criteria.
        """
        # TODO: should use other methods
        s = self.__class__(
            self.lazy_data.filter(*args, **kwargs)
            .filter(pl.col.timestamp == pl.col.timestamp.max())
            .collect()
        )

        if len(s) == 0:
            raise KeyError("No survey found")

        if len(s.surveys) != 1:
            raise ValueError(f"Expected exactly one survey, got {len(s.surveys)}")

        return s

    def _get_single_survey(self, timestamp: datetime) -> Self:
        return self.__class__(self.data.filter(pl.col("timestamp") == timestamp))
        # fixme: check uniqueness?

    def with_plate_name(self, name: str, overwrite: bool = True) -> Self:
        if not overwrite:
            raise NotImplementedError
        return self.with_columns(plate_name=pl.lit(name))

    def with_comment(self, comment: str, overwrite: bool = True) -> Self:
        if not overwrite:
            raise NotImplementedError
        return self.with_columns(comment=pl.lit(comment))

    def with_columns(self, *args: Any, **kwargs: Any) -> Self:
        return self.__class__(self.data.with_columns(*args, **kwargs))

    def _repr_html_(self):
        return self.data._repr_html_()

    def __len__(self):
        return len(self.data)
