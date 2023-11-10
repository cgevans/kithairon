import os
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, cast

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self  # noqa: UP035

from loguru import logger
from lxml import etree as ET
from pydantic import model_validator
from pydantic_xml import BaseXmlModel, element

if TYPE_CHECKING:
    from .surveydata import SurveyData


class EchoReportHeader(BaseXmlModel, tag="reportheader"):
    RunID: str = element()
    RunDateTime: datetime = element()
    AppName: str = element()
    AppVersion: str = element()
    ProtocolName: str = element()
    OrderID: str = element()  # FIXME
    ReferenceID: str = element()  # FIXME
    UserName: str = element()
    Comment: str | None = element(default=None)
    """Comment. Additional element, added by echo_utils."""

    @model_validator(mode="after")
    def warn_on_untested_versions(self) -> "EchoReportHeader":
        version = self.AppVersion
        version_triple = version.split(".")
        if len(version_triple) != 3:  # noqa: PLR2004
            logger.warning(
                f"Unexpected version format {self.AppVersion} for {self.AppName}.  There may be errors in parsing."
            )
            return self
        major, minor, patch = version_triple

        match self.AppName:
            case "Echo Plate Reformat" | "Echo Cherry Pick":
                if major != "1":
                    logger.warning(
                        f"Unexpected major version {version} for {self.AppName}.  There may be errors in parsing. This library has been tested on 1.8.2."
                    )
                elif minor != "8":
                    logger.warning(
                        f"Untested minor version {version} for {self.AppName}.  There may be errors in parsing.  This library has been tested on 1.8.2."
                    )
                elif patch != "2":
                    logger.info(
                        f"Untested patch version {version} for {self.AppName}.  This library has been tested on 1.8.2."
                    )
            case _:
                logger.warning(
                    f"Unknown application {self.AppName} {version} generated this survey.  There may be errors in parsing."
                )

        return self


class EchoReportRecord(BaseXmlModel, tag="record"):
    SrcPlateName: str = element()
    """The name of the plate surveyed (necessarily a source plate for a ReportRecord).

    .. warning::

        It is not currently known whether multiple plates can be present in a single report, or
        if this is just redundantly specified for every well.
    """
    SrcPlateBarcode: str = element()
    """The barcode of the plate surveyed (necessarily a source plate for a ReportRecord).

    .. warning::

        It is not currently known whether multiple plates can be present in a single report, or
        if this is just redundantly specified for every well.
    """
    SrcPlateType: str = element()
    """The surveyed plate type.

    .. warning::

        It is not currently known whether multiple plate types can be present in a single report, or
        if this is just redundantly specified for every well.
    """
    SrcWell: str = element()
    """The surveyed well, in standard, non-zero padded format (eg, "A1" or "C12", not "A01")."""
    SurveyFluidHeight: float = element()
    SurveyFluidVolume: float = element()
    FluidComposition: float = element()  # FIXME
    FluidUnits: str = element()  # FIXME
    FluidType: str = element()
    SurveyStatus: str = element()  # FIXME)


class EchoReportFooter(BaseXmlModel, tag="reportfooter"):
    InstrName: str = element()
    InstrModel: str = element()
    InstrSN: str = element()
    InstrSWVersion: str = element()


class EchoReportBody(BaseXmlModel, tag="reportbody"):
    records: list[EchoReportRecord]

    @model_validator(mode="after")
    def check_equal_plate_names(self) -> "EchoReportBody":
        if len(names := {r.SrcPlateName for r in self.records}) != 1:
            raise ValueError(
                f"All records must have the same SrcPlateName, but the names {names} were found. "
                "This is unexpected but supported by the format, please file an issue."
            )
        return self

    @model_validator(mode="after")
    def check_equal_plate_barcodes(self) -> "EchoReportBody":
        if len(barcodes := {r.SrcPlateBarcode for r in self.records}) != 1:
            raise ValueError(
                f"All records must have the same SrcPlateBarcode, but the barcodes {barcodes} were found. "
                "This is unexpected but supported by the format, please file an issue."
            )
        return self

    @model_validator(mode="after")
    def check_equal_plate_types(self) -> "EchoReportBody":
        if len(types := {r.SrcPlateType for r in self.records}) != 1:
            raise ValueError(
                f"All records must have the same SrcPlateType, but the types {types} were found. "
                "This is unexpected but supported by the format, please file an issue."
            )
        return self


ROW_NUMBER_DICT = {chr(v): i for i, v in enumerate(range(0x41, 0x61))}

RECORD_TRANSLATION = {
    "SrcPlateName": "plate_name",
    "SrcPlateBarcode": "plate_barcode",
    "SrcPlateType": "plate_type",
    "SrcWell": "well",
    "SurveyFluidHeight": "fluid_thickness",
    "SurveyFluidVolume": "volume",
    "FluidComposition": "fluid_composition",
    "FluidUnits": "fluid_units",
    "FluidType": "fluid",
    "SurveyStatus": "status",
}

HEADER_TRANSLATION = {
    "RunID": "run_id",
    "RunDateTime": "timestamp",
    "AppName": "app_name",
    "AppVersion": "app_version",
    "ProtocolName": "protocol_name",
    "OrderID": "order_id",
    "ReferenceID": "reference_id",
    "UserName": "user_name",
    "Comment": "comment",
    "InstrName": "instrument_name",
    "InstrModel": "instrument_model",
    "InstrSN": "instrument_serial_number",
    "InstrSWVersion": "instrument_software_version",
}


class EchoSurveyReport(BaseXmlModel, tag="report"):
    reportheader: EchoReportHeader
    reportbody: EchoReportBody
    reportfooter: EchoReportFooter

    def to_surveydata(self) -> "SurveyData":
        """Convert this EchoSurveyReport to a SurveyData."""
        import polars as pl

        from .surveydata import SurveyData

        const_columns = {
            HEADER_TRANSLATION[k]: pl.lit(v)
            for k, v in self.reportheader.model_dump().items()
            | self.reportfooter.model_dump().items()
        }

        d = pl.from_records(
            [
                {RECORD_TRANSLATION[k]: v for k, v in r.model_dump().items()}
                for r in self.reportbody.records
            ]
        ).with_columns(
            pl.col("well").str.slice(0, 1).map_dict(ROW_NUMBER_DICT).alias("row"),
            (pl.col("well").str.slice(1).cast(pl.Int32) - 1).alias("column"),
            **const_columns,
        )

        a = d.with_columns(
            survey_rows=pl.col("row").max() - pl.col("row").min() + 1,
            survey_columns=pl.col("column").max() - pl.col("column").min() + 1,
            survey_total_wells=len(d),
            data_format_version=pl.lit(1),
        )

        return SurveyData(a)

    @classmethod
    def read_xml(cls, path: os.PathLike | str) -> Self:
        """Read a platesurvey XML file."""
        return cls.from_xml_tree(ET.parse(path, parser=ET.XMLParser()).getroot())

    def write_xml(
        self,
        path: os.PathLike[str] | str | Callable[[Self], str],
        path_str_format: bool = True,
        **kwargs,
    ) -> str | os.PathLike[str]:
        """Write a platesurvey XML file."""
        if hasattr(path, "format") and path_str_format:
            path = cast(str, path).format(self.model_dump(exclude=["wells"]))  # type: ignore
        elif isinstance(path, Callable):
            path = path(self)
        ET.ElementTree(self.to_xml_tree()).write(path, **kwargs)
        return path
