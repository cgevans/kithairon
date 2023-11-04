import polars as pl
from pydantic_xml import BaseXmlModel, attr, element
import numpy as np
import functools
from typing import TYPE_CHECKING, Callable
from datetime import datetime
from enum import Enum

class SurveyType(Enum):
    MEDMAN = 1
    REPORT = 2
    UNKNOWN = 3

if TYPE_CHECKING:
    import matplotlib.pyplot as plt

_WELL_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


class WellSurvey(BaseXmlModel, tag="w"):
    row: int = attr(name="r")
    column: int = attr(name="c")
    well_name: str = attr(name="n")
    volume: float = attr(name="vl")
    current_volume: float = attr(name="cvl")
    status: str = attr()
    fluid: str = attr(name="fld")
    fluid_units: str = attr(name="fldu")
    meniscus_x: float = attr(name="x")
    meniscus_y: float = attr(name="y")
    s_value_fixme: float = attr(name="s")
    dmso_homogeneous: float = attr(name="fsh")
    dmso_imhomogeneous: float = attr(name="fsinh")
    fluid_thickness: float = attr(name="t")
    current_fluid_thickness: float = attr(name="ct")
    bottom_thickness: float = attr(name="b")
    fluid_thickness_homogeneous: float = attr(name="fth")
    fluid_thickness_imhomogeneous: float = attr(name="ftinh")
    outlier: float = attr(name="o")
    corrective_action: str = attr(name="a")


class SignalFeature(BaseXmlModel, tag="f"):
    feature_type: str = attr(name="t")
    tof: float = attr(name="o")
    vpp: float = attr(name="v")


class EchoSignal(BaseXmlModel, tag="e"):
    signal_type: str = attr(name="t")
    transducer_x: float = attr(name="x")
    transducer_y: float = attr(name="y")
    transducer_z: float = attr(name="z")
    features: list[SignalFeature]


class PlateSurveyXML(BaseXmlModel, tag="platesurvey"):
    plate_type: str = attr(name="name")
    plate_barcode: str = attr(name="barcode")
    date: datetime = attr(name="date")
    machine_serial_number: str = attr(name="serial_number")
    vtl: int = attr(name="vtl")  # fixme
    original: int = attr(name="original")  # fixme
    data_format_version: int = attr(name="frmt")  # fixme
    rows: int = attr(name="rows")
    columns: int = attr(name="cols")
    total_wells: int = attr(name="totalWells")
    wells: list[WellSurvey]

class EchoReportHeader(BaseXmlModel, tag="reportheader"):
    RunID: str = element()
    RunDateTime: datetime = element()
    AppName: str = element()
    AppVersion: str = element()
    ProtocolName: str = element()
    OrderID: str = element() # FIXME
    ReferenceID: str = element() # FIXME
    UserName: str = element()
    
class EchoReportRecord(BaseXmlModel, tag="record"):
    SrcPlateName: str = element()
    SrcPlateBarcode: str = element()
    SrcPlateType: str = element()
    SrcWell: str = element()
    SurveyFluidHeight: float = element()
    SurveyFluidVolume: float = element()
    FluidComposition: float = element() # FIXME
    FluidUnits: str = element() # FIXME
    FluidType: str = element()
    SurveyStatus: str = element() # FIXME
    
class EchoReportFooter(BaseXmlModel, tag="reportfooter"):
    InstrName: str = element()
    InstrModel: str = element()
    InstrSN: str = element()
    InstrSWVersion: str = element()
    
class EchoReportBody(BaseXmlModel, tag="reportbody"):
    records: list[EchoReportRecord]

class EchoSurveyReport(BaseXmlModel, tag="report"):
    reportheader: EchoReportHeader
    reportbody: EchoReportBody
    reportfooter: EchoReportFooter


class Survey:
    """An class for all Echo plate surveys.

    This class holds results from Echo plate surveys, regardless of source.  It currently
    supports:

      - Medman / raw surveys
      - (future) Cherry Pick surveys
    """

    raw: PlateSurveyXML | EchoSurveyReport
    survey_type: SurveyType = SurveyType.UNKNOWN
    
    @functools.cached_property
    def plate_barcode(self) -> str | None:
        match self.survey_type:
            case SurveyType.MEDMAN:
                bc = self.raw.plate_barcode
            case SurveyType.REPORT:
                bc = self.raw.reportbody.records[0].SrcPlateBarcode # FIXME
            case SurveyType.UNKNOWN:
                raise ValueError("Unknown survey type")
        if bc == "UnknownBarCode":
            return None
        else:
            return bc                
    
    @functools.cached_property
    def date(self) -> datetime | None:
        match self.survey_type:
            case SurveyType.MEDMAN:
                return self.raw.date
            case SurveyType.REPORT:
                return self.raw.reportheader.RunDateTime
            case SurveyType.UNKNOWN:
                raise ValueError("Unknown survey type")
    
    @functools.cached_property
    def well_extents(self) -> tuple[int, int, int, int]:
        """Return the extents of the wells in the survey."""
        df = self._dataframe
        return (
            df.get_column("row").min(),
            df.get_column("row").max()+1,
            df.get_column("column").min(),
            df.get_column("column").max()+1,
        )
    
    
    @functools.cached_property
    def shape(self) -> tuple[int, int]:
        return (self.well_extents[1] - self.well_extents[0], self.well_extents[3] - self.well_extents[2])
    
    @functools.cached_property
    def _dataframe(self) -> pl.DataFrame:
        match self.survey_type:
            case SurveyType.MEDMAN:
                return pl.DataFrame(
                    {
                        "well": [w.well_name for w in self.raw.wells],
                        "row": [w.row for w in self.raw.wells],
                        "column": [w.column for w in self.raw.wells],
                        "volume": [w.volume for w in self.raw.wells],
                    }
                )
            case SurveyType.REPORT:
                return pl.DataFrame(
                    {
                        "well": [r.SrcWell for r in self.raw.reportbody.records],
                        "row": [_WELL_ALPHABET.index(r.SrcWell[0]) for r in self.raw.reportbody.records],
                        "column": [int(r.SrcWell[1:]) - 1 for r in self.raw.reportbody.records],
                        "volume": [r.SurveyFluidVolume for r in self.raw.reportbody.records],
                    }
                )
            case SurveyType.UNKNOWN:
                raise ValueError("Unknown survey type")
                

    @functools.cached_property
    def volumes_array(self) -> np.ndarray:
        """Returns an array of volumes."""
        arr = np.full(self.shape, np.nan)
        we = self.well_extents
        df = self._dataframe
        arr[df.get_column("row") - we[0], df.get_column("column") - we[2]] = df.get_column("volume")
        return arr

    def plot_volumes(
        self,
        ax: "plt.Axes | None" = None,
        annot: bool = True,
        annot_fmt: str = ".0f",
        cbar: bool = False,
        title: str | Callable | None = None,
    ) -> "plt.Axes":
        import seaborn as sns
        import matplotlib.pyplot as plt

        rstart, rend, cstart, cend = self.well_extents

        va = self.volumes_array
        if ax is None:
            fig, ax = plt.subplots(figsize=(6 + int(cbar), 4))
        p = sns.heatmap(
            va,
            annot=annot,
            fmt=annot_fmt,
            cmap="viridis",
            ax=ax,
            cbar=cbar,
            cbar_kws={"label": "well volume (µL)"},
            annot_kws={"fontsize": 6},
        )
        # put x tick labels on top
        ax.xaxis.tick_top()
        ax.set_aspect("equal")
        # set y tick labels by alphabet
        ax.set_yticklabels(_WELL_ALPHABET[rstart : rend])
        ax.set_xticklabels([i + 1 for i in range(cstart, cend)])

        if title is None:
            te = ["Volumes"]
            if self.plate_barcode:
                te.append(f"of {self.plate_barcode}")
            if self.date:
                te.append(f"on {self.date}")
            title = " ".join(te)
        elif isinstance(title, str):
            title = title.format(self)
        else:
            title = title(self)
        
        ax.set_title(title)
        return p

    def __init__(self, raw: PlateSurveyXML, survey_type: SurveyType):
        self.raw = raw
        self.survey_type = survey_type

    @classmethod
    def from_file(cls, path: str) -> "Survey":
        """Create a Survey from an XML string."""
        xml_data = open(path).read()
        
        errors = []
        
        try:
            raw = PlateSurveyXML.from_xml(xml_data)
            survey_type = SurveyType.MEDMAN
            return cls(raw, survey_type)
        except Exception as e:
            errors.append(e)
            pass
        
        try:
            raw = EchoSurveyReport.from_xml(xml_data)
            survey_type = SurveyType.REPORT
            return cls(raw, survey_type)
        except Exception as e:
            errors.append(e)
            pass
        
        raise ValueError(f"Cannot parse {path} as a survey: {errors}") from Exception(errors)
