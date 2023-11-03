import polars as pl
from pydantic_xml import BaseXmlModel, attr

class WellSurvey(BaseXmlModel, tag="w"):
    row: int = attr(name="r")
    column: int = attr(name="c")
    well_name: str = attr(name="n")
    volume: float = attr(name="vl")
    current_volume: float = attr(name="cvl")
    status: str = attr()
    fluid: str = attr(name='fld')
    fluid_units: str = attr(name='fldu')
    meniscus_x: float = attr(name='x')
    meniscus_y: float = attr(name='y')
    s_value_fixme: float = attr(name='s')
    dmso_homogeneous: float = attr(name='fsh')
    dmso_imhomogeneous: float = attr(name='fsinh')
    fluid_thickness: float = attr(name='t')
    current_fluid_thickness: float = attr(name='ct')
    bottom_thickness: float = attr(name='b')
    fluid_thickness_homogeneous: float = attr(name='fth')
    fluid_thickness_imhomogeneous: float = attr(name='ftinh')
    outlier: float = attr(name='o')
    corrective_action: str = attr(name='a')
    
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
    date: str = attr(name="date")
    machine_serial_number: str = attr(name="serial_number")
    vtl: int = attr(name="vtl") #fixme
    original: int = attr(name="original") #fixme
    data_format_version: int = attr(name="frmt") #fixme
    rows: int = attr(name="rows")
    columns: int = attr(name="cols")
    total_wells: int = attr(name="totalWells")
    wells: list[WellSurvey]


# class SurveyLog:
#     def from_file(path: str):
#         return SurveyLog.from_xml(ET.parse(path).getroot())
    
#     def from_xml(root: ET.Element):
        