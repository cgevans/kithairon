from typing import Optional
from pydantic_xml import BaseXmlModel, attr, computed_attr

class PlateInfo(BaseXmlModel, tag="plateinfo"):
    platetype: str = attr()
    plateformat: str = attr()
    usage: str = attr()
    fluid: Optional[str] = attr(default=None)
    manufacturer: str = attr()
    lotnumber: str = attr()
    partnumber: str = attr()
    rows: int = attr()
    cols: int = attr(name="cols") # FIXME
    a1offsety: int = attr()
    centerspacingx: int = attr()
    centerspacingy: int = attr()
    plateheight: int = attr()
    skirtheight: int = attr()
    wellwidth: int = attr()
    welllength: int = attr()
    wellcapacity: int = attr()
    bottominset: float = attr()
    centerwellposx: float = attr()
    centerwellposy: float = attr()
    minwellvol: Optional[float] = attr(default=None)
    maxwellvol: Optional[float] = attr(default=None)
    maxvoltotal: Optional[float] = attr(default=None)
    minvolume: Optional[float] = attr(default=None)
    dropvolume: Optional[float] = attr(default=None)

class PlateInfoELWDest(PlateInfo):
    @property
    def usage(self) -> str:
        return "DEST"
    
    @property
    def welllength(self) -> int:
        return self.wellwidth
    
    @property
    def plateformat(self) -> str:
        return "UNKNOWN"

class PlateInfoELWSrc(PlateInfo):
    @property
    def usage(self) -> str:
        return "SRC"
    
    @property
    def welllength(self) -> int:
        return self.wellwidth
    
    @property
    def plateformat(self) -> str:
        return "UNKNOWN"    


class SourcePlateListELWX(BaseXmlModel, tag="sourceplates"):
    plates: list[PlateInfo]
    
class DestinationPlateListELWX(BaseXmlModel, tag="destinationplates"):
    plates: list[PlateInfo]

class SourcePlateListELW(BaseXmlModel, tag="sourceplates"):
    plates: list[PlateInfoELWSrc]
    
class DestinationPlateListELW(BaseXmlModel, tag="destinationplates"):
    plates: list[PlateInfoELWDest]

class EchoLabwareELWX(BaseXmlModel, tag="EchoLabware"):
    sourceplates: SourcePlateListELWX
    destinationplates: DestinationPlateListELWX
    
class EchoLabwareELW(BaseXmlModel, tag="EchoLabware"):
    sourceplates: SourcePlateListELW
    destinationplates: DestinationPlateListELW
    
class Labware:
    raw: EchoLabwareELWX | EchoLabwareELW
    
    def __init__(self, raw: EchoLabwareELWX):
        self.raw = raw
        
    @classmethod
    def from_file(cls, path: str) -> 'Labware':
        xmlstr = open(path).read()
        try:
            return cls(EchoLabwareELWX.from_xml(xmlstr))
        except Exception:
            return cls(EchoLabwareELW.from_xml(xmlstr))
    
    def __getitem__(self, platetype: str):
        for plate in self.raw.sourceplates.plates:
            if plate.platetype == platetype:
                return plate
        for plate in self.raw.destinationplates.plates:
            if plate.platetype == platetype:
                return plate
        raise KeyError(platetype)