from pydantic import BaseModel
from typing import Optional, List


class FacturaUpdate(BaseModel):
    responsable_1: Optional[str] = None
    responsable_2: Optional[str] = None
    porcentaje_1: Optional[float] = None
    porcentaje_2: Optional[float] = None
    monto_total: Optional[float] = None


class ComisionPersonalizada(BaseModel):
    nombre: str
    porcentaje: float
    comision_calculada: Optional[float] = 0


class ComisionesPersonalizadasConfig(BaseModel):
    activa: bool
    responsables: List[ComisionPersonalizada]
