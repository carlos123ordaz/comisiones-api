from pydantic import BaseModel
from typing import Optional


class VendedorBase(BaseModel):
    nombre: str
    meta_mensual: float
    porcentaje_umbral: float
    unidad_negocio: str


class VendedorCreate(VendedorBase):
    pass


class VendedorUpdate(BaseModel):
    meta_mensual: Optional[float] = None
    porcentaje_umbral: Optional[float] = None
    unidad_negocio: Optional[str] = None


class Vendedor(VendedorBase):
    id: str
    umbral_mensual: float
    umbral_trimestral: float
    umbral_meta: float

    class Config:
        from_attributes = True
