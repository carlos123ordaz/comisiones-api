from pydantic import BaseModel, field_validator
from typing import Optional, List


class Responsable(BaseModel):
    nombre: str
    porcentaje: float  # Puede ser cualquier valor >= 0
    comision: Optional[float] = 0.0

    @field_validator('porcentaje')
    @classmethod
    def validar_porcentaje(cls, v):
        if v < 0:
            raise ValueError('El porcentaje no puede ser negativo')
        if v > 2:  # Límite razonable (200% del 1%)
            raise ValueError('El porcentaje no puede superar 200%')
        return v


class FacturaUpdate(BaseModel):
    monto_total: Optional[float] = None
    responsables: Optional[List[Responsable]] = None

    @field_validator('responsables')
    @classmethod
    def validar_responsables(cls, v):
        if v is not None:
            if len(v) == 0:
                raise ValueError('Debe haber al menos un responsable')

            nombres = [r.nombre for r in v]
            if len(nombres) != len(set(nombres)):
                raise ValueError('No puede haber responsables duplicados')

        return v
