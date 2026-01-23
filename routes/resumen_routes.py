from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from services import invoice_service, vendedor_service
from utils.helpers import clean_nan_values

router = APIRouter(tags=["resumen-comisiones"])


@router.get("/resumen/{name}/{trimestre}")
def get_invoice_by_user(
    name: str,
    trimestre: int,
    anio: Optional[int] = Query(None, description="Año para filtrar")
):
    try:
        resumen = invoice_service.get_resumen_by_user(name, trimestre, anio)
        return clean_nan_values(resumen)
    except ValueError as e:
        if "Trimestre" in str(e):
            raise HTTPException(status_code=400, detail=str(e))
        else:
            raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al procesar datos: {str(e)}")


@router.get("/comisiones/{name}/{trimestre}")
def get_comisiones_by_user(
    name: str,
    trimestre: int,
    anio: Optional[int] = Query(None, description="Año para filtrar")
):
    try:
        comisiones = invoice_service.get_comisiones_by_user(
            name, trimestre, anio)
        return clean_nan_values(comisiones)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al procesar datos: {str(e)}")


@router.post("/recalcular-comisiones")
def recalcular_comisiones_endpoint():
    try:
        success = invoice_service.recalcular_comisiones()
        if success:
            return {"message": "Comisiones recalculadas correctamente"}
        else:
            raise HTTPException(
                status_code=500, detail="Error al recalcular comisiones")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/usuarios")
def get_usuarios():
    try:
        usuarios_info = vendedor_service.get_usuarios_info()
        return usuarios_info
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al procesar datos: {str(e)}")
