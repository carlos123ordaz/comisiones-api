from fastapi import APIRouter, HTTPException
from models.invoice import FacturaUpdate
from services import invoice_service
from utils.helpers import clean_nan_values

router = APIRouter(prefix="/invoice", tags=["invoice"])


@router.get("/{factura_id}")
def get_factura_detalle(factura_id: str):
    """Obtiene el detalle de una factura"""
    try:
        factura = invoice_service.get_factura_detalle(factura_id)
        return clean_nan_values(factura)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al obtener factura: {str(e)}")


@router.put("/{factura_id}")
def update_factura(factura_id: str, factura_update: FacturaUpdate):
    """Actualiza una factura"""
    try:
        factura_actualizada = invoice_service.update_factura(
            factura_id, factura_update)
        return clean_nan_values(factura_actualizada)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print(f"Error actualizando factura: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Error al actualizar factura: {str(e)}")
