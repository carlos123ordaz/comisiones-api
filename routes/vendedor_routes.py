from fastapi import APIRouter, HTTPException
from typing import List
from models.vendedor import VendedorCreate, VendedorUpdate, Vendedor
from services import vendedor_service, invoice_service

router = APIRouter(prefix="/vendedores", tags=["vendedores"])


@router.get("", response_model=List[Vendedor])
def get_vendedores():
    try:
        vendedores = vendedor_service.get_all_vendedores()
        return vendedores
    except Exception as e:
        print(e)
        raise HTTPException(
            status_code=500, detail=f"Error al obtener vendedores: {str(e)}")


@router.post("", response_model=Vendedor)
def create_vendedor(vendedor: VendedorCreate):
    try:
        vendedor_creado = vendedor_service.create_vendedor(vendedor)
        invoice_service.recalcular_comisiones()

        return vendedor_creado
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al crear vendedor: {str(e)}")


@router.put("/{vendedor_id}", response_model=Vendedor)
def update_vendedor(vendedor_id: str, vendedor: VendedorUpdate):
    try:
        vendedor_actualizado = vendedor_service.update_vendedor(
            vendedor_id, vendedor)
        return vendedor_actualizado
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al actualizar vendedor: {str(e)}")


@router.delete("/{vendedor_id}")
def delete_vendedor(vendedor_id: str):
    try:
        vendedor_service.delete_vendedor(vendedor_id)
        return {"message": "Vendedor eliminado correctamente"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al eliminar vendedor: {str(e)}")


@router.get("/usuarios", tags=["usuarios"])
def get_usuarios():
    try:
        usuarios_info = vendedor_service.get_usuarios_info()
        return usuarios_info
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al procesar datos: {str(e)}")
