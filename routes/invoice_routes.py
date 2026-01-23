from io import StringIO
import pandas as pd
from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from typing import Optional
from models.invoice import FacturaUpdate
from services import invoice_service
from services import report_service
from utils.helpers import clean_nan_values
from fastapi.responses import FileResponse
import os
import tempfile
from openpyxl import load_workbook

router = APIRouter(prefix="/invoices", tags=["invoices"])


@router.get("/dashboard")
def get_invoices_dashboard(
    responsable: Optional[str] = None,
    producto: Optional[str] = None,
    trimestre: Optional[int] = None,
    anio: Optional[int] = None,
):
    try:
        facturas = invoice_service.get_invoices_dashboard(
            responsable=responsable,
            producto=producto,
            trimestre=trimestre,
            anio=anio
        )
        return clean_nan_values(facturas)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error en get_invoices_dashboard: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error al procesar datos: {str(e)}"
        )


@router.get("/filtros")
def get_facturas_filtros():
    try:
        filtros = invoice_service.get_facturas_filtros()
        return filtros
    except Exception as e:
        print(f"Error en get_facturas_filtros: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Error al obtener filtros: {str(e)}")


@router.get("/export_report")
def export_report():
    try:
        file_path = 'reporte.xlsx'
        if not os.path.exists(file_path):
            raise HTTPException(
                status_code=404, detail="Archivo no encontrado")

        return FileResponse(
            path=file_path,
            filename="reporte_invoices.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        print(f"Error en export_report: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error al exportar archivo: {str(e)}"
        )


@router.post("/execute_report")
async def get_facturas_filtros(file: Optional[UploadFile] = File(None)):
    try:
        if file:
            if not file.filename.endswith('.csv'):
                raise HTTPException(
                    status_code=400,
                    detail="El archivo debe ser formato CSV"
                )
            contents = await file.read()
            df = pd.read_csv(StringIO(contents.decode('utf-8')), sep=';')
            report_service.execute_report(data_invoices=df)

            return {
                'message': 'Actualización exitosa con archivo'
            }
        else:
            report_service.execute_report()
            return {'message': 'Actualización exitosa sin archivo'}

    except Exception as e:
        print(f"Error en get_facturas_filtros: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error al procesar: {str(e)}"
        )


@router.get("/execute_report_by_user")
async def get_facturas_by_user(name_user: str = Query(..., description="Nombre del usuario")):
    try:
        if not os.path.exists('reporte.xlsx'):
            raise HTTPException(
                status_code=404, detail="Archivo no encontrado")

        df = pd.read_excel('reporte.xlsx')
        df_filtrado = df[(df['Responsable 1'] == name_user) |
                         (df['Responsable 2'] == name_user)]
        if df_filtrado.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No se encontraron datos para: {name_user}"
            )

        wb_nuevo = load_workbook('reporte.xlsx')
        ws_nuevo = wb_nuevo.active
        indices_filtrados = df_filtrado.index.tolist()
        indices_a_mantener = [i + 2 for i in indices_filtrados]
        indices_a_mantener.insert(0, 1)
        filas_totales = ws_nuevo.max_row
        for row_idx in range(filas_totales, 0, -1):
            if row_idx not in indices_a_mantener:
                ws_nuevo.delete_rows(row_idx)

        temp_dir = tempfile.gettempdir()
        temp_filename = f"reporte_{name_user.replace(' ', '_')}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        temp_path = os.path.join(temp_dir, temp_filename)

        wb_nuevo.save(temp_path)
        wb_nuevo.close()

        return FileResponse(
            path=temp_path,
            filename=f"reporte_{name_user.replace(' ', '_')}.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("")
def get_all_facturas(
    skip: int = 0,
    limit: int = 100,
    search: Optional[str] = None,
    producto: Optional[str] = None,
    responsable: Optional[str] = None,
    mes: Optional[int] = None,
    anio: Optional[int] = None,
):
    try:
        result = invoice_service.get_all_facturas(
            skip, limit, search, producto, responsable, mes, anio
        )
        result['facturas'] = clean_nan_values(result['facturas'])
        return result
    except Exception as e:
        print(e)
        raise HTTPException(
            status_code=500, detail=f"Error al obtener facturas: {str(e)}")
