from io import StringIO
import pandas as pd
from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Form
from typing import Optional
from models.invoice import FacturaUpdate
from services import invoice_service
from services import report_service
from utils.helpers import clean_nan_values
from fastapi.responses import FileResponse
import os
import tempfile
from openpyxl import load_workbook
from fastapi.responses import StreamingResponse
from io import BytesIO
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

        wb = load_workbook(file_path)
        rename_map = {
            "Hoja1": "Resumen",
            "Hoja2": "Monto ERP = Excel",
            "Hoja3": "Responsable B24 vs Excel",
            "Hoja4": "OPCI Responsable Único",
            "Hoja5": "Servicios Responsable Fredy",
            "Hoja6": "Nota Crédito Compensada",
        }
        for old_name, new_name in rename_map.items():
            if old_name in wb.sheetnames:
                wb[old_name].title = new_name

        buffer = BytesIO()
        wb.save(buffer)
        buffer.seek(0)

        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=reporte_invoices.xlsx"}
        )
    except Exception as e:
        print(f"Error en export_report: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error al exportar archivo: {str(e)}")


@router.post("/execute_report")
async def get_facturas_filtros(
    file: Optional[UploadFile] = File(None),
    ventas_data: Optional[str] = Form(None)
):
    try:
        df_invoices = None
        if file:
            contents = await file.read()
            df_invoices = pd.read_csv(
                StringIO(contents.decode('utf-8')),
                sep=';'
            )

        df_ventas = None
        if ventas_data:
            import json
            ventas_json = json.loads(ventas_data)
            df_ventas = pd.DataFrame(ventas_json)

        report_service.execute_report(
            data_invoices=df_invoices,
            data_ventas=df_ventas
        )
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

        # Obtener la primera hoja
        primera_hoja = wb_nuevo.worksheets[0]

        # Eliminar todas las hojas excepto la primera
        for sheet_name in wb_nuevo.sheetnames[1:]:
            wb_nuevo.remove(wb_nuevo[sheet_name])

        # Trabajar con la primera hoja
        ws_nuevo = primera_hoja
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
