import os
import pandas as pd
from config.database import invoices_collection


def _to_list(cursor, fields):
    results = []
    for doc in cursor:
        row = {}
        for f in fields:
            v = doc.get(f)
            row[f] = str(v) if v is not None else ''
        results.append(row)
    return results


def get_analisis() -> dict:
    ITEM_FIELDS = ['numero', 'nombre_empresa', 'producto_crm', 'cotizacion_num', 'monto_total', 'mes', 'anio']

    # ── 1. Sin Responsable ────────────────────────────────────────────────────
    sin_resp_cursor = invoices_collection.find(
        {'$or': [
            {'responsables': {'$exists': False}},
            {'responsables': {'$size': 0}},
            {'responsables': None},
        ]},
        {f: 1 for f in ITEM_FIELDS}
    )
    sin_resp = _to_list(sin_resp_cursor, ITEM_FIELDS)

    # ── 2. Sin Monto ──────────────────────────────────────────────────────────
    sin_monto_cursor = invoices_collection.find(
        {'$or': [
            {'monto_total': {'$exists': False}},
            {'monto_total': None},
            {'monto_total': 0},
        ]},
        {f: 1 for f in ITEM_FIELDS}
    )
    sin_monto = _to_list(sin_monto_cursor, ITEM_FIELDS)

    # ── 3. Sin Producto ───────────────────────────────────────────────────────
    sin_prod_cursor = invoices_collection.find(
        {'$or': [
            {'producto_crm': {'$exists': False}},
            {'producto_crm': None},
            {'producto_crm': ''},
            {'producto_crm': '-'},
        ]},
        {f: 1 for f in ITEM_FIELDS}
    )
    sin_prod = _to_list(sin_prod_cursor, ITEM_FIELDS)

    # ── 4. Sin Margen ─────────────────────────────────────────────────────────
    sin_margen_cursor = invoices_collection.find(
        {'$or': [
            {'utilidad_bruta': {'$exists': False}},
            {'utilidad_bruta': None},
        ]},
        {f: 1 for f in ITEM_FIELDS}
    )
    sin_margen = _to_list(sin_margen_cursor, ITEM_FIELDS)

    # ── 5. Sin OPCI ───────────────────────────────────────────────────────────
    sin_opci_cursor = invoices_collection.find(
        {'$or': [
            {'cotizacion_num': {'$exists': False}},
            {'cotizacion_num': None},
            {'cotizacion_num': ''},
            {'cotizacion_num': '-'},
        ]},
        {f: 1 for f in ITEM_FIELDS}
    )
    sin_opci = _to_list(sin_opci_cursor, ITEM_FIELDS)

    # ── 6. Sin responsable por falta de OPCI ─────────────────────────────────
    sin_resp_opci_cursor = invoices_collection.find(
        {'$and': [
            {'$or': [
                {'responsables': {'$exists': False}},
                {'responsables': {'$size': 0}},
                {'responsables': None},
            ]},
            {'$or': [
                {'cotizacion_num': {'$exists': False}},
                {'cotizacion_num': None},
                {'cotizacion_num': ''},
                {'cotizacion_num': '-'},
            ]},
        ]},
        {f: 1 for f in ITEM_FIELDS}
    )
    sin_resp_opci = _to_list(sin_resp_opci_cursor, ITEM_FIELDS)

    # ── 7. ERP → Facturación mismatches (Hoja2 del reporte) ──────────────────
    erp_mismatch = []
    report_path = 'reporte.xlsx'
    if os.path.exists(report_path):
        try:
            df2 = pd.read_excel(report_path, sheet_name='Hoja2', dtype=str)
            # Normalize column names to handle encoding issues
            df2.columns = [c.strip() for c in df2.columns]
            # Find the mismatch column (Monto (-) or similar)
            monto_diff_col = next(
                (c for c in df2.columns if 'Monto' in c and '-' in c and 'ERP' not in c and 'Excel' not in c),
                None
            )
            if monto_diff_col:
                df2[monto_diff_col] = pd.to_numeric(df2[monto_diff_col], errors='coerce').fillna(0)
                # Find the Excel amount column to exclude credit notes (negative amounts are intentional)
                monto_excel_col = next(
                    (c for c in df2.columns if 'Monto' in c and 'Excel' in c), None
                )
                mask = df2[monto_diff_col].abs() > 0.01
                if monto_excel_col:
                    df2[monto_excel_col] = pd.to_numeric(df2[monto_excel_col], errors='coerce').fillna(0)
                    # Exclude credit notes: rows where Monto Excel is negative
                    mask = mask & (df2[monto_excel_col] >= 0)
                mismatches = df2[mask]
                erp_mismatch = mismatches.fillna('').to_dict(orient='records')
        except Exception as e:
            print(f"[analisis] Error leyendo Hoja2: {e}")

    return {
        'sin_responsable':      {'count': len(sin_resp),      'items': sin_resp},
        'sin_monto':            {'count': len(sin_monto),      'items': sin_monto},
        'sin_producto':         {'count': len(sin_prod),       'items': sin_prod},
        'sin_margen':           {'count': len(sin_margen),     'items': sin_margen},
        'sin_opci':             {'count': len(sin_opci),       'items': sin_opci},
        'sin_resp_por_opci':    {'count': len(sin_resp_opci),  'items': sin_resp_opci},
        'erp_mismatch':         {'count': len(erp_mismatch),   'items': erp_mismatch},
    }
