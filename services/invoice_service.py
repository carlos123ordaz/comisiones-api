from bson import ObjectId
from typing import List, Optional, Dict
import pandas as pd
import numpy as np
from config.database import invoices_collection, vendedores_collection
from utils.constants import TRIMESTRE_MESES
from models.invoice import FacturaUpdate


def get_resumen_by_user(name: str, trimestre: int, anio: Optional[int] = None) -> dict:
    if trimestre not in TRIMESTRE_MESES:
        raise ValueError("Trimestre debe ser 1, 2, 3 o 4")

    vendedor = vendedores_collection.find_one({'nombre': name})
    if not vendedor:
        raise ValueError("Vendedor no encontrado")

    unidad_negocio = vendedor['unidad_negocio']
    meses = TRIMESTRE_MESES[trimestre]

    # Construir el filtro base - Buscar en el array de responsables
    match_filter = {
        "responsables.nombre": name,
        "mes": {"$in": meses}
    }
    if anio:
        match_filter["anio"] = anio

    pipeline_productos = [
        {"$match": match_filter},
        {"$group": {
            "_id": {"producto": "$producto", "mes": "$mes"},
            "total": {"$sum": "$monto_total"}
        }},
        {"$group": {
            "_id": "$_id.producto",
            "meses": {"$push": {"mes": "$_id.mes", "total": "$total"}},
            "total_general": {"$sum": "$total"}
        }},
        {"$sort": {"total_general": -1}}
    ]

    resultado_productos = list(
        invoices_collection.aggregate(pipeline_productos))

    data_productos = []
    for item in resultado_productos:
        producto_data = {
            "Producto": item["_id"],
            "Total": item["total_general"]
        }

        for mes_data in item["meses"]:
            producto_data[mes_data["mes"]] = mes_data["total"]

        for mes in meses:
            if mes not in producto_data:
                producto_data[mes] = 0

        data_productos.append(producto_data)

    response = {
        'tipo': 'producto',
        'unidad_negocio': unidad_negocio,
        'data_productos': data_productos
    }

    if unidad_negocio == 'UNAU':
        # Construir filtro para endress
        match_filter_endress = {
            "responsables.nombre": name,
            "mes": {"$in": meses},
            "producto": "Endress"
        }
        if anio:
            match_filter_endress["anio"] = anio

        pipeline_endress = [
            {"$match": match_filter_endress},
            {"$group": {
                "_id": "$mes",
                "total": {"$sum": "$monto_total"}
            }}
        ]

        resultado_endress = list(
            invoices_collection.aggregate(pipeline_endress))

        endress_data = {
            "Responsable 1": name,
            "Total": 0
        }

        for mes in meses:
            endress_data[mes] = 0

        for item in resultado_endress:
            endress_data[item["_id"]] = item["total"]
            endress_data["Total"] += item["total"]

        endress_data['Umbral Mensual'] = vendedor['umbral_mensual']
        endress_data['Umbral Meta'] = vendedor['umbral_meta']
        endress_data['Umbral Trimestral'] = vendedor['umbral_trimestral']
        endress_data['Unidad negocio'] = unidad_negocio
        endress_data['Paso'] = endress_data['Total'] > endress_data['Umbral Trimestral']

        response['data_endress'] = endress_data

    return response


def get_invoices_dashboard(
    responsable: Optional[str] = None,
    producto: Optional[str] = None,
    trimestre: Optional[int] = None,
    anio: Optional[int] = None
) -> List[dict]:
    query = {}

    if responsable and responsable != 'Todas':
        # Buscar en el array de responsables
        query["responsables.nombre"] = responsable

    if producto and producto != 'Todas':
        query["producto"] = producto

    if trimestre:
        if trimestre not in TRIMESTRE_MESES:
            raise ValueError("Trimestre debe ser 1, 2, 3 o 4")
        meses = TRIMESTRE_MESES[trimestre]
        query["mes"] = {"$in": meses}

    if anio:
        query["anio"] = anio

    # Proyectar campos necesarios
    facturas = list(invoices_collection.find(
        query,
        {
            "_id": 0,
            "origen_deal": 1,
            "producto_crm": 1,
            "producto": 1,
            "nombre_empresa": 1,
            "monto_total": 1,
            "mes": 1,
            "fecha": 1,
            "responsables": 1,  # Nuevo formato
            "anio": 1
        }
    ))

    return facturas


def get_comisiones_by_user(name: str, trimestre: int, anio: Optional[int] = None) -> dict:
    if trimestre not in TRIMESTRE_MESES:
        raise ValueError("Trimestre debe ser 1, 2, 3 o 4")

    meses = TRIMESTRE_MESES[trimestre]

    # Construir filtro base - Buscar en el array de responsables
    match_filter = {
        "responsables.nombre": name,
        "mes": {"$in": meses}
    }

    if anio:
        match_filter["anio"] = anio

    # Pipeline para calcular comisiones totales del responsable
    pipeline = [
        {"$match": match_filter},
        {"$unwind": "$responsables"},  # Descomponer el array
        # Filtrar por nombre específico
        {"$match": {"responsables.nombre": name}},
        {"$group": {
            "_id": None,
            "comision_total": {"$sum": "$responsables.comision"}
        }}
    ]

    resultado = list(invoices_collection.aggregate(pipeline))
    comision_total = resultado[0]["comision_total"] if resultado else 0

    return {
        'responsable': name,
        'comision_total': comision_total,
        'trimestre': trimestre,
        'anio': anio
    }


def get_facturas_filtros() -> dict:
    # Productos
    productos_raw = invoices_collection.distinct("producto")
    productos = [str(p) for p in productos_raw if p is not None and p != '']
    productos = sorted(list(set(productos)))

    # Responsables - Extraer del array de responsables
    pipeline_responsables = [
        {"$unwind": "$responsables"},
        {"$group": {"_id": "$responsables.nombre"}},
        {"$sort": {"_id": 1}}
    ]

    resultado_responsables = list(
        invoices_collection.aggregate(pipeline_responsables))
    responsables = [r["_id"] for r in resultado_responsables if r["_id"]]

    # Años
    anios = []
    try:
        anios_directos = invoices_collection.distinct("anio")
        anios = [int(a) for a in anios_directos if a and not pd.isna(a)]
    except:
        pass

    if not anios:
        try:
            meses_str = invoices_collection.distinct("mes")
            for mes_str in meses_str:
                if mes_str and isinstance(mes_str, str) and '/' in mes_str:
                    anio_str = mes_str.split('/')[-1]
                    try:
                        anio = int(
                            '20' + anio_str) if len(anio_str) == 2 else int(anio_str)
                        if anio not in anios:
                            anios.append(anio)
                    except:
                        pass
        except:
            pass

    if not anios:
        from datetime import datetime
        current_year = datetime.now().year
        anios = [current_year, current_year - 1]

    anios.sort(reverse=True)

    return {
        'productos': productos,
        'responsables': responsables,
        'anios': anios
    }


def get_all_facturas(
    skip: int = 0,
    limit: int = 100,
    search: Optional[str] = None,
    producto: Optional[str] = None,
    responsable: Optional[str] = None,
    mes: Optional[int] = None,
    anio: Optional[int] = None
) -> dict:
    query = {}

    if search:
        query["$or"] = [
            {"nombre_empresa": {"$regex": search, "$options": "i"}},
            {"responsables.nombre": {"$regex": search, "$options": "i"}},
            {"origen_deal": {"$regex": search, "$options": "i"}},
            {"producto_crm": {"$regex": search, "$options": "i"}},
            {"numero": {"$regex": search, "$options": "i"}},
            {"cotizacion_num": {"$regex": search, "$options": "i"}},
        ]

    if producto:
        query["producto"] = producto

    if responsable:
        query["responsables.nombre"] = responsable

    if mes is not None:
        query["mes"] = mes

    if anio is not None:
        query["anio"] = anio

    total = invoices_collection.count_documents(query)

    facturas = list(invoices_collection.find(query)
                    .skip(skip)
                    .limit(limit)
                    .sort("fecha", -1))

    for factura in facturas:
        factura['_id'] = str(factura['_id'])

    return {
        'facturas': facturas,
        'total': total,
        'skip': skip,
        'limit': limit
    }


# Las funciones get_factura_detalle, update_factura, y recalcular_comisiones
# ya las actualizamos anteriormente, así que no las repito aquí
def get_factura_detalle(factura_id: str) -> dict:
    factura = invoices_collection.find_one({'_id': ObjectId(factura_id)})

    if not factura:
        raise ValueError("Factura no encontrada")

    factura['_id'] = str(factura['_id'])

    return factura


def update_factura(factura_id: str, factura_update: FacturaUpdate) -> dict:
    factura_actual = invoices_collection.find_one(
        {'_id': ObjectId(factura_id)})
    if not factura_actual:
        raise ValueError("Factura no encontrada")

    update_data = {}

    # Actualizar monto total
    if factura_update.monto_total is not None:
        update_data['monto_total'] = float(factura_update.monto_total)

        # Calcular monto actualizado según utilidad bruta
        if factura_actual['utilidad_bruta'] < 0.22:
            update_data['monto_actualizado'] = update_data['monto_total'] * \
                factura_actual['utilidad_bruta'] / 0.22
        else:
            update_data['monto_actualizado'] = update_data['monto_total']

    # Actualizar responsables
    if factura_update.responsables is not None:
        update_data['responsables'] = [
            {
                'nombre': r.nombre,
                'porcentaje': r.porcentaje,
                'comision': r.comision
            }
            for r in factura_update.responsables
        ]

    # Recalcular comisiones si cambió el monto o los responsables
    if any(key in update_data for key in ['monto_actualizado', 'responsables']):
        monto_actualizado = update_data.get(
            'monto_actualizado',
            factura_actual.get('monto_actualizado', 0)
        )

        responsables = update_data.get(
            'responsables',
            factura_actual.get('responsables', [])
        )

        comisiona = factura_actual.get('comisiona', True)

        if comisiona:
            comision_total = monto_actualizado * 0.01
            update_data['comision_total'] = comision_total

            # Actualizar comisiones individuales de cada responsable
            for responsable in responsables:
                responsable['comision'] = comision_total * \
                    responsable['porcentaje']

            update_data['responsables'] = responsables
        else:
            update_data['comision_total'] = 0
            for responsable in responsables:
                responsable['comision'] = 0
            update_data['responsables'] = responsables

    if update_data:
        invoices_collection.update_one(
            {'_id': ObjectId(factura_id)},
            {'$set': update_data}
        )

    factura_actualizada = invoices_collection.find_one(
        {'_id': ObjectId(factura_id)})
    factura_actualizada['_id'] = str(factura_actualizada['_id'])

    return factura_actualizada


def recalcular_comisiones() -> bool:
    try:
        vendedores = list(vendedores_collection.find(
            {'esLider': {'$exists': False}}))
        umbrales = {v['nombre']: v['umbral_mensual'] for v in vendedores}
        uns = {v['nombre']: v['unidad_negocio'] for v in vendedores}

        todas_facturas = list(invoices_collection.find())
        if not todas_facturas:
            return True

        df = pd.DataFrame(todas_facturas)

        # Validar que todas las facturas tengan responsables
        df = df[df['responsables'].notna()]
        df = df[df['responsables'].apply(lambda x: len(x) > 0)]

        # Extraer el responsable principal para análisis de umbrales
        df['responsable_principal'] = df['responsables'].apply(
            lambda x: x[0]['nombre'] if isinstance(
                x, list) and len(x) > 0 else None
        )
        df = df[df['responsable_principal'].notna()]

        # Filtrar productos Endress
        endress = df[df['producto_crm'].str.contains('Endress', na=False)]

        trimestres = {
            'Q1': [1, 2, 3],
            'Q2': [4, 5, 6],
            'Q3': [7, 8, 9],
            'Q4': [10, 11, 12]
        }

        data = {}

        # Calcular umbrales por trimestre (solo para responsables principales)
        for _, meses in trimestres.items():
            endress_trim = endress[endress['mes'].isin(meses)]
            if len(endress_trim) == 0:
                continue

            result = (
                endress_trim
                .pivot_table(
                    index='responsable_principal',
                    columns='mes',
                    values='monto_total',
                    aggfunc='sum',
                    fill_value=0,
                )
                .assign(Total=lambda x: x.sum(axis=1))
                .reset_index()
            )

            result['Umbral Mensual'] = result['responsable_principal'].map(
                umbrales)
            result['Umbral Trimestral'] = result['Umbral Mensual'] * 3
            result['Unidad negocio'] = result['responsable_principal'].map(uns)
            result['Paso'] = result['Total'] > result['Umbral Trimestral']
            result = result[result['Unidad negocio'] == 'UNAU']

            for mes in meses:
                if mes in result.columns:
                    result[f'Paso_{mes}'] = result['Paso'] | (
                        result[mes] > result['Umbral Mensual'])
                else:
                    result[f'Paso_{mes}'] = result['Paso']

            for index, row in result.iterrows():
                for mes in meses:
                    if mes in result.columns:
                        data[f'{mes}_{row["responsable_principal"]}'] = bool(
                            row[f'Paso_{mes}'])

        # Marcar si comisiona
        df['MES_RESPONSABLE'] = df['mes'].astype(
            str) + '_' + df['responsable_principal']
        df['comisiona'] = df['MES_RESPONSABLE'].map(data)
        df['comisiona'] = np.where(
            df['producto_crm'].str.contains('Endress', na=False),
            df['comisiona'],
            True
        )
        df['comisiona'] = df['comisiona'].fillna(True)

        if df['comisiona'].dtype == 'object':
            df['comisiona'] = df['comisiona'].infer_objects(copy=False)

        # Calcular comisión total (1% del monto)
        df['comision_total'] = np.where(
            df['comisiona'],
            df['monto_actualizado'] * 0.01,
            0
        )

        # Distribuir comisiones entre responsables
        def calcular_comisiones_responsables(row):
            if not isinstance(row['responsables'], list):
                return row['responsables']

            responsables = row['responsables'].copy()
            comision_total = row['comision_total']

            # Validar que porcentajes sumen 1
            suma_porcentajes = sum(r.get('porcentaje', 0)
                                   for r in responsables)
            if abs(suma_porcentajes - 1.0) > 0.01:
                # Normalizar porcentajes si no suman exactamente 1
                for r in responsables:
                    if suma_porcentajes > 0:
                        r['porcentaje'] = r.get(
                            'porcentaje', 0) / suma_porcentajes

            # Calcular comisión individual
            for responsable in responsables:
                responsable['comision'] = comision_total * \
                    responsable.get('porcentaje', 0)

            return responsables

        df['responsables'] = df.apply(calcular_comisiones_responsables, axis=1)

        # Limpiar valores infinitos o NaN
        df['comision_total'] = df['comision_total'].replace(
            [np.inf, -np.inf, np.nan], 0).astype(float)

        # Actualizar base de datos
        invoices_collection.delete_many({})
        invoices_collection.insert_many(df.to_dict(orient='records'))

        return True

    except Exception as e:
        print(f"Error recalculando comisiones: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
