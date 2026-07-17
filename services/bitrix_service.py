import requests
import pandas as pd

BITRIX_WEBHOOK = "https://corsusaint.bitrix24.com/rest/6238/dbakwyqx9fxrblp1"

# Campos employee: ufCrm_650A1F772DB8A = Responsable Principal
#                  ufCrm_650A1F77369DA = Responsable Secundario
# Campo enum:      ufCrm_650A1F760FCC5 = Unidad de Negocio

UN_MAP = {
    '3394': 'UN AU',
    '3396': 'UN AI',
    '3398': 'UN VA',
}

STAGE_MAP = {
    'DT31_2:P': 'New Order',
    'DT31_2:1': 'Order Processed',
    'DT31_2:2': 'Invoice Sent to Customer',
    'DT31_2:3': 'Collected',
    'DT31_2:4': 'Commision Paid to Salesman',
    'DT31_2:D': 'TBD',
}


def _fetch_all_users() -> dict:
    """Devuelve {user_id: 'Nombre Apellido'} de todos los usuarios activos."""
    users = {}
    start = 0
    while True:
        resp = requests.get(f"{BITRIX_WEBHOOK}/user.get", params={'start': start}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for u in data.get('result', []):
            users[int(u['ID'])] = f"{u['NAME']} {u['LAST_NAME']}"
        if not data.get('next'):
            break
        start = data['next']
    return users


def fetch_invoices_from_bitrix() -> pd.DataFrame:
    """
    Obtiene todos los invoices del Smart Process 31 de Bitrix24 y
    devuelve un DataFrame con las mismas columnas clave que el CSV exportado.
    """
    users = _fetch_all_users()

    rows = []
    start = 0

    while True:
        resp = requests.get(
            f"{BITRIX_WEBHOOK}/crm.item.list",
            params=[
                ('entityTypeId', '31'),
                ('select[]', 'id'),
                ('select[]', 'title'),
                ('select[]', 'accountNumber'),
                ('select[]', 'stageId'),
                ('select[]', 'assignedById'),
                ('select[]', 'ufCrm_650A1F772DB8A'),
                ('select[]', 'ufCrm_650A1F77369DA'),
                ('select[]', 'ufCrm_650A1F760FCC5'),
                ('filter[>=begindate]', '2025-07-01'),
                ('start', start),
            ],
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        for item in data.get('result', {}).get('items', []):
            r1_id = item.get('ufCrm_650A1F772DB8A')
            r2_id = item.get('ufCrm_650A1F77369DA')

            r1_name = users.get(int(r1_id), '') if r1_id else ''
            r2_name = users.get(int(r2_id), '') if r2_id else ''

            un_ids = item.get('ufCrm_650A1F760FCC5') or []
            un_str = ', '.join(filter(None, [UN_MAP.get(str(uid), '') for uid in un_ids]))

            stage = STAGE_MAP.get(item.get('stageId', ''), item.get('stageId', ''))

            rows.append({
                'ID':                          item.get('id'),
                'Nombre':                      item.get('title', ''),
                'Factura #':                   item.get('accountNumber', ''),
                'Etapa':                       stage,
                'Responsable Deal - Principal': r1_name,
                'Responsable Deal - Secundario': r2_name,
                'Unidad de Negocio':           un_str,
            })

        if not data.get('next'):
            break
        start = data['next']

    return pd.DataFrame(rows)
