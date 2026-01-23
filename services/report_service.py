import pandas as pd
from msal import ConfidentialClientApplication
import requests
import os
import pymssql
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.formatting.rule import CellIsRule
from openpyxl.styles.differential import DifferentialStyle
from openpyxl.formatting.rule import FormulaRule
import numpy as np
import unicodedata
from typing import Optional
from config import database
from dotenv import load_dotenv
load_dotenv()


def execute_report(
    data_invoices: Optional[pd.DataFrame] = None,
    data_ventas: Optional[pd.DataFrame] = None
):
    TENANT_ID = os.getenv("TENANT_ID")
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")

    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"])

    token = result["access_token"]
    headers = {'Authorization': f'Bearer {token}'}

    site_url = "https://graph.microsoft.com/v1.0/sites/corsusaadmin.sharepoint.com:/sites/logistica"
    site_response = requests.get(site_url, headers=headers)
    site_id = site_response.json()['id']

    drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    drives_response = requests.get(drives_url, headers=headers)
    drives = drives_response.json()['value']

    document_drive = None
    for d in drives:
        if 'Documentos' in d['name'] or 'Documents' in d['name']:
            document_drive = d
            break

    if not document_drive:
        document_drive = drives[0]

    drive_id = document_drive['id']

    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"])

    token = result["access_token"]
    headers = {'Authorization': f'Bearer {token}'}

    site_url = "https://graph.microsoft.com/v1.0/sites/corsusaadmin.sharepoint.com:/sites/logistica"
    site_response = requests.get(site_url, headers=headers)
    site_id = site_response.json()['id']

    drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    drives_response = requests.get(drives_url, headers=headers)
    drives = drives_response.json()['value']

    document_drive = None
    for d in drives:
        if 'Documentos' in d['name'] or 'Documents' in d['name']:
            document_drive = d
            break

    if not document_drive:
        document_drive = drives[0]

    drive_id = document_drive['id']
    archivos_config = [
        {
            'unique_id': '861DBDF6-FB6C-4DB5-A6ED-3F48BE93EBAC',
            'nombre': '001_Ventas_OP.xlsx'
        },
        {
            'unique_id': '8C6018D0-2120-4457-89EB-ED641E69B3EA',
            'nombre': '004_Facturacion_OP.xlsx'
        }
    ]

    download_dir = './descargas'
    os.makedirs(download_dir, exist_ok=True)

    def descargar_archivo(config, index, total):
        unique_id = config['unique_id']
        nombre = config['nombre']
        file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{unique_id}"
        file_response = requests.get(file_url, headers=headers)

        if file_response.status_code != 200:
            return None, f"Error al obtener info del archivo {nombre}"

        file_data = file_response.json()
        download_url = file_data.get('@microsoft.graph.downloadUrl')
        file_content = requests.get(download_url)
        ruta_archivo = os.path.join(download_dir, nombre)

        with open(ruta_archivo, 'wb') as f:
            f.write(file_content.content)

        return ruta_archivo, f"Archivo {nombre} descargado exitosamente"

    for idx, config in enumerate(archivos_config, 1):
        ruta, mensaje = descargar_archivo(config, idx, len(archivos_config))
        print(mensaje)

    a = pd.read_excel('./descargas/001_Ventas_OP.xlsx',
                      sheet_name='OP_Cabecera', skiprows=2)
    b = pd.read_excel('./descargas/004_Facturacion_OP.xlsx', skiprows=3)
    c = pd.read_excel('./descargas/001_Ventas_OP.xlsx',
                      sheet_name='OP_Detalle-Venta', skiprows=2)

    if data_invoices is not None:
        bt = data_invoices
        bt.to_csv('./descargas/invoices.csv', sep=';', index=False)
    else:
        bt = pd.read_csv('./descargas/invoices.csv', sep=';')

    num_deal = {}
    for _, row in c.iterrows():
        num_deal[row['Correlativo_OPCI']] = row['Numero_Deal']

    # conn = pymssql.connect(server='192.168.10.33', user='SA',
    #                        password='%C0rsus77%', database='ERP')
    # f = pd.read_sql("select v.NroSre, v.NroDoc, v.FecMov, v.CamMda, v.Cd_Mda, v.ValorNeto, v.CA10, v.Cd_TD, v.DR_NSre,v.DR_NDoc, v.IB_Anulado, v.Cliente  from venta v join Cliente2 c on v.Cd_Clt = c.Cd_Clt where v.FecMov >= '2025-01-01' and v.ValorNeto is not NULL and v.IB_Anulado = 0 and c.Cd_TDI != '01' order by v.FecMov ASC", conn)
    f = data_ventas

    bt['Factura #'] = bt['Factura #'].str.split(' ').str[0]
    un = {}
    for index, row in bt.iterrows():
        un[row['Factura #']] = row['Unidad de Negocio']

    f['Num_Factura'] = f['NroSre'] + '-' + f['NroDoc']
    f['ValorNeto'] = np.where(
        f['Cd_Mda'] == '01', f['ValorNeto']/f['CamMda'], f['ValorNeto'])
    b['Monto'] = np.where(b['Moneda'] == 'PEN', b['MontoTotal_SinIGV'] /
                          b['T/C_USD-Sol'], b['MontoTotal_SinIGV'])
    grupo = b.groupby('Num_Factura').agg({
        'Monto': 'sum',
        'Status_Factura': 'first',
        'T/C_USD-Sol': 'first'
    }).reset_index()
    one = pd.merge(f, grupo, on='Num_Factura', how='inner')
    one = one[['Num_Factura', 'ValorNeto', 'Monto',
               'Status_Factura', 'CamMda', 'T/C_USD-Sol']]
    one['Monto (-)'] = round(abs(one['ValorNeto'] - one['Monto']), 2)
    one['T/C (-)'] = round(abs(one['CamMda'] - one['T/C_USD-Sol']), 2)
    one = one.rename(columns={'Num_Factura': 'Número', 'ValorNeto': 'Monto ERP',
                     'Monto': 'Monto Excel', 'CamMda': 'T/C ERP', 'T/C_USD-Sol': 'T/C Excel'})
    bt['OPCI'] = 'OPCI-' + \
        bt['Nombre'].str.extract(r'OPCI-(\d+)', expand=False)
    obj_r1 = {}
    obj_r2 = {}
    for index, row in bt.iterrows():
        obj_r1[row['OPCI']] = row['Responsable Deal - Principal']
        obj_r2[row['OPCI']] = row['Responsable Deal - Secundario']

    df = pd.merge(f, b, how='left', on='Num_Factura')
    df = pd.merge(df, a, how='left', on='Correlativo_OPCI')
    df['UN'] = df['Num_Factura'].map(un)
    df['FecMov'] = pd.to_datetime(df['FecMov'], errors='coerce')
    df['AÑO'] = df['FecMov'].dt.year
    df['MES'] = df['FecMov'].dt.month
    df['Subject'] = '-'
    df['Codigos'] = '-'
    df['Diferencia'] = '-'
    df['Notas'] = '-'
    df['Observaciones'] = '-'
    df['Periodo'] = df['AÑO'].astype(
        str) + '-Q' + ((df['MES'] - 1) // 3 + 1).astype(str)
    df['EstadoPago-Vendedor'] = ''
    df['Lider 2'] = ''
    df['EstadoPago-Lideres'] = ''
    df['Umbral'] = 0.22
    df['Invoice items: Name'] = '-'
    df['OK'] = ''
    df['Cotizacion #'] = df['Correlativo_OPCI']
    df['deal'] = df['Correlativo_OPCI'].map(num_deal)
    df['Subject'] = df['CA10']
    df['Monto Total'] = np.where(
        df['Moneda_x'] == 'USD',
        df['MontoTotal_SinIGV_x'],
        df['MontoTotal_SinIGV_x'] / df['CamMda']
    )
    df['Monto Total'] = np.where(
        df['Cd_TD'] == '07',
        -abs(df['Monto Total']),
        df['Monto Total']
    )
    df['Estado'] = np.where(df['Cd_TD'] == '07',
                            'Nota Crédito', df['Status_Factura'])
    df['R1'] = df['Correlativo_OPCI'].map(obj_r1)
    df['R2'] = df['Correlativo_OPCI'].map(obj_r2)
    nc = df[df['Cd_TD'] == '07'].copy()
    nc['REF'] = nc['DR_NSre'] + '-' + nc['DR_NDoc']
    ref_nc = {}
    umb = {}
    for index, row in df.iterrows():
        umb[row['Num_Factura']] = row['UBrutaCoti']

    ref_umb = {}
    for index, row in nc.iterrows():
        ref_nc[row['REF']] = row['Num_Factura']
        ref_umb[row['Num_Factura']] = umb.get(row['REF'])
    df['REF'] = df['Num_Factura'].map(ref_nc)
    df['REF_2'] = df['Num_Factura'].map(ref_umb)
    df['UBrutaCoti'] = np.where(
        df['Cd_TD'] == '07', df['REF_2'], df['UBrutaCoti'])
    df['Estado'] = np.where(
        df['REF'].notna() & (df['REF'] != ''),
        'Factura - ' + df['REF'].astype(str),
        df['Estado']
    )

    df['Monto Total'] = np.where(
        df['Estado'].str.contains('Nota Crédito', na=False),
        -abs(df['ValorNeto']),
        df['Monto Total']
    )

    df['Monto Total'] = np.where(
        df['Estado'].isna() |
        (df['Estado'] == '') |
        (df['Estado'].str.contains('Factura', na=False) & df['Monto Total'].isna()),
        df['ValorNeto'],
        df['Monto Total']
    )

    r1 = {}
    r2 = {}
    un = {}
    l1 = {}
    for index, row in df.iterrows():
        v1 = row['Vendedor1']
        if (row['CA10'] not in r1) and pd.notna(v1) and v1 != "":
            r1[row['CA10']] = v1
        v2 = row['Vendedor2']
        if (row['CA10'] not in r2) and pd.notna(v2) and v2 != "":
            r2[row['CA10']] = v2
        n1 = row['UN']
        if (row['CA10'] not in un) and pd.notna(n1) and n1 != "":
            un[row['CA10']] = n1
        l = row['Lider']
        if (row['CA10'] not in l1) and pd.notna(l) and l != "":
            l1[row['CA10']] = l

    df['Responsable 1_A'] = df['CA10'].map(r1)
    df['Responsable 2_A'] = df['CA10'].map(r2)
    df['Unidad de Negocio_A'] = df['CA10'].map(un)
    df['Lider_A'] = df['CA10'].map(l1)
    df['Lider'] = np.where(df['Lider'].isna(), df['Lider_A'], df['Lider'])
    df['Responsable 1'] = np.where(
        df['Vendedor1'].isna(), df['Responsable 1_A'], df['Vendedor1'])
    df['Responsable 2'] = np.where(
        df['Vendedor2'].isna(), df['Responsable 2_A'], df['Vendedor2'])
    df['Unidad de Negocio'] = np.where(
        df['UN'].isna(), df['Unidad de Negocio_A'], df['UN'])
    df['Proviene EPC/OEM/Canal Deal?'] = np.where(
        df['Responsable 1'] == df['Responsable 2'], 'No', 'Si')
    df['UBrutaCoti'] = df['UBrutaCoti'].fillna(0)
    df['UBrutaCoti'] = np.where(
        df['UBrutaCoti'] >= 0.22, 0.22, df['UBrutaCoti'])
    df['Monto Actualizado'] = np.where(
        df['UBrutaCoti'] >= 0.22, df['Monto Total'], df['Monto Total'] * df['UBrutaCoti'] / 0.22)
    df['Producto_CRM'] = df['Producto_CRM'].fillna('-')
    new_column = {
        'FecMov': 'Fecha',
        'Num_Factura': 'Número',
        'Producto_CRM': 'Producto CRM',
        'UBrutaCoti': 'UBruta',
        'Cliente': 'Nombre Empresa',
        'CamMda': 'T/C de la Factura',
        'Lider': 'Lider 1'
    }
    df.rename(columns=new_column, inplace=True)

    df_c1 = df[['AÑO', 'MES', 'Fecha', 'Número', 'Correlativo_OPCI',
                'Responsable 1', 'Responsable 2', 'R1', 'R2']].copy()
    df_c1['Responsable 1'] = df_c1['Responsable 1'].apply(lambda x: unicodedata.normalize(
        'NFD', x).encode('ascii', 'ignore').decode('utf-8') if isinstance(x, str) else x)
    df_c1['Responsable 2'] = df_c1['Responsable 2'].apply(lambda x: unicodedata.normalize(
        'NFD', x).encode('ascii', 'ignore').decode('utf-8') if isinstance(x, str) else x)
    df_c1['R1'] = df_c1['R1'].apply(lambda x: unicodedata.normalize('NFD', x).encode(
        'ascii', 'ignore').decode('utf-8') if isinstance(x, str) else x)
    df_c1['R2'] = df_c1['R2'].apply(lambda x: unicodedata.normalize('NFD', x).encode(
        'ascii', 'ignore').decode('utf-8') if isinstance(x, str) else x)
    df_c1 = df_c1[df_c1['Correlativo_OPCI'].notna()]
    df_c1.rename(columns={'Responsable 1': 'Responsable 1 E.', 'Responsable 2': 'Responsable 2 E.',
                 'R1': 'Responsable 1 B.', 'R2': 'Responsable 2 B.'}, inplace=True)

    grupo_opci = bt.groupby('OPCI')
    conflictos = []

    for opci, grupo in grupo_opci:
        responsables_1 = grupo['Responsable Deal - Principal'].dropna().unique()
        responsables_2 = grupo['Responsable Deal - Secundario'].dropna().unique()

        if len(responsables_1) > 1 or len(responsables_2) > 1:
            conflictos.append(grupo[['OPCI', 'Nombre', 'Factura #', 'Fecha de la factura',
                              'Etapa', 'Responsable Deal - Principal', 'Responsable Deal - Secundario']])

    df_conflictos = pd.concat(conflictos, ignore_index=True)

    df_servicios = df[df['Producto CRM'].str.contains('Serv-', na=False)]
    df_servicios = df_servicios[['AÑO', 'MES', 'Fecha', 'Número', 'Producto CRM',
                                 'Cotizacion #', 'Responsable 1', 'Responsable 2', 'Lider 1']]

    df['Factura Relacionada'] = df['DR_NSre'] + '-' + df['DR_NDoc']
    facturas = set(df['Número'])
    nc = df[df['Cd_TD'] == '07'].copy()
    df_group = (
        df.groupby('Número', as_index=False)
        .agg(Total_Factura=('Monto Total', 'sum'))
    )
    nc = nc.merge(
        df_group,
        left_on='Factura Relacionada',
        right_on='Número',
        how='left',
        suffixes=('', '_Factura')
    )
    nc['Factura encontrada'] = nc['Factura Relacionada'].isin(facturas)
    nc['Diferencia'] = abs(nc['Total_Factura'] + nc['Monto Total'])
    hoja6 = nc[['AÑO', 'MES', 'Estado', 'Número', 'Monto Total', 'Producto CRM', 'Cotizacion #', 'Responsable 1',
                'Responsable 2', 'Lider 1', 'Factura Relacionada', 'Total_Factura', 'Diferencia', 'Factura encontrada']]

    df = df[['OK', 'AÑO', 'MES', 'Responsable 1', 'Responsable 2', 'Unidad de Negocio', 'Fecha', 'Estado', 'Número', 'Monto Total', 'Producto CRM', 'UBruta',
            'Nombre Empresa', 'Subject', 'Codigos', 'Cotizacion #', 'Proviene EPC/OEM/Canal Deal?', 'T/C de la Factura', 'Monto Actualizado', 'Diferencia', 'Notas',
             'Observaciones', 'Periodo', 'EstadoPago-Vendedor', 'Lider 1', 'Lider 2', 'EstadoPago-Lideres', 'Umbral']]

    with pd.ExcelWriter('reporte.xlsx', engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Hoja1', index=False)
        one.to_excel(writer, sheet_name='Hoja2', index=False)
        df_c1.to_excel(writer, sheet_name='Hoja3', index=False)
        df_conflictos.to_excel(writer, sheet_name='Hoja4', index=False)
        df_servicios.to_excel(writer, sheet_name='Hoja5', index=False)
        hoja6.to_excel(writer, sheet_name='Hoja6', index=False)

    nombre_archivo = "reporte.xlsx"
    wb = load_workbook(nombre_archivo)
    ws = wb.active

    num_filas = len(df)
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.font = Font(name="Tahoma", size=9)

    for i in range(2, num_filas + 2):
        ws[f'T{i}'] = f'=IF(L{i}="",0,S{i}-J{i})'

    items = [
        [1, 3, 'A5A5A5'],
        [4, 18, '5B9BD5'],
        [19, 21, 'A5A5A5'],
        [22, 22, '92D050'],
        [23, 28, 'A5A5A5'],

    ]

    for item in items:
        inicio, fin, color = item
        for col in range(inicio, fin + 1):
            cell = ws.cell(row=1, column=col)
            cell.fill = PatternFill(start_color=color,  fill_type="solid")
            cell.font = Font(
                name="Tahoma",
                size=9,
                bold=True,
                color="FFFFFFFF"
            )

    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = max(
            len(str(c.value or "")) for c in col) + 2

    for i in range(2, num_filas + 2):
        cell = ws[f"L{i}"]
        if cell.value >= 0.22:
            cell.number_format = '">"0%'
        else:
            cell.number_format = "0.00%"
        cell.fill = PatternFill(start_color="A9D08E", fill_type="solid")
        cell.font = Font(color="375623")
        cell = ws[f"AB{i}"]
        cell.number_format = "0.00%"
        cell = ws[f"S{i}"]
        cell.number_format = "0.00"
        cell = ws[f"T{i}"]
        cell.number_format = "0.00"
        cell = ws[f"R{i}"]
        cell.number_format = "0.00"
        cell = ws[f'G{i}']
        cell.number_format = 'DD/MM/YYYY'

    ws.conditional_formatting.add(
        f"S2:S{num_filas + 1}",
        FormulaRule(formula=[f"S2<0"], font=Font(color="FF0000"))
    )
    ws.conditional_formatting.add(
        f"T2:T{num_filas + 1}",
        FormulaRule(formula=[f"T2<0"], font=Font(color="FF0000"))
    )

    ws.conditional_formatting.add(
        f"L2:L{num_filas + 1}",
        FormulaRule(
            formula=[f"L2=0"],
            font=Font(color="FFFFFF"),
            fill=PatternFill(start_color="FF0000",
                             end_color="FF0000", fill_type="solid")
        )
    )

    ws.conditional_formatting.add(
        f"L2:L{num_filas + 1}",
        FormulaRule(
            formula=[f"L2<0.22"],
            font=Font(color="9C002A"),
            fill=PatternFill(start_color="F8CBAD",
                             end_color="F8CBAD", fill_type="solid")
        )
    )

    ws.conditional_formatting.add(
        f"K2:K{num_filas + 1}",
        FormulaRule(
            formula=[f'COUNTIF(K2,"*Proy-*")>0'],
            fill=PatternFill(start_color="DBDBDB",
                             end_color="DBDBDB", fill_type="solid")
        )
    )
    ws.conditional_formatting.add(
        f"K2:K{num_filas + 1}",
        FormulaRule(
            formula=[f'COUNTIF(K2,"*Serv-*")>0'],
            fill=PatternFill(start_color="FFD966",
                             end_color="FFD966", fill_type="solid")
        )
    )
    ws.conditional_formatting.add(
        f"J2:J{num_filas + 1}",
        FormulaRule(
            formula=[f'J2<0'],
            font=Font(color="FF0000"),
            fill=PatternFill(start_color="FFC7CE",
                             end_color="FFC7CE", fill_type="solid")
        )
    )
    ws.column_dimensions['K'].width = 28
    ws.column_dimensions['M'].width = 35
    ws.column_dimensions['T'].width = 14
    ws.column_dimensions['S'].width = 23
    ws.column_dimensions['N'].width = 44
    ws.auto_filter.ref = ws.dimensions

    # Hoja N° 2

    ws = wb["Hoja2"]
    ws.conditional_formatting.add(
        f"G2:G{num_filas + 1}",
        FormulaRule(
            formula=[f'G2>0'],
            font=Font(color="FF0000"),
            fill=PatternFill(start_color="FFC7CE",
                             end_color="FFC7CE", fill_type="solid")
        )
    )
    ws.conditional_formatting.add(
        f"H2:H{num_filas + 1}",
        FormulaRule(
            formula=[f'H2>0'],
            font=Font(color="FF0000"),
            fill=PatternFill(start_color="FFC7CE",
                             end_color="FFC7CE", fill_type="solid")
        )
    )
    ws.auto_filter.ref = ws.dimensions
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = max(
            len(str(c.value or "")) for c in col) + 2

    # Hoja N° 3

    ws = wb["Hoja3"]
    ws.conditional_formatting.add(
        f"H2:H{num_filas + 1}",
        FormulaRule(
            formula=[f'H2<>F2'],
            font=Font(color="FF0000"),
            fill=PatternFill(start_color="FFC7CE",
                             end_color="FFC7CE", fill_type="solid")
        )
    )
    ws.conditional_formatting.add(
        f"I2:I{num_filas + 1}",
        FormulaRule(
            formula=[f'I2<>G2'],
            font=Font(color="FF0000"),
            fill=PatternFill(start_color="FFC7CE",
                             end_color="FFC7CE", fill_type="solid")
        )
    )

    ws.auto_filter.ref = ws.dimensions
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = max(
            len(str(c.value or "")) for c in col) + 2

    # Hoja N° 4
    ws = wb["Hoja4"]
    ws.auto_filter.ref = ws.dimensions
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = max(
            len(str(c.value or "")) for c in col) + 2

    color1 = PatternFill(start_color="DAEEF3",
                         end_color="DAEEF3", fill_type="solid")
    color2 = PatternFill(start_color="FDE9D9",
                         end_color="FDE9D9", fill_type="solid")
    grupo_actual = None
    color_actual = color1
    alternar = True

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        valor_grupo = row[0].value
        if valor_grupo != grupo_actual:
            grupo_actual = valor_grupo
            color_actual = color1 if alternar else color2
            alternar = not alternar
        for cell in row:
            cell.fill = color_actual

    # Hoja N° 5
    ws = wb["Hoja5"]
    num_filas = len(df_servicios)
    ws.conditional_formatting.add(
        f"H2:H{num_filas + 1}",
        FormulaRule(
            formula=['H2<>"Fredy Huaman R."'],
            font=Font(color="FF0000"),
            fill=PatternFill(start_color="FFC7CE",
                             end_color="FFC7CE", fill_type="solid")
        )
    )
    ws.conditional_formatting.add(
        f"G2:G{num_filas + 1}",
        FormulaRule(
            formula=['G2<>"Fredy Huaman R."'],
            font=Font(color="FF0000"),
            fill=PatternFill(start_color="FFC7CE",
                             end_color="FFC7CE", fill_type="solid")
        )
    )

    ws.auto_filter.ref = ws.dimensions
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = max(
            len(str(c.value or "")) for c in col) + 2

    # Hoja N° 6
    ws = wb["Hoja6"]
    num_filas = len(hoja6)
    ws.conditional_formatting.add(
        f"M2:M{num_filas + 1}",
        FormulaRule(
            formula=['M2 > 0'],
            font=Font(color="FF0000"),
            fill=PatternFill(start_color="FFC7CE",
                             end_color="FFC7CE", fill_type="solid")
        )
    )
    ws.conditional_formatting.add(
        f"N2:N{num_filas + 1}",
        FormulaRule(
            formula=['N2 = FALSE'],
            font=Font(color="FF0000"),
            fill=PatternFill(start_color="FFC7CE",
                             end_color="FFC7CE", fill_type="solid")
        )
    )

    ws.auto_filter.ref = ws.dimensions
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = max(
            len(str(c.value or "")) for c in col) + 2

    wb.save(nombre_archivo)

    df = pd.read_excel('reporte.xlsx')
    df = df[df['Responsable 1'].notna()]
    endress = df[df['Producto CRM'].str.contains('Endress', na=False)]

    umbrales = {
        'Jesus Alvarado M.': 66000, 'Alexandra Cruz C.': 36500, 'Shyla Quiroz C.': 39000,
        'Lino Castro V.': 63600, 'Gianella Belleza Z.': 30250, 'Alberto Gutierrez G.': 26650,
        'Yovany Barrera C.': 200000, 'Katherine Llerena C.': 57000, 'Martin Jordan C.': 21000,
        'Sergio Villena M.': 2000, 'Fernando Gomez D.': 1000, 'Janeth Quico D.': 1000
    }

    uns = {
        'Jesus Alvarado M.': 'UNAU', 'Alexandra Cruz C.': 'UNAU', 'Shyla Quiroz C.': 'UNAU',
        'Lino Castro V.': 'UNAU', 'Gianella Belleza Z.': 'UNAU', 'Alberto Gutierrez G.': 'UNAU',
        'Yovany Barrera C.': 'UNAI', 'Katherine Llerena C.': 'UNAU', 'Martin Jordan C.': 'UNAU',
        'Sergio Villena M.': 'UNAU', 'Fernando Gomez D.': 'UNVA', 'Janeth Quico D.': 'UNAI'
    }

    trimestres = {
        'Q1': [1, 2, 3],
        'Q2': [4, 5, 6],
        'Q3': [7, 8, 9],
        'Q4': [10, 11, 12]
    }

    data = {}

    for _, meses in trimestres.items():
        result = (
            endress[endress['MES'].isin(meses)]
            .pivot_table(
                index='Responsable 1',
                columns='MES',
                values='Monto Total',
                aggfunc='sum',
                fill_value=0,
            )
            .assign(Total=lambda x: x.sum(axis=1))
            .reset_index()
        )
        result['Umbral Mensual'] = result['Responsable 1'].map(umbrales)
        result['Umbral Trimestral'] = result['Umbral Mensual'] * 3
        result['Unidad negocio'] = result['Responsable 1'].map(uns)
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
                    data[f'{mes}_{row["Responsable 1"]}'] = row[f'Paso_{mes}']

    df['MES_RESPONSABLE'] = df['MES'].astype(str) + '_' + df['Responsable 1']
    df['Comisiona'] = df['MES_RESPONSABLE'].map(data)
    df['Comisiona'] = np.where(df['Producto CRM'].str.contains(
        'Endress'), df['Comisiona'], True)
    df['Comisiona'] = df['Comisiona'].fillna(True)
    df['Comisión'] = np.where(df['Comisiona'], df['Monto Actualizado']*0.01, 0)
    df['Porcentaje 1'] = 0.7
    df['Porcentaje 2'] = np.where((df['Responsable 2'].str.contains(
        'Paolo', na=False) & (df['Producto CRM'].str.contains('Proy', na=False))), 0.5, 0.3)
    df['Comisión 1'] = df['Comisión'] * df['Porcentaje 1']
    df['Comisión 2'] = df['Comisión'] * df['Porcentaje 2']
    df['Producto'] = np.where(df['Producto CRM'].str.contains(
        'Endress', na=False), 'Endress', df['Producto CRM'])
    df.drop(columns=['MES_RESPONSABLE'], inplace=True)

    COLUMN_MAP = {
        "OK": "ok",
        "AÑO": "anio",
        "MES": "mes",
        "Responsable 1": "responsable_1",
        "Responsable 2": "responsable_2",
        "Unidad de Negocio": "unidad_negocio",
        "Fecha": "fecha",
        "Estado": "estado",
        "Número": "numero",
        "Monto Total": "monto_total",
        "Producto CRM": "producto_crm",
        "UBruta": "utilidad_bruta",
        "Nombre Empresa": "nombre_empresa",
        "Subject": "subject",
        "Codigos": "codigos",
        "Cotizacion #": "cotizacion_num",
        "Proviene EPC/OEM/Canal Deal?": "origen_deal",
        "T/C de la Factura": "tipo_cambio_factura",
        "Monto Actualizado": "monto_actualizado",
        "Diferencia": "diferencia",
        "Notas": "notas",
        "Observaciones": "observaciones",
        "Periodo": "periodo",
        "EstadoPago-Vendedor": "estado_pago_vendedor",
        "Lider 1": "lider_1",
        "Lider 2": "lider_2",
        "EstadoPago-Lideres": "estado_pago_lideres",
        "Umbral": "umbral",
        "Producto": "producto",
        "Comisiona": "comisiona",
        "Comisión": "comision",
        "Porcentaje 1": "porcentaje_1",
        "Porcentaje 2": "porcentaje_2",
        "Comisión 1": "comision_1",
        "Comisión 2": "comision_2"
    }

    df = df.rename(columns=COLUMN_MAP)
    records = df.to_dict(orient="records")
    database.invoices_collection.delete_many({})
    database.invoices_collection.insert_many(records)
