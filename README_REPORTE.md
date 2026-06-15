# Reporte de Comisiones — Documentación del Excel Generado

## Descripción General

El sistema genera un archivo **`reporte.xlsx`** con **6 hojas** que centraliza la información de facturas, comisiones de vendedores y validaciones de datos. El reporte se produce combinando tres fuentes de datos:

- **SharePoint** — archivos Excel del ERP (`001_Ventas_OP.xlsx`, `004_Facturacion_OP.xlsx`) descargados vía Microsoft Graph API
- **`data_ventas`** — DataFrame con la data transaccional del ERP: tipos de documento, montos, tipos de cambio, vendedores
- **`data_invoices`** / `invoices.csv` — DataFrame del CRM/B24 con asignación de responsables, unidades de negocio y facturas por deal

El reporte se accede desde el endpoint `/invoices/export_report`.

---

## Flujo de datos general

```
data_ventas (ERP)  ──────────────────────────────────┐
                                                      │  merge por Num_Factura
004_Facturacion_OP.xlsx (b) ─────────────────────────┤
                                                      │  merge por Correlativo_OPCI
001_Ventas_OP / OP_Cabecera (a) ─────────────────────┘
                                                      ▼
                                                  df  (base de todas las hojas)

data_invoices / invoices.csv (bt) ──► diccionarios OPCI→Responsable, Factura#→UN
```

---

## Hoja 1 — Resumen

**Propósito**: Resumen principal de facturas con cálculo de comisiones aplicadas.

### Columnas (28 en total)

| Col | Nombre | Descripción |
|-----|--------|-------------|
| A | `OK` | Campo de aprobación manual, vacío por defecto |
| B | `AÑO` | Año extraído de `FecMov` |
| C | `MES` | Mes (1–12) extraído de `FecMov` |
| D | `Responsable 1` | Vendedor principal: `Vendedor1` del ERP; si es nulo, se rellena con el primer `Vendedor1` no nulo del mismo deal (`CA10`) |
| E | `Responsable 2` | Vendedor secundario: igual que Responsable 1 pero para `Vendedor2` |
| F | `Unidad de Negocio` | UNAU, UNAI o UNVA — viene de `bt` (invoices) mapeado por `Factura #`; si es nulo, se rellena por deal |
| G | `Fecha` | `FecMov` formateado DD/MM/YYYY |
| H | `Estado` | Tres valores posibles (ver lógica abajo) |
| I | `Número` | `NroSre + '-' + NroDoc` del ERP |
| J | `Monto Total` | Monto en USD (negativo para Notas de Crédito) |
| K | `Producto CRM` | Categoría del producto (`Producto_CRM`); "-" si está vacío |
| L | `UBruta` | Margen bruto (`UBrutaCoti`), capeado en 0.22; 0 si es nulo |
| M | `Nombre Empresa` | Razón social del cliente (`Cliente`) |
| N | `Subject` | Nombre del deal (`CA10`) |
| O | `Codigos` | Fijo "-" |
| P | `Cotizacion #` | `Correlativo_OPCI` del ERP (identificador del deal/orden) |
| Q | `Proviene EPC/OEM/Canal Deal?` | "Si" cuando Responsable 1 ≠ Responsable 2; "No" cuando son el mismo |
| R | `T/C de la Factura` | Tipo de cambio (`CamMda`) del ERP |
| S | `Monto Actualizado` | Monto ajustado por margen (ver fórmula abajo) |
| T | `Diferencia` | Fórmula Excel: `=IF(L{i}="",0,S{i}-J{i})` |
| U | `Notas` | Fijo "-" |
| V | `Observaciones` | Fijo "-" |
| W | `Periodo` | `AÑO + '-Q' + ceil(MES/3)` — ej: "2024-Q1" |
| X | `EstadoPago-Vendedor` | Vacío (llenado manualmente) |
| Y | `Lider 1` | Líder/gerente (`Lider`); si es nulo, se rellena por deal |
| Z | `Lider 2` | Vacío (llenado manualmente) |
| AA | `EstadoPago-Lideres` | Vacío (llenado manualmente) |
| AB | `Umbral` | Fijo 0.22 (22%) |

### Lógica de Estado (columna H)

| Condición | Valor en `Estado` |
|-----------|-------------------|
| `Cd_TD = '07'` (Nota de Crédito) | `"Nota Crédito"` |
| La factura es referenciada por una NC (aparece en `DR_NSre+DR_NDoc` de alguna NC) | `"Factura - [número_de_la_NC]"` |
| Cualquier otro caso | `Status_Factura` del archivo `004_Facturacion_OP.xlsx` |

### Lógica de Monto Total (columna J)

```
1. Base:  Si Moneda = USD  → MontoTotal_SinIGV
          Si Moneda = PEN  → MontoTotal_SinIGV / CamMda
2. Si Cd_TD = '07'  → -abs(ValorNeto del ERP)
3. Si Estado está vacío/nulo o es "Factura-..." y Monto es nulo  → ValorNeto del ERP
```

### Lógica de UBruta (columna L)

```
UBrutaCoti del archivo de facturación
  → se capea a 0.22 si es mayor (df['UBrutaCoti'] >= 0.22 → 0.22)
  → se rellena con 0 si es nulo
  → en Notas de Crédito (Cd_TD=07) se toma la UBrutaCoti de la factura original referenciada
```

### Lógica de Monto Actualizado (columna S)

```
Si UBruta >= 0.22  →  Monto Actualizado = Monto Total
Si UBruta < 0.22   →  Monto Actualizado = Monto Total × (UBruta / 0.22)
```

### Consolidación de Responsables/UN por deal

Si una fila tiene `Vendedor1`, `Vendedor2`, `UN` o `Lider` nulos, el sistema recorre todas las filas del mismo deal (`CA10`) y rellena con el **primer valor no nulo** encontrado para ese campo.

### Formato Visual

**Colores de encabezado (fila 1):**

| Columnas | Color |
|----------|-------|
| A–C (OK, Año, Mes) | Gris `#A5A5A5` |
| D–R (Responsables → T/C) | Azul `#5B9BD5` |
| S–U (Monto Actualizado → Notas) | Gris `#A5A5A5` |
| V (Observaciones) | Verde `#92D050` |
| W–AB (Periodo → Umbral) | Gris `#A5A5A5` |

Fuente de cabecera: Tahoma 9pt bold, texto blanco.

**Formato base de columna L (UBruta) — aplicado celda a celda:**
- Fondo verde `#A9D08E`, texto verde oscuro `#375623`
- Si `>= 0.22`: formato de número `">0%"`
- Si `< 0.22`: formato de número `"0.00%"`

**Formato condicional (sobrescribe el base):**

| Rango | Condición | Resultado |
|-------|-----------|-----------|
| L | `= 0` | Fondo rojo `#FF0000`, texto blanco |
| L | `< 0.22` | Fondo salmón `#F8CBAD`, texto rojo oscuro `#9C002A` |
| K | Contiene `"Proy-"` | Fondo gris `#DBDBDB` |
| K | Contiene `"Serv-"` | Fondo amarillo `#FFD966` |
| J | `< 0` | Fondo rojo claro `#FFC7CE`, texto rojo |
| S | `< 0` | Texto rojo `#FF0000` |
| T | `< 0` | Texto rojo `#FF0000` |

**Anchos fijos de columna:** K=28, M=35, N=44, S=23, T=14. El resto se auto-ajusta.

Filtro automático activado en todas las columnas.

---

## Hoja 2 — Monto ERP = Excel

**Propósito**: Análisis de discrepancias entre los montos del ERP y los del archivo Excel de facturación.

### Origen de datos

Se hace un merge **inner** entre `data_ventas` (ERP) y `004_Facturacion_OP.xlsx` por `Num_Factura`. Solo aparecen facturas presentes en ambas fuentes.

### Columnas (8 en total)

| Col | Nombre | Descripción |
|-----|--------|-------------|
| A | `Número` | `NroSre + '-' + NroDoc` |
| B | `Monto ERP` | `ValorNeto` del ERP, convertido a USD si moneda es PEN (`/CamMda`) |
| C | `Monto Excel` | `MontoTotal_SinIGV` del Excel, convertido a USD si moneda es PEN (`/T/C_USD-Sol`) |
| D | `Status_Factura` | Estado de la factura en el Excel (`004_Facturacion_OP.xlsx`) |
| E | `T/C ERP` | Tipo de cambio del ERP (`CamMda`) |
| F | `T/C Excel` | Tipo de cambio del Excel (`T/C_USD-Sol`) |
| G | `Monto (-)` | `abs(Monto ERP - Monto Excel)` |
| H | `T/C (-)` | `abs(T/C ERP - T/C Excel)` |

**Formato condicional:**
- G o H `> 0` → Fondo rojo `#FFC7CE`, texto rojo (indica discrepancia)

Filtro automático activado.

---

## Hoja 3 — Responsable B24 vs Excel

**Propósito**: Validar que los responsables asignados en el ERP coincidan con los asignados en el CRM/B24.

### Columnas (9 en total)

| Col | Nombre | Descripción |
|-----|--------|-------------|
| A | `AÑO` | Año |
| B | `MES` | Mes |
| C | `Fecha` | Fecha de la factura |
| D | `Número` | Número de factura |
| E | `Correlativo_OPCI` | ID de la Orden de Confirmación |
| F | `Responsable 1 E.` | Responsable principal según el ERP (`Vendedor1`) |
| G | `Responsable 2 E.` | Responsable secundario según el ERP (`Vendedor2`) |
| H | `Responsable 1 B.` | Responsable principal según el CRM/B24 (`Responsable Deal - Principal`) |
| I | `Responsable 2 B.` | Responsable secundario según el CRM/B24 (`Responsable Deal - Secundario`) |

Solo se incluyen filas donde `Correlativo_OPCI` no es nulo.

### Lógica de match

La llave de unión es el **`Correlativo_OPCI`**.

- **Fuente ERP** (columnas F y G): vienen de `Vendedor1`/`Vendedor2` en `data_ventas`, enriquecidos con `OP_Cabecera` mediante merge por `Correlativo_OPCI`.
- **Fuente B24** (columnas H e I): vienen del archivo `bt` (invoices del CRM). El OPCI se extrae del campo `Nombre` del deal con la expresión regular `OPCI-(\d+)`, y se construyen dos diccionarios `OPCI → Responsable` que se mapean al dataframe:

```python
bt['OPCI'] = 'OPCI-' + bt['Nombre'].str.extract(r'OPCI-(\d+)')
obj_r1[OPCI] = Responsable Deal - Principal   # → columna H
obj_r2[OPCI] = Responsable Deal - Secundario  # → columna I

df['R1'] = df['Correlativo_OPCI'].map(obj_r1)
df['R2'] = df['Correlativo_OPCI'].map(obj_r2)
```

Si el nombre del deal en `bt` no contiene el patrón `OPCI-XXXXX`, las columnas H e I quedan vacías para esa fila.

Todos los nombres se normalizan eliminando tildes (`unicodedata NFD → ascii`) antes de escribirlos en el Excel.

**Formato condicional:**
- H ≠ F → Fondo rojo `#FFC7CE`, texto rojo (discrepancia en Responsable 1)
- I ≠ G → Fondo rojo `#FFC7CE`, texto rojo (discrepancia en Responsable 2)

Filtro automático activado.

---

## Hoja 4 — OPCI Responsable Único

**Propósito**: Detectar OPCIs que tienen responsables en conflicto — la misma orden asignada a múltiples vendedores distintos dentro del archivo `bt`.

### Columnas (7 en total)

| Col | Nombre | Descripción |
|-----|--------|-------------|
| A | `OPCI` | ID de la Orden de Confirmación |
| B | `Nombre` | Nombre del deal |
| C | `Factura #` | Número de factura asociada |
| D | `Fecha de la factura` | Fecha de la factura |
| E | `Etapa` | Etapa del deal |
| F | `Responsable Deal - Principal` | Responsable principal asignado |
| G | `Responsable Deal - Secundario` | Responsable secundario asignado |

### Lógica

Agrupa `bt` por `OPCI`. Si un OPCI tiene más de un valor único en `Responsable Deal - Principal` **o** en `Responsable Deal - Secundario` (ignorando nulos), se considera en conflicto y todas sus filas aparecen en esta hoja. Si no hay conflictos, la hoja queda vacía.

**Formato visual — filas alternas por grupo OPCI:**
- Grupo 1: Fondo azul claro `#DAEEF3`
- Grupo 2: Fondo naranja claro `#FDE9D9`
- El color alterna cada vez que cambia el valor de la columna A (OPCI)

Filtro automático activado.

---

## Hoja 5 — Servicios Responsable Fredy

**Propósito**: Auditar que los productos de servicio (`Serv-*`) estén asignados al responsable dedicado **Fredy Huaman R.**

### Columnas (9 en total)

| Col | Nombre | Descripción |
|-----|--------|-------------|
| A | `AÑO` | Año |
| B | `MES` | Mes |
| C | `Fecha` | Fecha de la factura |
| D | `Número` | Número de factura |
| E | `Producto CRM` | Nombre del producto (contiene `"Serv-"`) |
| F | `Cotizacion #` | `Correlativo_OPCI` |
| G | `Responsable 1` | Responsable principal |
| H | `Responsable 2` | Responsable secundario |
| I | `Lider 1` | Líder principal |

### Lógica

Filtra las filas del dataframe principal donde `Producto CRM` contiene `"Serv-"`.

**Formato condicional:**
- G ≠ `"Fredy Huaman R."` → Fondo rojo `#FFC7CE`, texto rojo
- H ≠ `"Fredy Huaman R."` → Fondo rojo `#FFC7CE`, texto rojo

Filtro automático activado.

---

## Hoja 6 — Nota Crédito Compensada

**Propósito**: Validar que cada Nota de Crédito esté correctamente reconciliada contra su factura original.

### Columnas (14 en total)

| Col | Nombre | Descripción |
|-----|--------|-------------|
| A | `AÑO` | Año |
| B | `MES` | Mes |
| C | `Estado` | Siempre `"Nota Crédito"` |
| D | `Número` | Número de la Nota de Crédito |
| E | `Monto Total` | Monto de la NC (valor negativo) |
| F | `Producto CRM` | Categoría del producto |
| G | `Cotizacion #` | `Correlativo_OPCI` |
| H | `Responsable 1` | Responsable principal |
| I | `Responsable 2` | Responsable secundario |
| J | `Lider 1` | Líder principal |
| K | `Factura Relacionada` | `DR_NSre + '-' + DR_NDoc` — factura original que esta NC cancela |
| L | `Total_Factura` | Suma de `Monto Total` de todas las líneas de la factura original |
| M | `Diferencia` | `abs(Total_Factura + Monto Total)` — si = 0, la NC está completamente compensada |
| N | `Factura encontrada` | TRUE/FALSE — indica si la factura original existe en el dataset |

### Lógica

1. Filtra todas las filas con `Cd_TD = '07'` (Notas de Crédito).
2. Construye la referencia a la factura original: `DR_NSre + '-' + DR_NDoc`.
3. Agrupa el dataframe completo por `Número` y suma `Monto Total` → `Total_Factura`.
4. Hace un merge left de las NCs con ese agregado para obtener el total de la factura referenciada.
5. Calcula `Diferencia = abs(Total_Factura + Monto Total)`. Como el monto de la NC es negativo, la suma es el neto; si es 0, está completamente compensada.
6. `Factura encontrada` = TRUE si `Factura Relacionada` existe en el conjunto de números de factura del dataset.

**Formato condicional:**
- M `> 0` → Fondo rojo `#FFC7CE`, texto rojo (monto no reconciliado)
- N `= FALSE` → Fondo rojo `#FFC7CE`, texto rojo (factura original no encontrada)

Filtro automático activado.

---

## Cálculo de Comisiones (post-Excel, guardado en MongoDB)

Las comisiones se calculan después de generar el Excel, leyendo `reporte.xlsx` nuevamente, y los resultados se insertan en la colección `invoices` de MongoDB.

### 1. Determinación de `Comisiona`

Solo aplica a productos **Endress**. Para el resto (`Proy-*`, `Serv-*`, etc.) `Comisiona = True` siempre.

Para Endress, el check de umbral aplica **únicamente a vendedores de UNAU**. Los vendedores de UNAI y UNVA siempre tienen `Comisiona = True` en Endress.

Para cada vendedor UNAU y cada trimestre, se calcula:

```
Paso trimestral = Total ventas Endress del trimestre > Umbral Mensual × 3
Paso mensual    = Ventas Endress del mes > Umbral Mensual

Comisiona (mes M) = Paso trimestral OR Paso mensual
```

La clave de búsqueda es `"{MES}_{Responsable 1}"`. Si no se encuentra (vendedor no UNAU o no Endress), `Comisiona = True`.

**Umbrales mensuales por vendedor:**

| Vendedor | Umbral mensual (USD) | UN |
|----------|---------------------|----|
| Jesus Alvarado M. | 66,000 | UNAU |
| Alexandra Cruz C. | 36,500 | UNAU |
| Shyla Quiroz C. | 39,000 | UNAU |
| Lino Castro V. | 63,600 | UNAU |
| Gianella Belleza Z. | 30,250 | UNAU |
| Alberto Gutierrez G. | 26,650 | UNAU |
| Katherine Llerena C. | 57,000 | UNAU |
| Martin Jordan C. | 21,000 | UNAU |
| Sergio Villena M. | 2,000 | UNAU |
| Yovany Barrera C. | 200,000 | UNAI (siempre comisiona) |
| Fernando Gomez D. | 1,000 | UNVA (siempre comisiona) |
| Janeth Quico D. | 1,000 | UNAI (siempre comisiona) |

### 2. Comisión Total base (1%)

```
Si Comisiona = True   →  Comisión Total = Monto Actualizado × 0.01
Si Comisiona = False  →  Comisión Total = 0
```

### 3. Distribución por responsable

| Condición | Porcentaje Resp. 1 | Porcentaje Resp. 2 |
|-----------|--------------------|--------------------|
| General | 70% | 30% |
| Resp. 2 contiene `"Paolo"` **y** `Producto CRM` contiene `"Proy"` | 70% | 50% |

```
Comisión 1 = Comisión Total × Porcentaje 1
Comisión 2 = Comisión Total × Porcentaje 2
```

### 4. Consolidación de responsables duplicados

Si `Responsable 1` y `Responsable 2` son el mismo nombre (comparado en minúsculas), se genera un único registro en el array:

```
porcentaje = Porcentaje 1 + Porcentaje 2  →  1.0 (100%)
comision   = Comisión 1 + Comisión 2
```

### 5. Estructura guardada en MongoDB

Cada documento en la colección `invoices` incluye el campo `responsables` como array:

```json
"responsables": [
  { "nombre": "Jesus Alvarado M.", "porcentaje": 0.7, "comision": 123.45 },
  { "nombre": "Lino Castro V.",    "porcentaje": 0.3, "comision":  52.90 }
]
```

La colección se vacía completamente (`delete_many({})`) y se repuebla en cada ejecución del reporte.

---

## Columnas requeridas de `invoices.csv`

El archivo `invoices.csv` (separador `;`) es la fuente del CRM/Bitrix24. El sistema lee **7 columnas** de este archivo:

| Columna | Uso en el reporte |
|---------|-------------------|
| `Factura #` | Se limpia (primer token antes del espacio) y se usa para construir el diccionario `Factura # → Unidad de Negocio` que alimenta la columna F (UN) de la Hoja 1. También aparece en la Hoja 4. |
| `Nombre` | Se extrae el patrón `OPCI-(\d+)` para crear la columna derivada `OPCI`, que luego mapea responsables B24 a cada deal (Hojas 3 y 4). |
| `Unidad de Negocio` | Valor de UN (UNAU / UNAI / UNVA) mapeado a cada factura mediante `Factura #`. Columna F de la Hoja 1. |
| `Responsable Deal - Principal` | Construye el diccionario `OPCI → Responsable 1 B24` para la Hoja 3 (columna H) y la Hoja 4 (columna F). |
| `Responsable Deal - Secundario` | Construye el diccionario `OPCI → Responsable 2 B24` para la Hoja 3 (columna I) y la Hoja 4 (columna G). |
| `Fecha de la factura` | Se expone directamente en la Hoja 4 (columna D). |
| `Etapa` | Se expone directamente en la Hoja 4 (columna E). |

> **Nota**: el archivo se genera/actualiza desde el endpoint `POST /invoices/upload_csv` y se persiste en `./descargas/invoices.csv`. Las columnas no listadas aquí (p. ej. `Tipo de Negocio`, `Cotizacion #`, etc.) son ignoradas por el procesamiento actual.

---

## Archivos Fuente

| Archivo | Hojas leídas | `skiprows` | Uso |
|---------|-------------|-----------|-----|
| `001_Ventas_OP.xlsx` | `OP_Cabecera` | 2 | Datos de cabecera de órdenes: responsables, OPCI, líderes, CA10 |
| `001_Ventas_OP.xlsx` | `OP_Detalle-Venta` | 2 | Mapeo `Correlativo_OPCI → Numero_Deal` |
| `004_Facturacion_OP.xlsx` | (hoja por defecto) | 3 | Montos, tipos de cambio, estado de factura, margen bruto |

Ambos archivos se descargan automáticamente desde **SharePoint** (`corsusaadmin.sharepoint.com/sites/logistica`) vía Microsoft Graph API usando IDs únicos fijos.

---

## Archivo de Salida

- **Nombre**: `reporte.xlsx`
- **Directorio**: raíz del proyecto
- **Motor**: openpyxl
- **Endpoint**: `GET /invoices/export_report`
