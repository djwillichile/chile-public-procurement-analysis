# Guía de Contexto para Modelos de IA (Claude Code)

Este documento proporciona un mapa detallado de la estructura de datos procesados en formato Parquet dentro del proyecto `chile-public-procurement-analysis`. Está diseñado para ayudar a modelos de lenguaje (LLMs) como Claude Code a entender rápidamente el contenido y las relaciones entre los datasets, facilitando consultas y análisis.

## 1. Estructura del Directorio de Datos Procesados

Todos los datos relevantes para el análisis se encuentran en el directorio `data/processed/`. Estos archivos están en formato Parquet, optimizados para lectura eficiente y análisis con Pandas en Python.

## 2. Descripción de los Archivos Parquet

A continuación, se detalla cada archivo Parquet, su propósito y las columnas clave que contiene:

### `licitaciones_unicas.parquet`
- **Descripción:** Contiene una entrada única por cada licitación identificada, con sus características principales.
- **Columnas Clave:**
    - `CodigoExterno`: Identificador único de la licitación.
    - `Nombre`: Nombre o descripción de la licitación.
    - `Estado`: Estado actual de la licitación (ej. Adjudicada, Desierta).
    - `NombreOrganismo`: Nombre del organismo comprador.
    - `sector`: Sector al que pertenece el organismo comprador.
    - `RegionUnidad`: Región donde se realiza la compra.
    - `Tipo`: Tipo de licitación (ej. Licitación Pública, Trato Directo).
    - `MontoTotalAdjudicado`: Monto final adjudicado (en CLP).
    - `FechaPublicacion`: Fecha de publicación de la licitación.
    - `FechaAdjudicacion`: Fecha de adjudicación.
    - `NumeroOferentes`: Número de empresas que ofertaron.
    - `Rubro1`: Categoría principal de la licitación.
    - `anio`, `mes`: Año y mes de publicación.

### `agregado_mensual_categoria.parquet`
- **Descripción:** Agregaciones mensuales del gasto y actividad por categoría (`Rubro1`).
- **Columnas Clave:**
    - `anio`, `mes`: Año y mes.
    - `Rubro1`: Categoría de la licitación.
    - `n_licitaciones`: Número de licitaciones en el mes para esa categoría.
    - `monto_adjudicado`: Monto total adjudicado en el mes para esa categoría.
    - `oferentes_promedio`: Promedio de oferentes por licitación.
    - `n_proveedores`: Número de proveedores únicos.
    - `n_organismos`: Número de organismos únicos.

### `agregado_anual_organismo_clean.parquet`
- **Descripción:** Agregaciones anuales del gasto y actividad por organismo comprador.
- **Columnas Clave:**
    - `anio`: Año.
    - `NombreOrganismo`: Nombre del organismo comprador.
    - `sector`: Sector del organismo.
    - `n_licitaciones`: Número de licitaciones anuales.
    - `monto_total`: Monto total adjudicado anualmente.
    - `oferentes_promedio`: Promedio de oferentes.

### `agregado_anual_region_clean.parquet`
- **Descripción:** Agregaciones anuales del gasto y actividad por región.
- **Columnas Clave:**
    - `anio`: Año.
    - `RegionUnidad`: Región.
    - `n_licitaciones`: Número de licitaciones anuales.
    - `monto_total`: Monto total adjudicado anualmente.
    - `n_organismos`: Número de organismos únicos.

### `agregado_anual_proveedor_top500.parquet`
- **Descripción:** Top 500 proveedores por monto adjudicado anualmente.
- **Columnas Clave:**
    - `NombreProveedor`: Nombre del proveedor.
    - `n_adjudicaciones`: Número de adjudicaciones.
    - `monto_total`: Monto total adjudicado.
    - `n_rubros`: Número de rubros en los que participa.

### `competencia_por_rubro.parquet`
- **Descripción:** Métricas de competencia por rubro y año.
- **Columnas Clave:**
    - `anio`: Año.
    - `Rubro1`: Categoría de la licitación.
    - `oferentes_promedio`: Promedio de oferentes por licitación.
    - `oferentes_mediana`: Mediana de oferentes por licitación.
    - `n_licitaciones`: Número de licitaciones.

### `metricas_anuales_categoria.parquet`
- **Descripción:** Métricas anuales detalladas por categoría, incluyendo crecimiento.
- **Columnas Clave:**
    - `anio`: Año.
    - `Rubro1`: Categoría de la licitación.
    - `monto_adjudicado`: Monto total adjudicado.
    - `crecimiento_monto_pct`: Crecimiento porcentual del monto respecto al año anterior.
    - `crecimiento_lic_pct`: Crecimiento porcentual de licitaciones respecto al año anterior.
    - `indice_oportunidad`: Índice compuesto de oportunidad de mercado.

### `sankey_tops_2024.json`
- **Descripción:** Archivo JSON que contiene los top 15 organismos, categorías, proveedores y regiones para el año 2024, utilizados para generar el diagrama Sankey.
- **Estructura:** Diccionario con claves `organismos`, `categorias`, `proveedores`, `regiones`, cada una conteniendo una lista de diccionarios con `NombreOrganismo`/`Rubro1`/`NombreProveedor`/`RegionUnidad` y `monto_total`/`monto_adjudicado`.

## 3. Relaciones Clave entre Archivos

- **`anio`**: Presente en la mayoría de los archivos agregados para análisis temporal.
- **`Rubro1`**: Permite unir información entre categorías (`agregado_mensual_categoria.parquet`, `competencia_por_rubro.parquet`, `metricas_anuales_categoria.parquet`).
- **`NombreOrganismo`**: Conecta información de organismos (`agregado_anual_organismo_clean.parquet`).
- **`NombreProveedor`**: Conecta información de proveedores (`agregado_anual_proveedor_top500.parquet`).
- **`RegionUnidad`**: Conecta información de regiones (`agregado_anual_region_clean.parquet`).

## 4. Ejemplo de Uso para IA

Para cargar y explorar los datos, un modelo de IA podría usar el siguiente patrón en Python:

```python
import pandas as pd

# Cargar un archivo Parquet
df_organismos = pd.read_parquet('data/processed/agregado_anual_organismo_clean.parquet')

# Mostrar las primeras filas y columnas
print(df_organismos.head())
print(df_organismos.columns)

# Realizar una consulta, por ejemplo, los organismos con mayor gasto en 2024
top_organismos_2024 = df_organismos[df_organismos['anio'] == 2024].nlargest(10, 'monto_total')
print(top_organismos_2024)
```

Este contexto debería ser suficiente para que un modelo de IA navegue y analice los datos de manera efectiva.
