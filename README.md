# Análisis del Mercado de Compras Públicas en Chile (2019–2025)

[![Deploy to GitHub Pages](https://github.com/djwillichile/chile-public-procurement-analysis/actions/workflows/deploy-pages.yml/badge.svg)](https://github.com/djwillichile/chile-public-procurement-analysis/actions/workflows/deploy-pages.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> **Sitio publicado:** [djwillichile.github.io/chile-public-procurement-analysis](https://djwillichile.github.io/chile-public-procurement-analysis/)

Análisis completo del sistema de compras públicas de Chile usando datos abiertos de **Mercado Público (ChileCompra)**. Identifica patrones, tendencias y oportunidades de mercado en 807.597 licitaciones (2019–2025).

## Pregunta de Investigación

> ¿Qué sectores de productos o servicios muestran mayor crecimiento en la demanda del Estado chileno y representan oportunidades de negocio para proveedores?

## Características

- **Pipeline reproducible:** Descarga → Streaming ETL (30 GB+) → Limpieza → Features → Modelos → Visualización
- **Índice de Oportunidad de Mercado:** Combina gasto, crecimiento y competencia para rankear categorías
- **Modelamiento predictivo:** Prophet + XGBoost con horizonte 2025–2028
- **12+ visualizaciones** profesionales incluyendo Sankey interactivo del flujo de inversión
- **GitHub Pages:** Sitio web publicado automáticamente en cada push a `main`

## Estructura del Repositorio

```
chile-public-procurement-analysis/
│
├── notebooks/
│   └── chile_public_procurement_analysis.ipynb  ← Informe técnico principal
│
├── src/
│   ├── download_data.py        ← Descarga masiva desde ChileCompra (Azure Blob)
│   ├── download_bulk.py        ← Descarga alternativa con manejo de errores
│   ├── process_streaming.py    ← ETL en streaming (>15M registros)
│   ├── clean_data.py           ← Limpieza y normalización de datos
│   ├── feature_engineering.py  ← Métricas derivadas e Índice de Oportunidad
│   ├── modeling.py             ← Prophet + XGBoost (pronóstico 2025–2028)
│   ├── visualizations.py       ← 12+ figuras estáticas profesionales
│   └── sankey_api.py           ← Sankey interactivo desde datos de la API
│
├── figures/                    ← Visualizaciones PNG + HTML interactivos
│
├── docs/                       ← Sitio GitHub Pages (landing + Sankey)
│   ├── index.html
│   └── sankey.html
│
├── data/
│   ├── raw/                    ← Datos crudos (excluidos del repo por tamaño)
│   └── processed/              ← Parquet procesados (excluidos del repo)
│
├── .github/workflows/
│   └── deploy-pages.yml        ← CI/CD: build notebook → deploy GitHub Pages
│
└── requirements.txt
```

## Hallazgos Clave

| # | Oportunidad | Índice | Por qué |
|---|-------------|--------|---------|
| 1 | **Obras** | 0.675 (Alta) | Alto gasto + crecimiento + moderada competencia |
| 2 | **Vehículos y Equipamiento** | 0.590 (Alta) | Crecimiento explosivo reciente |
| 3 | **Servicios Financieros** | 0.300 (Media) | Baja competencia (2.1 oferentes promedio) |

- **Estacionalidad:** Gasto se acelera consistentemente en Q4 de cada año
- **Concentración regional:** R. Metropolitana lidera; Coquimbo y Atacama destacan per cápita
- **Pronóstico:** Estabilización del gasto con estacionalidad mantenida hasta 2028

## Uso

### Instalación

```bash
git clone https://github.com/djwillichile/chile-public-procurement-analysis.git
cd chile-public-procurement-analysis
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### Ejecutar el pipeline completo

```bash
# 1. Descargar datos (requiere ~40 GB de espacio y varias horas)
python3 src/download_bulk.py

# 2. Procesar, limpiar y generar features
python3 src/process_streaming.py
python3 src/clean_data.py
python3 src/feature_engineering.py

# 3. Modelos predictivos y visualizaciones
python3 src/modeling.py
python3 src/visualizations.py

# 4. Sankey interactivo desde API / datos procesados
python3 src/sankey_api.py

# 5. Explorar el informe
jupyter lab notebooks/chile_public_procurement_analysis.ipynb
```

### Generar solo el Sankey

```bash
python3 src/sankey_api.py
# → figures/sankey_flujo_api.html  (interactivo)
# → figures/fig14_sankey_api.png   (estático)
```

## GitHub Pages (deploy automático)

El sitio se publica automáticamente en cada push a `main` mediante GitHub Actions:
1. Convierte el notebook a HTML con `nbconvert`
2. Despliega `docs/` + `figures/` + el notebook HTML a GitHub Pages

**Para activar GitHub Pages en tu fork:**
1. Ve a `Settings → Pages`
2. En *Source* selecciona **GitHub Actions**
3. Haz un push a `main` — el workflow se ejecuta automáticamente

## Autor

**Guillermo Fuentes Jaque** — Científico de Datos Geoespaciales / Consultor Ambiental

## Licencia

MIT — ver [LICENSE](LICENSE)
