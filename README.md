# Análisis del Mercado de Compras Públicas en Chile (2019-2025)

Este repositorio contiene un análisis completo del sistema de compras públicas de Chile, utilizando datos abiertos de **Mercado Público (ChileCompra)**. El objetivo es identificar patrones, tendencias y oportunidades de mercado a través de la exploración de datos de licitaciones desde 2019 hasta principios de 2025.

## Resumen del Proyecto

El análisis se enfoca en responder preguntas clave para proveedores del Estado y analistas de políticas públicas:

- ¿Cómo ha evolucionado el gasto público en los últimos años?
- ¿Qué sectores y regiones concentran la mayor inversión?
- ¿Qué categorías de productos o servicios son las más demandadas?
- ¿Dónde existen oportunidades de mercado con alto potencial y baja competencia?
- ¿Es posible predecir la demanda futura en ciertas categorías?

## Características

- **Pipeline de Datos Completo:** Desde la descarga y procesamiento de más de 30 GB de datos crudos hasta la generación de datasets analíticos limpios.
- **Análisis Exploratorio Profundo:** Más de 10 visualizaciones profesionales que revelan patrones de gasto, estacionalidad y concentración de mercado.
- **Índice de Oportunidad de Mercado:** Una métrica innovadora que combina gasto, crecimiento y competencia para rankear las categorías más atractivas.
- **Modelamiento Predictivo:** Implementación de modelos de series de tiempo (Prophet) para pronosticar el gasto futuro.
- **Código Reproducible:** Scripts de Python y un notebook Jupyter que documentan todo el proceso, permitiendo la verificación y extensión del análisis.

## Estructura del Repositorio

```
/chile-public-procurement-analysis
│
├── data/
│   ├── raw/          # (Vacío, los datos crudos se eliminan para ahorrar espacio)
│   └── processed/    # Datasets limpios y agregados en formato Parquet
│
├── figures/          # Visualizaciones generadas por el análisis
│
├── src/
│   ├── download_data.py       # Script para descargar datos masivos
│   ├── process_streaming.py   # Script para procesar datos crudos en streaming
│   ├── clean_data.py          # Script para limpieza y normalización
│   ├── feature_engineering.py # Script para crear métricas e índices
│   ├── modeling.py            # Script para modelamiento predictivo
│   └── visualizations.py      # Script para generar todas las figuras
│
├── chilecompra_analysis.ipynb  # Notebook Jupyter con el informe completo del análisis
├── README.md                   # Este archivo
└── requirements.txt            # Dependencias del proyecto
```

## Cómo Usar este Repositorio

1.  **Clonar el repositorio:**
    ```bash
    git clone <URL_DEL_REPOSITORIO>
    cd chile-public-procurement-analysis
    ```

2.  **Instalar dependencias:**
    Se recomienda crear un entorno virtual.
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Re-generar los datos y el análisis (Opcional):**
    Los scripts en la carpeta `src/` permiten reproducir todo el pipeline. La ejecución completa puede tardar varias horas y requiere una cantidad significativa de espacio en disco (>40 GB) durante el procesamiento.

    ```bash
    # (Opcional) Descargar datos crudos (¡Tarda mucho tiempo!)
    # python3 src/download_bulk.py

    # Procesar datos, limpiar, generar features y visualizaciones
    python3 src/process_streaming.py
    python3 src/clean_data.py
    python3 src/feature_engineering.py
    python3 src/modeling.py
    python3 src/visualizations.py
    ```

4.  **Explorar el análisis:**
    El notebook `chilecompra_analysis.ipynb` contiene el informe completo con todas las visualizaciones y conclusiones. Puede ser abierto con Jupyter Lab o Jupyter Notebook.

    ```bash
    jupyter lab chilecompra_analysis.ipynb
    ```

## Conclusiones Clave

- **Fuerte Estacionalidad:** El gasto público se acelera consistentemente en el último trimestre del año.
- **Concentración del Gasto:** El sector Salud y la Región Metropolitana son los principales focos de inversión.
- **Oportunidades en Construcción:** El sector de `SERVICIOS DE CONSTRUCCIÓN Y MANTENIMIENTO` domina el gasto, pero el análisis de competencia y crecimiento revela nichos en otras áreas.
- **Impacto COVID-19:** Se observa un pico de gasto anómalo en 2020, evidenciando la capacidad de respuesta del sistema de compras ante emergencias.

## Autor

- **Guillermo Fuentes Jaque**
  - Científico de Datos Geoespaciales
  - Consultor Ambiental

## Licencia

Este proyecto se distribuye bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles.
