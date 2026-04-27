#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sankey_api.py — Diagrama Sankey interactivo de flujo de inversión pública

Genera un Sankey de tres niveles a partir de datos de ChileCompra:
    Sector Comprador → Categoría de Licitación → Región

Fuentes de datos (en orden de prioridad):
  1. Parquet procesados en data/processed/ (pipeline local completo)
  2. API pública de Mercado Público (si no hay datos procesados)
  3. Datos de demostración embebidos (fallback para GitHub Pages)

Salidas:
  - figures/sankey_flujo_api.html  (interactivo, para incrustar en GitHub Pages)
  - figures/fig14_sankey_api.png   (estático, para el notebook)

Uso:
    python3 src/sankey_api.py
"""

import json
import logging
import time
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
FIGURES_DIR = PROJECT_ROOT / "figures"
FIGURES_DIR.mkdir(exist_ok=True)

API_BASE = "https://api.mercadopublico.cl/servicios/v1/publico"
API_TICKET = "56A21800-89E6-437D-B03D-7A4B236DAC9C"

# ── Colores por sector ───────────────────────────────────────────────────────
SECTOR_COLORS = {
    "Salud": "#1565C0",
    "Min. Obras Públicas": "#2E7D32",
    "Municipalidades": "#546E7A",
    "FF.AA. y Defensa": "#E65100",
    "Educación": "#6A1B9A",
    "Otros Organismos": "#37474F",
}

CATEGORY_COLORS = {
    "Construcción y Mant.": "#0288D1",
    "Obras Públicas": "#43A047",
    "Vehículos y Equip.": "#FB8C00",
    "Servicios Varios": "#8E24AA",
}

REGION_COLORS = {
    "R. Metropolitana": "#00838F",
    "Macrozona Norte": "#F9A825",
    "Macrozona Centro": "#AD1457",
    "Macrozona Sur": "#37474F",
}


# ── 1. Carga de datos ────────────────────────────────────────────────────────

def load_from_parquet() -> pd.DataFrame | None:
    """Carga datos del pipeline local (parquet procesados)."""
    parquet_path = PROCESSED_DIR / "agregado_anual_organismo.parquet"
    if not parquet_path.exists():
        logger.info("Parquet local no disponible.")
        return None

    logger.info("Cargando datos desde parquet procesado...")
    df = pd.read_parquet(parquet_path)

    # Agrupamos por Sector, Rubro1 y Region para los tres niveles del Sankey
    base = PROCESSED_DIR / "licitaciones_clean.parquet"
    if not base.exists():
        return None

    df2 = pd.read_parquet(
        base,
        columns=["Sector", "Rubro1", "Region", "MontoAdjudicado"],
    )
    df2 = df2.dropna(subset=["Sector", "Rubro1", "Region", "MontoAdjudicado"])
    df2["MontoAdjudicado"] = pd.to_numeric(df2["MontoAdjudicado"], errors="coerce")

    top_sectores = df2.groupby("Sector")["MontoAdjudicado"].sum().nlargest(5).index
    top_rubros = df2.groupby("Rubro1")["MontoAdjudicado"].sum().nlargest(4).index
    top_regiones = df2.groupby("Region")["MontoAdjudicado"].sum().nlargest(4).index

    df2 = df2[
        df2["Sector"].isin(top_sectores)
        & df2["Rubro1"].isin(top_rubros)
        & df2["Region"].isin(top_regiones)
    ]

    flows = (
        df2.groupby(["Sector", "Rubro1", "Region"])["MontoAdjudicado"]
        .sum()
        .reset_index()
    )
    flows.columns = ["sector", "categoria", "region", "valor"]
    flows["valor"] = (flows["valor"] / 1e9).round(0)  # en miles de millones CLP
    return flows


def load_from_api(n_pages: int = 5) -> pd.DataFrame | None:
    """Consulta la API de Mercado Público para obtener licitaciones recientes."""
    logger.info("Consultando API de Mercado Público...")
    records = []
    for page in range(1, n_pages + 1):
        try:
            url = f"{API_BASE}/licitaciones.json"
            params = {
                "ticket": API_TICKET,
                "pagina": page,
                "estado": "adjudicada",
            }
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("Listado", [])
            if not items:
                break
            for item in items:
                records.append({
                    "sector": item.get("Organismo", {}).get("Tipo", "Otros"),
                    "categoria": item.get("Tipo", {}).get("Nombre", "Otros"),
                    "region": item.get("Region", {}).get("Nombre", "Otras"),
                    "valor": float(item.get("MontoEstimado", 0) or 0) / 1e9,
                })
            time.sleep(0.5)
        except Exception as exc:
            logger.warning(f"Error API (página {page}): {exc}")
            break

    if not records:
        return None

    df = pd.DataFrame(records)
    flows = (
        df.groupby(["sector", "categoria", "region"])["valor"]
        .sum()
        .reset_index()
    )
    return flows


def demo_data() -> pd.DataFrame:
    """Datos representativos embebidos (fallback) basados en los hallazgos reales del análisis."""
    rows = [
        # sector                     categoria               region            valor
        ("Salud",                   "Construcción y Mant.", "R. Metropolitana", 1200),
        ("Salud",                   "Vehículos y Equip.",   "R. Metropolitana",  550),
        ("Salud",                   "Servicios Varios",     "R. Metropolitana",  450),
        ("Salud",                   "Vehículos y Equip.",   "Macrozona Centro",  200),
        ("Salud",                   "Construcción y Mant.", "Macrozona Sur",      300),
        ("Min. Obras Públicas",     "Obras Públicas",       "R. Metropolitana",  900),
        ("Min. Obras Públicas",     "Obras Públicas",       "Macrozona Norte",   600),
        ("Min. Obras Públicas",     "Construcción y Mant.", "R. Metropolitana",  800),
        ("Min. Obras Públicas",     "Construcción y Mant.", "Macrozona Norte",   400),
        ("Min. Obras Públicas",     "Obras Públicas",       "Macrozona Sur",     400),
        ("Municipalidades",         "Obras Públicas",       "R. Metropolitana",  500),
        ("Municipalidades",         "Obras Públicas",       "Macrozona Sur",     400),
        ("Municipalidades",         "Construcción y Mant.", "Macrozona Centro",  350),
        ("Municipalidades",         "Construcción y Mant.", "R. Metropolitana",  250),
        ("FF.AA. y Defensa",        "Vehículos y Equip.",   "R. Metropolitana",  450),
        ("FF.AA. y Defensa",        "Vehículos y Equip.",   "Macrozona Norte",   200),
        ("FF.AA. y Defensa",        "Construcción y Mant.", "R. Metropolitana",  200),
        ("Educación",               "Construcción y Mant.", "R. Metropolitana",  300),
        ("Educación",               "Servicios Varios",     "R. Metropolitana",  250),
        ("Educación",               "Construcción y Mant.", "Macrozona Sur",     250),
        ("Otros Organismos",        "Obras Públicas",       "Macrozona Norte",   300),
        ("Otros Organismos",        "Servicios Varios",     "R. Metropolitana",  250),
        ("Otros Organismos",        "Servicios Varios",     "Macrozona Centro",  200),
    ]
    df = pd.DataFrame(rows, columns=["sector", "categoria", "region", "valor"])
    return df


# ── 2. Construcción del Sankey ───────────────────────────────────────────────

def build_sankey_html(flows: pd.DataFrame, output_path: Path) -> None:
    """Genera el HTML interactivo del Sankey con Plotly CDN."""

    sectores = sorted(flows["sector"].unique())
    categorias = sorted(flows["categoria"].unique())
    regiones = sorted(flows["region"].unique())

    all_nodes = sectores + categorias + regiones
    node_indices = {n: i for i, n in enumerate(all_nodes)}

    # Colores de nodos
    def node_color(name: str) -> str:
        for d in (SECTOR_COLORS, CATEGORY_COLORS, REGION_COLORS):
            if name in d:
                return d[name]
        return "#90A4AE"

    node_colors = [node_color(n) for n in all_nodes]

    # Links
    sources, targets, values, link_colors = [], [], [], []
    for _, row in flows.iterrows():
        src_sec = node_indices[row["sector"]]
        src_cat = node_indices[row["categoria"]]
        tgt_reg = node_indices[row["region"]]
        val = float(row["valor"])

        # sector → categoria
        sources.append(src_sec)
        targets.append(src_cat)
        values.append(val)
        c = node_colors[src_sec]
        link_colors.append(c.replace("#", "rgba(") + ",0.3)")  # quick transparency hack

        # categoria → region
        sources.append(src_cat)
        targets.append(tgt_reg)
        values.append(val)
        c = node_colors[src_cat]
        link_colors.append(c.replace("#", "rgba(") + ",0.3)")

    # Convertir hex→rgba correctamente
    def hex_to_rgba(hex_color: str, alpha: float = 0.3) -> str:
        h = hex_color.lstrip("#")
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return f"rgba({r},{g},{b},{alpha})"

    link_colors_fixed = []
    for _, row in flows.iterrows():
        src_col = node_colors[node_indices[row["sector"]]]
        cat_col = node_colors[node_indices[row["categoria"]]]
        link_colors_fixed.append(hex_to_rgba(src_col))
        link_colors_fixed.append(hex_to_rgba(cat_col))

    sankey_data = {
        "type": "sankey",
        "orientation": "h",
        "arrangement": "snap",
        "node": {
            "pad": 20,
            "thickness": 28,
            "line": {"color": "rgba(255,255,255,0.8)", "width": 1},
            "label": all_nodes,
            "color": node_colors,
            "hovertemplate": "<b>%{label}</b><br>Flujo: %{value:.0f} MM CLP<extra></extra>",
        },
        "link": {
            "source": sources,
            "target": targets,
            "value": values,
            "color": link_colors_fixed,
            "hovertemplate": "%{source.label} → %{target.label}<br><b>%{value:.0f} MM CLP</b><extra></extra>",
        },
    }

    layout = {
        "title": {
            "text": (
                "Flujo de Inversión Pública: Sector → Categoría → Región<br>"
                "<sub>Chile 2019–2025 · Datos: Mercado Público (ChileCompra) · "
                "Montos en Miles de Millones CLP</sub>"
            ),
            "font": {"size": 18, "family": "Segoe UI, sans-serif"},
            "x": 0.5,
            "xanchor": "center",
        },
        "font": {"size": 13, "family": "Segoe UI, sans-serif", "color": "#2c3e50"},
        "paper_bgcolor": "#f9fafb",
        "plot_bgcolor": "#f9fafb",
        "margin": {"l": 20, "r": 20, "t": 100, "b": 20},
        "height": 620,
    }

    html_content = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Sankey — Flujo Inversión Pública Chile</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
      font-family: 'Segoe UI', system-ui, sans-serif;
      background: #f4f6f9;
      padding: 1rem;
    }}
    .back-link {{
      display: inline-block;
      margin-bottom: 1rem;
      color: #00a0b0;
      text-decoration: none;
      font-size: 0.9rem;
    }}
    .back-link:hover {{ text-decoration: underline; }}
    #sankey-plot {{
      background: white;
      border-radius: 12px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.10);
      padding: 1rem;
    }}
    .legend-section {{
      display: flex;
      gap: 2rem;
      flex-wrap: wrap;
      margin-top: 1rem;
      background: white;
      border-radius: 12px;
      padding: 1.25rem 1.5rem;
      box-shadow: 0 2px 6px rgba(0,0,0,0.07);
    }}
    .legend-group h4 {{
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: #777;
      margin-bottom: 0.5rem;
    }}
    .legend-item {{
      display: flex;
      align-items: center;
      gap: 0.4rem;
      font-size: 0.88rem;
      color: #333;
      margin-bottom: 0.3rem;
    }}
    .legend-dot {{
      width: 12px;
      height: 12px;
      border-radius: 3px;
      flex-shrink: 0;
    }}
    .note {{
      margin-top: 0.75rem;
      font-size: 0.8rem;
      color: #888;
    }}
  </style>
</head>
<body>
  <a class="back-link" href="index.html">← Volver al inicio</a>

  <div id="sankey-plot"></div>

  <div class="legend-section">
    <div class="legend-group">
      <h4>Sector Comprador</h4>
      {"".join(f'<div class="legend-item"><div class="legend-dot" style="background:{v}"></div>{k}</div>' for k, v in SECTOR_COLORS.items())}
    </div>
    <div class="legend-group">
      <h4>Categoría de Licitación</h4>
      {"".join(f'<div class="legend-item"><div class="legend-dot" style="background:{v}"></div>{k}</div>' for k, v in CATEGORY_COLORS.items())}
    </div>
    <div class="legend-group">
      <h4>Zona Geográfica</h4>
      {"".join(f'<div class="legend-item"><div class="legend-dot" style="background:{v}"></div>{k}</div>' for k, v in REGION_COLORS.items())}
    </div>
    <p class="note">
      Datos: ChileCompra / Mercado Público (2019–2025). Montos estimados en Miles de Millones CLP (MM CLP).
    </p>
  </div>

  <script>
    var sankey = {json.dumps(sankey_data)};
    var layout = {json.dumps(layout)};
    Plotly.newPlot('sankey-plot', [sankey], layout, {{
      responsive: true,
      displayModeBar: true,
      modeBarButtonsToRemove: ['lasso2d', 'select2d'],
      displaylogo: false
    }});
  </script>
</body>
</html>"""

    output_path.write_text(html_content, encoding="utf-8")
    logger.info(f"Sankey HTML guardado en: {output_path}")


def build_sankey_png(flows: pd.DataFrame, output_path: Path) -> None:
    """Genera una imagen PNG estática del Sankey usando matplotlib (sin kaleido)."""
    try:
        import plotly.graph_objects as go
        import plotly.io as pio

        sectores = sorted(flows["sector"].unique())
        categorias = sorted(flows["categoria"].unique())
        regiones = sorted(flows["region"].unique())
        all_nodes = sectores + categorias + regiones
        node_indices = {n: i for i, n in enumerate(all_nodes)}

        def hex_rgba(hex_color: str, alpha: float = 0.3) -> str:
            h = hex_color.lstrip("#")
            r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
            return f"rgba({r},{g},{b},{alpha})"

        def node_color(name: str) -> str:
            for d in (SECTOR_COLORS, CATEGORY_COLORS, REGION_COLORS):
                if name in d:
                    return d[name]
            return "#90A4AE"

        node_colors = [node_color(n) for n in all_nodes]
        sources, targets, values, lcolors = [], [], [], []

        for _, row in flows.iterrows():
            sc = node_indices[row["sector"]]
            cc = node_indices[row["categoria"]]
            rc = node_indices[row["region"]]
            v = float(row["valor"])
            sources += [sc, cc]
            targets += [cc, rc]
            values += [v, v]
            lcolors += [hex_rgba(node_colors[sc]), hex_rgba(node_colors[cc])]

        fig = go.Figure(go.Sankey(
            node=dict(
                pad=20, thickness=28,
                label=all_nodes,
                color=node_colors,
                line=dict(color="white", width=0.5),
            ),
            link=dict(source=sources, target=targets, value=values, color=lcolors),
        ))
        fig.update_layout(
            title_text="Flujo de Inversión Pública: Sector → Categoría → Región (Chile 2019–2025)",
            font_size=13,
            height=600,
            paper_bgcolor="#f9fafb",
        )
        try:
            pio.write_image(fig, str(output_path), format="png", width=1400, height=600, scale=1.5)
            logger.info(f"PNG guardado en: {output_path}")
        except Exception:
            logger.warning("kaleido no disponible — se omite PNG estático. Use el HTML interactivo.")
    except ImportError:
        logger.warning("plotly no instalado — instale con: pip install plotly")


# ── 3. Main ──────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("GENERACIÓN SANKEY — Flujo de Inversión Pública ChileCompra")
    logger.info("=" * 60)

    flows = load_from_parquet()
    if flows is None:
        flows = load_from_api()
    if flows is None:
        logger.info("Usando datos de demostración embebidos.")
        flows = demo_data()

    logger.info(f"Flujos cargados: {len(flows)} filas")

    html_out = FIGURES_DIR / "sankey_flujo_api.html"
    png_out = FIGURES_DIR / "fig14_sankey_api.png"

    build_sankey_html(flows, html_out)
    build_sankey_png(flows, png_out)

    logger.info("=" * 60)
    logger.info("SANKEY GENERADO")
    logger.info(f"  HTML interactivo: {html_out}")
    logger.info(f"  PNG estático:     {png_out}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
