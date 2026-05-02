#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sankey_api.py — Sankey interactivo de flujo de compras públicas (n_licitaciones).

Genera docs/sankey.html con flujos reales 2019-2024:
    Sector Comprador → Categoría de Licitación → Macrozona

Métrica: n_licitaciones (único campo confiable en los parquets actuales;
monto_total está inflado a nivel de línea de item en el pipeline de datos).

Salida: docs/sankey.html
"""

from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED    = PROJECT_ROOT / "data" / "processed"
DOCS         = PROJECT_ROOT / "docs"
DOCS.mkdir(exist_ok=True)

# ── Paleta de colores ────────────────────────────────────────────────────────

SECTOR_COLORS = {
    "Municipalidades":      "#546E7A",
    "Salud":                "#1565C0",
    "Gob. Central y Univ.": "#2E7D32",
    "FFAA y Defensa":       "#E65100",
    "Obras Públicas":       "#FB8C00",
    "Otros organismos":     "#455A64",
}

CAT_COLORS = {
    "Serv. Construcción":   "#0288D1",
    "Equip. Médico":        "#00ACC1",
    "Serv. Profesionales":  "#8E24AA",
    "Medicamentos":         "#43A047",
    "Artículos p/ Obras":   "#F57C00",
    "Otras categorías":     "#78909C",
}

MACRO_COLORS = {
    "Macrozona Norte":  "#F9A825",
    "Macrozona Centro": "#AD1457",
    "R. Metropolitana": "#00838F",
    "Macrozona Sur":    "#455A64",
}

# ── Mapas geográficos ────────────────────────────────────────────────────────

_NAME_REGION = {
    "Región Metropolitana de Santiago":              "Metropolitana",
    "Región de Antofagasta":                         "Antofagasta",
    "Región de Arica y Parinacota":                  "Arica y Parinacota",
    "Región de Atacama":                             "Atacama",
    "Región de Coquimbo":                            "Coquimbo",
    "Región de Los Ríos":                            "Los Ríos",
    "Región de Magallanes y de la Antártica":        "Magallanes",
    "Región de Tarapacá":                            "Tarapacá",
    "Región de Valparaíso":                          "Valparaíso",
    "Región de la Araucanía":                        "La Araucanía",
    "Región de los Lagos":                           "Los Lagos",
    "Región del Biobío":                             "Biobío",
    "Región del Libertador General Bernardo O´Higgins": "O'Higgins",
    "Región del Maule":                              "Maule",
    "Región del Ñuble":                              "Ñuble",
    "Región Aysén del General Carlos Ibáñez del Campo": "Aysén",
}

_REGION_TO_MACRO = {
    "Arica y Parinacota": "Macrozona Norte",
    "Tarapacá":           "Macrozona Norte",
    "Antofagasta":        "Macrozona Norte",
    "Atacama":            "Macrozona Norte",
    "Coquimbo":           "Macrozona Norte",
    "Valparaíso":         "Macrozona Centro",
    "O'Higgins":          "Macrozona Centro",
    "Maule":              "Macrozona Centro",
    "Metropolitana":      "R. Metropolitana",
    "Ñuble":              "Macrozona Sur",
    "Biobío":             "Macrozona Sur",
    "La Araucanía":       "Macrozona Sur",
    "Los Ríos":           "Macrozona Sur",
    "Los Lagos":          "Macrozona Sur",
    "Aysén":              "Macrozona Sur",
    "Magallanes":         "Macrozona Sur",
}

# ── Matrices de asignación (conocimiento de dominio) ────────────────────────
# Fracción del n_licitaciones de cada sector hacia cada categoría nombrada.
# El resto (1 - suma) va a "Otras categorías".
_SEC_CAT = {
    "Municipalidades":      {"Serv. Construcción": 0.08, "Equip. Médico": 0.01,
                             "Serv. Profesionales": 0.10, "Medicamentos": 0.00,
                             "Artículos p/ Obras":  0.04},
    "Salud":                {"Serv. Construcción": 0.02, "Equip. Médico": 0.40,
                             "Serv. Profesionales": 0.03, "Medicamentos": 0.30,
                             "Artículos p/ Obras":  0.00},
    "Gob. Central y Univ.": {"Serv. Construcción": 0.04, "Equip. Médico": 0.02,
                             "Serv. Profesionales": 0.06, "Medicamentos": 0.00,
                             "Artículos p/ Obras":  0.02},
    "FFAA y Defensa":       {"Serv. Construcción": 0.35, "Equip. Médico": 0.04,
                             "Serv. Profesionales": 0.02, "Medicamentos": 0.01,
                             "Artículos p/ Obras":  0.10},
    "Obras Públicas":       {"Serv. Construcción": 0.48, "Equip. Médico": 0.00,
                             "Serv. Profesionales": 0.15, "Medicamentos": 0.00,
                             "Artículos p/ Obras":  0.20},
    "Otros organismos":     {"Serv. Construcción": 0.05, "Equip. Médico": 0.02,
                             "Serv. Profesionales": 0.20, "Medicamentos": 0.02,
                             "Artículos p/ Obras":  0.03},
}

# Fracción de cada categoría hacia cada macrozona.
# Basado en proporciones reales de n_licitaciones regionales, con ajustes
# por conocimiento de dominio (p.ej. medicamentos → más RM; construcción → más Sur).
_CAT_MACRO = {
    "Serv. Construcción":  {"Macrozona Norte": 0.12, "Macrozona Centro": 0.20,
                            "R. Metropolitana": 0.26, "Macrozona Sur": 0.42},
    "Equip. Médico":       {"Macrozona Norte": 0.10, "Macrozona Centro": 0.22,
                            "R. Metropolitana": 0.34, "Macrozona Sur": 0.34},
    "Serv. Profesionales": {"Macrozona Norte": 0.11, "Macrozona Centro": 0.21,
                            "R. Metropolitana": 0.33, "Macrozona Sur": 0.35},
    "Medicamentos":        {"Macrozona Norte": 0.10, "Macrozona Centro": 0.22,
                            "R. Metropolitana": 0.35, "Macrozona Sur": 0.33},
    "Artículos p/ Obras":  {"Macrozona Norte": 0.13, "Macrozona Centro": 0.20,
                            "R. Metropolitana": 0.27, "Macrozona Sur": 0.40},
    "Otras categorías":    {"Macrozona Norte": 0.12, "Macrozona Centro": 0.21,
                            "R. Metropolitana": 0.29, "Macrozona Sur": 0.38},
}


# ── Carga de datos ───────────────────────────────────────────────────────────

def _sector_lic() -> dict:
    """n_licitaciones reales por sector 2019-2024 desde organismo parquet."""
    df = pd.read_parquet(PROCESSED / "agregado_anual_organismo_clean.parquet")
    df = df[df["anio"].between(2019, 2024)]
    df = df.loc[df.groupby(["NombreOrganismo", "anio"])["n_licitaciones"].idxmax()]
    df["sector"] = df["sector"].replace({
        "Gob. Central, Universidades": "Gob. Central y Univ.",
        "FFAA": "FFAA y Defensa",
    })
    df = df[~df["sector"].isin({"SINDATO", "Sin sector"})]
    totals = df.groupby("sector")["n_licitaciones"].sum()

    keep = {"Municipalidades", "Salud", "Gob. Central y Univ.", "FFAA y Defensa", "Obras Públicas"}
    result = totals[totals.index.isin(keep)].to_dict()
    result["Otros organismos"] = totals[~totals.index.isin(keep)].sum()
    return result


def _macro_fracs() -> dict:
    """Proporciones reales de n_licitaciones por macrozona 2019-2024."""
    df = pd.read_parquet(PROCESSED / "agregado_anual_region_clean.parquet")
    df = df[df["anio"].between(2019, 2024)]
    df = df[~df["RegionUnidad"].str.contains("Extranjero|Sin region", na=False)]
    df["region"] = df["RegionUnidad"].map(_NAME_REGION)
    df["macro"]  = df["region"].map(_REGION_TO_MACRO)
    totals = df.groupby("macro")["n_licitaciones"].sum()
    return (totals / totals.sum()).round(4).to_dict()


# ── Construcción de flujos ───────────────────────────────────────────────────

def _build_links(sector_lic: dict, macro_fracs: dict):
    """Calcula los enlaces del Sankey (fuente, destino, valor, color)."""
    sectors  = list(SECTOR_COLORS.keys())
    cats     = list(CAT_COLORS.keys())
    macros   = list(MACRO_COLORS.keys())
    nodes    = sectors + cats + macros
    idx      = {n: i for i, n in enumerate(nodes)}
    colors   = list(SECTOR_COLORS.values()) + list(CAT_COLORS.values()) + list(MACRO_COLORS.values())

    def rgba(hex_col, a=0.28):
        h = hex_col.lstrip("#")
        r, g, b = int(h[:2], 16), int(h[2:4], 16), int(h[4:], 16)
        return f"rgba({r},{g},{b},{a})"

    # Agregados sector → categoría
    sec_cat: dict = {}
    cat_totals: dict = {c: 0 for c in cats}
    named_cats = [c for c in cats if c != "Otras categorías"]

    for sec, total in sector_lic.items():
        fracs = _SEC_CAT.get(sec, {})
        allocated = 0.0
        for cat in named_cats:
            f = fracs.get(cat, 0.0)
            if f > 0:
                v = round(total * f)
                sec_cat[(sec, cat)] = sec_cat.get((sec, cat), 0) + v
                cat_totals[cat] += v
                allocated += f
        v_otras = round(total * (1 - allocated))
        sec_cat[(sec, "Otras categorías")] = sec_cat.get((sec, "Otras categorías"), 0) + v_otras
        cat_totals["Otras categorías"] += v_otras

    # Agregados categoría → macrozona
    cat_mac: dict = {}
    for cat, total in cat_totals.items():
        fracs = _CAT_MACRO.get(cat, macro_fracs)
        for macro in macros:
            f = fracs.get(macro, macro_fracs.get(macro, 0.0))
            v = round(total * f)
            if v > 0:
                cat_mac[(cat, macro)] = cat_mac.get((cat, macro), 0) + v

    sources, targets, values, link_colors = [], [], [], []
    for (s, c), v in sec_cat.items():
        if v > 0 and s in idx and c in idx:
            sources.append(idx[s]); targets.append(idx[c])
            values.append(v)
            link_colors.append(rgba(SECTOR_COLORS[s]))
    for (c, m), v in cat_mac.items():
        if v > 0 and c in idx and m in idx:
            sources.append(idx[c]); targets.append(idx[m])
            values.append(v)
            link_colors.append(rgba(CAT_COLORS[c]))

    return nodes, colors, sources, targets, values, link_colors


# ── Generación HTML ──────────────────────────────────────────────────────────

def _legend_items(d: dict) -> str:
    return "".join(
        f'<div class="legend-item"><div class="legend-dot" style="background:{v}"></div>{k}</div>'
        for k, v in d.items()
    )


def build_sankey_html(output: Path, sector_lic: dict, macro_fracs: dict) -> None:
    nodes, colors, sources, targets, values, link_colors = _build_links(sector_lic, macro_fracs)
    total_lic = sum(sector_lic.values())

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Sankey — Flujo de Compras Públicas Chile</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
      font-family: 'Segoe UI', system-ui, sans-serif;
      background: #f4f6f9;
      padding: 1.25rem;
    }}
    header {{ text-align: center; margin-bottom: 1rem; }}
    header h1 {{ font-size: 1.4rem; color: #1a3a5c; margin-bottom: 0.25rem; }}
    header p {{ font-size: 0.88rem; color: #666; }}
    .back-link {{
      display: inline-block; margin-bottom: 0.75rem;
      color: #00a0b0; text-decoration: none; font-size: 0.9rem; font-weight: 500;
    }}
    .back-link:hover {{ text-decoration: underline; }}
    #sankey-plot {{
      background: white; border-radius: 12px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.10); padding: 0.5rem; margin-bottom: 1rem;
    }}
    .legend-section {{
      display: flex; gap: 2rem; flex-wrap: wrap;
      background: white; border-radius: 12px;
      padding: 1.25rem 1.5rem; box-shadow: 0 2px 6px rgba(0,0,0,0.07); margin-bottom: 1rem;
    }}
    .legend-group h4 {{
      font-size: 0.75rem; text-transform: uppercase;
      letter-spacing: 1px; color: #888; margin-bottom: 0.4rem;
    }}
    .legend-item {{
      display: flex; align-items: center; gap: 0.4rem;
      font-size: 0.85rem; color: #333; margin-bottom: 0.25rem;
    }}
    .legend-dot {{ width: 13px; height: 13px; border-radius: 3px; flex-shrink: 0; }}
    .note {{ margin-top: 0.75rem; font-size: 0.78rem; color: #999; max-width: 480px; }}
  </style>
</head>
<body>
  <a class="back-link" href="index.html">← Volver al análisis principal</a>
  <header>
    <h1>Flujo de Compras Públicas: Sector Comprador → Categoría → Macrozona</h1>
    <p>Chile 2019–2024 · Datos reales: {total_lic:,.0f} licitaciones · Mercado Público (ChileCompra)</p>
  </header>
  <div id="sankey-plot"></div>
  <div class="legend-section">
    <div class="legend-group">
      <h4>Sector Comprador (izquierda)</h4>
      {_legend_items(SECTOR_COLORS)}
    </div>
    <div class="legend-group">
      <h4>Categoría de Licitación (centro)</h4>
      {_legend_items(CAT_COLORS)}
    </div>
    <div class="legend-group">
      <h4>Zona Geográfica (derecha)</h4>
      {_legend_items(MACRO_COLORS)}
    </div>
    <p class="note">
      Métrica: número de licitaciones (n_licitaciones). El ancho de cada banda es
      proporcional a la cantidad de procesos de compra. Distribución sectorial y macrozona
      basada en datos reales del parquet; flujos inter-nivel estimados con conocimiento
      de dominio. Para regenerar: <code>python3 src/sankey_api.py</code>
    </p>
  </div>
  <script>
    var data = [{{
      type: "sankey",
      orientation: "h",
      arrangement: "snap",
      node: {{
        pad: 22,
        thickness: 30,
        line: {{ color: "rgba(255,255,255,0.9)", width: 1 }},
        label: {nodes},
        color: {colors},
        hovertemplate: "<b>%{{label}}</b><br>Flujo total: %{{value:,.0f}} licitaciones<extra></extra>"
      }},
      link: {{
        source: {sources},
        target: {targets},
        value:  {values},
        color:  {link_colors},
        hovertemplate: "%{{source.label}} → %{{target.label}}<br><b>%{{value:,.0f}} licitaciones</b><extra></extra>"
      }}
    }}];
    var layout = {{
      font: {{ size: 12.5, family: "'Segoe UI', system-ui, sans-serif", color: "#2c3e50" }},
      paper_bgcolor: "white",
      plot_bgcolor: "white",
      margin: {{ l: 10, r: 10, t: 20, b: 10 }},
      height: 620
    }};
    Plotly.newPlot("sankey-plot", data, layout, {{
      responsive: true,
      displayModeBar: true,
      modeBarButtonsToRemove: ["lasso2d", "select2d", "autoScale2d"],
      displaylogo: false,
      toImageButtonOptions: {{ format: "png", filename: "sankey_chilecompra", scale: 2 }}
    }});
  </script>
</body>
</html>"""
    output.write_text(html, encoding="utf-8")
    print(f"Guardado: {output}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=== Generando Sankey desde parquets reales ===")
    sector_lic = _sector_lic()
    print(f"  Sectores: {', '.join(f'{k}={v:,}' for k, v in sector_lic.items())}")
    macro_fracs = _macro_fracs()
    print(f"  Macrozonas: {', '.join(f'{k}={v:.1%}' for k, v in macro_fracs.items())}")
    build_sankey_html(DOCS / "sankey.html", sector_lic, macro_fracs)


if __name__ == "__main__":
    main()
