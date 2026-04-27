#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
visualizations.py — Visualizaciones profesionales para el análisis de compras públicas.

Genera figuras de alta calidad para el informe técnico.
Los montos se expresan en miles de millones de CLP (MM CLP).
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import warnings
import logging
from pathlib import Path

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
FIGURES_DIR = PROJECT_ROOT / "figures"
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Estilo global ---
plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams.update({
    "figure.dpi": 150,
    "savefig.dpi": 150,
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.labelsize": 12,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
    "figure.figsize": (12, 7),
    "axes.spines.top": False,
    "axes.spines.right": False,
})

# Paleta de colores profesional
COLORS = ["#2563EB", "#DC2626", "#059669", "#D97706", "#7C3AED",
          "#DB2777", "#0891B2", "#65A30D", "#EA580C", "#4F46E5",
          "#BE123C", "#0D9488", "#CA8A04", "#9333EA", "#E11D48"]

# Factor de escala: convertir CLP a miles de millones (MM CLP)
SCALE = 1e9
UNIT = "MM CLP"


def fmt_mmclp(x, pos):
    """Formatea valores en miles de millones CLP."""
    if abs(x) >= 1e12:
        return f"{x/1e12:,.0f}B"
    elif abs(x) >= 1e9:
        return f"{x/1e9:,.0f} MM"
    elif abs(x) >= 1e6:
        return f"{x/1e6:,.0f}M"
    else:
        return f"{x:,.0f}"


def fig01_evolucion_gasto_total():
    """Figura 1: Evolución del gasto total en compras públicas (mensual)."""
    logger.info("Generando Fig 01: Evolución gasto total...")

    df = pd.read_parquet(PROCESSED_DIR / "serie_temporal_total.parquet")
    df = df.sort_values("fecha")

    # Escalar a miles de millones
    y_monto = df["monto_adjudicado"] / SCALE

    fig, ax1 = plt.subplots(figsize=(14, 7))

    ax1.fill_between(df["fecha"], y_monto, alpha=0.3, color=COLORS[0])
    ax1.plot(df["fecha"], y_monto, color=COLORS[0], linewidth=2, label="Monto adjudicado")
    ax1.set_xlabel("Fecha")
    ax1.set_ylabel(f"Monto Adjudicado ({UNIT})", color=COLORS[0])
    ax1.tick_params(axis="y", labelcolor=COLORS[0])

    ax2 = ax1.twinx()
    ax2.plot(df["fecha"], df["n_licitaciones"], color=COLORS[1], linewidth=2,
             linestyle="--", alpha=0.8, label="N° Licitaciones")
    ax2.set_ylabel("N° Licitaciones", color=COLORS[1])
    ax2.tick_params(axis="y", labelcolor=COLORS[1])
    ax2.spines["right"].set_visible(True)

    ax1.axvline(pd.Timestamp("2020-03-01"), color="gray", linestyle=":", alpha=0.7)
    ax1.text(pd.Timestamp("2020-04-01"), ax1.get_ylim()[1]*0.95, "COVID-19",
             fontsize=9, color="gray", style="italic")

    ax1.set_title("Evolución Mensual del Gasto en Compras Públicas — Chile (2019–2025)",
                   fontweight="bold", pad=15)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig01_evolucion_gasto_total.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig01_evolucion_gasto_total.png")


def fig02_gasto_anual_sector():
    """Figura 2: Gasto anual por sector."""
    logger.info("Generando Fig 02: Gasto anual por sector...")

    df = pd.read_parquet(PROCESSED_DIR / "metricas_sector.parquet")

    top_sectors = df.groupby("sector")["monto_total"].sum().nlargest(8).index
    df_top = df[df["sector"].isin(top_sectors)]
    df_top = df_top.copy()
    df_top["monto_scaled"] = df_top["monto_total"] / SCALE

    pivot = df_top.pivot_table(index="anio", columns="sector", values="monto_scaled", aggfunc="sum")
    pivot = pivot.fillna(0)

    fig, ax = plt.subplots(figsize=(14, 8))
    pivot.plot(kind="bar", stacked=True, ax=ax, color=COLORS[:len(pivot.columns)], width=0.7)

    ax.set_title("Gasto Anual en Compras Públicas por Sector — Chile (2019–2025)",
                  fontweight="bold", pad=15)
    ax.set_xlabel("Año")
    ax.set_ylabel(f"Monto Total ({UNIT})")
    ax.legend(title="Sector", bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=0)

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig02_gasto_anual_sector.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig02_gasto_anual_sector.png")


def fig03_top_categorias():
    """Figura 3: Top 15 categorías por monto total adjudicado."""
    logger.info("Generando Fig 03: Top categorías...")

    df = pd.read_parquet(PROCESSED_DIR / "metricas_anuales_categoria.parquet")

    top = df.groupby("Rubro1")["monto_adjudicado"].sum().nlargest(15).reset_index()
    top["monto_scaled"] = top["monto_adjudicado"] / SCALE
    top["Rubro1_short"] = top["Rubro1"].str[:50]
    top = top.sort_values("monto_scaled")

    fig, ax = plt.subplots(figsize=(14, 9))
    bars = ax.barh(top["Rubro1_short"], top["monto_scaled"], color=COLORS[0], alpha=0.85)

    for bar in bars:
        width = bar.get_width()
        label = f"{width:,.0f}" if width < 1000 else f"{width/1000:,.1f}B"
        ax.text(width * 1.01, bar.get_y() + bar.get_height()/2,
                label, va="center", fontsize=9)

    ax.set_title("Top 15 Categorías por Monto Total Adjudicado — Chile (2019–2025)",
                  fontweight="bold", pad=15)
    ax.set_xlabel(f"Monto Total Adjudicado ({UNIT})")

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig03_top_categorias.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig03_top_categorias.png")


def fig04_gasto_region():
    """Figura 4: Distribución del gasto por región."""
    logger.info("Generando Fig 04: Gasto por región...")

    df = pd.read_parquet(PROCESSED_DIR / "agregado_anual_region_clean.parquet")

    region_total = df.groupby("RegionUnidad").agg(
        monto_total=("monto_total", "sum"),
        n_licitaciones=("n_licitaciones", "sum")
    ).reset_index()

    region_total = region_total[~region_total["RegionUnidad"].isin(["Sin Región", ""])]
    region_total["monto_scaled"] = region_total["monto_total"] / SCALE
    region_total = region_total.sort_values("monto_scaled", ascending=True)

    fig, ax = plt.subplots(figsize=(14, 9))
    bars = ax.barh(region_total["RegionUnidad"], region_total["monto_scaled"],
                    color=COLORS[2], alpha=0.85)

    for bar in bars:
        width = bar.get_width()
        label = f"{width:,.0f}" if width < 1000 else f"{width/1000:,.1f}B"
        ax.text(width * 1.01, bar.get_y() + bar.get_height()/2,
                label, va="center", fontsize=9)

    ax.set_title("Gasto Total en Compras Públicas por Región — Chile (2019–2025)",
                  fontweight="bold", pad=15)
    ax.set_xlabel(f"Monto Total ({UNIT})")

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig04_gasto_region.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig04_gasto_region.png")


def fig05_competencia():
    """Figura 5: Nivel de competencia por categoría."""
    logger.info("Generando Fig 05: Competencia por categoría...")

    df = pd.read_parquet(PROCESSED_DIR / "competencia_por_rubro.parquet")

    comp = df.groupby("Rubro1").agg(
        oferentes_promedio=("oferentes_promedio", "mean"),
        n_licitaciones=("n_licitaciones", "sum")
    ).reset_index()

    comp = comp[comp["n_licitaciones"] >= 100]
    comp = comp.sort_values("oferentes_promedio", ascending=False).head(20)
    comp["Rubro1_short"] = comp["Rubro1"].str[:50]

    fig, ax = plt.subplots(figsize=(14, 8))

    colors_comp = [COLORS[1] if x < 3 else COLORS[3] if x < 5 else COLORS[2]
                   for x in comp["oferentes_promedio"]]

    ax.barh(comp["Rubro1_short"], comp["oferentes_promedio"], color=colors_comp, alpha=0.85)

    ax.axvline(3, color="red", linestyle="--", alpha=0.5, label="Baja competencia (<3)")
    ax.axvline(5, color="orange", linestyle="--", alpha=0.5, label="Competencia media (5)")

    ax.set_title("Nivel de Competencia Promedio por Categoría — Chile (2019–2025)",
                  fontweight="bold", pad=15)
    ax.set_xlabel("N° Promedio de Oferentes por Licitación")
    ax.legend(loc="lower right")

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig05_competencia.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig05_competencia.png")


def fig06_indice_oportunidad():
    """Figura 6: Índice de oportunidad de mercado."""
    logger.info("Generando Fig 06: Índice de oportunidad...")

    df = pd.read_parquet(PROCESSED_DIR / "indice_oportunidad_mercado.parquet")
    df = df.sort_values("indice_oportunidad", ascending=True).tail(20)
    df["Rubro1_short"] = df["Rubro1"].str[:50]

    color_map = {"Baja": COLORS[1], "Media": COLORS[3], "Alta": COLORS[2], "Muy Alta": COLORS[0]}
    colors = [color_map.get(str(c), "gray") for c in df["clasificacion"]]

    fig, ax = plt.subplots(figsize=(14, 8))
    bars = ax.barh(df["Rubro1_short"], df["indice_oportunidad"], color=colors, alpha=0.85)

    for bar, cls in zip(bars, df["clasificacion"]):
        width = bar.get_width()
        ax.text(width + 0.01, bar.get_y() + bar.get_height()/2,
                f"{width:.3f} ({cls})", va="center", fontsize=9)

    ax.set_title("Índice de Oportunidad de Mercado por Categoría — 2024",
                  fontweight="bold", pad=15)
    ax.set_xlabel("Índice de Oportunidad (0-1)")
    ax.set_xlim(0, 1)

    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLORS[0], alpha=0.85, label="Muy Alta"),
        Patch(facecolor=COLORS[2], alpha=0.85, label="Alta"),
        Patch(facecolor=COLORS[3], alpha=0.85, label="Media"),
        Patch(facecolor=COLORS[1], alpha=0.85, label="Baja"),
    ]
    ax.legend(handles=legend_elements, title="Clasificación", loc="lower right")

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig06_indice_oportunidad.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig06_indice_oportunidad.png")


def fig07_forecast_total():
    """Figura 7: Forecast Prophet del gasto total."""
    logger.info("Generando Fig 07: Forecast total...")

    fc = pd.read_parquet(PROCESSED_DIR / "forecast_prophet_total.parquet")
    actual = pd.read_parquet(PROCESSED_DIR / "serie_temporal_total.parquet")
    actual = actual.sort_values("fecha")

    fig, ax = plt.subplots(figsize=(14, 7))

    ax.plot(actual["fecha"], actual["monto_adjudicado"]/SCALE, color=COLORS[0],
            linewidth=2, label="Datos reales", marker="o", markersize=3)

    fc_future = fc[fc["ds"] > actual["fecha"].max()]
    ax.plot(fc_future["ds"], fc_future["yhat"]/SCALE, color=COLORS[1],
            linewidth=2, linestyle="--", label="Predicción Prophet")
    ax.fill_between(fc_future["ds"], fc_future["yhat_lower"]/SCALE,
                    fc_future["yhat_upper"]/SCALE,
                    alpha=0.2, color=COLORS[1], label="Intervalo de confianza 80%")

    ax.axvline(actual["fecha"].max(), color="gray", linestyle=":", alpha=0.7)
    ax.text(actual["fecha"].max(), ax.get_ylim()[1]*0.95, " Predicción →",
            fontsize=10, color="gray")

    ax.set_title("Predicción del Gasto en Compras Públicas — Prophet (2019–2028)",
                  fontweight="bold", pad=15)
    ax.set_xlabel("Fecha")
    ax.set_ylabel(f"Monto Adjudicado ({UNIT})")
    ax.legend(loc="upper left")

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig07_forecast_total.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig07_forecast_total.png")


def fig08_top_organismos():
    """Figura 8: Top 15 organismos compradores."""
    logger.info("Generando Fig 08: Top organismos...")

    df = pd.read_parquet(PROCESSED_DIR / "metricas_organismos.parquet")
    top = df.nlargest(15, "monto_total")
    top = top.copy()
    top["monto_scaled"] = top["monto_total"] / SCALE
    top["NombreOrganismo_short"] = top["NombreOrganismo"].str[:55]
    top = top.sort_values("monto_scaled")

    fig, ax = plt.subplots(figsize=(14, 9))
    bars = ax.barh(top["NombreOrganismo_short"], top["monto_scaled"],
                    color=COLORS[4], alpha=0.85)

    for bar in bars:
        width = bar.get_width()
        label = f"{width:,.0f}" if width < 1000 else f"{width/1000:,.1f}B"
        ax.text(width * 1.01, bar.get_y() + bar.get_height()/2,
                label, va="center", fontsize=9)

    ax.set_title("Top 15 Organismos Compradores por Monto Total — Chile (2019–2025)",
                  fontweight="bold", pad=15)
    ax.set_xlabel(f"Monto Total ({UNIT})")

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig08_top_organismos.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig08_top_organismos.png")


def fig09_estacionalidad():
    """Figura 9: Estacionalidad mensual del gasto."""
    logger.info("Generando Fig 09: Estacionalidad...")

    df = pd.read_parquet(PROCESSED_DIR / "serie_temporal_total.parquet")
    df["month"] = df["fecha"].dt.month
    df["year"] = df["fecha"].dt.year

    monthly_avg = df.groupby("month")["monto_adjudicado"].mean().reset_index()

    fig, ax = plt.subplots(figsize=(12, 6))

    for year in sorted(df["year"].unique()):
        year_data = df[df["year"] == year]
        ax.plot(year_data["month"], year_data["monto_adjudicado"]/SCALE,
                alpha=0.3, color="gray", linewidth=1)

    ax.plot(monthly_avg["month"], monthly_avg["monto_adjudicado"]/SCALE,
            color=COLORS[0], linewidth=3, marker="o", markersize=8,
            label="Promedio mensual", zorder=5)

    months_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                     "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(months_labels)

    ax.set_title("Patrón Estacional del Gasto en Compras Públicas — Chile",
                  fontweight="bold", pad=15)
    ax.set_xlabel("Mes")
    ax.set_ylabel(f"Monto Adjudicado ({UNIT})")
    ax.legend()

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig09_estacionalidad.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig09_estacionalidad.png")


def fig10_heatmap_categoria_anio():
    """Figura 10: Heatmap de gasto por categoría y año."""
    logger.info("Generando Fig 10: Heatmap categoría-año...")

    df = pd.read_parquet(PROCESSED_DIR / "metricas_anuales_categoria.parquet")

    top_cats = df.groupby("Rubro1")["monto_adjudicado"].sum().nlargest(15).index
    df_top = df[df["Rubro1"].isin(top_cats)]

    pivot = df_top.pivot_table(index="Rubro1", columns="anio",
                                values="monto_adjudicado", aggfunc="sum")
    pivot.index = pivot.index.str[:50]

    pivot_norm = pivot.div(pivot.sum(axis=1), axis=0) * 100

    fig, ax = plt.subplots(figsize=(14, 9))
    sns.heatmap(pivot_norm, annot=True, fmt=".1f", cmap="YlOrRd",
                ax=ax, linewidths=0.5, cbar_kws={"label": "% del total"})

    ax.set_title("Distribución Temporal del Gasto por Categoría (% del total por categoría)",
                  fontweight="bold", pad=15)
    ax.set_xlabel("Año")
    ax.set_ylabel("")

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig10_heatmap_categoria_anio.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig10_heatmap_categoria_anio.png")


def fig11_proveedores_concentracion():
    """Figura 11: Concentración de proveedores (Pareto)."""
    logger.info("Generando Fig 11: Concentración proveedores...")

    df = pd.read_parquet(PROCESSED_DIR / "agregado_anual_proveedor_top500.parquet")

    prov_total = df.groupby("NombreProveedor")["monto_total"].sum().sort_values(ascending=False)
    prov_total = prov_total.reset_index()
    prov_total["pct_acumulado"] = prov_total["monto_total"].cumsum() / prov_total["monto_total"].sum() * 100
    prov_total["rank"] = range(1, len(prov_total) + 1)

    fig, ax1 = plt.subplots(figsize=(12, 7))

    ax1.bar(prov_total["rank"][:50], prov_total["monto_total"][:50]/SCALE,
            color=COLORS[0], alpha=0.7)
    ax1.set_xlabel("Ranking de Proveedor")
    ax1.set_ylabel(f"Monto Total ({UNIT})", color=COLORS[0])

    ax2 = ax1.twinx()
    ax2.plot(prov_total["rank"][:50], prov_total["pct_acumulado"][:50],
             color=COLORS[1], linewidth=2, marker="o", markersize=3)
    ax2.set_ylabel("% Acumulado del Gasto", color=COLORS[1])
    ax2.axhline(80, color=COLORS[1], linestyle="--", alpha=0.5)
    ax2.text(45, 81, "80%", color=COLORS[1], fontsize=10)
    ax2.spines["right"].set_visible(True)

    ax1.set_title("Concentración de Proveedores — Top 50 (Análisis de Pareto)",
                   fontweight="bold", pad=15)

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig11_proveedores_concentracion.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig11_proveedores_concentracion.png")


def fig12_estado_licitaciones():
    """Figura 12: Distribución de estados de licitaciones."""
    logger.info("Generando Fig 12: Estados de licitaciones...")

    df = pd.read_parquet(PROCESSED_DIR / "licitaciones_clean.parquet",
                          columns=["Estado", "anio"])

    estado_count = df.groupby(["anio", "Estado"]).size().reset_index(name="count")

    top_estados = estado_count.groupby("Estado")["count"].sum().nlargest(6).index
    estado_count = estado_count[estado_count["Estado"].isin(top_estados)]

    pivot = estado_count.pivot_table(index="anio", columns="Estado", values="count", aggfunc="sum")
    pivot = pivot.fillna(0)

    fig, ax = plt.subplots(figsize=(12, 7))
    pivot.plot(kind="bar", stacked=True, ax=ax, color=COLORS[:len(pivot.columns)], width=0.7)

    ax.set_title("Distribución de Estados de Licitaciones por Año — Chile (2019–2025)",
                  fontweight="bold", pad=15)
    ax.set_xlabel("Año")
    ax.set_ylabel("N° de Licitaciones")
    ax.legend(title="Estado", bbox_to_anchor=(1.02, 1), loc="upper left")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=0)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"{x/1000:.0f}K"))

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "fig12_estado_licitaciones.png", bbox_inches="tight")
    plt.close()
    logger.info("  Guardado: fig12_estado_licitaciones.png")


def main():
    logger.info("=" * 60)
    logger.info("GENERACIÓN DE VISUALIZACIONES")
    logger.info("=" * 60)

    fig01_evolucion_gasto_total()
    fig02_gasto_anual_sector()
    fig03_top_categorias()
    fig04_gasto_region()
    fig05_competencia()
    fig06_indice_oportunidad()
    fig07_forecast_total()
    fig08_top_organismos()
    fig09_estacionalidad()
    fig10_heatmap_categoria_anio()
    fig11_proveedores_concentracion()
    fig12_estado_licitaciones()

    logger.info("\n" + "=" * 60)
    logger.info(f"TOTAL: {len(list(FIGURES_DIR.glob('*.png')))} figuras generadas")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
