#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generar_figuras.py — Regenera todas las figuras desde los parquets procesados.

Se ejecuta automáticamente en CI/CD cuando se actualizan los datos en
data/processed/. Si un parquet no existe, la figura correspondiente se omite
y se conserva la versión comprometida en el repo.

Uso:
    python3 src/generar_figuras.py
"""

import json
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED    = PROJECT_ROOT / "data" / "processed"
FIGURES      = PROJECT_ROOT / "figures"
FIGURES.mkdir(exist_ok=True)

plt.rcParams.update({
    "font.family":        "DejaVu Sans",
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "figure.facecolor":   "white",
    "axes.facecolor":     "#f9fafb",
    "figure.dpi":         150,
})

COLORS = [
    "#1565C0", "#2E7D32", "#E65100", "#6A1B9A",
    "#546E7A", "#37474F", "#FB8C00", "#0288D1",
]

SECTOR_COLORS = {
    "Salud":                        "#1565C0",
    "Municipalidades":              "#546E7A",
    "Gob. Central, Universidades":  "#2E7D32",
    "Obras Públicas":               "#FB8C00",
    "FFAA":                         "#E65100",
    "Otros":                        "#37474F",
    "Legislativo y judicial":       "#6A1B9A",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _cargar(filename):
    """Carga un parquet; retorna None si no existe."""
    p = PROCESSED / filename
    if not p.exists():
        print(f"  ⚠ No encontrado: {filename}")
        return None
    return pd.read_parquet(p)


def _deduplicar(df, keys):
    """Toma la fila con más n_licitaciones por grupo (elimina duplicados de batch)."""
    if df is None or "n_licitaciones" not in df.columns:
        return df
    idx = df.groupby(keys)["n_licitaciones"].idxmax()
    return df.loc[idx].reset_index(drop=True)


def _sin_outliers(df, col, pct=0.97):
    """Elimina el top (1-pct)% de valores en la columna indicada."""
    if df is None or col not in df.columns:
        return df
    cap = df[col].quantile(pct)
    return df[df[col] <= cap].copy()


def _rango(df, col_anio, inicio=2019, fin=2025):
    if df is None:
        return None
    return df[df[col_anio].between(inicio, fin)].copy()


# ── Figuras ──────────────────────────────────────────────────────────────────

def fig02_sector():
    """Gasto anual por sector económico (barras apiladas)."""
    df = _cargar("agregado_anual_organismo_clean.parquet")
    df = _deduplicar(df, ["NombreOrganismo", "anio"])
    df = _sin_outliers(df, "monto_total")
    df = _rango(df, "anio")
    if df is None:
        return
    df = df[df["sector"] != "SINDATO"]

    top_sec = df.groupby("sector")["monto_total"].sum().nlargest(6).index
    df = df[df["sector"].isin(top_sec)]
    pivot = (
        df.pivot_table(index="anio", columns="sector", values="monto_total", aggfunc="sum")
        / 1e9
    )

    fig, ax = plt.subplots(figsize=(13, 7))
    colors_use = [SECTOR_COLORS.get(c, COLORS[i % len(COLORS)]) for i, c in enumerate(pivot.columns)]
    pivot.plot(kind="bar", stacked=True, ax=ax, color=colors_use, alpha=0.88, width=0.65)

    for xi, tot in enumerate(pivot.sum(axis=1)):
        ax.text(xi, tot * 1.01, f"{tot:,.0f}", ha="center", va="bottom",
                fontsize=8.5, fontweight="bold", color="#333")

    ax.set_xticklabels([str(y) for y in pivot.index], rotation=0, fontsize=10)
    ax.set_xlabel("Año", fontsize=11)
    ax.set_ylabel("Gasto (MM CLP)", fontsize=11)
    ax.set_title(
        "Gasto en Compras Públicas por Sector Económico (2019–2025)\n"
        "Mercado Público / ChileCompra",
        fontweight="bold", pad=12,
    )
    ax.legend(title="Sector", loc="upper left", fontsize=8, title_fontsize=8, framealpha=0.9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"{y:,.0f}"))
    fig.text(0.5, 0.01, "Fuente: Mercado Público / ChileCompra · Montos en MM CLP",
             ha="center", fontsize=8, color="#888")
    plt.tight_layout(pad=2.5)
    plt.savefig(FIGURES / "fig02_gasto_anual_sector.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ fig02_gasto_anual_sector.png")


def fig03_categorias():
    """Top 15 categorías por monto adjudicado."""
    df = _cargar("metricas_anuales_categoria.parquet")
    df = _rango(df, "anio")
    if df is None:
        return

    top = df.groupby("Rubro1")["monto_adjudicado"].sum().nlargest(15).reset_index()
    top["monto_mm"] = top["monto_adjudicado"] / 1e9
    top = top.sort_values("monto_mm")

    fig, ax = plt.subplots(figsize=(14, 9))
    bars = ax.barh(top["Rubro1"].str[:55], top["monto_mm"], color=COLORS[0], alpha=0.85)
    for bar in bars:
        w = bar.get_width()
        ax.text(w * 1.01, bar.get_y() + bar.get_height() / 2,
                f"{w:,.0f}", va="center", fontsize=8.5, color="#333")
    ax.set_xlabel("Monto Total Adjudicado (MM CLP)", fontsize=11)
    ax.set_title("Top 15 Categorías por Monto Adjudicado (2019–2025)", fontweight="bold", pad=12)
    fig.text(0.5, 0.01, "Fuente: Mercado Público / ChileCompra",
             ha="center", fontsize=8, color="#888")
    plt.tight_layout(pad=2.5)
    plt.savefig(FIGURES / "fig03_top_categorias.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ fig03_top_categorias.png")


def fig04_region():
    """Distribución del gasto por región."""
    df = _cargar("agregado_anual_region_clean.parquet")
    df = _deduplicar(df, ["RegionUnidad", "anio"])
    df = _sin_outliers(df, "monto_total")
    df = _rango(df, "anio")
    if df is None:
        return
    df = df[~df["RegionUnidad"].str.contains("Extranjero|SINDATO", na=False)]

    reg = df.groupby("RegionUnidad")["monto_total"].sum().reset_index()
    reg["monto_mm"] = reg["monto_total"] / 1e9
    reg["label"] = (reg["RegionUnidad"]
                    .str.replace(r"Región (de(l)? |Aysén.*Campo|Metropolitana.*Santiago)", "", regex=True)
                    .str.replace(r"Libertador.*Higgins", "O'Higgins", regex=True)
                    .str.strip().str[:45])
    reg = reg.sort_values("monto_mm")

    fig, ax = plt.subplots(figsize=(14, 9))
    bars = ax.barh(reg["label"], reg["monto_mm"], color=COLORS[2], alpha=0.85)
    for bar in bars:
        w = bar.get_width()
        ax.text(w * 1.01, bar.get_y() + bar.get_height() / 2,
                f"{w:,.0f}", va="center", fontsize=8.5, color="#333")
    ax.set_xlabel("Monto Total (MM CLP)", fontsize=11)
    ax.set_title("Gasto Total en Compras Públicas por Región (2019–2025)", fontweight="bold", pad=12)
    fig.text(0.5, 0.01, "Fuente: Mercado Público / ChileCompra",
             ha="center", fontsize=8, color="#888")
    plt.tight_layout(pad=2.5)
    plt.savefig(FIGURES / "fig04_gasto_region.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ fig04_gasto_region.png")


def fig09_estacionalidad():
    """Patrón estacional mensual del gasto."""
    df = _cargar("agregado_mensual_categoria.parquet")
    if df is None:
        return
    df = _deduplicar(df, ["Rubro1", "anio", "mes"])
    df = _sin_outliers(df, "monto_adjudicado")
    df = _rango(df, "anio")

    monthly = df.groupby(["anio", "mes"])["monto_adjudicado"].sum().reset_index()
    monthly["monto_mm"] = monthly["monto_adjudicado"] / 1e9
    avg = monthly.groupby("mes")["monto_mm"].mean()

    fig, ax = plt.subplots(figsize=(12, 6))
    for year in sorted(monthly["anio"].unique()):
        yd = monthly[monthly["anio"] == year]
        ax.plot(yd["mes"], yd["monto_mm"], alpha=0.22, color="gray", lw=1.2)
    ax.plot(avg.index, avg.values, color=COLORS[0], lw=3, marker="o",
            markersize=7, label="Promedio 2019–2025", zorder=5)
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                         "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"])
    ax.set_xlabel("Mes", fontsize=11)
    ax.set_ylabel("Gasto (MM CLP)", fontsize=11)
    ax.set_title("Patrón Estacional del Gasto en Compras Públicas — Chile",
                 fontweight="bold", pad=12)
    ax.legend(fontsize=10)
    fig.text(0.5, 0.01, "Fuente: Mercado Público / ChileCompra",
             ha="center", fontsize=8, color="#888")
    plt.tight_layout(pad=2.5)
    plt.savefig(FIGURES / "fig09_estacionalidad.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ fig09_estacionalidad.png")


def fig10_heatmap():
    """Heatmap: distribución temporal del gasto por categoría."""
    df = _cargar("metricas_anuales_categoria.parquet")
    df = _rango(df, "anio")
    if df is None:
        return

    top_cats = df.groupby("Rubro1")["monto_adjudicado"].sum().nlargest(15).index
    df = df[df["Rubro1"].isin(top_cats)]
    pivot = df.pivot_table(index="Rubro1", columns="anio",
                           values="monto_adjudicado", aggfunc="sum")
    pivot_norm = pivot.div(pivot.sum(axis=1), axis=0) * 100
    pivot_norm.index = pivot_norm.index.str[:50]

    fig, ax = plt.subplots(figsize=(14, 9))
    sns.heatmap(pivot_norm, annot=True, fmt=".1f", cmap="YlOrRd", ax=ax,
                linewidths=0.5, cbar_kws={"label": "% del total por categoría"})
    ax.set_title("Distribución Temporal del Gasto por Categoría (% del total)",
                 fontweight="bold", pad=12)
    ax.set_xlabel("Año", fontsize=11)
    ax.set_ylabel("")
    plt.tight_layout(pad=2.5)
    plt.savefig(FIGURES / "fig10_heatmap_categoria_anio.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ fig10_heatmap_categoria_anio.png")


def fig11_pareto():
    """Concentración de proveedores — Análisis de Pareto."""
    df = _cargar("agregado_anual_proveedor_top500.parquet")
    df = _sin_outliers(df, "monto_total")
    df = _rango(df, "anio")
    if df is None:
        return

    prov = (df.groupby("NombreProveedor")["monto_total"].sum()
            .sort_values(ascending=False).reset_index())
    prov["pct_acum"] = prov["monto_total"].cumsum() / prov["monto_total"].sum() * 100
    prov["rank"] = range(1, len(prov) + 1)
    top50 = prov.head(50)

    fig, ax1 = plt.subplots(figsize=(12, 7))
    ax1.bar(top50["rank"], top50["monto_total"] / 1e9,
            color=COLORS[0], alpha=0.72)
    ax1.set_xlabel("Ranking de Proveedor", fontsize=11)
    ax1.set_ylabel("Monto Total (MM CLP)", color=COLORS[0], fontsize=11)
    ax1.tick_params(axis="y", labelcolor=COLORS[0])

    ax2 = ax1.twinx()
    ax2.plot(top50["rank"], top50["pct_acum"],
             color=COLORS[2], lw=2, marker="o", markersize=3)
    ax2.axhline(80, color=COLORS[2], linestyle="--", alpha=0.6)
    ax2.text(47, 81, "80%", color=COLORS[2], fontsize=10)
    ax2.set_ylabel("% Acumulado del Gasto", color=COLORS[2], fontsize=11)
    ax2.tick_params(axis="y", labelcolor=COLORS[2])
    ax2.spines["right"].set_visible(True)

    ax1.set_title("Concentración de Proveedores — Top 50 (Pareto)\n2019–2025",
                  fontweight="bold", pad=12)
    fig.text(0.5, 0.01, "Fuente: Mercado Público / ChileCompra",
             ha="center", fontsize=8, color="#888")
    plt.tight_layout(pad=2.5)
    plt.savefig(FIGURES / "fig11_proveedores_concentracion.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ fig11_proveedores_concentracion.png")


def fig15_scatter():
    """Scatter gasto vs población por región (per cápita)."""
    import subprocess, sys
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "src" / "scatter_gasto_poblacion.py")],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT),
    )
    if result.returncode == 0:
        print("  ✓ fig15_scatter_gasto_poblacion.png")
    else:
        print(f"  ✗ fig15 error: {result.stderr[-300:]}")


def sankey_update():
    """Regenera docs/sankey.html desde sankey_tops_2024.json."""
    import subprocess, sys
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "src" / "sankey_api.py")],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT),
    )
    if result.returncode == 0:
        print("  ✓ Sankey HTML actualizado")
    else:
        print(f"  ✗ Sankey error: {result.stderr[-300:]}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parquets = list(PROCESSED.glob("*.parquet"))
    if not parquets:
        print("⚠  data/processed/ vacío — se conservan las figuras comprometidas.")
        return

    print(f"=== Regenerando figuras ({len(parquets)} parquets disponibles) ===\n")

    fig02_sector()
    fig03_categorias()
    fig04_region()
    fig09_estacionalidad()
    fig10_heatmap()
    fig11_pareto()
    fig15_scatter()
    sankey_update()

    print(f"\n=== Listo: {len(list(FIGURES.glob('*.png')))} figuras en figures/ ===")


if __name__ == "__main__":
    main()
