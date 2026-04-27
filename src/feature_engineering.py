#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
feature_engineering.py — Ingeniería de variables para análisis de compras públicas.

Genera métricas derivadas, índices de oportunidad de mercado y datasets
analíticos listos para modelamiento y visualización.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def compute_annual_category_metrics():
    """
    Calcula métricas anuales por categoría (Rubro1):
    - Gasto anual por categoría
    - Número de licitaciones por categoría
    - Crecimiento anual (%)
    - Competencia promedio
    - Monto promedio por contrato
    """
    logger.info("Calculando métricas anuales por categoría...")

    df = pd.read_parquet(PROCESSED_DIR / "agregado_mensual_categoria_clean.parquet")

    # Agregar a nivel anual
    annual = df.groupby(["anio", "Rubro1"]).agg(
        n_licitaciones=("n_licitaciones", "sum"),
        monto_adjudicado=("monto_adjudicado", "sum"),
        oferentes_promedio=("oferentes_promedio", "mean"),
        n_proveedores=("n_proveedores", "sum"),
        n_organismos=("n_organismos", "sum")
    ).reset_index()

    # Monto promedio por licitación
    annual["monto_promedio_licitacion"] = (
        annual["monto_adjudicado"] / annual["n_licitaciones"].replace(0, np.nan)
    )

    # Calcular crecimiento anual
    annual = annual.sort_values(["Rubro1", "anio"])
    annual["monto_prev"] = annual.groupby("Rubro1")["monto_adjudicado"].shift(1)
    annual["crecimiento_monto_pct"] = (
        (annual["monto_adjudicado"] - annual["monto_prev"]) / annual["monto_prev"].replace(0, np.nan) * 100
    )

    annual["lic_prev"] = annual.groupby("Rubro1")["n_licitaciones"].shift(1)
    annual["crecimiento_lic_pct"] = (
        (annual["n_licitaciones"] - annual["lic_prev"]) / annual["lic_prev"].replace(0, np.nan) * 100
    )

    annual.drop(columns=["monto_prev", "lic_prev"], inplace=True)

    annual.to_parquet(PROCESSED_DIR / "metricas_anuales_categoria.parquet", index=False)
    logger.info(f"  Guardado: metricas_anuales_categoria.parquet ({len(annual):,} filas)")

    return annual


def compute_market_opportunity_index(annual_df):
    """
    Construye un índice de oportunidad de mercado que combina:
    - Alto gasto (normalizado)
    - Alto crecimiento (normalizado)
    - Baja competencia (normalizado inverso)
    
    Índice = 0.4 * gasto_norm + 0.3 * crecimiento_norm + 0.3 * (1 - competencia_norm)
    """
    logger.info("Calculando índice de oportunidad de mercado...")

    # Usar datos del último año completo disponible (2024)
    # y crecimiento respecto al año anterior
    latest_year = annual_df["anio"].max()
    if latest_year == 2025:
        # 2025 puede estar incompleto, usar 2024
        target_year = 2024
    else:
        target_year = latest_year

    df = annual_df[annual_df["anio"] == target_year].copy()

    # Filtrar categorías con datos suficientes
    df = df[df["n_licitaciones"] >= 10]
    df = df[df["monto_adjudicado"] > 0]

    if len(df) == 0:
        logger.warning("No hay datos suficientes para calcular índice")
        return None

    # Normalizar métricas (min-max scaling)
    def normalize(series):
        min_val = series.min()
        max_val = series.max()
        if max_val == min_val:
            return pd.Series(0.5, index=series.index)
        return (series - min_val) / (max_val - min_val)

    df["gasto_norm"] = normalize(df["monto_adjudicado"])
    df["crecimiento_norm"] = normalize(df["crecimiento_monto_pct"].fillna(0))
    df["competencia_norm"] = normalize(df["oferentes_promedio"].fillna(df["oferentes_promedio"].median()))

    # Índice de oportunidad: alto gasto + alto crecimiento + baja competencia
    df["indice_oportunidad"] = (
        0.4 * df["gasto_norm"] +
        0.3 * df["crecimiento_norm"] +
        0.3 * (1 - df["competencia_norm"])
    )

    # Clasificar oportunidades
    df["clasificacion"] = pd.cut(
        df["indice_oportunidad"],
        bins=[0, 0.3, 0.5, 0.7, 1.0],
        labels=["Baja", "Media", "Alta", "Muy Alta"],
        include_lowest=True
    )

    df = df.sort_values("indice_oportunidad", ascending=False)

    df.to_parquet(PROCESSED_DIR / "indice_oportunidad_mercado.parquet", index=False)
    logger.info(f"  Guardado: indice_oportunidad_mercado.parquet ({len(df):,} filas)")

    # Mostrar top 15
    logger.info("\n  TOP 15 OPORTUNIDADES DE MERCADO:")
    top = df.head(15)[["Rubro1", "monto_adjudicado", "crecimiento_monto_pct",
                        "oferentes_promedio", "indice_oportunidad", "clasificacion"]]
    for _, row in top.iterrows():
        logger.info(
            f"    {row['Rubro1'][:50]:50s} | "
            f"Monto: {row['monto_adjudicado']/1e9:>10.1f}B | "
            f"Crec: {row['crecimiento_monto_pct']:>7.1f}% | "
            f"Ofer: {row['oferentes_promedio']:>5.1f} | "
            f"Idx: {row['indice_oportunidad']:.3f} ({row['clasificacion']})"
        )

    return df


def compute_time_series_data():
    """
    Genera series temporales mensuales por categoría para modelamiento predictivo.
    """
    logger.info("Generando series temporales para modelamiento...")

    df = pd.read_parquet(PROCESSED_DIR / "agregado_mensual_categoria_clean.parquet")

    # Crear columna de fecha
    df["fecha"] = pd.to_datetime(df["anio"].astype(str) + "-" + df["mes"].astype(str) + "-01")

    # Top 20 categorías por monto total
    top_cats = (
        df.groupby("Rubro1")["monto_adjudicado"]
        .sum()
        .nlargest(20)
        .index.tolist()
    )

    df_top = df[df["Rubro1"].isin(top_cats)].copy()
    df_top = df_top.sort_values(["Rubro1", "fecha"])

    df_top.to_parquet(PROCESSED_DIR / "series_temporales_top20.parquet", index=False)
    logger.info(f"  Guardado: series_temporales_top20.parquet ({len(df_top):,} filas)")

    # Serie temporal total (todas las categorías)
    ts_total = df.groupby("fecha").agg(
        n_licitaciones=("n_licitaciones", "sum"),
        monto_adjudicado=("monto_adjudicado", "sum"),
        oferentes_promedio=("oferentes_promedio", "mean")
    ).reset_index()
    ts_total = ts_total.sort_values("fecha")

    ts_total.to_parquet(PROCESSED_DIR / "serie_temporal_total.parquet", index=False)
    logger.info(f"  Guardado: serie_temporal_total.parquet ({len(ts_total):,} filas)")

    return df_top, ts_total


def compute_institutional_metrics():
    """Calcula métricas por organismo comprador."""
    logger.info("Calculando métricas institucionales...")

    df = pd.read_parquet(PROCESSED_DIR / "agregado_anual_organismo_clean.parquet")

    # Métricas acumuladas por organismo
    org_total = df.groupby(["NombreOrganismo", "sector"]).agg(
        n_licitaciones_total=("n_licitaciones", "sum"),
        monto_total=("monto_total", "sum"),
        n_anios_activo=("anio", "nunique"),
        monto_promedio_anual=("monto_total", "mean")
    ).reset_index()

    org_total = org_total.sort_values("monto_total", ascending=False)

    org_total.to_parquet(PROCESSED_DIR / "metricas_organismos.parquet", index=False)
    logger.info(f"  Guardado: metricas_organismos.parquet ({len(org_total):,} filas)")

    # Métricas por sector
    sector = df.groupby(["anio", "sector"]).agg(
        n_licitaciones=("n_licitaciones", "sum"),
        monto_total=("monto_total", "sum"),
        n_organismos=("NombreOrganismo", "nunique")
    ).reset_index()

    sector.to_parquet(PROCESSED_DIR / "metricas_sector.parquet", index=False)
    logger.info(f"  Guardado: metricas_sector.parquet ({len(sector):,} filas)")


def main():
    logger.info("=" * 60)
    logger.info("INGENIERÍA DE VARIABLES")
    logger.info("=" * 60)

    annual = compute_annual_category_metrics()
    compute_market_opportunity_index(annual)
    compute_time_series_data()
    compute_institutional_metrics()

    logger.info("\n" + "=" * 60)
    logger.info("ARCHIVOS GENERADOS:")
    for f in sorted(PROCESSED_DIR.iterdir()):
        size_mb = f.stat().st_size / (1024 * 1024)
        logger.info(f"  {f.name}: {size_mb:.1f} MB")
    logger.info("=" * 60)
    logger.info("INGENIERÍA DE VARIABLES COMPLETADA")


if __name__ == "__main__":
    main()
