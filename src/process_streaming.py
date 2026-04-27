#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
process_streaming.py — Procesa los CSVs raw de licitaciones en streaming.

Estrategia: procesar cada CSV mensual individualmente, generar agregaciones
parciales y luego combinar solo las agregaciones (no los datos crudos).
Esto evita cargar todos los datos en memoria.
"""

import os
import sys
import csv
import gc
import logging
from pathlib import Path

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Columnas a extraer de licitaciones
LIC_COLS = [
    "CodigoExterno", "Nombre", "CodigoEstado", "Estado",
    "NombreOrganismo", "sector", "RegionUnidad",
    "Tipo", "Moneda Adquisicion", "MontoEstimado",
    "FechaPublicacion", "FechaAdjudicacion",
    "NumeroOferentes", "Rubro1", "Rubro2", "Rubro3",
    "MontoLineaAdjudica", "NombreProveedor",
    "CantidadAdjudicada", "Correlativo"
]


def process_single_lic_file(csv_file):
    """Procesa un solo archivo CSV de licitaciones y retorna agregaciones."""
    try:
        header_df = pd.read_csv(csv_file, sep=';', encoding='latin-1', nrows=0)
        available_cols = [c for c in LIC_COLS if c in header_df.columns]

        df = pd.read_csv(
            csv_file, sep=';', encoding='latin-1',
            usecols=available_cols, dtype=str,
            on_bad_lines='skip', low_memory=False
        )

        n_rows = len(df)
        logger.info(f"    -> {n_rows:,} filas")

        # Convertir tipos
        for col in ["MontoEstimado", "MontoLineaAdjudica", "CantidadAdjudicada"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if "NumeroOferentes" in df.columns:
            df["NumeroOferentes"] = pd.to_numeric(df["NumeroOferentes"], errors='coerce')

        for col in ["FechaPublicacion", "FechaAdjudicacion"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if "FechaPublicacion" in df.columns:
            df["anio"] = df["FechaPublicacion"].dt.year
            df["mes"] = df["FechaPublicacion"].dt.month
        else:
            parts = csv_file.stem.replace("lic_", "").split("-")
            df["anio"] = int(parts[0])
            df["mes"] = int(parts[1])

        df = df[df["anio"].between(2019, 2025)]

        if len(df) == 0:
            return None, None, None, None, None, None

        # --- Licitaciones únicas (nivel licitación) ---
        lic_unique = df.groupby("CodigoExterno").agg({
            "Nombre": "first",
            "Estado": "first",
            "CodigoEstado": "first",
            "NombreOrganismo": "first",
            "sector": "first",
            "RegionUnidad": "first",
            "Tipo": "first",
            "MontoEstimado": "first",
            "FechaPublicacion": "first",
            "FechaAdjudicacion": "first",
            "NumeroOferentes": "first",
            "Rubro1": "first",
            "MontoLineaAdjudica": "sum",
            "anio": "first",
            "mes": "first"
        }).reset_index()
        lic_unique.rename(columns={"MontoLineaAdjudica": "MontoTotalAdjudicado"}, inplace=True)

        # --- Agregación mensual por categoría ---
        agg_cat = df.groupby(["anio", "mes", "Rubro1"]).agg(
            n_licitaciones=("CodigoExterno", "nunique"),
            monto_adjudicado=("MontoLineaAdjudica", "sum"),
            oferentes_promedio=("NumeroOferentes", "mean"),
            n_proveedores=("NombreProveedor", "nunique"),
            n_organismos=("NombreOrganismo", "nunique")
        ).reset_index()

        # --- Agregación por organismo ---
        agg_org = df.groupby(["anio", "NombreOrganismo", "sector"]).agg(
            n_licitaciones=("CodigoExterno", "nunique"),
            monto_total=("MontoLineaAdjudica", "sum"),
            oferentes_promedio=("NumeroOferentes", "mean")
        ).reset_index()

        # --- Agregación por región ---
        agg_reg = df.groupby(["anio", "RegionUnidad"]).agg(
            n_licitaciones=("CodigoExterno", "nunique"),
            monto_total=("MontoLineaAdjudica", "sum"),
            n_organismos=("NombreOrganismo", "nunique")
        ).reset_index()

        # --- Agregación por proveedor (top proveedores) ---
        agg_prov = df.groupby(["anio", "NombreProveedor"]).agg(
            n_adjudicaciones=("CodigoExterno", "nunique"),
            monto_total=("MontoLineaAdjudica", "sum"),
            n_rubros=("Rubro1", "nunique")
        ).reset_index()

        # --- Competencia por licitación ---
        comp = df.groupby(["CodigoExterno", "anio", "Rubro1"]).agg(
            n_oferentes=("NumeroOferentes", "first"),
            n_ofertas_reales=("NombreProveedor", "nunique")
        ).reset_index()

        del df
        gc.collect()

        return lic_unique, agg_cat, agg_org, agg_reg, agg_prov, comp

    except Exception as e:
        logger.error(f"  Error: {e}")
        return None, None, None, None, None, None


def main():
    logger.info("=" * 60)
    logger.info("PROCESAMIENTO EN STREAMING DE DATOS CHILECOMPRA")
    logger.info("=" * 60)

    lic_dir = RAW_DIR / "licitaciones"
    csv_files = sorted(lic_dir.glob("lic_*.csv"))
    logger.info(f"Total archivos de licitaciones: {len(csv_files)}")

    all_lic = []
    all_cat = []
    all_org = []
    all_reg = []
    all_prov = []
    all_comp = []

    for csv_file in csv_files:
        logger.info(f"  Procesando: {csv_file.name}")
        lic, cat, org, reg, prov, comp = process_single_lic_file(csv_file)

        if lic is not None:
            all_lic.append(lic)
        if cat is not None:
            all_cat.append(cat)
        if org is not None:
            all_org.append(org)
        if reg is not None:
            all_reg.append(reg)
        if prov is not None:
            all_prov.append(prov)
        if comp is not None:
            all_comp.append(comp)

        gc.collect()

    # --- Combinar y guardar licitaciones únicas ---
    logger.info("\nCombinando licitaciones únicas...")
    df_lic = pd.concat(all_lic, ignore_index=True)
    # Deduplicar por CodigoExterno (puede aparecer en múltiples meses)
    df_lic = df_lic.drop_duplicates(subset="CodigoExterno", keep="last")
    logger.info(f"  Licitaciones únicas: {len(df_lic):,}")
    df_lic.to_parquet(PROCESSED_DIR / "licitaciones_unicas.parquet", index=False)
    logger.info(f"  Guardado: licitaciones_unicas.parquet")
    del df_lic, all_lic
    gc.collect()

    # --- Combinar y guardar agregación por categoría ---
    logger.info("Combinando agregación mensual por categoría...")
    df_cat = pd.concat(all_cat, ignore_index=True)
    # Re-agregar (mismo año-mes-rubro puede venir de archivos distintos)
    df_cat = df_cat.groupby(["anio", "mes", "Rubro1"]).agg({
        "n_licitaciones": "sum",
        "monto_adjudicado": "sum",
        "oferentes_promedio": "mean",
        "n_proveedores": "sum",
        "n_organismos": "sum"
    }).reset_index()
    df_cat.to_parquet(PROCESSED_DIR / "agregado_mensual_categoria.parquet", index=False)
    logger.info(f"  Guardado: agregado_mensual_categoria.parquet ({len(df_cat):,} filas)")
    del df_cat, all_cat
    gc.collect()

    # --- Combinar y guardar agregación por organismo ---
    logger.info("Combinando agregación por organismo...")
    df_org = pd.concat(all_org, ignore_index=True)
    df_org = df_org.groupby(["anio", "NombreOrganismo", "sector"]).agg({
        "n_licitaciones": "sum",
        "monto_total": "sum",
        "oferentes_promedio": "mean"
    }).reset_index()
    df_org.to_parquet(PROCESSED_DIR / "agregado_anual_organismo.parquet", index=False)
    logger.info(f"  Guardado: agregado_anual_organismo.parquet ({len(df_org):,} filas)")
    del df_org, all_org
    gc.collect()

    # --- Combinar y guardar agregación por región ---
    logger.info("Combinando agregación por región...")
    df_reg = pd.concat(all_reg, ignore_index=True)
    df_reg = df_reg.groupby(["anio", "RegionUnidad"]).agg({
        "n_licitaciones": "sum",
        "monto_total": "sum",
        "n_organismos": "sum"
    }).reset_index()
    df_reg.to_parquet(PROCESSED_DIR / "agregado_anual_region.parquet", index=False)
    logger.info(f"  Guardado: agregado_anual_region.parquet ({len(df_reg):,} filas)")
    del df_reg, all_reg
    gc.collect()

    # --- Combinar y guardar agregación por proveedor ---
    logger.info("Combinando agregación por proveedor...")
    df_prov = pd.concat(all_prov, ignore_index=True)
    df_prov = df_prov.groupby(["anio", "NombreProveedor"]).agg({
        "n_adjudicaciones": "sum",
        "monto_total": "sum",
        "n_rubros": "max"
    }).reset_index()
    # Guardar solo top 500 proveedores por año
    df_prov_top = df_prov.groupby("anio").apply(
        lambda x: x.nlargest(500, "monto_total"), include_groups=False
    ).reset_index(drop=True)
    df_prov_top.to_parquet(PROCESSED_DIR / "agregado_anual_proveedor_top500.parquet", index=False)
    logger.info(f"  Guardado: agregado_anual_proveedor_top500.parquet ({len(df_prov_top):,} filas)")
    del df_prov, df_prov_top, all_prov
    gc.collect()

    # --- Combinar y guardar datos de competencia ---
    logger.info("Combinando datos de competencia...")
    df_comp = pd.concat(all_comp, ignore_index=True)
    df_comp = df_comp.drop_duplicates(subset="CodigoExterno", keep="last")
    # Agregar competencia por rubro y año
    comp_rubro = df_comp.groupby(["anio", "Rubro1"]).agg(
        oferentes_promedio=("n_oferentes", "mean"),
        oferentes_mediana=("n_oferentes", "median"),
        n_licitaciones=("CodigoExterno", "nunique")
    ).reset_index()
    comp_rubro.to_parquet(PROCESSED_DIR / "competencia_por_rubro.parquet", index=False)
    logger.info(f"  Guardado: competencia_por_rubro.parquet ({len(comp_rubro):,} filas)")
    del df_comp, comp_rubro, all_comp
    gc.collect()

    # --- Resumen ---
    logger.info("\n" + "=" * 60)
    logger.info("ARCHIVOS PROCESADOS:")
    for f in sorted(PROCESSED_DIR.iterdir()):
        size_mb = f.stat().st_size / (1024 * 1024)
        logger.info(f"  {f.name}: {size_mb:.1f} MB")
    logger.info("=" * 60)
    logger.info("PROCESAMIENTO COMPLETADO")


if __name__ == "__main__":
    main()
