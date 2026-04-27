#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
clean_data.py — Limpieza y normalización de datos procesados de ChileCompra.

Lee los archivos parquet generados por process_streaming.py,
aplica limpieza adicional y genera datasets listos para análisis.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def clean_licitaciones():
    """Limpia y normaliza el dataset de licitaciones únicas."""
    logger.info("Limpiando licitaciones únicas...")
    df = pd.read_parquet(PROCESSED_DIR / "licitaciones_unicas.parquet")
    logger.info(f"  Filas iniciales: {len(df):,}")

    # Eliminar filas sin código
    df = df.dropna(subset=["CodigoExterno"])

    # Normalizar Rubro1
    df["Rubro1"] = df["Rubro1"].fillna("SIN CLASIFICAR")
    df["Rubro1"] = df["Rubro1"].str.strip().str.upper()

    # Normalizar sector
    df["sector"] = df["sector"].fillna("Sin Sector")
    df["sector"] = df["sector"].str.strip()

    # Normalizar región
    df["RegionUnidad"] = df["RegionUnidad"].fillna("Sin Región")
    df["RegionUnidad"] = df["RegionUnidad"].str.strip()
    # Estandarizar nombres de regiones
    region_map = {
        "Región de Arica y Parinacota": "Arica y Parinacota",
        "Región de Arica y Parinacota ": "Arica y Parinacota",
        "Región de Tarapacá": "Tarapacá",
        "Región de Tarapacá ": "Tarapacá",
        "Región de Antofagasta": "Antofagasta",
        "Región de Antofagasta ": "Antofagasta",
        "Región de Atacama": "Atacama",
        "Región de Atacama ": "Atacama",
        "Región de Coquimbo": "Coquimbo",
        "Región de Coquimbo ": "Coquimbo",
        "Región de Valparaíso": "Valparaíso",
        "Región de Valparaíso ": "Valparaíso",
        "Región Metropolitana de Santiago": "Metropolitana",
        "Región Metropolitana de Santiago ": "Metropolitana",
        "Región del Libertador General Bernardo O'Higgins": "O'Higgins",
        "Región del Libertador General Bernardo O'Higgins ": "O'Higgins",
        "Región del Maule": "Maule",
        "Región del Maule ": "Maule",
        "Región de Ñuble": "Ñuble",
        "Región de Ñuble ": "Ñuble",
        "Región del Biobío": "Biobío",
        "Región del Biobío ": "Biobío",
        "Región de La Araucanía": "La Araucanía",
        "Región de La Araucanía ": "La Araucanía",
        "Región de Los Ríos": "Los Ríos",
        "Región de Los Ríos ": "Los Ríos",
        "Región de Los Lagos": "Los Lagos",
        "Región de Los Lagos ": "Los Lagos",
        "Región de Aysén del General Carlos Ibáñez del Campo": "Aysén",
        "Región de Aysén del General Carlos Ibáñez del Campo ": "Aysén",
        "Región de Magallanes y de la Antártica Chilena": "Magallanes",
        "Región de Magallanes y de la Antártica Chilena ": "Magallanes",
    }
    df["RegionUnidad"] = df["RegionUnidad"].replace(region_map)

    # Normalizar Estado
    df["Estado"] = df["Estado"].fillna("Desconocido")
    df["Estado"] = df["Estado"].str.strip()

    # Normalizar NombreOrganismo
    df["NombreOrganismo"] = df["NombreOrganismo"].fillna("Sin Organismo")
    df["NombreOrganismo"] = df["NombreOrganismo"].str.strip().str.upper()

    # Limpiar montos: eliminar valores negativos o extremos
    if "MontoEstimado" in df.columns:
        df.loc[df["MontoEstimado"] < 0, "MontoEstimado"] = np.nan
    if "MontoTotalAdjudicado" in df.columns:
        df.loc[df["MontoTotalAdjudicado"] < 0, "MontoTotalAdjudicado"] = np.nan

    # Limpiar oferentes
    if "NumeroOferentes" in df.columns:
        df.loc[df["NumeroOferentes"] < 0, "NumeroOferentes"] = np.nan
        df.loc[df["NumeroOferentes"] > 500, "NumeroOferentes"] = np.nan

    # Filtrar años válidos
    df = df[df["anio"].between(2019, 2025)]

    logger.info(f"  Filas finales: {len(df):,}")

    # Guardar
    df.to_parquet(PROCESSED_DIR / "licitaciones_clean.parquet", index=False)
    logger.info("  Guardado: licitaciones_clean.parquet")

    return df


def clean_agregados():
    """Limpia los datasets agregados."""
    # Agregado mensual por categoría
    logger.info("Limpiando agregado mensual por categoría...")
    df_cat = pd.read_parquet(PROCESSED_DIR / "agregado_mensual_categoria.parquet")
    df_cat["Rubro1"] = df_cat["Rubro1"].fillna("SIN CLASIFICAR").str.strip().str.upper()
    df_cat = df_cat[df_cat["anio"].between(2019, 2025)]
    df_cat = df_cat[df_cat["Rubro1"] != "SIN CLASIFICAR"]
    df_cat.to_parquet(PROCESSED_DIR / "agregado_mensual_categoria_clean.parquet", index=False)
    logger.info(f"  Guardado: {len(df_cat):,} filas")

    # Agregado por organismo
    logger.info("Limpiando agregado por organismo...")
    df_org = pd.read_parquet(PROCESSED_DIR / "agregado_anual_organismo.parquet")
    df_org["NombreOrganismo"] = df_org["NombreOrganismo"].str.strip().str.upper()
    df_org["sector"] = df_org["sector"].fillna("Sin Sector").str.strip()
    df_org = df_org[df_org["anio"].between(2019, 2025)]
    df_org.to_parquet(PROCESSED_DIR / "agregado_anual_organismo_clean.parquet", index=False)
    logger.info(f"  Guardado: {len(df_org):,} filas")

    # Agregado por región
    logger.info("Limpiando agregado por región...")
    df_reg = pd.read_parquet(PROCESSED_DIR / "agregado_anual_region.parquet")
    region_map = {
        "Región de Arica y Parinacota": "Arica y Parinacota",
        "Región de Arica y Parinacota ": "Arica y Parinacota",
        "Región de Tarapacá": "Tarapacá",
        "Región de Tarapacá ": "Tarapacá",
        "Región de Antofagasta": "Antofagasta",
        "Región de Antofagasta ": "Antofagasta",
        "Región de Atacama": "Atacama",
        "Región de Atacama ": "Atacama",
        "Región de Coquimbo": "Coquimbo",
        "Región de Coquimbo ": "Coquimbo",
        "Región de Valparaíso": "Valparaíso",
        "Región de Valparaíso ": "Valparaíso",
        "Región Metropolitana de Santiago": "Metropolitana",
        "Región Metropolitana de Santiago ": "Metropolitana",
        "Región del Libertador General Bernardo O'Higgins": "O'Higgins",
        "Región del Libertador General Bernardo O'Higgins ": "O'Higgins",
        "Región del Maule": "Maule",
        "Región del Maule ": "Maule",
        "Región de Ñuble": "Ñuble",
        "Región de Ñuble ": "Ñuble",
        "Región del Biobío": "Biobío",
        "Región del Biobío ": "Biobío",
        "Región de La Araucanía": "La Araucanía",
        "Región de La Araucanía ": "La Araucanía",
        "Región de Los Ríos": "Los Ríos",
        "Región de Los Ríos ": "Los Ríos",
        "Región de Los Lagos": "Los Lagos",
        "Región de Los Lagos ": "Los Lagos",
        "Región de Aysén del General Carlos Ibáñez del Campo": "Aysén",
        "Región de Aysén del General Carlos Ibáñez del Campo ": "Aysén",
        "Región de Magallanes y de la Antártica Chilena": "Magallanes",
        "Región de Magallanes y de la Antártica Chilena ": "Magallanes",
    }
    df_reg["RegionUnidad"] = df_reg["RegionUnidad"].replace(region_map)
    df_reg = df_reg[df_reg["anio"].between(2019, 2025)]
    # Re-agregar después de normalizar nombres
    df_reg = df_reg.groupby(["anio", "RegionUnidad"]).agg({
        "n_licitaciones": "sum",
        "monto_total": "sum",
        "n_organismos": "sum"
    }).reset_index()
    df_reg.to_parquet(PROCESSED_DIR / "agregado_anual_region_clean.parquet", index=False)
    logger.info(f"  Guardado: {len(df_reg):,} filas")


def main():
    logger.info("=" * 60)
    logger.info("LIMPIEZA DE DATOS")
    logger.info("=" * 60)

    clean_licitaciones()
    clean_agregados()

    logger.info("\nLIMPIEZA COMPLETADA")


if __name__ == "__main__":
    main()
