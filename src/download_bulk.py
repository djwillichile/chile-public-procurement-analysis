#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download_bulk.py — Descarga masiva robusta de datos de ChileCompra.

Descarga archivos ZIP mensuales de licitaciones y órdenes de compra,
los extrae y los deja como CSV listos para procesar.
"""

import os
import sys
import time
import zipfile
import subprocess
import requests
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

LIC_URL = "https://transparenciachc.blob.core.windows.net/lic-da/{year}-{month}.zip"
OC_URL  = "https://transparenciachc.blob.core.windows.net/oc-da/{year}-{month}.zip"

START_YEAR = 2019
END_YEAR = 2025

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def download_and_extract(url, dest_dir, csv_name):
    """Descarga un ZIP y extrae el CSV."""
    csv_path = dest_dir / csv_name
    if csv_path.exists() and csv_path.stat().st_size > 1000:
        logger.info(f"  Ya existe: {csv_name}")
        return True

    zip_path = dest_dir / (csv_name.replace(".csv", ".zip"))
    
    # Descargar
    try:
        logger.info(f"  Descargando: {url}")
        resp = requests.get(url, timeout=300, stream=True)
        if resp.status_code == 404:
            logger.warning(f"  No encontrado (404)")
            return False
        resp.raise_for_status()
        
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
        
        size_mb = zip_path.stat().st_size / (1024*1024)
        logger.info(f"  Descargado: {size_mb:.1f} MB")
    except Exception as e:
        logger.error(f"  Error descarga: {e}")
        if zip_path.exists():
            zip_path.unlink()
        return False

    # Extraer
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            zf.extractall(dest_dir)
            # Renombrar al nombre estándar
            for name in names:
                src = dest_dir / name
                if src.exists() and src.suffix == ".csv":
                    if src.name != csv_name:
                        src.rename(csv_path)
                    break
        logger.info(f"  Extraído OK")
    except zipfile.BadZipFile:
        # Intentar con 7z
        try:
            result = subprocess.run(
                ["7z", "x", str(zip_path), f"-o{dest_dir}", "-y"],
                capture_output=True, text=True, timeout=120
            )
            # Buscar CSV extraído
            for f in dest_dir.iterdir():
                if f.suffix == ".csv" and f.name != csv_name and "lic_" in f.name or "oc_" in f.name:
                    f.rename(csv_path)
                    break
            logger.info(f"  Extraído con 7z")
        except Exception as e:
            logger.error(f"  Error extracción: {e}")
            return False
    finally:
        if zip_path.exists():
            zip_path.unlink()

    return csv_path.exists()


def main():
    for dtype, url_tpl, prefix in [
        ("licitaciones", LIC_URL, "lic"),
        ("ordenes", OC_URL, "oc")
    ]:
        dest_dir = RAW_DIR / dtype
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"\n{'='*50}")
        logger.info(f"DESCARGANDO: {dtype.upper()}")
        logger.info(f"{'='*50}")
        
        count = 0
        for year in range(START_YEAR, END_YEAR + 1):
            max_month = 12 if year < 2025 else 2  # hasta feb 2025 (datos disponibles)
            if year == 2026:
                max_month = 2
            
            for month in range(1, max_month + 1):
                csv_name = f"{prefix}_{year}-{month}.csv"
                url = url_tpl.format(year=year, month=month)
                
                if download_and_extract(url, dest_dir, csv_name):
                    count += 1
                
                time.sleep(0.3)
        
        logger.info(f"Total {dtype}: {count} archivos")

    logger.info("\nDESCARGA COMPLETADA")


if __name__ == "__main__":
    main()
