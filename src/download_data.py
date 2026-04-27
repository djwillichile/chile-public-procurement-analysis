#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download_data.py — Descarga masiva de datos de ChileCompra (Mercado Público)

Descarga archivos CSV mensuales de licitaciones y órdenes de compra desde
el portal de datos abiertos de ChileCompra. Adicionalmente, consulta la API
de Mercado Público para obtener información complementaria.

Fuentes:
    - Descarga masiva: https://datos-abiertos.chilecompra.cl/descargas
    - API: https://api.mercadopublico.cl

Autor: Guillermo Fuentes Jaque
Fecha: 2026-03
"""

import os
import sys
import time
import zipfile
import subprocess
import requests
import json
import logging
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

API_TICKET = "56A21800-89E6-437D-B03D-7A4B236DAC9C"
API_BASE = "https://api.mercadopublico.cl/servicios/v1/publico"

# URLs de descarga masiva (Azure Blob Storage de ChileCompra)
LIC_URL_TEMPLATE = "https://transparenciachc.blob.core.windows.net/lic-da/{year}-{month}.zip"
OC_URL_TEMPLATE  = "https://transparenciachc.blob.core.windows.net/oc-da/{year}-{month}.zip"

# Rango de años a descargar
START_YEAR = 2019
END_YEAR = 2025

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Funciones de descarga masiva
# ---------------------------------------------------------------------------
def download_file(url: str, dest_path: Path, retries: int = 3) -> bool:
    """Descarga un archivo desde una URL con reintentos."""
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Descargando {url} (intento {attempt}/{retries})...")
            resp = requests.get(url, stream=True, timeout=120)
            if resp.status_code == 200:
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                size_mb = dest_path.stat().st_size / (1024 * 1024)
                logger.info(f"  -> Guardado: {dest_path.name} ({size_mb:.1f} MB)")
                return True
            elif resp.status_code == 404:
                logger.warning(f"  -> No encontrado (404): {url}")
                return False
            else:
                logger.warning(f"  -> HTTP {resp.status_code}")
        except Exception as e:
            logger.error(f"  -> Error: {e}")
        time.sleep(2)
    return False


def extract_zip(zip_path: Path, dest_dir: Path) -> list:
    """Extrae un archivo ZIP y retorna la lista de archivos extraídos."""
    extracted = []
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(dest_dir)
            extracted = zf.namelist()
            logger.info(f"  -> Extraído: {', '.join(extracted)}")
    except zipfile.BadZipFile:
        # Intentar con 7z si es un .7z disfrazado de .zip
        logger.info(f"  -> No es ZIP estándar, intentando con 7z...")
        try:
            subprocess.run(
                ["7z", "x", str(zip_path), f"-o{dest_dir}", "-y"],
                capture_output=True, text=True, check=True
            )
            extracted = [f.name for f in dest_dir.iterdir() if f.is_file()]
            logger.info(f"  -> Extraído con 7z: {', '.join(extracted)}")
        except Exception as e:
            logger.error(f"  -> Error extrayendo: {e}")
    return extracted


def download_bulk_data(data_type: str = "licitaciones"):
    """
    Descarga masiva de datos mensuales.
    
    Args:
        data_type: 'licitaciones' u 'ordenes'
    """
    if data_type == "licitaciones":
        url_template = LIC_URL_TEMPLATE
        prefix = "lic"
    else:
        url_template = OC_URL_TEMPLATE
        prefix = "oc"

    dest_dir = RAW_DIR / data_type
    dest_dir.mkdir(parents=True, exist_ok=True)

    current_year = datetime.now().year
    current_month = datetime.now().month

    downloaded_files = []

    for year in range(START_YEAR, END_YEAR + 1):
        max_month = 12
        if year == current_year:
            max_month = current_month

        for month in range(1, max_month + 1):
            csv_name = f"{prefix}_{year}-{month}.csv"
            csv_path = dest_dir / csv_name

            # Saltar si ya existe
            if csv_path.exists() and csv_path.stat().st_size > 1000:
                logger.info(f"Ya existe: {csv_name}, saltando...")
                downloaded_files.append(csv_path)
                continue

            url = url_template.format(year=year, month=month)
            zip_name = f"{prefix}_{year}-{month}.zip"
            zip_path = dest_dir / zip_name

            if download_file(url, zip_path):
                extracted = extract_zip(zip_path, dest_dir)
                # Renombrar el CSV extraído
                for fname in extracted:
                    src = dest_dir / fname
                    if src.exists() and src.suffix == ".csv":
                        src.rename(csv_path)
                        downloaded_files.append(csv_path)
                        break
                # Eliminar el ZIP
                if zip_path.exists():
                    zip_path.unlink()

            time.sleep(0.5)  # Pausa entre descargas

    logger.info(f"\nTotal archivos {data_type}: {len(downloaded_files)}")
    return downloaded_files


# ---------------------------------------------------------------------------
# Funciones de API
# ---------------------------------------------------------------------------
def api_get(endpoint: str, params: dict = None) -> dict:
    """Consulta un endpoint de la API de Mercado Público."""
    if params is None:
        params = {}
    params["ticket"] = API_TICKET
    url = f"{API_BASE}/{endpoint}"
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error API {endpoint}: {e}")
        return {}


def download_compradores():
    """Descarga el listado de organismos compradores del Estado."""
    logger.info("Descargando listado de organismos compradores...")
    data = api_get("Empresas/BuscarComprador")
    if data:
        dest = RAW_DIR / "organismos_compradores.json"
        with open(dest, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"  -> Guardado: {dest.name}")
    return data


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    """Ejecuta la descarga completa de datos."""
    logger.info("=" * 60)
    logger.info("INICIO DESCARGA DE DATOS - ChileCompra")
    logger.info(f"Rango: {START_YEAR} - {END_YEAR}")
    logger.info(f"Destino: {RAW_DIR}")
    logger.info("=" * 60)

    # 1. Descargar licitaciones
    logger.info("\n--- LICITACIONES ---")
    lic_files = download_bulk_data("licitaciones")

    # 2. Descargar órdenes de compra
    logger.info("\n--- ÓRDENES DE COMPRA ---")
    oc_files = download_bulk_data("ordenes")

    # 3. Descargar organismos compradores
    download_compradores()

    logger.info("\n" + "=" * 60)
    logger.info("DESCARGA COMPLETADA")
    logger.info(f"  Licitaciones: {len(lic_files)} archivos")
    logger.info(f"  Órdenes de compra: {len(oc_files)} archivos")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
