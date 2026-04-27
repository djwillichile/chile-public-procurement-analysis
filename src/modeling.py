#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modeling.py — Modelamiento predictivo de demanda de compras públicas.

Implementa modelos Prophet y XGBoost para predecir:
- Gasto futuro por categoría
- Número de licitaciones futuras

Horizonte de predicción: 2-3 años.
"""

import pandas as pd
import numpy as np
import warnings
import logging
import json
from pathlib import Path
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
FIGURES_DIR = PROJECT_ROOT / "figures"
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def prophet_forecast_total():
    """Modelo Prophet para serie temporal total de compras públicas."""
    from prophet import Prophet

    logger.info("=" * 50)
    logger.info("MODELO PROPHET - Serie total")
    logger.info("=" * 50)

    df = pd.read_parquet(PROCESSED_DIR / "serie_temporal_total.parquet")
    df = df.sort_values("fecha")

    # Preparar datos para Prophet
    # Predecir monto adjudicado
    prophet_df = df[["fecha", "monto_adjudicado"]].rename(
        columns={"fecha": "ds", "monto_adjudicado": "y"}
    )
    prophet_df = prophet_df.dropna()

    # Filtrar datos hasta 2024 para entrenamiento, 2025 para validación
    train = prophet_df[prophet_df["ds"] < "2025-01-01"]
    test = prophet_df[prophet_df["ds"] >= "2025-01-01"]

    logger.info(f"  Train: {len(train)} meses, Test: {len(test)} meses")

    # Entrenar modelo
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        changepoint_prior_scale=0.05,
        seasonality_prior_scale=10
    )
    model.fit(train)

    # Predicción: hasta 2027
    future = model.make_future_dataframe(periods=36, freq="MS")
    forecast = model.predict(future)

    # Evaluar en test
    if len(test) > 0:
        forecast_test = forecast[forecast["ds"].isin(test["ds"])]
        if len(forecast_test) > 0:
            merged = test.merge(forecast_test[["ds", "yhat"]], on="ds")
            mae = mean_absolute_error(merged["y"], merged["yhat"])
            rmse = np.sqrt(mean_squared_error(merged["y"], merged["yhat"]))
            logger.info(f"  MAE: {mae:,.0f}")
            logger.info(f"  RMSE: {rmse:,.0f}")

    # Guardar forecast
    forecast_save = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    forecast_save.to_parquet(PROCESSED_DIR / "forecast_prophet_total.parquet", index=False)
    logger.info("  Guardado: forecast_prophet_total.parquet")

    # Predecir también número de licitaciones
    prophet_lic = df[["fecha", "n_licitaciones"]].rename(
        columns={"fecha": "ds", "n_licitaciones": "y"}
    ).dropna()

    model_lic = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False
    )
    model_lic.fit(prophet_lic[prophet_lic["ds"] < "2025-01-01"])
    future_lic = model_lic.make_future_dataframe(periods=36, freq="MS")
    forecast_lic = model_lic.predict(future_lic)

    forecast_lic_save = forecast_lic[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    forecast_lic_save.to_parquet(PROCESSED_DIR / "forecast_prophet_licitaciones.parquet", index=False)
    logger.info("  Guardado: forecast_prophet_licitaciones.parquet")

    return forecast, forecast_lic


def prophet_forecast_by_category():
    """Modelo Prophet por categoría (top 10)."""
    from prophet import Prophet

    logger.info("\n" + "=" * 50)
    logger.info("MODELO PROPHET - Por categoría (Top 10)")
    logger.info("=" * 50)

    df = pd.read_parquet(PROCESSED_DIR / "series_temporales_top20.parquet")
    df = df.sort_values(["Rubro1", "fecha"])

    # Top 10 categorías por monto
    top_cats = (
        df.groupby("Rubro1")["monto_adjudicado"]
        .sum()
        .nlargest(10)
        .index.tolist()
    )

    results = {}
    all_forecasts = []

    for cat in top_cats:
        logger.info(f"\n  Categoría: {cat[:60]}")
        cat_df = df[df["Rubro1"] == cat][["fecha", "monto_adjudicado"]].rename(
            columns={"fecha": "ds", "monto_adjudicado": "y"}
        ).dropna()

        if len(cat_df) < 12:
            logger.warning(f"    Datos insuficientes ({len(cat_df)} meses)")
            continue

        train = cat_df[cat_df["ds"] < "2025-01-01"]
        test = cat_df[cat_df["ds"] >= "2025-01-01"]

        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            changepoint_prior_scale=0.05
        )
        model.fit(train)

        future = model.make_future_dataframe(periods=36, freq="MS")
        forecast = model.predict(future)

        # Evaluar
        mae, rmse = np.nan, np.nan
        if len(test) > 0:
            merged = test.merge(forecast[["ds", "yhat"]], on="ds")
            if len(merged) > 0:
                mae = mean_absolute_error(merged["y"], merged["yhat"])
                rmse = np.sqrt(mean_squared_error(merged["y"], merged["yhat"]))

        results[cat] = {"mae": mae, "rmse": rmse, "n_train": len(train)}
        logger.info(f"    Train: {len(train)}, MAE: {mae:,.0f}, RMSE: {rmse:,.0f}")

        fc = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
        fc["Rubro1"] = cat
        all_forecasts.append(fc)

    if all_forecasts:
        df_fc = pd.concat(all_forecasts, ignore_index=True)
        df_fc.to_parquet(PROCESSED_DIR / "forecast_prophet_categorias.parquet", index=False)
        logger.info(f"\n  Guardado: forecast_prophet_categorias.parquet")

    return results


def xgboost_forecast():
    """Modelo XGBoost para predicción de gasto total."""
    import xgboost as xgb

    logger.info("\n" + "=" * 50)
    logger.info("MODELO XGBOOST - Serie total")
    logger.info("=" * 50)

    df = pd.read_parquet(PROCESSED_DIR / "serie_temporal_total.parquet")
    df = df.sort_values("fecha")

    # Feature engineering para XGBoost
    df["year"] = df["fecha"].dt.year
    df["month"] = df["fecha"].dt.month
    df["quarter"] = df["fecha"].dt.quarter

    # Lags
    for lag in [1, 2, 3, 6, 12]:
        df[f"monto_lag_{lag}"] = df["monto_adjudicado"].shift(lag)
        df[f"lic_lag_{lag}"] = df["n_licitaciones"].shift(lag)

    # Rolling features
    df["monto_rolling_3"] = df["monto_adjudicado"].rolling(3).mean()
    df["monto_rolling_6"] = df["monto_adjudicado"].rolling(6).mean()
    df["monto_rolling_12"] = df["monto_adjudicado"].rolling(12).mean()

    df = df.dropna()

    features = [c for c in df.columns if c not in [
        "fecha", "monto_adjudicado", "n_licitaciones", "oferentes_promedio"
    ]]

    # Split temporal
    train = df[df["fecha"] < "2024-07-01"]
    test = df[df["fecha"] >= "2024-07-01"]

    logger.info(f"  Train: {len(train)}, Test: {len(test)}")
    logger.info(f"  Features: {features}")

    X_train = train[features]
    y_train = train["monto_adjudicado"]
    X_test = test[features]
    y_test = test["monto_adjudicado"]

    model = xgb.XGBRegressor(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    logger.info(f"  MAE: {mae:,.0f}")
    logger.info(f"  RMSE: {rmse:,.0f}")

    # Feature importance
    importance = pd.DataFrame({
        "feature": features,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)

    importance.to_parquet(PROCESSED_DIR / "xgboost_feature_importance.parquet", index=False)
    logger.info(f"\n  Feature Importance:")
    for _, row in importance.head(10).iterrows():
        logger.info(f"    {row['feature']:30s}: {row['importance']:.4f}")

    # Guardar predicciones
    test_results = test[["fecha", "monto_adjudicado"]].copy()
    test_results["prediccion_xgb"] = y_pred
    test_results.to_parquet(PROCESSED_DIR / "xgboost_predictions.parquet", index=False)

    # Guardar métricas
    metrics = {
        "prophet_total": {},
        "xgboost": {"mae": float(mae), "rmse": float(rmse)}
    }
    with open(PROCESSED_DIR / "model_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    return model, importance


def main():
    logger.info("=" * 60)
    logger.info("MODELAMIENTO PREDICTIVO")
    logger.info("=" * 60)

    # Prophet - Total
    fc_total, fc_lic = prophet_forecast_total()

    # Prophet - Por categoría
    cat_results = prophet_forecast_by_category()

    # XGBoost
    xgb_model, importance = xgboost_forecast()

    logger.info("\n" + "=" * 60)
    logger.info("MODELAMIENTO COMPLETADO")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
