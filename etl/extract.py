import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import os
from logger import get_logger
import numpy as np

logger = get_logger("EXTRACT")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def get_gspread_client():
    credentials = Credentials.from_service_account_info(
        json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")),
        scopes=SCOPES
    )
    return gspread.authorize(credentials)


def extract_sheet(sheet_id, worksheet_name, year, month):
    logger.info(f"Extrayendo datos | Sheet: {sheet_id}")

    ws = (
        get_gspread_client()
        .open_by_key(sheet_id)
        .worksheet(worksheet_name)
    )

    # 🔥 CLAVE: UNFORMATTED_VALUE
    records = ws.get_all_records(
        value_render_option="UNFORMATTED_VALUE"
    )

    df = pd.DataFrame(records)
    logger.info(f"Registros totales extraídos: {len(df)}")

    # =========================
    # DEBUG TIPOS DE DATO
    # =========================
    if "monto_total" in df.columns:
        logger.info("Tipos de datos en 'monto_total':")
        logger.info(df["monto_total"].apply(type).value_counts())

    # =========================
    # CONVERSIÓN DE FECHA
    # =========================
    def parse_google_date(value):
        if pd.isna(value):
            return pd.NaT

        # 🔥 Caso 1: fecha serial de Google Sheets
        if isinstance(value, (int, float)):
            return pd.to_datetime("1899-12-30") + pd.to_timedelta(int(value), unit="D")

        # Caso 2: string normal
        return pd.to_datetime(value, dayfirst=True, errors="coerce")


    df["fecha"] = df["fecha_pago"].apply(parse_google_date)

    invalid_dates = df["fecha"].isna().sum()
    if invalid_dates > 0:
        logger.warning(
            f"Fechas inválidas detectadas en 'fecha_pago': {invalid_dates}"
        )

    # =========================
    # FILTRO MENSUAL
    # =========================
    total_antes = len(df)
    df = df[
        (df["fecha"].dt.year == year) &
        (df["fecha"].dt.month == month)
    ]

    logger.info(
        f"Filtro mensual {year}-{month:02d} | "
        f"Antes: {total_antes} | Después: {len(df)}"
    )

    # =========================
    # SAMPLE
    # =========================
    if not df.empty:
        logger.info("Sample de registros extraídos:")
        logger.info("\n" + df.head(5).to_string(index=False))
    else:
        logger.warning("No hay registros luego de aplicar los filtros")

    return df

