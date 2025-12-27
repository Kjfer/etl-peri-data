import os
import pandas as pd
from datetime import date
from dotenv import load_dotenv

from extract import extract_sheet
from transform import (
    transform_pagos_peri,
    transform_pagos_proy_peri
)
from load import load
from logger import get_logger

logger = get_logger("PIPELINE")

load_dotenv()

def run_pipeline(year=None, month=None):
    # =========================
    # DEFINICIÓN DE PERIODO
    # =========================
    today = date.today()
    if today.month == 1:
        target_year = today.year - 1
        target_month = 12
    else:
        target_year = today.year
        target_month = today.month - 1

    logger.info(
        f"===== ETL MENSUAL | Periodo: {target_year}-{target_month:02d} ====="
    )


    # =========================
    # HOJA 1 - PAGOS PERI
    # =========================
    logger.info("Procesando hoja de PAGOS PERI")

    df_salary_peri_raw = extract_sheet(
        os.getenv("ASISTENCIA_SHEET_ID"),
        os.getenv("WORKSHEET_NAME_1"),
        target_year,
        target_month
    )

    if df_salary_peri_raw.empty:
        logger.warning("No hay egresos este mes")
        df_final_pagos = pd.DataFrame()
    else:
        df_final_pagos = transform_pagos_peri(df_salary_peri_raw)
        # =========================
    # HOJA 2 – PAGOSPROY
    # =========================
 
    logger.info("Procesando hoja de PAGOSPROY")

    df_salary_proy_raw = extract_sheet(
        os.getenv("ASISTENCIA_SHEET_ID"),
        os.getenv("WORKSHEET_NAME_2"),
        target_year,
        target_month
    )

    if df_salary_proy_raw.empty:
        logger.warning("No hay egresos este mes")
        df_final_pagos_proy = pd.DataFrame()
    else:
        df_final_pagos_proy = transform_pagos_proy_peri(df_salary_proy_raw)
    # =========================
    # CONSOLIDACIÓN FINAL
    # =========================
    df_final = pd.concat(
        [df_final_pagos, df_final_pagos_proy],
        ignore_index=True
    )

    logger.info(f"Total registros consolidados: {len(df_final)}")

    if df_final.empty:
        logger.warning("No hay datos para cargar este mes")
        return
    
    load(df_final)

    logger.info("===== ETL MENSUAL FINALIZADO CORRECTAMENTE =====")

if __name__ == "__main__":
    run_pipeline()