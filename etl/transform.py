import pandas as pd
from logger import get_logger

logger = get_logger("TRANSFORM")
def parse_decimal(value):
    if pd.isna(value):
        return 0.0

    # =========================
    # CASO 1: STRING (ideal)
    # =========================
    if isinstance(value, str):
        value = value.strip()

        # Formato latino: 1.234,56
        if "," in value:
            value = value.replace(".", "").replace(",", ".")
            try:
                return float(value)
            except ValueError:
                return 0.0

        # String numérico simple: "1120"
        try:
            return float(value)
        except ValueError:
            return 0.0

    # =========================
    # CASO 2: NUMÉRICO (Sheets)
    # =========================
    if isinstance(value, (int, float)):
        value = float(value)

        # 🔥 Heurística CLAVE:
        # Si es muy grande para ser un sueldo, asumir 2 decimales implícitos
        if value >= 10000:
            return round(value / 100, 2)

        # Caso normal: 1120 → 1120.00
        return round(value, 2)

    return 0.0


def transform_pagos_peri(df):
    

    # =========================
    # TRANSFORMACIÓN
    # =========================
    df_transformed = pd.DataFrame({
        "date": df["fecha"].dt.strftime("%Y-%m-%d"),
        "type": "expense",
        "business_id": "negocio1",
        "category_id": 13,
        "amount": df["monto_total"].apply(parse_decimal),
        "description": "Sueldos del personal de PERI",
        "from_account": "Yape",
        "to_account": None,
        "is_invoiced": False,
        "id_referenced": df["id_pago"].astype(str)
    })

    logger.info(
        f"Registros transformados correctamente: {len(df_transformed)}"
    )

    # =========================
    # SAMPLE DE DATOS
    # =========================
    logger.info("Sample de registros transformados:")
    logger.info(
        "\n" + df_transformed.head(5).to_string(index=False)
    )

    return df_transformed

def transform_pagos_proy_peri(df):

    # =========================
    # TRANSFORMACIÓN
    # =========================
    df_transformed = pd.DataFrame({
        "date": df["fecha"].dt.strftime("%Y-%m-%d"),
        "type": "expense",
        "business_id": "negocio1",
        "category_id": 13,
        "amount": df["monto_total"].apply(parse_decimal),
        "description": "Sueldos del equipo de proyectos de PERI",
        "from_account": "Yape",
        "to_account": None,
        "is_invoiced": False,
        "id_referenced": df["id_pago"].astype(str)
    })

    logger.info(
        f"Registros transformados correctamente: {len(df_transformed)}"
    )

    # =========================
    # SAMPLE DE DATOS
    # =========================
    logger.info("Sample de registros transformados:")
    logger.info(
        "\n" + df_transformed.head(5).to_string(index=False)
    )

    return df_transformed