"""
Extractor Colombia: API DKAN postdata.gov.co.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd
import requests

sys.path.append(str(Path(__file__).parent.parent))
import config


SOURCE_TAG = "postdata_dkan"


def build_params(limit: int = 100, offset: int = 0) -> dict:
    return {
        "resource_id": config.RESOURCE_ID,
        "limit": limit,
        "offset": offset,
    }


def extract_data_from_api(limit: int = 100, max_pages: int | None = None) -> pd.DataFrame:
    """Extrae datos crudos de Colombia con paginacion."""
    if not config.RESOURCE_ID:
        raise ValueError("RESOURCE_ID no esta configurado en config.py")

    url = f"{config.API_BASE_URL}/search.json"
    all_data = []
    offset = 0
    pages = 0

    print("Extrayendo Colombia desde API...")
    while True:
        params = build_params(limit=limit, offset=offset)
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise ValueError(f"Respuesta JSON invalida en Colombia: {exc}") from exc

        if not payload.get("success", False):
            raise RuntimeError(f"Error de API Colombia: {payload.get('error', 'Unknown error')}")

        records = payload.get("result", {}).get("records", [])
        if not records:
            break

        all_data.extend(records)
        pages += 1
        print(f"  - pagina {pages}: +{len(records)} (total={len(all_data)})")

        total_records = payload.get("result", {}).get("total", len(all_data))
        if len(all_data) >= total_records:
            break
        if len(records) < limit:
            break
        if max_pages is not None and pages >= max_pages:
            break
        offset += limit

    return pd.DataFrame(all_data)


def to_canonical(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Convierte raw Colombia a esquema canonico trimestral por operador."""
    required = {"id_empresa", "empresa", "anno", "trimestre", "accesos"}
    missing = required - set(df_raw.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas Colombia: {sorted(missing)}")

    df = df_raw.copy()
    df["id_empresa"] = df["id_empresa"].astype(str).str.strip()
    df["empresa"] = df["empresa"].astype(str).str.strip()
    df["anno"] = pd.to_numeric(df["anno"], errors="coerce")
    df["trimestre"] = pd.to_numeric(df["trimestre"], errors="coerce")
    df["accesos"] = pd.to_numeric(df["accesos"], errors="coerce").fillna(0)

    grouped = (
        df.groupby(["id_empresa", "empresa", "anno", "trimestre"], as_index=False)["accesos"]
        .sum()
        .rename(
            columns={
                "id_empresa": "id_operador",
                "empresa": "operador",
                "accesos": "num_accesos",
            }
        )
    )
    grouped["pais"] = "COL"
    grouped["fuente"] = SOURCE_TAG

    canonical = grouped[config.CANONICAL_COLUMNS].copy()
    canonical["anno"] = canonical["anno"].astype("Int64")
    canonical["trimestre"] = canonical["trimestre"].astype("Int64")
    canonical["num_accesos"] = canonical["num_accesos"].astype(float)
    return canonical


def save_outputs(df_raw: pd.DataFrame, df_canonical: pd.DataFrame) -> None:
    raw_path = config.RAW_DATA_DIR / config.RAW_COL_FILENAME
    canonical_path = config.RAW_DATA_DIR / f"canonical_colombia_{config.RAW_COL_FILENAME}"
    df_raw.to_csv(raw_path, index=False, sep=";")
    df_canonical.to_csv(canonical_path, index=False)
    print(f"Raw Colombia guardado: {raw_path}")
    print(f"Canonico Colombia guardado: {canonical_path}")


def run(limit: int = 100, max_pages: int | None = None, save: bool = True) -> pd.DataFrame:
    df_raw = extract_data_from_api(limit=limit, max_pages=max_pages)
    if df_raw.empty:
        print("No se obtuvieron datos Colombia")
        return pd.DataFrame(columns=config.CANONICAL_COLUMNS)
    df_canonical = to_canonical(df_raw)
    if save:
        save_outputs(df_raw, df_canonical)
    return df_canonical


if __name__ == "__main__":
    run()

