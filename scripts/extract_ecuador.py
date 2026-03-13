"""
Extractor Ecuador: archivos XLSX/CSV de ARCOTEL.
"""
from __future__ import annotations

from pathlib import Path
import re
import sys

import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))
import config
from scripts.extract_utils import normalize_colname, month_to_quarter


SOURCE_TAG = "arcotel_xlsx"


def _find_column(columns_norm: dict[str, str], candidates: list[str]) -> str | None:
    for col_norm, original in columns_norm.items():
        for candidate in candidates:
            if candidate in col_norm:
                return original
    return None


def _read_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Formato no soportado para Ecuador: {path}")


def _infer_year_quarter(df: pd.DataFrame, source_name: str) -> tuple[pd.Series, pd.Series]:
    cols_norm = {normalize_colname(c): c for c in df.columns}
    year_col = _find_column(cols_norm, ["ano", "anio", "year"])
    quarter_col = _find_column(cols_norm, ["trimestre", "quarter"])
    month_col = _find_column(cols_norm, ["mes", "month"])

    if year_col and quarter_col:
        anno = pd.to_numeric(df[year_col], errors="coerce")
        trimestre = pd.to_numeric(df[quarter_col], errors="coerce")
        return anno, trimestre
    if year_col and month_col:
        anno = pd.to_numeric(df[year_col], errors="coerce")
        trimestre = pd.to_numeric(df[month_col], errors="coerce").apply(
            lambda m: month_to_quarter(m) if pd.notna(m) else pd.NA
        )
        return anno, trimestre

    # Fallback por nombre de archivo: ..._sep_2025.xlsx
    year_match = re.search(r"(20\d{2})", source_name)
    month_map = {
        "ene": 1,
        "feb": 2,
        "mar": 3,
        "abr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dic": 12,
    }
    month_value = None
    source_lower = source_name.lower()
    for month_txt, month_num in month_map.items():
        if month_txt in source_lower:
            month_value = month_num
            break

    if year_match and month_value:
        anno = pd.Series([int(year_match.group(1))] * len(df))
        trimestre = pd.Series([month_to_quarter(month_value)] * len(df))
        return anno, trimestre

    raise ValueError(
        "No se pudo inferir anno/trimestre en Ecuador. Incluye columnas de periodo o usa nombre de archivo con mes y anno."
    )


def normalize_to_canonical(df_raw: pd.DataFrame, source_name: str) -> pd.DataFrame:
    cols_norm = {normalize_colname(c): c for c in df_raw.columns}

    operador_col = _find_column(cols_norm, ["empresa", "operador", "prestador", "proveedor"])
    accesos_col = _find_column(cols_norm, ["acceso", "abonado", "conexion", "cuenta", "suscriptor"])
    id_col = _find_column(cols_norm, ["id", "ruc", "identificacion"])

    if not operador_col or not accesos_col:
        raise ValueError(
            f"No fue posible mapear columnas minimas en Ecuador (operador/accesos). Columnas: {list(df_raw.columns)}"
        )

    anno, trimestre = _infer_year_quarter(df_raw, source_name=source_name)

    df = df_raw.copy()
    df["__operador"] = df[operador_col].astype(str).str.strip()
    df["__id_operador"] = (
        df[id_col].astype(str).str.strip() if id_col else df["__operador"].str.upper().str.replace(r"\s+", "_", regex=True)
    )
    df["__anno"] = anno
    df["__trimestre"] = trimestre
    df["__num_accesos"] = pd.to_numeric(df[accesos_col], errors="coerce").fillna(0)

    grouped = (
        df.groupby(["__id_operador", "__operador", "__anno", "__trimestre"], as_index=False)["__num_accesos"]
        .sum()
        .rename(
            columns={
                "__id_operador": "id_operador",
                "__operador": "operador",
                "__anno": "anno",
                "__trimestre": "trimestre",
                "__num_accesos": "num_accesos",
            }
        )
    )
    grouped["pais"] = "ECU"
    grouped["fuente"] = SOURCE_TAG
    canonical = grouped[config.CANONICAL_COLUMNS].copy()
    canonical["anno"] = pd.to_numeric(canonical["anno"], errors="coerce").astype("Int64")
    canonical["trimestre"] = pd.to_numeric(canonical["trimestre"], errors="coerce").astype("Int64")
    canonical["num_accesos"] = pd.to_numeric(canonical["num_accesos"], errors="coerce").fillna(0)
    return canonical


def run(source_files: list[str] | None = None, save: bool = True) -> pd.DataFrame:
    source_files = source_files or config.ECUADOR_SOURCE_FILES
    if not source_files:
        raise ValueError("No hay archivos fuente para Ecuador. Pasa source_files o configura ECUADOR_SOURCE_FILES.")

    canonical_parts = []
    raw_parts = []
    for file_path in source_files:
        path = Path(file_path).expanduser()
        print(f"Leyendo Ecuador: {path}")
        raw_df = _read_file(path)
        canonical_df = normalize_to_canonical(raw_df, source_name=path.name)
        canonical_parts.append(canonical_df)
        raw_parts.append(raw_df)

    canonical = pd.concat(canonical_parts, ignore_index=True)
    raw_joined = pd.concat(raw_parts, ignore_index=True)

    if save:
        raw_path = config.RAW_DATA_DIR / config.RAW_ECU_FILENAME
        canonical_path = config.RAW_DATA_DIR / "canonical_ecuador.csv"
        raw_joined.to_csv(raw_path, index=False)
        canonical.to_csv(canonical_path, index=False)
        print(f"Raw Ecuador guardado: {raw_path}")
        print(f"Canonico Ecuador guardado: {canonical_path}")

    return canonical


if __name__ == "__main__":
    run()

