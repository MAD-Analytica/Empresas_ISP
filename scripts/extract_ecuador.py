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
        # Los reportes ARCOTEL traen encabezado visual y la tabla real
        # suele iniciar varias filas mas abajo en la hoja "D Prestador".
        raw = pd.read_excel(path, sheet_name="D Prestador", header=None)
        raw = raw.dropna(axis=1, how="all")

        header_idx = None
        for idx in raw.index[:60]:
            values = [normalize_colname(v) for v in raw.loc[idx].tolist() if pd.notna(v)]
            has_prestadores = any(value == "prestadores" or "prestadores" in value for value in values)
            has_no = any(value == "no" for value in values)
            has_month_or_total = any(
                bool(re.search(r"20\d{2}", value)) or "cuentas_de_internet" in value for value in values
            )
            if has_prestadores and has_no and has_month_or_total:
                header_idx = idx
                break

        if header_idx is None:
            raise ValueError(
                f"No se encontro encabezado de tabla en hoja 'D Prestador' para {path.name}."
            )

        headers = []
        seen = {}
        for col_idx, value in enumerate(raw.loc[header_idx].tolist()):
            if pd.isna(value):
                header = f"col_{col_idx}"
            else:
                header = str(value).strip()
            count = seen.get(header, 0)
            if count:
                header = f"{header}_{count}"
            seen[str(value).strip() if pd.notna(value) else f'col_{col_idx}'] = count + 1
            headers.append(header)

        df = raw.loc[header_idx + 1 :].copy()
        df.columns = headers
        df = df.dropna(how="all").reset_index(drop=True)

        # Limpiar fila de total si viene al final.
        cols_norm = {normalize_colname(c): c for c in df.columns}
        operador_col = _find_column(cols_norm, ["prestador", "empresa", "operador", "proveedor"])
        if operador_col:
            mask_total = df[operador_col].astype(str).str.contains("total", case=False, na=False)
            df = df.loc[~mask_total].copy()

        return df
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, sep=";", encoding="latin1")
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

    no_col = _find_column(cols_norm, ["no"])
    operador_col = _find_column(cols_norm, ["empresa", "operador", "prestador", "proveedor"])
    accesos_col = _find_column(cols_norm, ["acceso", "abonado", "conexion", "cuenta", "suscriptor"])
    id_col = _find_column(cols_norm, ["id", "ruc", "identificacion"])

    if not operador_col or not accesos_col:
        raise ValueError(
            f"No fue posible mapear columnas minimas en Ecuador (operador/accesos). Columnas: {list(df_raw.columns)}"
        )

    anno, trimestre = _infer_year_quarter(df_raw, source_name=source_name)

    df = df_raw.copy()
    # Regla de negocio validada: en ARCOTEL D Prestador solo filas con "No." numerico.
    if no_col:
        df = df.loc[pd.to_numeric(df[no_col], errors="coerce").notna()].copy()

    df["__operador"] = df[operador_col].astype(str).str.strip()
    df["__id_operador"] = (
        df[id_col].astype(str).str.strip() if id_col else df["__operador"].str.upper().str.replace(r"\s+", "_", regex=True)
    )
    df["__anno"] = anno
    df["__trimestre"] = trimestre

    # Priorizar columnas mensuales del trimestre (suelen venir como fechas en el header).
    monthly_cols = []
    for column in df.columns:
        is_timestamp = isinstance(column, pd.Timestamp)
        looks_date = bool(re.search(r"20\d{2}-\d{2}-\d{2}", str(column)))
        looks_month_short = bool(re.search(r"\b(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)[-_ ]?\d{2,4}\b", str(column).lower()))
        if is_timestamp or looks_date or looks_month_short:
            monthly_cols.append(column)

    if monthly_cols:
        month_values = df[monthly_cols].apply(pd.to_numeric, errors="coerce")
        df["__num_accesos"] = month_values.max(axis=1).fillna(0)
    else:
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

