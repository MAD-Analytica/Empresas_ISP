"""
Calculo ICP por pais para la ventana 2024-2025.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))
import config
from scripts import extract_colombia, extract_ecuador, extract_peru


def _list_files(folder: Path, suffixes: tuple[str, ...]) -> list[Path]:
    if not folder.exists():
        return []
    files = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in suffixes]
    files.sort()
    return files


def _load_colombia_canonical() -> pd.DataFrame:
    folder = config.RAW_DATA_DIR / "colombia"
    candidates = _list_files(folder, (".csv",))
    if not candidates:
        raise ValueError(f"No se encontraron CSV en {folder}")

    # Priorizar archivo mas grande para evitar tomar muestras pequenas.
    selected = max(candidates, key=lambda p: p.stat().st_size)
    print(f"Colombia raw seleccionado: {selected.name}")
    df_raw = pd.read_csv(selected, sep=";", na_values=["", "NA", "null"], keep_default_na=True)
    return extract_colombia.to_canonical(df_raw)


def _load_ecuador_canonical() -> pd.DataFrame:
    folder = config.RAW_DATA_DIR / "ecuador"
    files = _list_files(folder, (".xlsx", ".xls", ".csv"))
    if not files:
        raise ValueError(f"No se encontraron archivos ECU en {folder}")
    print(f"Ecuador archivos: {len(files)}")
    return extract_ecuador.run(source_files=[str(p) for p in files], save=False)


def _load_peru_canonical() -> pd.DataFrame:
    folder = config.RAW_DATA_DIR / "peru"
    files = _list_files(folder, (".xlsx", ".xls", ".csv"))
    if not files:
        raise ValueError(f"No se encontraron archivos PER en {folder}")
    print(f"Peru archivos: {len(files)}")
    return extract_peru.run(source_files=[str(p) for p in files], save=False)


def build_canonical(include_colombia: bool = True, include_ecuador: bool = True, include_peru: bool = True) -> pd.DataFrame:
    parts = []
    if include_colombia:
        parts.append(_load_colombia_canonical())
    if include_ecuador:
        parts.append(_load_ecuador_canonical())
    if include_peru:
        parts.append(_load_peru_canonical())

    if not parts:
        raise ValueError("Debes incluir al menos un pais.")

    canonical = pd.concat(parts, ignore_index=True)
    canonical = canonical.dropna(subset=["pais", "id_operador", "operador", "anno", "trimestre"])
    canonical["anno"] = pd.to_numeric(canonical["anno"], errors="coerce")
    canonical["trimestre"] = pd.to_numeric(canonical["trimestre"], errors="coerce")
    canonical["num_accesos"] = pd.to_numeric(canonical["num_accesos"], errors="coerce").fillna(0)
    canonical = canonical.dropna(subset=["anno", "trimestre"])
    canonical = canonical.groupby(
        ["pais", "id_operador", "operador", "anno", "trimestre", "fuente"],
        as_index=False,
    )["num_accesos"].sum()
    canonical = canonical[config.CANONICAL_COLUMNS].copy()
    canonical = canonical.sort_values(["pais", "id_operador", "anno", "trimestre"])
    return canonical


def calculate_icp_tables(canonical: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    start_year, end_year = config.WINDOW_YEARS
    window = canonical.loc[canonical["anno"].between(start_year, end_year, inclusive="both")].copy()
    if window.empty:
        raise ValueError(f"No hay datos en ventana {start_year}-{end_year}.")

    window["periodo"] = window["anno"].astype(int).astype(str) + "Q" + window["trimestre"].astype(int).astype(str)

    by_operator = (
        window.groupby(["pais", "id_operador", "operador"], as_index=False)
        .agg(
            max_accesos_2024_2025=("num_accesos", "max"),
            accesos_ventana=("num_accesos", "sum"),
            periodos_reportados=("periodo", "nunique"),
        )
    )
    by_operator["cumple_icp"] = by_operator["max_accesos_2024_2025"].between(1000, 100000, inclusive="both")

    totals = (
        by_operator.groupby("pais", as_index=False)
        .agg(total_accesos_ventana_pais=("accesos_ventana", "sum"))
    )
    by_operator = by_operator.merge(totals, on="pais", how="left")
    by_operator["market_share_en_ventana"] = (
        by_operator["accesos_ventana"] / by_operator["total_accesos_ventana_pais"] * 100
    )

    # Resumen por pais usando el trimestre pico de usuarios totales.
    total_by_period = (
        window.groupby(["pais", "anno", "trimestre"], as_index=False)["num_accesos"]
        .sum()
        .rename(columns={"num_accesos": "usuarios_totales_trimestre"})
    )
    peak_periods = (
        total_by_period.sort_values(
            ["pais", "usuarios_totales_trimestre", "anno", "trimestre"],
            ascending=[True, False, False, False],
        )
        .drop_duplicates(subset=["pais"], keep="first")
        .rename(columns={"anno": "anno_pico", "trimestre": "trimestre_pico"})
    )

    peak_window = window.merge(
        peak_periods[["pais", "anno_pico", "trimestre_pico"]],
        left_on=["pais", "anno", "trimestre"],
        right_on=["pais", "anno_pico", "trimestre_pico"],
        how="inner",
    )

    resumen_base = by_operator.groupby("pais", as_index=False).agg(num_isps=("id_operador", "nunique"))
    resumen_peak = (
        peak_periods[["pais", "anno_pico", "trimestre_pico", "usuarios_totales_trimestre"]]
        .rename(columns={"usuarios_totales_trimestre": "num_usuarios"})
        .copy()
    )
    resumen_peak["market_share"] = 100.0

    # Para ICP en resumen, usar el mismo trimestre pico pais.
    icp_ids = by_operator.loc[by_operator["cumple_icp"], ["pais", "id_operador"]].drop_duplicates()
    peak_icp = peak_window.merge(icp_ids, on=["pais", "id_operador"], how="inner")
    resumen_icp = (
        peak_icp.groupby("pais", as_index=False)
        .agg(
            num_isps_icp=("id_operador", "nunique"),
            num_usuarios_icp=("num_accesos", "sum"),
        )
    )

    resumen = resumen_base.merge(resumen_peak, on="pais", how="left")
    resumen = resumen.merge(resumen_icp, on="pais", how="left")
    resumen["num_isps_icp"] = resumen["num_isps_icp"].fillna(0).astype(int)
    resumen["num_usuarios_icp"] = resumen["num_usuarios_icp"].fillna(0.0)
    resumen["market_share_icp"] = (
        resumen["num_usuarios_icp"] / resumen["num_usuarios"] * 100
    ).fillna(0.0)

    return by_operator, resumen


def run(include_colombia: bool = True, include_ecuador: bool = True, include_peru: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    canonical = build_canonical(
        include_colombia=include_colombia,
        include_ecuador=include_ecuador,
        include_peru=include_peru,
    )

    raw_canonical_path = config.RAW_DATA_DIR / config.RAW_CANONICAL_FILENAME
    canonical.to_csv(raw_canonical_path, index=False)
    print(f"Canonico multi-pais guardado: {raw_canonical_path}")

    by_operator, resumen = calculate_icp_tables(canonical)

    operators_path = config.PROCESSED_DATA_DIR / config.OUTPUT_ICP_FILENAME
    resumen_path = config.PROCESSED_DATA_DIR / config.OUTPUT_ICP_RESUMEN_FILENAME
    by_operator.to_csv(operators_path, index=False)
    resumen.to_csv(resumen_path, index=False)
    print(f"ICP operadores guardado: {operators_path}")
    print(f"ICP resumen guardado: {resumen_path}")

    return by_operator, resumen


if __name__ == "__main__":
    run()

