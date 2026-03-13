"""
Orquestador de extraccion multi-pais y consolidacion canonica.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))
import config
from scripts import extract_colombia, extract_ecuador, extract_peru


def _filter_window(df: pd.DataFrame) -> pd.DataFrame:
    start_year, end_year = config.WINDOW_YEARS
    return df.loc[df["anno"].between(start_year, end_year, inclusive="both")].copy()


def run(
    include_colombia: bool = True,
    include_ecuador: bool = False,
    include_peru: bool = False,
    ecuador_files: list[str] | None = None,
    peru_files: list[str] | None = None,
    save: bool = True,
) -> pd.DataFrame:
    parts = []

    if include_colombia:
        parts.append(extract_colombia.run(save=True))

    if include_ecuador:
        parts.append(extract_ecuador.run(source_files=ecuador_files, save=True))

    if include_peru:
        parts.append(extract_peru.run(source_files=peru_files, save=True))

    if not parts:
        raise ValueError("Debes incluir al menos un pais para extraer.")

    canonical = pd.concat(parts, ignore_index=True)
    canonical = _filter_window(canonical)

    canonical = canonical.dropna(subset=["pais", "id_operador", "operador", "anno", "trimestre"])
    canonical = canonical.groupby(
        ["pais", "id_operador", "operador", "anno", "trimestre", "fuente"], as_index=False
    )["num_accesos"].sum()
    canonical = canonical[config.CANONICAL_COLUMNS]
    canonical = canonical.sort_values(["pais", "id_operador", "anno", "trimestre"])

    if save:
        out_path = config.RAW_DATA_DIR / config.RAW_CANONICAL_FILENAME
        canonical.to_csv(out_path, index=False)
        print(f"Canonico multi-pais guardado: {out_path}")

    return canonical


if __name__ == "__main__":
    # Ejecucion por defecto: solo Colombia para evitar fallar si no hay archivos ECU/PER.
    run(include_colombia=True, include_ecuador=False, include_peru=False, save=True)

