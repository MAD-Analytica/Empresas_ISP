"""
Utilidades compartidas para extractores multi-pais.
"""
from __future__ import annotations

import re
import unicodedata


def normalize_colname(value: str) -> str:
    """Normaliza nombres de columnas para matching flexible."""
    value = str(value).strip().lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def month_to_quarter(month: int) -> int:
    """Convierte mes (1-12) a trimestre (1-4)."""
    month = int(month)
    if month < 1 or month > 12:
        raise ValueError(f"Mes invalido: {month}")
    return ((month - 1) // 3) + 1

