"""
Enriquecimiento WHOIS para operadores ICP multicountry.

Entrada:
- data_ISPs/processed/icp_operadores_2024_2025.csv

Salida:
- data_ISPs/processed/icp_operadores_whois_2024_2025.csv
"""
from __future__ import annotations

from html import unescape
from pathlib import Path
import re
import sys
import time
from urllib.parse import quote

import pandas as pd
import requests

sys.path.append(str(Path(__file__).parent.parent))
import config


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


def clean_operator_name(name: str) -> str:
    """
    Limpia sufijos corporativos para mejorar la busqueda ASN.
    """
    cleaned = str(name).strip().upper()
    patterns = [
        r"\s+EN\s+LIQUIDACION\s*$",
        r"\s+BIC\s*$",
        r"\s+ZOMAC\s*$",
        r"\s*E\.?\s*S\.?\s*P\.?\s*$",
        r"\s*S\.?\s*A\.?\s*S\.?\s*$",
        r"\s*S\.?\s*A\.?\s*$",
        r"\s*L\.?\s*T\.?\s*D\.?\s*A\.?\s*$",
        r"\s*S\.?\s*R\.?\s*L\.?\s*$",
        r"\s*S\.?\s*A\.?\s*C\.?\s*$",
        r"\s*C\.?\s*L\.?\s*T\.?\s*D\.?\s*A\.?\s*$",
        r"\s*&\s*CIA\.?\s*$",
        r"\s+Y\s+CIA\.?\s*$",
    ]
    changed = True
    while changed:
        changed = False
        for pattern in patterns:
            new_value = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
            if new_value != cleaned:
                cleaned = new_value.strip()
                changed = True
                break
    cleaned = re.sub(r"[\s,.\-]+$", "", cleaned).strip()
    return cleaned


def search_asn(operator_name: str) -> tuple[str | None, str]:
    """
    Busca el primer ASN asociado a un nombre de operador.
    """
    query_name = clean_operator_name(operator_name)
    url = f"{config.WHOIS_SEARCH_BASE_URL}/{quote(query_name)}"

    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        html = response.text
    except Exception as exc:
        print(f"   Error search ASN para '{query_name}': {exc}")
        return None, query_name

    # Coincide enlaces tipo /AS273166
    match = re.search(r'href="/(AS\d+)"', html)
    if not match:
        return None, query_name
    return match.group(1), query_name


def parse_whois_fields(whois_text: str, asn: str) -> dict:
    """
    Parsea bloque WHOIS crudo a campos estandar.
    """
    result = {
        "whois_asn": asn,
        "whois_owner": "",
        "whois_responsible": "",
        "whois_address": "",
        "whois_phone": "",
        "whois_contact_person": "",
        "whois_contact_email": "",
        "whois_contact_phone": "",
    }
    address_lines: list[str] = []
    in_contact_block = False

    for raw_line in whois_text.splitlines():
        line = unescape(raw_line.strip())
        if ":" not in line:
            continue
        key, value = [part.strip() for part in line.split(":", 1)]
        key_lower = key.lower()

        if key_lower == "nic-hdl":
            in_contact_block = True
            continue

        if not in_contact_block:
            if key_lower == "owner":
                result["whois_owner"] = value
            elif key_lower == "responsible":
                result["whois_responsible"] = value
            elif key_lower == "address":
                address_lines.append(value)
            elif key_lower == "phone":
                result["whois_phone"] = value.replace("&#43;", "+")
        else:
            if key_lower == "person":
                result["whois_contact_person"] = value
            elif key_lower in {"e-mail", "email"}:
                result["whois_contact_email"] = value
            elif key_lower == "phone":
                result["whois_contact_phone"] = value.replace("&#43;", "+")

    result["whois_address"] = " | ".join(address_lines)
    return result


def get_whois_data(asn: str) -> dict:
    """
    Obtiene datos WHOIS a partir de ASN.
    """
    url = f"{config.WHOIS_ASN_BASE_URL}/{asn}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        html = response.text
    except Exception as exc:
        print(f"   Error WHOIS para {asn}: {exc}")
        return {}

    pre_match = re.search(r'<div[^>]*id="whois"[^>]*>.*?<pre[^>]*>(.*?)</pre>', html, flags=re.DOTALL | re.IGNORECASE)
    if not pre_match:
        return {}
    whois_text = re.sub(r"<[^>]+>", "", pre_match.group(1))
    return parse_whois_fields(whois_text, asn=asn)


def load_icp_candidates(
    only_icp: bool = True,
    min_max_accesos: int = 1000,
    max_max_accesos: int = 100000,
) -> pd.DataFrame:
    """
    Carga operadores desde ICP y filtra candidatos a enriquecer.
    """
    input_path = config.PROCESSED_DATA_DIR / config.OUTPUT_ICP_FILENAME
    if not input_path.exists():
        raise FileNotFoundError(
            f"No existe {input_path}. Ejecuta primero scripts/calculate_icp.py"
        )

    df = pd.read_csv(input_path)
    if only_icp and "cumple_icp" in df.columns:
        df = df.loc[df["cumple_icp"] == True].copy()
    if "max_accesos_2024_2025" in df.columns:
        df = df.loc[
            df["max_accesos_2024_2025"].between(min_max_accesos, max_max_accesos, inclusive="both")
        ].copy()

    df = df.sort_values(["pais", "max_accesos_2024_2025"], ascending=[True, False])
    df = df.reset_index(drop=True)
    return df


def enrich_whois(df_ops: pd.DataFrame, sleep_seconds: float = 0.4) -> pd.DataFrame:
    """
    Enriquece DataFrame de operadores con WHOIS.
    """
    records = []
    total = len(df_ops)
    print(f"Enriqueciendo WHOIS para {total} operadores...")

    for idx, row in df_ops.iterrows():
        operator_name = str(row["operador"])
        print(f"[{idx + 1}/{total}] {row['pais']} - {operator_name}")

        base = row.to_dict()
        asn, query_name = search_asn(operator_name)
        base["whois_query_name"] = query_name

        if asn:
            time.sleep(sleep_seconds)
            whois = get_whois_data(asn)
            if whois:
                base.update(whois)
            else:
                base["whois_asn"] = asn
        else:
            base["whois_asn"] = ""

        # Completa campos faltantes estandar
        for field in [
            "whois_owner",
            "whois_responsible",
            "whois_address",
            "whois_phone",
            "whois_contact_person",
            "whois_contact_email",
            "whois_contact_phone",
        ]:
            base.setdefault(field, "")

        records.append(base)
        time.sleep(sleep_seconds)

    return pd.DataFrame(records)


def run(
    only_icp: bool = True,
    min_max_accesos: int = 1000,
    max_max_accesos: int = 100000,
) -> pd.DataFrame:
    """
    Ejecuta enriquecimiento WHOIS y guarda resultado.
    """
    candidates = load_icp_candidates(
        only_icp=only_icp,
        min_max_accesos=min_max_accesos,
        max_max_accesos=max_max_accesos,
    )
    if candidates.empty:
        print("No hay operadores para enriquecer con los filtros actuales.")
        return pd.DataFrame()

    enriched = enrich_whois(candidates)
    output_path = config.PROCESSED_DATA_DIR / config.OUTPUT_WHOIS_FILENAME
    enriched.to_csv(output_path, index=False)
    print(f"WHOIS enriquecido guardado: {output_path}")
    return enriched


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Enriquecimiento WHOIS para operadores ICP.")
    parser.add_argument("--all-icp", action="store_true", help="Forzar filtro ICP completo (1000 a 100000).")
    parser.add_argument("--min-max-accesos", type=int, default=1000, help="Minimo de max_accesos_2024_2025.")
    parser.add_argument("--max-max-accesos", type=int, default=100000, help="Maximo de max_accesos_2024_2025.")
    args = parser.parse_args()

    if args.all_icp:
        run(only_icp=True, min_max_accesos=1000, max_max_accesos=100000)
    else:
        run(
            only_icp=True,
            min_max_accesos=args.min_max_accesos,
            max_max_accesos=args.max_max_accesos,
        )

