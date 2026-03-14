import pandas as pd
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))
import config


def load_enriched():
    path = config.PROCESSED_DATA_DIR / config.OUTPUT_WHOIS_FILENAME
    if not path.exists():
        raise FileNotFoundError(f"No existe {path}. Corre antes scripts/enrich.py")
    return pd.read_csv(path)


def build_empresas(df):
    cols = [
        "pais", "id_operador", "operador",
        "max_accesos_2024_2025", "accesos_ventana",
        "cumple_icp", "whois_asn", "whois_owner", "whois_address"
    ]
    cols = [c for c in cols if c in df.columns]
    out = df[cols].copy().drop_duplicates(subset=["pais", "id_operador"])
    return out.rename(columns={"id_operador": "id_empresa"})


def build_leads(df):
    leads = []
    for _, r in df.iterrows():
        common = {
            "pais": r.get("pais", ""),
            "id_empresa": r.get("id_operador", ""),
            "empresa": r.get("operador", ""),
            "fuente": "WHOIS",
        }

        if str(r.get("whois_responsible", "")).strip():
            leads.append({
                **common,
                "nombre": r.get("whois_responsible", ""),
                "puesto": "Responsable Técnico",
                "tipo": "Technical Buyer",
                "email": "",
                "telefono": r.get("whois_phone", ""),
            })

        if str(r.get("whois_contact_person", "")).strip():
            leads.append({
                **common,
                "nombre": r.get("whois_contact_person", ""),
                "puesto": "Contacto Técnico",
                "tipo": "Technical Buyer",
                "email": r.get("whois_contact_email", ""),
                "telefono": r.get("whois_contact_phone", ""),
            })
        
    leads_df = pd.DataFrame(leads)
    if leads_df.empty:
        return leads_df
    return leads_df.loc[leads_df["nombre"].notna()].copy()


def run():
    df = load_enriched()
    empresas = build_empresas(df)
    leads = build_leads(df)

    empresas_path = config.FINAL_DATA_DIR / config.OUTPUT_EMPRESAS_TABLA_FILENAME
    leads_path = config.FINAL_DATA_DIR / config.OUTPUT_LEADS_FILENAME

    empresas.to_csv(empresas_path, index=False)
    leads.to_csv(leads_path, index=False)

    print(f"Empresas: {empresas_path} ({len(empresas)})")
    print(f"Leads: {leads_path} ({len(leads)})")


if __name__ == "__main__":
    run()