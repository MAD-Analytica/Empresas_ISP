"""
Script para separar el archivo de empresas enriquecidas en dos tablas:
- empresas: información corporativa
- leads: personas de contacto (economic y technical buyers)
"""
import pandas as pd
import sys
from pathlib import Path
import os
from glob import glob

sys.path.append(str(Path(__file__).parent.parent))
import config


def load_latest_enriched_data():
    """
    Encuentra el archivo de empresas enriquecidas más reciente y lo carga.
    """
    pattern = os.path.join(config.PROCESSED_DATA_DIR, "empresas_enriquecidas-*.csv")
    files = glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"No se encontró archivo de empresas enriquecidas en {config.PROCESSED_DATA_DIR}")
    
    latest = max(files, key=os.path.getmtime)
    print(f"Cargando: {latest}")
    df = pd.read_csv(latest)
    print(f"Cargadas {len(df)} empresas")
    return df


def generate_empresas_table(df):
    """
    Genera tabla de empresas con información corporativa.
    """
    print("\nGenerando tabla de EMPRESAS...")
    
    cols_empresas = [
        'id_empresa',
        'empresa', 
        'max_accesos',
        'rues_razon_social',
        'rues_fecha_matricula',
        'rues_estado',
        'rues_actividad_principal',
        'whois_asn',
        'whois_owner',
        'whois_address'
    ]
    
    # Solo incluir columnas que existen
    cols_existentes = [c for c in cols_empresas if c in df.columns]
    df_empresas = df[cols_existentes].copy()
    
    # Renombrar para claridad
    rename_map = {
        'id_empresa': 'nit',
        'rues_razon_social': 'razon_social',
        'rues_fecha_matricula': 'fecha_matricula',
        'rues_estado': 'estado_matricula',
        'rues_actividad_principal': 'actividad_ciiu',
        'whois_asn': 'asn',
        'whois_owner': 'asn_owner',
        'whois_address': 'direccion'
    }
    df_empresas = df_empresas.rename(columns=rename_map)
    
    print(f"Tabla empresas: {len(df_empresas)} registros")
    return df_empresas


def generate_leads_table(df):
    """
    Genera tabla de leads (personas) a partir del archivo enriquecido.
    Cada empresa puede tener hasta 3 leads:
    - Representante Legal (Economic Buyer) - del RUES
    - Responsable Técnico - del WHOIS
    - Contacto Técnico - del WHOIS
    """
    print("\nGenerando tabla de LEADS...")
    
    leads = []
    
    for _, row in df.iterrows():
        id_empresa = row.get('id_empresa', '')
        empresa = row.get('empresa', '')
        
        rep_legal = row.get('rues_representante_legal', '')
        if pd.notna(rep_legal) and rep_legal.strip():
            leads.append({
                'nit': id_empresa,
                'empresa': empresa,
                'nombre': rep_legal.strip(),
                'cedula': row.get('rues_cedula_rl', ''),
                'puesto': 'Representante Legal',
                'tipo': 'Economic Buyer',
                'email': '',
                'telefono': '',
                'fuente': 'RUES'
            })
        
        responsible = row.get('whois_responsible', '')
        if pd.notna(responsible) and responsible.strip():
            leads.append({
                'nit': id_empresa,
                'empresa': empresa,
                'nombre': responsible.strip(),
                'cedula': '',
                'puesto': 'Responsable Técnico',
                'tipo': 'Technical Buyer',
                'email': '',
                'telefono': row.get('whois_phone', ''),
                'fuente': 'WHOIS'
            })
        
        contact = row.get('whois_contact_person', '')
        if pd.notna(contact) and contact.strip():
            leads.append({
                'nit': id_empresa,
                'empresa': empresa,
                'nombre': contact.strip(),
                'cedula': '',
                'puesto': 'Contacto Técnico',
                'tipo': 'Technical Buyer',
                'email': row.get('whois_contact_email', ''),
                'telefono': row.get('whois_contact_phone', ''),
                'fuente': 'WHOIS'
            })
    
    df_leads = pd.DataFrame(leads)
    
    df_leads = df_leads.fillna('')
    
    print(f"Tabla leads: {len(df_leads)} registros")
    print(f"  - Representantes Legales: {len(df_leads[df_leads['puesto'] == 'Representante Legal'])}")
    print(f"  - Responsables Técnicos: {len(df_leads[df_leads['puesto'] == 'Responsable Técnico'])}")
    print(f"  - Contactos Técnicos: {len(df_leads[df_leads['puesto'] == 'Contacto Técnico'])}")
    
    return df_leads


def save_tables(df_empresas, df_leads):
    """Guarda las tablas en CSV"""
    filepath_empresas = os.path.join(config.FINAL_DATA_DIR, config.OUTPUT_EMPRESAS_TABLA_FILENAME)
    filepath_leads = os.path.join(config.FINAL_DATA_DIR, config.OUTPUT_LEADS_FILENAME)
    
    df_empresas.to_csv(filepath_empresas, index=False)
    print(f"\nGuardado: {filepath_empresas}")
    
    df_leads.to_csv(filepath_leads, index=False)
    print(f"Guardado: {filepath_leads}")
    
    return filepath_empresas, filepath_leads


def run():
    """Ejecuta la separación de tablas"""
    print("=" * 60)
    print("SEPARACIÓN DE TABLAS: EMPRESAS Y LEADS")
    print("=" * 60)
    
    try:
        df = load_latest_enriched_data()
        
        df_empresas = generate_empresas_table(df)
        df_leads = generate_leads_table(df)
        
        save_tables(df_empresas, df_leads)
        
        print("\n" + "=" * 60)
        print("SEPARACIÓN COMPLETADA")
        print("=" * 60)
        
        return df_empresas, df_leads
        
    except Exception as e:
        print(f"\nERROR: {e}")
        raise


if __name__ == "__main__":
    run()
