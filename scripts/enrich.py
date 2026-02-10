"""
Script para enriquecer datos de empresas con RUES y WHOIS
"""
import requests
import pandas as pd
import time
import re
import sys
from pathlib import Path
from urllib.parse import quote
from bs4 import BeautifulSoup

sys.path.append(str(Path(__file__).parent.parent))
import config


# Headers para requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}


def get_rues_data(nit: str) -> dict:
    """
    Consulta Datos Colombia por NIT para obtener datos del RUES.
    
    Args:
        nit: NIT de la empresa (id_empresa en CRC)
    
    Returns:
        dict con datos de RUES o dict vacío si no encuentra
    """
    try:
        url = f"{config.RUES_API_URL}?nit={nit}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data and len(data) > 0:
            record = data[0]
            return {
                'rues_razon_social': record.get('razon_social', ''),
                'rues_representante_legal': record.get('representante_legal', ''),
                'rues_cedula_rl': record.get('num_identificacion_representante_legal', ''),
                'rues_fecha_matricula': record.get('fecha_matricula', ''),
                'rues_estado': record.get('estado_matricula', ''),
                'rues_actividad_principal': record.get('cod_ciiu_act_econ_pri', '')
            }
        
        return {}
        
    except Exception as e:
        print(f"   Error RUES para NIT {nit}: {e}")
        return {}


def search_asn(empresa_name: str) -> str | None:
    """
    Busca el ASN de una empresa en whois.ipip.net.
    
    Args:
        empresa_name: Nombre de la empresa
    
    Returns:
        ASN (ej: "AS273166") o None si no encuentra
    """
    try:
        name_encoded = quote(empresa_name)
        url = f"https://whois.ipip.net/search/{name_encoded}"
        
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        asn_links = soup.find_all('a', href=re.compile(r'^/AS\d+$'))
        
        if asn_links:
            href = asn_links[0].get('href')
            asn = href.lstrip('/')
            return asn
        
        return None
        
    except Exception as e:
        print(f"   Error buscando ASN para '{empresa_name}': {e}")
        return None


def get_whois_data(asn: str) -> dict:
    """
    Obtiene datos WHOIS de un ASN desde whois.ipip.net.
    
    Args:
        asn: Número de ASN (ej: "AS273166")
    
    Returns:
        dict con datos parseados del WHOIS
    """
    try:
        url = f"https://whois.ipip.net/{asn}"
        
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        whois_div = soup.find('div', id='whois')
        if not whois_div:
            return {}
        
        pre_tag = whois_div.find('pre')
        if not pre_tag:
            return {}
        
        whois_text = pre_tag.get_text()
        
        result = {
            'whois_asn': asn,
            'whois_owner': '',
            'whois_responsible': '',
            'whois_address': '',
            'whois_phone': '',
            'whois_contact_person': '',
            'whois_contact_email': '',
            'whois_contact_phone': ''
        }
        
        lines = whois_text.strip().split('\n')
        in_contact_block = False
        address_lines = []
        
        for line in lines:
            if ':' not in line:
                continue
            
            parts = line.split(':', 1)
            if len(parts) != 2:
                continue
            
            key = parts[0].strip().lower()
            value = parts[1].strip()
            
            if key == 'nic-hdl':
                in_contact_block = True
                continue
            
            if not in_contact_block:
                if key == 'owner':
                    result['whois_owner'] = value
                elif key == 'responsible':
                    result['whois_responsible'] = value
                elif key == 'address':
                    address_lines.append(value)
                elif key == 'phone':
                    result['whois_phone'] = value.replace('&#43;', '+')
            else:
                if key == 'person':
                    result['whois_contact_person'] = value
                elif key == 'e-mail':
                    result['whois_contact_email'] = value
                elif key == 'phone':
                    result['whois_contact_phone'] = value.replace('&#43;', '+')
        
        result['whois_address'] = ' | '.join(address_lines)
        
        return result
        
    except Exception as e:
        print(f"   Error obteniendo WHOIS para {asn}: {e}")
        return {}


def enrich_empresas(df_empresas: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece DataFrame de empresas con datos de RUES y WHOIS.
    
    Args:
        df_empresas: DataFrame con columnas id_empresa, empresa, max_accesos
    
    Returns:
        DataFrame enriquecido con columnas adicionales de RUES y WHOIS
    """
    print(f"\nEnriqueciendo {len(df_empresas)} empresas...")
    
    enriched_data = []
    
    for idx, row in df_empresas.iterrows():
        id_empresa = str(row['id_empresa'])
        empresa = row['empresa']
        max_accesos = row['max_accesos']
        
        print(f"\n[{idx + 1}/{len(df_empresas)}] {empresa}")
        
        record = {
            'id_empresa': id_empresa,
            'empresa': empresa,
            'max_accesos': max_accesos
        }
        
        rues_data = get_rues_data(id_empresa)
        record.update(rues_data)
        time.sleep(0.2)
        
        asn = search_asn(empresa)
        
        if asn:
            time.sleep(0.5)
            whois_data = get_whois_data(asn)
            record.update(whois_data)
            time.sleep(0.5)
        else:
            record.update({
                'whois_asn': '',
                'whois_owner': '',
                'whois_responsible': '',
                'whois_address': '',
                'whois_phone': '',
                'whois_contact_person': '',
                'whois_contact_email': '',
                'whois_contact_phone': ''
            })
        
        enriched_data.append(record)
    
    df_enriched = pd.DataFrame(enriched_data)
    print(f"\nEnriquecimiento completado. {len(df_enriched)} empresas procesadas.")
    
    return df_enriched


if __name__ == "__main__":
    print("=== Test de enriquecimiento ===\n")
    
    print("1. Test RUES (NIT ejemplo):")
    rues = get_rues_data("901941232")
    print(f"   {rues}\n")
    
    print("2. Test búsqueda ASN:")
    asn = search_asn("GIGANAV CONNECTIONS")
    print(f"   ASN encontrado: {asn}\n")
    
    if asn:
        print("3. Test WHOIS:")
        whois = get_whois_data(asn)
        for k, v in whois.items():
            print(f"   {k}: {v}")
