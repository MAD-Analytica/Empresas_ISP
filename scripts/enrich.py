"""
Script para enriquecer datos de empresas con RUES y WHOIS
"""
import requests
import pandas as pd
import time
import re
import sys
import os
from pathlib import Path
from urllib.parse import quote
from glob import glob
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


def clean_empresa_name(empresa_name: str) -> tuple[str, str]:
    """
    Limpia el nombre de empresa quitando sufijos corporativos comunes.
    
    Args:
        empresa_name: Nombre original de la empresa
    
    Returns:
        tuple: (nombre_limpio, sufijos_encontrados)
    """
    name = empresa_name.strip().upper()
    sufijos_encontrados = []
    
    # Patrones de sufijos a remover (orden importa - de más específico a menos)
    sufijos_patterns = [
        (r'\s+EN\s+LIQUIDACION\s*$', 'EN LIQUIDACION'),
        (r'\s+ZOMAC\s*$', 'ZOMAC'),
        (r'\s+BIC\s*$', 'BIC'),
        (r'\s*E\.?\s*S\.?\s*P\.?\s*$', 'ESP'),           # E.S.P., ESP, E S P
        (r'\s*S\.?\s*A\.?\s*S\.?\s*$', 'SAS'),           # S.A.S., SAS, S A S
        (r'\s*S\.?\s*A\.?\s*$', 'SA'),                   # S.A., SA
        (r'\s*LTDA\.?\s*$', 'LTDA'),                     # LTDA., LTDA
        (r'\s*S\.?\s*C\.?\s*A\.?\s*$', 'SCA'),           # S.C.A.
        (r'\s*&\s*CIA\.?\s*$', 'CIA'),                   # & CIA.
        (r'\s*Y\s+CIA\.?\s*$', 'CIA'),                   # Y CIA.
    ]
    
    # Aplicar múltiples veces porque puede haber combinaciones (ej: S.A. E.S.P.)
    changed = True
    while changed:
        changed = False
        for pattern, sufijo in sufijos_patterns:
            new_name = re.sub(pattern, '', name, flags=re.IGNORECASE)
            if new_name != name:
                sufijos_encontrados.append(sufijo)
                name = new_name.strip()
                changed = True
                break
    
    # Limpiar espacios y puntos sueltos al final
    name = re.sub(r'[\s,.\-]+$', '', name).strip()
    
    return name, ' '.join(sufijos_encontrados)


def search_asn(empresa_name: str) -> tuple[str | None, str]:
    """
    Busca el ASN de una empresa en whois.ipip.net.
    
    Args:
        empresa_name: Nombre de la empresa
    
    Returns:
        tuple: (ASN o None, nombre_limpio usado para búsqueda)
    """
    nombre_limpio, sufijos = clean_empresa_name(empresa_name)
    
    try:
        name_encoded = quote(nombre_limpio)
        url = f"https://whois.ipip.net/search/{name_encoded}"
        
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        asn_links = soup.find_all('a', href=re.compile(r'^/AS\d+$'))
        
        if asn_links:
            href = asn_links[0].get('href')
            asn = href.lstrip('/')
            return asn, nombre_limpio
        
        return None, nombre_limpio
        
    except Exception as e:
        print(f"   Error buscando ASN para '{nombre_limpio}': {e}")
        return None, nombre_limpio


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
        
        asn, nombre_limpio = search_asn(empresa)
        record['empresa_busqueda'] = nombre_limpio  # Nombre usado para búsqueda ASN
        
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


def load_empresas_grandes(min_accesos=10000):
    """Carga emp-trim más reciente y filtra empresas >= min_accesos"""
    pattern = os.path.join(config.PROCESSED_DATA_DIR, "accesos-emp-trim-*.csv")
    files = glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"No se encontró archivo emp-trim en {config.PROCESSED_DATA_DIR}")
    
    df = pd.read_csv(max(files, key=os.path.getmtime))
    
    df_max = df.groupby(['id_empresa', 'empresa']).agg(
        max_accesos=('num_accesos', 'max')
    ).reset_index()
    
    return df_max[df_max['max_accesos'] >= min_accesos].sort_values('max_accesos', ascending=False)


def run(min_accesos=10000):
    """Ejecuta el enriquecimiento completo de empresas >= min_accesos"""
    df_empresas = load_empresas_grandes(min_accesos=min_accesos)
    
    if len(df_empresas) == 0:
        print("No hay empresas que cumplan el criterio")
        return None
    
    print(f"Enriqueciendo {len(df_empresas)} empresas...")
    df_enriched = enrich_empresas(df_empresas)
    
    filepath = os.path.join(config.PROCESSED_DATA_DIR, config.OUTPUT_EMPRESAS_FILENAME)
    df_enriched.to_csv(filepath, index=False)
    print(f"Guardado: {filepath}")
    
    return df_enriched


def test():
    """Ejecuta tests de las funciones"""
    print("=== Test de enriquecimiento ===\n")
    
    print("1. Test limpieza de nombres:")
    tests = [
        "UNE EPM TELECOMUNICACIONES S.A.",
        "COLOMBIA TELECOMUNICACIONES S.A. E.S.P.",
        "FIBRAZO S.A.S.",
        "EDATEL S.A. EN LIQUIDACION",
        "EMPRESA MUNICIPAL DE CALI EICE E.S.P."
    ]
    for t in tests:
        limpio, sufijos = clean_empresa_name(t)
        print(f"   '{t}' -> '{limpio}' (sufijos: {sufijos})")
    
    print("\n2. Test RUES (NIT ejemplo):")
    rues = get_rues_data("901941232")
    print(f"   {rues}\n")
    
    print("3. Test búsqueda ASN:")
    asn, nombre = search_asn("GIGANAV CONNECTIONS S.A.S.")
    print(f"   Nombre limpio: {nombre}")
    print(f"   ASN encontrado: {asn}\n")
    
    if asn:
        print("4. Test WHOIS:")
        whois = get_whois_data(asn)
        for k, v in whois.items():
            print(f"   {k}: {v}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Enriquecer empresas con RUES y WHOIS')
    parser.add_argument('--test', action='store_true', help='Ejecutar tests')
    parser.add_argument('--min-accesos', type=int, default=10000, help='Mínimo de accesos (default: 10000)')
    
    args = parser.parse_args()
    
    if args.test:
        test()
    else:
        run(min_accesos=args.min_accesos)
