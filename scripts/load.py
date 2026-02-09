"""
Script para copiar archivos procesados a carpeta de Drive
"""
import shutil
import sys
from pathlib import Path
import os
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))
import config


def copy_to_drive():
    """Copia los CSVs procesados a la carpeta de Drive"""
    print("=" * 60)
    print("COPIANDO ARCHIVOS A DRIVE")
    print("=" * 60)
    
    # Archivos fuente
    source_base = Path(config.PROCESSED_DATA_DIR) / config.OUTPUT_BASE_FILENAME
    source_resumen = Path(config.PROCESSED_DATA_DIR) / config.OUTPUT_RESUMEN_FILENAME
    source_emp_trim = Path(config.PROCESSED_DATA_DIR) / config.OUTPUT_EMPRESA_TRIM_FILENAME
    
    # Archivos destino
    drive_folder = Path(config.LOAD_DATA_DIR)
    dest_base = drive_folder / f"base_accesos_emp-trim-{datetime.today().strftime('%Y%m%d')}.csv"
    dest_resumen = drive_folder / f"accesos-emp-mpio-trim-{datetime.today().strftime('%Y%m%d')}.csv"
    dest_emp_trim = drive_folder / f"accesos-emp-trim-{datetime.today().strftime('%Y%m%d')}.csv"
    
    try:
        if source_base.exists():
            shutil.copy2(source_base, dest_base)
            print(f"✓ Copiado: {dest_base.name}")
        else:
            print(f"✗ No encontrado: {source_base}")
        
        if source_resumen.exists():
            shutil.copy2(source_resumen, dest_resumen)
            print(f"✓ Copiado: {dest_resumen.name}")
        else:
            print(f"✗ No encontrado: {source_resumen}")
        
        if source_emp_trim.exists():
            shutil.copy2(source_emp_trim, dest_emp_trim)
            print(f"✓ Copiado: {dest_emp_trim.name}")
        else:
            print(f"✗ No encontrado: {source_emp_trim}")
    
        print("ARCHIVOS COPIADOS A DRIVE")
        print(f"Carpeta: {config.LOAD_DATA_DIR}")
        
    except Exception as e:
        print(f"\nError al copiar archivos: {e}")
        raise


def run():
    """Ejecuta la copia a Drive"""
    copy_to_drive()


if __name__ == "__main__":
    run()

