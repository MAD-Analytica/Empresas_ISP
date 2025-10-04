"""
Script para copiar archivos procesados a carpeta de Drive
"""
import shutil
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
import config


def copy_to_drive():
    """Copia los CSVs procesados a la carpeta de Drive"""
    print("=" * 60)
    print("COPIANDO ARCHIVOS A DRIVE")
    print("=" * 60)
    
    # Archivos fuente
    source_base = config.PROCESSED_DATA_DIR / config.OUTPUT_BASE_FILENAME
    source_resumen = config.PROCESSED_DATA_DIR / config.OUTPUT_RESUMEN_FILENAME
    source_emp_trim = config.PROCESSED_DATA_DIR / config.OUTPUT_EMPRESA_TRIM_FILENAME
    
    # Archivos destino
    drive_folder = Path(config.LOAD_DATA_DIR)
    dest_base = drive_folder / "base_accesos_emp-trim.csv"
    dest_resumen = drive_folder / "accesos-emp-mpio-trim.csv"
    dest_emp_trim = drive_folder / "accesos-emp-trim.csv"
    
    try:
        # Copiar archivo base
        if source_base.exists():
            shutil.copy2(source_base, dest_base)
            print(f"✓ Copiado: {dest_base.name}")
        else:
            print(f"✗ No encontrado: {source_base}")
        
        # Copiar resumen
        if source_resumen.exists():
            shutil.copy2(source_resumen, dest_resumen)
            print(f"✓ Copiado: {dest_resumen.name}")
        else:
            print(f"✗ No encontrado: {source_resumen}")
        
        # Copiar empresa/trimestre
        if source_emp_trim.exists():
            shutil.copy2(source_emp_trim, dest_emp_trim)
            print(f"✓ Copiado: {dest_emp_trim.name}")
        else:
            print(f"✗ No encontrado: {source_emp_trim}")
        
        print("\n" + "=" * 60)
        print("ARCHIVOS COPIADOS A DRIVE")
        print("=" * 60)
        print(f"Carpeta: {config.LOAD_DATA_DIR}")
        
    except Exception as e:
        print(f"\nError al copiar archivos: {e}")
        raise


def run():
    """Ejecuta la copia a Drive"""
    copy_to_drive()


if __name__ == "__main__":
    run()

