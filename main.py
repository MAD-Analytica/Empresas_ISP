"""
Pipeline completo: Extracción, Transformación y Carga
"""
from scripts import extract, transform, load


def run_pipeline():
    """Ejecuta el pipeline completo"""
    print("\n" + "=" * 70)
    print(" PIPELINE COMPLETO - ACCESOS INTERNET FIJO")
    print("=" * 70 + "\n")
    
    print("PASO 1: Extracción desde API...")
    extract.run()
    
    print("\n" + "-" * 70 + "\n")
    
    print("PASO 2: Transformación y generación de bases...")
    transform.run()
    
    print("\n" + "-" * 70 + "\n")
    
    print("PASO 3: Copia a Drive...")
    load.run()
    
    print("\n" + "=" * 70)
    print(" PIPELINE COMPLETADO")
    print("=" * 70)


if __name__ == "__main__":
    run_pipeline()

