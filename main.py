"""
Pipeline multicountry: ICP + WHOIS + tablas finales.
"""
from scripts import calculate_icp, enrich, split_tables


def run_pipeline() -> None:
    """Ejecuta el flujo multicountry completo."""
    print("\n" + "=" * 70)
    print(" PIPELINE MULTICOUNTRY - ICP + WHOIS")
    print("=" * 70 + "\n")

    print("PASO 1: Calculo ICP (COL/ECU/PER)...")
    calculate_icp.run(include_colombia=True, include_ecuador=True, include_peru=True)

    print("\n" + "-" * 70 + "\n")

    print("PASO 2: Enriquecimiento WHOIS (rango ICP 1.000 a 100.000)...")
    enrich.run(only_icp=True, min_max_accesos=1000, max_max_accesos=100000)

    print("\n" + "-" * 70 + "\n")

    print("PASO 3: Separacion de tablas (empresas y leads)...")
    split_tables.run()

    print("\n" + "=" * 70)
    print(" PIPELINE COMPLETADO")
    print("=" * 70)


if __name__ == "__main__":
    run_pipeline()

