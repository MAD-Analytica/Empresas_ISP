"""
Dashboard one-shot para visualizar ISPs (sin foco en leads).

Ejecucion:
    streamlit run dashboard_isp.py
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

import config


st.set_page_config(page_title="ISP Dashboard", page_icon=":bar_chart:", layout="wide")


def _choose_first_existing(paths: list[Path]) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None


@st.cache_data(show_spinner=False)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    empresas_candidates = [
        config.FINAL_DATA_DIR / config.OUTPUT_EMPRESAS_TABLA_FILENAME,
        config.PROCESSED_DATA_DIR / config.OUTPUT_WHOIS_FILENAME,
        config.PROCESSED_DATA_DIR / config.OUTPUT_ICP_FILENAME,
    ]
    leads_candidates = [
        config.FINAL_DATA_DIR / config.OUTPUT_LEADS_FILENAME,
    ]

    empresas_path = _choose_first_existing(empresas_candidates)
    if empresas_path is None:
        raise FileNotFoundError(
            f"No se encontro dataset de ISPs. Busque en: {[str(p) for p in empresas_candidates]}"
        )

    empresas = pd.read_csv(empresas_path)
    leads_path = _choose_first_existing(leads_candidates)
    leads = pd.read_csv(leads_path) if leads_path else pd.DataFrame()
    return empresas, leads


def normalize_empresas(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "empresa" not in out.columns and "operador" in out.columns:
        out["empresa"] = out["operador"]
    if "id_empresa" not in out.columns and "id_operador" in out.columns:
        out["id_empresa"] = out["id_operador"]
    if "pais" not in out.columns:
        out["pais"] = "N/A"

    # Priorizamos max_accesos_2024_2025 como usuarios.
    if "max_accesos_2024_2025" in out.columns:
        out["usuarios"] = pd.to_numeric(out["max_accesos_2024_2025"], errors="coerce").fillna(0)
    elif "num_accesos" in out.columns:
        out["usuarios"] = pd.to_numeric(out["num_accesos"], errors="coerce").fillna(0)
    else:
        out["usuarios"] = 0

    out["empresa"] = out["empresa"].astype(str).str.strip()
    out["id_empresa"] = out["id_empresa"].astype(str).str.strip()
    out["pais"] = out["pais"].astype(str).str.strip().str.upper()
    out = out.loc[out["empresa"].ne("")].copy()
    return out


def build_filters(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str], tuple[int, int], str]:
    st.sidebar.header("Filtros")

    countries = sorted(df["pais"].dropna().unique().tolist())
    selected_countries = st.sidebar.multiselect("Pais", options=countries, default=countries)

    min_users = int(df["usuarios"].min()) if len(df) else 0
    max_users = int(df["usuarios"].max()) if len(df) else 0
    users_range = st.sidebar.slider(
        "Rango de usuarios",
        min_value=min_users,
        max_value=max_users if max_users > min_users else min_users + 1,
        value=(min_users, max_users if max_users > min_users else min_users + 1),
        step=max(1, (max_users - min_users) // 100 if max_users > min_users else 1),
    )

    search = st.sidebar.text_input("Buscar empresa (contiene)", value="").strip().lower()

    filtered = df.copy()
    if selected_countries:
        filtered = filtered.loc[filtered["pais"].isin(selected_countries)]
    filtered = filtered.loc[filtered["usuarios"].between(users_range[0], users_range[1])]
    if search:
        filtered = filtered.loc[filtered["empresa"].str.lower().str.contains(search, na=False)]

    return filtered, selected_countries, users_range, search


def count_leads(leads_df: pd.DataFrame, filtered_empresas: pd.DataFrame) -> int:
    if leads_df.empty:
        return 0
    if "id_empresa" not in leads_df.columns:
        return 0
    keys = filtered_empresas[["id_empresa", "pais"]].drop_duplicates()
    merged = leads_df.merge(keys, on=["id_empresa", "pais"], how="inner")
    if "nombre" in merged.columns:
        merged = merged.loc[merged["nombre"].astype(str).str.strip().ne("")]
    return len(merged)


def render_metrics(all_df: pd.DataFrame, filtered_df: pd.DataFrame, selected_countries: list[str], leads_df: pd.DataFrame) -> None:
    # Usuarios totales siempre globales (todos los paises), independiente de filtros.
    total_users_base = float(all_df["usuarios"].sum()) if len(all_df) else 0.0
    total_users_filtered = float(filtered_df["usuarios"].sum()) if len(filtered_df) else 0.0
    num_empresas = int(filtered_df["id_empresa"].nunique()) if len(filtered_df) else 0
    num_leads = count_leads(leads_df, filtered_df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Empresas", f"{num_empresas:,}")
    c2.metric("Usuarios", f"{int(total_users_base):,}")
    c3.metric("Leads identificados", f"{num_leads:,}")


def render_charts(filtered_df: pd.DataFrame) -> None:
    left, right = st.columns(2)

    with left:
        st.subheader("Top empresas por usuarios")
        top_n = min(20, len(filtered_df))
        top_df = (
            filtered_df.groupby("empresa", as_index=False)["usuarios"]
            .sum()
            .sort_values("usuarios", ascending=False)
            .head(top_n)
        )
        if top_df.empty:
            st.info("Sin datos para graficar.")
        else:
            st.bar_chart(top_df.set_index("empresa")["usuarios"])

    with right:
        st.subheader("Distribucion de usuarios por pais")
        by_country = (
            filtered_df.groupby("pais", as_index=False)["usuarios"].sum().sort_values("usuarios", ascending=False)
        )
        if by_country.empty:
            st.info("Sin datos para graficar.")
        else:
            fig, ax = plt.subplots(figsize=(6, 6))
            ax.pie(by_country["usuarios"], labels=by_country["pais"], autopct="%1.1f%%", startangle=90)
            ax.axis("equal")
            st.pyplot(fig)


def render_table(filtered_df: pd.DataFrame) -> None:
    st.subheader("Tabla de ISPs")
    cols = [
        "pais",
        "id_empresa",
        "empresa",
        "usuarios",
        "whois_asn",
        "whois_owner",
    ]
    cols = [c for c in cols if c in filtered_df.columns]
    table = filtered_df[cols].copy()
    table = table.sort_values(["usuarios", "pais", "empresa"], ascending=[False, True, True])
    st.dataframe(table, use_container_width=True, height=500)


def main() -> None:
    st.title("Dashboard ISPs")
    st.caption("Visualizacion pragmatica de empresas ISP (sin foco en tabla de leads).")

    try:
        empresas_raw, leads_raw = load_data()
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    empresas = normalize_empresas(empresas_raw)
    filtered, selected_countries, _, _ = build_filters(empresas)

    render_metrics(empresas, filtered, selected_countries, leads_raw)
    st.divider()
    render_charts(filtered)
    st.divider()
    render_table(filtered)


if __name__ == "__main__":
    main()

