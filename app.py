
"""Ponto de entrada do DataLens Studio."""

import streamlit as st

from components.sidebar import render_sidebar
from components.explorer import render_explorer
from components.pipeline import pipeline

# ============================================================
# CONFIGURAÇÃO DA PÁGINA
# ============================================================
st.markdown('')
st.set_page_config(
    page_title="DataLens Studio",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# ESTILOS
# ============================================================

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 2rem;
        }

        .main-title {
            font-size: 2.2rem;
            font-weight: 700;
            margin-bottom: 0.2rem;
        }

        .subtitle {
            color: #8b949e;
            margin-bottom: 1.5rem;
        }

        div[data-testid="stMetric"] {
            border: 1px solid rgba(128, 128, 128, 0.25);
            padding: 12px;
            border-radius: 10px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# ESTADO DA SESSÃO
# ============================================================

if "loaded_df" not in st.session_state:
    st.session_state["loaded_df"] = None

if "loaded_source" not in st.session_state:
    st.session_state["loaded_source"] = None


# ============================================================
# CABEÇALHO
# ============================================================

st.markdown(
    '<div class="main-title">🔎 DataLens Studio</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="subtitle">'
    "Leitura, inspeção e exploração de dados."
    "</div>",
    unsafe_allow_html=True,
)


# ============================================================
# CARREGAMENTO DA FONTE DE DADOS
# ============================================================

render_sidebar()


# ============================================================
# EXPLORAÇÃO DOS DADOS
# ============================================================

df = st.session_state.get("loaded_df")
source = st.session_state.get("loaded_source")

if df is not None:
    render_explorer(df, source)

else:
    st.info(
        "Selecione uma fonte de dados na barra lateral para começar."
    )

    st.markdown("### 🚀 Teste rápido com a DummyJSON")

    st.write(
        "Selecione API REST na barra lateral para consultar os produtos. "
        "Os registros serão extraídos da chave `products`."
    )

    st.code(
        "https://dummyjson.com/products?limit=1",
        language="text",
    )

# ============================================================
# PIPELINE DOS DADOS
# ============================================================
# Foi removido devido a diferentes validações de acordo a cada API
# pipeline()