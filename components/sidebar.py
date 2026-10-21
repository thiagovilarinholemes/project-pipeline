"""Componentes da barra lateral e carregamento de dados."""

import json

import streamlit as st

from utils.data_loader import (
    load_api,
    load_sql_query,
    read_uploaded_file,
)


def render_sidebar():
    """Exibe a barra lateral e permite carregar dados de diferentes fontes."""

    st.sidebar.header("📂 Fonte de dados")

    source_type = st.sidebar.radio(
        "Selecione a origem",
        options=[
            "Arquivo",
            "API REST",
            "Banco SQL",
        ],
        key="source_type",
    )

    # --------------------------------------------------------
    # CARREGAMENTO DE ARQUIVO
    # --------------------------------------------------------
    if source_type == "Arquivo":
        uploaded_file = st.sidebar.file_uploader(
            "Selecione um arquivo",
            type=["csv", "txt", "xlsx", "xls", "json", "parquet"],
            key="uploaded_file",
        )

        if uploaded_file is not None:
            if st.sidebar.button(
                "Carregar arquivo",
                use_container_width=True,
            ):
                try:
                    df = read_uploaded_file(uploaded_file)

                    st.session_state["loaded_df"] = df
                    st.session_state["loaded_source"] = (
                        f"Arquivo: {uploaded_file.name}"
                    )

                    st.sidebar.success("Arquivo carregado com sucesso!")
                except Exception as exc:
                    st.sidebar.error(
                        f"Não foi possível carregar o arquivo: {exc}"
                    )

    # --------------------------------------------------------
    # CARREGAMENTO DE API REST
    # --------------------------------------------------------
    elif source_type == "API REST":
        api_url = st.sidebar.text_input(
            "URL da API",
            value="",
            key="api_url",
        )

        api_method = st.sidebar.selectbox(
            "Método HTTP",
            options=["GET", "POST"],
            key="api_method",
        )

        headers_text = st.sidebar.text_area(
            "Headers JSON (opcional)",
            value="{}",
            key="api_headers",
        )

        body_text = "{}"

        if api_method == "POST":
            body_text = st.sidebar.text_area(
                "Body JSON",
                value="{}",
                key="api_body",
            )

        if st.sidebar.button(
            "Consultar API",
            use_container_width=True,
        ):
            try:
                headers = json.loads(headers_text or "{}")
                body = json.loads(body_text or "{}")

                if not isinstance(headers, dict):
                    raise ValueError(
                        "Os headers precisam ser um objeto JSON."
                    )

                if not isinstance(body, dict):
                    raise ValueError(
                        "O body precisa ser um objeto JSON."
                    )

                df = load_api(
                    url=api_url,
                    method=api_method,
                    headers=headers,
                    body=body if api_method == "POST" else None,
                    timeout=30,
                )

                st.session_state["loaded_df"] = df
                st.session_state["loaded_source"] = (
                    f"API REST ({api_method}): {api_url}"
                )

                st.sidebar.success("Dados recebidos da API!")

            except (ValueError, json.JSONDecodeError) as exc:
                st.sidebar.error(f"JSON inválido: {exc}")
            except Exception as exc:
                st.sidebar.error(f"Falha ao consultar a API: {exc}")

    # --------------------------------------------------------
    # CARREGAMENTO DE BANCO SQL
    # --------------------------------------------------------
    elif source_type == "Banco SQL":
        connection_url = st.sidebar.text_input(
            "URL de conexão SQLAlchemy",
            placeholder="sqlite:///dados.db",
            key="sql_connection_url",
        )

        query = st.sidebar.text_area(
            "Consulta SQL",
            value="SELECT * FROM sua_tabela",
            key="sql_query",
        )

        if st.sidebar.button(
            "Executar consulta",
            use_container_width=True,
        ):
            try:
                if not connection_url.strip():
                    raise ValueError(
                        "Informe a URL de conexão com o banco."
                    )

                if not query.strip():
                    raise ValueError(
                        "Informe uma consulta SQL."
                    )

                df = load_sql_query(
                    connection_url=connection_url,
                    query=query,
                )

                st.session_state["loaded_df"] = df
                st.session_state["loaded_source"] = (
                    "Banco SQL"
                )

                st.sidebar.success("Consulta executada com sucesso!")

            except Exception as exc:
                st.sidebar.error(f"Falha na consulta SQL: {exc}")

    st.sidebar.divider()

    st.sidebar.caption(
        "DataLens Studio — exploração e qualidade de dados"
    )

    # Retorna os dados já carregados, inclusive em novas execuções.
    return (
        st.session_state.get("loaded_df"),
        st.session_state.get("loaded_source", ""),
    )