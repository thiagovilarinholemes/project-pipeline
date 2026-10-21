"""
Componentes de visualização, qualidade e exploração de dados
do DataLens Studio.
"""

import json

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from utils.api_code_generator import generate_api_code

from utils.data_profile import (
    count_duplicate_rows,
    json_safe,
    profile_dataframe,
)

from utils.exporter import (
    create_csv,
    create_json,
    create_parquet,
)


def _render_copy_button(
    text: str,
    button_label: str,
    status_message: str,
    key_suffix: str,
):
    """
    Cria um botão HTML para copiar um bloco de texto para o clipboard.

    O componente recebe uma chave única para evitar conflito entre
    os três botões de cópia existentes na página.
    """

    text_json = json.dumps(
        text,
        ensure_ascii=False,
    )

    button_id = f"copy-button-{key_suffix}"
    status_id = f"copy-status-{key_suffix}"

    components.html(
        f"""
        <div style="
            font-family: sans-serif;
            width: 100%;
        ">

            <button
                id="{button_id}"
                style="
                    width: 100%;
                    padding: 10px 14px;
                    border: none;
                    border-radius: 8px;
                    background: #2563eb;
                    color: white;
                    font-size: 15px;
                    font-weight: 600;
                    cursor: pointer;
                "
            >
                {button_label}
            </button>

            <div
                id="{status_id}"
                role="status"
                style="
                    margin-top: 8px;
                    font-size: 13px;
                "
            ></div>

        </div>

        <script>
            const copyText = {text_json};

            const button = document.getElementById(
                "{button_id}"
            );

            const status = document.getElementById(
                "{status_id}"
            );

            button.addEventListener(
                "click",
                async () => {{

                    if (!copyText.trim()) {{

                        status.textContent =
                            "Não há conteúdo para copiar.";

                        return;
                    }}

                    try {{

                        await navigator.clipboard.writeText(
                            copyText
                        );

                        status.textContent =
                            "{status_message}";

                    }} catch (error) {{

                        status.textContent =
                            "O navegador bloqueou a cópia. "
                            + "Use Ctrl+C no código acima.";

                    }}
                }}
            );
        </script>
        """,
        height=75,
    )


def _render_generated_code(
    df: pd.DataFrame,
    selected_fields: list[str],
):
    """
    Gera e exibe os três blocos de código necessários para o pipeline:

    1. campos_selecionados
    2. SCHEMA_BRONZE
    3. df_silver
    """

    if not selected_fields:

        st.info(
            "Selecione pelo menos um campo para gerar "
            "o código do pipeline."
        )

        return

    # ================================================================
    # GERAÇÃO
    # ================================================================

    try:

        generated_code = generate_api_code(
            df=df,
            selected_fields=selected_fields,
        )

    except Exception as exc:

        st.error(
            "Não foi possível gerar o código automaticamente."
        )

        st.exception(exc)

        return

    # ================================================================
    # TÍTULO
    # ================================================================

    st.markdown("---")

    st.subheader(
        "⚙️ Código gerado para o pipeline"
    )

    st.caption(
        "Os blocos abaixo são gerados com base nos campos "
        "selecionados e nos tipos identificados na API."
    )

    # ================================================================
    # 1. CAMPOS SELECIONADOS
    # ================================================================

    st.markdown(
        "#### 1️⃣ `campos_selecionados`"
    )

    st.caption(
        "Substitua o conteúdo da variável "
        "`campos_selecionados` no arquivo "
        "`src/01_api_bronze.py`."
    )

    st.code(
        generated_code["campos_selecionados"],
        language="python",
    )

    _render_copy_button(
        text=generated_code["campos_selecionados"],
        button_label="📋 Copiar campos_selecionados",
        status_message=(
            "campos_selecionados copiado com sucesso!"
        ),
        key_suffix="campos",
    )

    # ================================================================
    # 2. SCHEMA BRONZE
    # ================================================================

    st.markdown(
        "#### 2️⃣ `SCHEMA_BRONZE`"
    )

    st.caption(
        "Substitua o bloco `SCHEMA_BRONZE` no arquivo "
        "`src/02_api_silver.py`."
    )

    st.code(
        generated_code["schema_bronze"],
        language="python",
    )

    _render_copy_button(
        text=generated_code["schema_bronze"],
        button_label="📋 Copiar SCHEMA_BRONZE",
        status_message=(
            "SCHEMA_BRONZE copiado com sucesso!"
        ),
        key_suffix="schema",
    )

    # ================================================================
    # 3. DF SILVER
    # ================================================================

    st.markdown(
        "#### 3️⃣ `df_silver`"
    )

    

    st.caption(
        "Substitua o bloco `df_silver` no arquivo "
        "`src/02_api_silver.py`."
    )

    st.code(
        generated_code["df_silver"],
        language="python",
    )

    _render_copy_button(
        text=generated_code["df_silver"],
        button_label="📋 Copiar df_silver",
        status_message=(
            "df_silver copiado com sucesso!"
        ),
        key_suffix="silver",
    )

    # ================================================================
    # 4. PRIMEIROS REGISTROS
    # ================================================================

    st.markdown(
        "#### 4️⃣ `PRIMEIROS REGISTROS`"
    )

    st.caption(
        "Substitua o bloco de exibição dos primeiros registros "
        "no arquivo `src/01_api_bronze.py`."
    )

    st.code(
        generated_code["first_records"],
        language="python",
    )

    _render_copy_button(
        text=generated_code["first_records"],
        button_label="📋 Copiar PRIMEIROS REGISTROS",
        status_message=(
            "Bloco PRIMEIROS REGISTROS copiado com sucesso!"
        ),
        key_suffix="first-records",
    )

def render_explorer(
    df,
    source,
):
    """
    Renderiza os indicadores e as abas do DataLens Studio.
    """

    # ========================================================
    # IDENTIFICA O CONJUNTO DE DADOS ATUAL
    # ========================================================
    #
    # Quando um novo arquivo/API é carregado, criamos uma
    # assinatura diferente. Isso permite detectar a troca
    # dos dados e limpar a seleção anterior de campos.
    #

    data_signature = (
        str(source),
        tuple(
            str(column)
            for column in df.columns
        ),
        len(df),
    )

    previous_signature = (
        st.session_state.get(
            "datalens_data_signature"
        )
    )

    data_changed = (
        previous_signature != data_signature
    )

    if data_changed:

        st.session_state[
            "datalens_data_signature"
        ] = data_signature

        # Limpa a seleção do arquivo/API anterior.
        st.session_state.pop(
            "selected_fields_datalens",
            None,
        )

    # ========================================================
    # FONTE ATUAL
    # ========================================================

    st.caption(
        f"**Fonte atual:** {source}"
    )

    # ========================================================
    # INDICADORES GERAIS
    # ========================================================

    metric1, metric2, metric3, metric4 = (
        st.columns(4)
    )

    metric1.metric(
        "Linhas",
        f"{len(df):,}",
    )

    metric2.metric(
        "Colunas",
        f"{len(df.columns):,}",
    )

    metric3.metric(
        "Duplicatas",
        f"{count_duplicate_rows(df):,}",
    )

    metric4.metric(
        "Valores ausentes",
        f"{int(df.isna().sum().sum()):,}",
    )

    st.divider()

    # ========================================================
    # ABAS
    # ========================================================

    (
        tab_preview,
        tab_schema,
        tab_quality,
        tab_stats,
        tab_export,
    ) = st.tabs(
        [
            "👁️ Prévia",
            "📋 Schema e tipos",
            "🧹 Qualidade",
            "📊 Estatísticas",
            "📥 Exportar",
        ]
    )

    # ========================================================
    # PRÉVIA
    # ========================================================

    with tab_preview:

        st.subheader(
            "Amostra dos registros"
        )

        if df.empty:

            st.warning(
                "A fonte não retornou registros."
            )

        else:

            max_preview_rows = min(
                len(df),
                500,
            )

            if max_preview_rows > 1:

                preview_rows = st.slider(
                    "Quantidade de linhas para exibir",
                    min_value=1,
                    max_value=max_preview_rows,
                    value=min(
                        max_preview_rows,
                        20,
                    ),
                    key="preview_rows",
                )

            else:

                preview_rows = max_preview_rows

            st.dataframe(
                df.head(preview_rows),
                use_container_width=True,
                height=450,
            )

        # ====================================================
        # LISTA DE CAMPOS
        # ====================================================

        st.markdown(
            "#### 📋 Lista de campos"
        )

        fields = [
            str(column)
            for column in df.columns
        ]

        selection_key = (
            "selected_fields_datalens"
        )

        # ====================================================
        # CALLBACKS
        # ====================================================

        def select_all_fields():

            st.session_state[
                selection_key
            ] = fields.copy()

        def clear_selected_fields():

            st.session_state[
                selection_key
            ] = []

        # ====================================================
        # SELEÇÃO INICIAL
        # ====================================================
        #
        # Para um novo arquivo/API, todos os campos começam
        # selecionados.
        #

        if selection_key not in st.session_state:

            st.session_state[
                selection_key
            ] = fields.copy()

        else:

            # Mantém apenas os campos existentes no DataFrame.

            st.session_state[
                selection_key
            ] = [
                field
                for field in st.session_state[
                    selection_key
                ]
                if field in fields
            ]

        st.caption(
            f"{len(fields)} campos identificados. "
            "Selecione os campos que deseja utilizar "
            "no pipeline."
        )

        # ====================================================
        # MULTISELECT
        # ====================================================

        if fields:

            selected_fields = st.multiselect(
                "Selecione os campos",
                options=fields,
                key=selection_key,
                placeholder=(
                    "Escolha um ou mais campos..."
                ),
                help=(
                    "Selecione os campos que deseja "
                    "utilizar no pipeline."
                ),
            )

            # ====================================================
            # BOTÕES DE SELEÇÃO
            # ====================================================

            col_select_all, col_clear = (
                st.columns(2)
            )

            with col_select_all:

                st.button(
                    "☑️ Selecionar todos",
                    key="select_all_fields",
                    use_container_width=True,
                    on_click=select_all_fields,
                )

            with col_clear:

                st.button(
                    "🧹 Limpar seleção",
                    key="clear_selected_fields",
                    use_container_width=True,
                    on_click=clear_selected_fields,
                )

            # ====================================================
            # CONTADOR
            # ====================================================

            st.caption(
                f"{len(selected_fields)} de "
                f"{len(fields)} campos selecionados."
            )

            # ====================================================
            # CÓDIGO DO PIPELINE
            # ====================================================

            _render_generated_code(
                df=df,
                selected_fields=selected_fields,
            )

        else:

            st.info(
                "Nenhum campo disponível para selecionar."
            )

    # ========================================================
    # SCHEMA E TIPOS
    # ========================================================

    with tab_schema:

        st.subheader(
            "📋 Colunas identificadas"
        )

        schema_df = profile_dataframe(df)

        st.metric(
            "Total de colunas",
            len(df.columns),
        )

        st.dataframe(
            schema_df,
            use_container_width=True,
            height=450,
        )

        if len(df.columns) > 0:

            st.markdown(
                "#### Inspecionar uma coluna"
            )

            selected_column = st.selectbox(
                "Selecione uma coluna",
                options=list(df.columns),
                key="schema_selected_column",
            )

            series = df[selected_column]

            col1, col2, col3 = st.columns(3)

            col1.metric(
                "Tipo",
                str(series.dtype),
            )

            col2.metric(
                "Não nulos",
                int(series.notna().sum()),
            )

            try:

                unique_count = (
                    series
                    .map(json_safe)
                    .nunique(
                        dropna=True
                    )
                )

            except (
                TypeError,
                ValueError,
            ):

                unique_count = (
                    "Indisponível"
                )

            col3.metric(
                "Valores únicos",
                unique_count,
            )

            st.markdown(
                "#### Exemplos de valores"
            )

            def format_example(value):

                if isinstance(
                    value,
                    (dict, list, tuple),
                ):

                    return json.dumps(
                        value,
                        ensure_ascii=False,
                        default=str,
                    )

                return value

            examples = pd.DataFrame(
                {
                    selected_column: (
                        series
                        .head(20)
                        .map(format_example)
                    )
                }
            )

            st.dataframe(
                examples,
                use_container_width=True,
            )

    # ========================================================
    # QUALIDADE
    # ========================================================

    with tab_quality:

        st.subheader(
            "🧹 Indicadores de qualidade"
        )

        st.dataframe(
            profile_dataframe(df),
            use_container_width=True,
            height=450,
        )

        missing_df = (
            df.isna()
            .sum()
            .rename("Valores ausentes")
            .to_frame()
            .reset_index(
                names="Coluna"
            )
            .sort_values(
                "Valores ausentes",
                ascending=False,
            )
        )

        st.markdown(
            "#### Valores ausentes por coluna"
        )

        st.dataframe(
            missing_df,
            use_container_width=True,
        )

        st.markdown(
            "#### Linhas duplicadas"
        )

        duplicate_count = (
            count_duplicate_rows(df)
        )

        st.write(
            f"Foram identificadas "
            f"**{duplicate_count} linhas duplicadas**."
        )

    # ========================================================
    # ESTATÍSTICAS
    # ========================================================

    with tab_stats:

        st.subheader(
            "📊 Estatísticas descritivas"
        )

        if (
            not df.empty
            and len(df.columns) > 0
        ):

            try:

                stats_df = (
                    df
                    .describe(
                        include="all"
                    )
                    .transpose()
                )

                stats_df = stats_df.map(
                    lambda value: (
                        json.dumps(
                            value,
                            ensure_ascii=False,
                            default=str,
                        )
                        if isinstance(
                            value,
                            (dict, list, tuple),
                        )
                        else value
                    )
                )

                st.dataframe(
                    stats_df,
                    use_container_width=True,
                    height=450,
                )

            except Exception as exc:

                st.warning(
                    "Não foi possível gerar "
                    f"estatísticas: {exc}"
                )

            categorical = []

            for column in df.columns:

                try:

                    unique_count = (
                        df[column]
                        .map(json_safe)
                        .nunique(
                            dropna=True
                        )
                    )

                    if (
                        1
                        < unique_count
                        <= 50
                    ):

                        categorical.append(
                            column
                        )

                except (
                    TypeError,
                    ValueError,
                ):

                    continue

            if categorical:

                selected_category = (
                    st.selectbox(
                        "Coluna para visualizar "
                        "a distribuição",
                        options=categorical,
                        key=(
                            "stats_category_column"
                        ),
                    )
                )

                values = (
                    df[selected_category]
                    .map(json_safe)
                    .fillna("(ausente)")
                    .value_counts()
                    .head(20)
                )

                st.bar_chart(
                    values.rename(
                        "Quantidade"
                    ).to_frame()
                )

            else:

                st.info(
                    "Nenhuma coluna com até 50 "
                    "valores distintos foi encontrada "
                    "para a distribuição."
                )

        else:

            st.info(
                "Carregue dados com linhas e colunas "
                "para ver estatísticas."
            )

    # ========================================================
    # EXPORTAÇÃO
    # ========================================================

    with tab_export:

        st.subheader(
            "📥 Exportar dados"
        )

        # ----------------------------------------------------
        # CSV
        # ----------------------------------------------------

        st.download_button(
            "⬇️ Baixar CSV",
            data=create_csv(df),
            file_name="datalens_export.csv",
            mime="text/csv",
            use_container_width=True,
        )

        # ----------------------------------------------------
        # JSON
        # ----------------------------------------------------

        st.download_button(
            "⬇️ Baixar JSON",
            data=create_json(df),
            file_name="datalens_export.json",
            mime="application/json",
            use_container_width=True,
        )

        # ----------------------------------------------------
        # PARQUET
        # ----------------------------------------------------

        try:

            parquet_data = create_parquet(df)

            st.download_button(
                "⬇️ Baixar Parquet",
                data=parquet_data,
                file_name="datalens_export.parquet",
                mime="application/octet-stream",
                use_container_width=True,
            )

        except Exception as exc:

            st.info(
                "Não foi possível gerar Parquet. "
                "Verifique se o pyarrow está instalado. "
                f"Detalhe: {exc}"
            )