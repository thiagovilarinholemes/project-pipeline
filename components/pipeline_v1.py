import streamlit as st
import streamlit.components.v1 as components


def pipeline():
    st.divider()
    st.markdown("#### 🗐 Pipeline dos Dados")
    # ========================================================
    # BOTÕES PIPELINE
    # ========================================================
    # col_bronze, col_silver, col_gold = st.columns(3)

    # with col_bronze:

    #     st.button(
    #         "Pipeline Bronze 📦",
    #         key="pipeline_bronze",
    #         use_container_width=True,
    #         # on_click=select_all_fields,
    #     )

    # with col_silver:

    #     st.button(
    #         "Pipeline Silver 🧹",
    #         key="pipeline_silver",
    #         use_container_width=True,
    #         # on_click=clear_selected_fields,
    #     )

    # with col_gold:

    #     st.button(
    #         "Pipeline Gold 📊",
    #         key="pipeline_gold",
    #         use_container_width=True,
    #         # on_click=clear_selected_fields,
    #     )

    components.html(
                    f"""
        <div>
            <div style="
                font-family: sans-serif;
                width: 100%;
                display: flex;
                gap: 8px;
            ">

                <button
                    id="pipeline-bronze"
                    style="
                        flex: 1;
                        width: 0;
                        padding: 10px 14px;
                        border: none;
                        border-radius: 8px;
                        background: rgb(205, 127, 50);
                        color: white;
                        font-size: 15px;
                        font-weight: 600;
                        cursor: pointer;
                    "
                >
                    Pipeline Bronze 📦
                </button>

                <button
                    id="pipeline-silver"
                    style="
                        flex: 1;
                        width: 0;
                        padding: 10px 14px;
                        border: none;
                        border-radius: 8px;
                        background: #64748b;
                        color: white;
                        font-size: 15px;
                        font-weight: 600;
                        cursor: pointer;
                    "
                >
                    Pipeline Silver 📦
                </button>

                <button
                    id="pipeline-gold"
                    style="
                        flex: 1;
                        width: 0;
                        padding: 10px 14px;
                        border: none;
                        border-radius: 8px;
                        background: #e1ad01;
                        color: white;
                        font-size: 15px;
                        font-weight: 600;
                        cursor: pointer;
                    "
                >
                    Pipeline Gold 📊
                </button>

            </div>
        </div>
                    """
    )