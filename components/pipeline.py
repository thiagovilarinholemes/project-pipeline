"""Componentes para execução dos pipelines de dados."""

import os
import subprocess
import sys
from pathlib import Path

import streamlit as st


# ============================================================
# CAMINHOS DO PROJETO
# ============================================================

# Diretório raiz do projeto.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Script responsável pela extração Bronze.
BRONZE_SCRIPT = PROJECT_ROOT / "src" / "01_api_bronze.py"

# Diretório padrão para armazenamento dos dados Bronze.
DEFAULT_BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"


# ============================================================
# ESTILOS DOS BOTÕES
# ============================================================

def aplicar_estilos():
    """Aplica cores aos botões do pipeline."""

    st.markdown(
        """
        <style>
            div.st-key-pipeline_bronze button {
                background-color: rgb(205, 127, 50) !important;
                color: white !important;
                border: none !important;
            }

            div.st-key-pipeline_silver button {
                background-color: #64748b !important;
                color: white !important;
                border: none !important;
            }

            div.st-key-pipeline_gold button {
                background-color: #e1ad01 !important;
                color: white !important;
                border: none !important;
            }

            div.st-key-pipeline_bronze button:hover,
            div.st-key-pipeline_silver button:hover,
            div.st-key-pipeline_gold button:hover {
                filter: brightness(0.90);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# EXECUÇÃO DO PIPELINE BRONZE
# ============================================================
def executar_pipeline_bronze():
    """Executa o pipeline Bronze dentro do contêiner PySpark."""

    import subprocess

    from pathlib import Path

    # Raiz do projeto que contém o docker-compose.yml.
    project_root = Path(__file__).resolve().parents[1]

    # Recupera a URL informada na barra lateral.
    api_url = st.session_state.get("api_url", "").strip()

    if not api_url:
        st.error("Informe a URL da API na barra lateral.")
        return

    # Verifica se o Docker Compose está configurado no projeto.
    compose_file = project_root / "docker-compose.yml"

    if not compose_file.is_file():
        compose_file = project_root / "compose.yaml"

    if not compose_file.is_file():
        st.error(
            "Arquivo docker-compose.yml ou compose.yaml "
            f"não encontrado em: {project_root}"
        )
        return

    # Caminho persistente, compartilhado pelo volume ./output.
    bronze_path = "/app/output/bronze"

    # Monta o comando para executar o script dentro do serviço PySpark.
    comando = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "run",
        "--rm",
        "-T",
        "-e",
        f"API_URL={api_url}",
        "-e",
        f"BRONZE_PATH={bronze_path}",
        "pyspark",
        "python",
        "/app/src/01_api_bronze.py",
    ]

    try:
        with st.spinner(
            "Executando o pipeline Bronze no contêiner PySpark..."
        ):
            resultado = subprocess.run(
                comando,
                cwd=str(project_root),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=1800,
                check=False,
            )

        # Exibe a saída produzida pelo Spark.
        if resultado.stdout.strip():
            st.code(resultado.stdout, language="text")

        # Exibe os avisos e os erros do contêiner.
        if resultado.stderr.strip():
            st.code(resultado.stderr, language="text")

        if resultado.returncode == 0:
            st.success("Pipeline Bronze executado com sucesso!")
        else:
            st.error(
                "Falha na execução do pipeline Bronze. "
                f"Código de saída: {resultado.returncode}"
            )

    except subprocess.TimeoutExpired:
        st.error("O pipeline excedeu o limite de 30 minutos.")

    except FileNotFoundError:
        st.error(
            "Docker não foi encontrado. Verifique se o Docker Desktop "
            "está instalado e em execução."
        )

    except OSError as exc:
        st.error(f"Erro ao executar o Docker Compose: {exc}")

# ============================================================
# EXECUÇÃO DO PIPELINE SILVER
# ============================================================
def executar_pipeline_silver():
    """Executa a transformação Bronze → Silver dentro do Docker."""

    import subprocess
    from pathlib import Path

    project_root = Path(__file__).resolve().parents[1]

    # URL utilizada pelo script para validar a configuração.
    api_url = st.session_state.get("api_url", "").strip()

    if not api_url:
        st.error("Informe a URL da API na barra lateral.")
        return

    # Localiza o arquivo Docker Compose.
    compose_file = project_root / "docker-compose.yml"

    if not compose_file.is_file():
        compose_file = project_root / "compose.yaml"

    if not compose_file.is_file():
        st.error(
            f"Arquivo Docker Compose não encontrado em {project_root}"
        )
        return

    # Caminhos compartilhados entre o Windows e o contêiner.
    bronze_path = "/app/output/bronze"
    silver_path = "/app/output/silver"

    # Confirma que o JSON produzido pela Bronze existe.
    verificar_bronze = [
        "docker", "compose",
        "-f", str(compose_file),
        "run", "--rm", "-T",
        "pyspark",
        "python", "-c",
        (
            "from pathlib import Path; "
            "p = Path('/app/output/bronze/dummyjson.json'); "
            "print(f'Arquivo Bronze: {p}'); "
            "print(f'Existe: {p.is_file()}'); "
            "print(f'Tamanho: {p.stat().st_size if p.is_file() else 0} bytes'); "
            "raise SystemExit(0 if p.is_file() else 1)"
        ),
    ]

    try:
        with st.spinner("Verificando os dados da camada Bronze..."):
            bronze_resultado = subprocess.run(
                verificar_bronze,
                cwd=str(project_root),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=120,
                check=False,
            )

        if bronze_resultado.stdout.strip():
            st.code(bronze_resultado.stdout, language="text")

        if bronze_resultado.returncode != 0:
            st.error(
                "O JSON da Bronze não foi encontrado no caminho esperado. "
                "Execute primeiro o Pipeline Bronze."
            )

            if bronze_resultado.stderr.strip():
                st.code(bronze_resultado.stderr, language="text")

            return

        # Executa a transformação dentro do contêiner PySpark.
        comando = [
            "docker", "compose",
            "-f", str(compose_file),
            "run", "--rm", "-T",
            "-e", f"API_URL={api_url}",
            "-e", f"BRONZE_PATH={bronze_path}",
            "-e", f"SILVER_PATH={silver_path}",
            "pyspark",
            "python", "/app/src/02_api_silver.py",
        ]

        with st.spinner("Executando a transformação Bronze → Silver..."):
            resultado = subprocess.run(
                comando,
                cwd=str(project_root),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=1800,
                check=False,
            )

        if resultado.stdout.strip():
            st.code(resultado.stdout, language="text")

        if resultado.stderr.strip():
            st.code(resultado.stderr, language="text")

        if resultado.returncode != 0:
            st.error(
                "Falha no Pipeline Silver. "
                f"Código de saída: {resultado.returncode}"
            )
            return

        # Confere se a camada Silver contém arquivos Parquet.
        verificar_silver = [
            "docker", "compose",
            "-f", str(compose_file),
            "run", "--rm", "-T",
            "pyspark",
            "python", "-c",
            (
                "from pathlib import Path; "
                "p = Path('/app/output/silver'); "
                "arquivos = list(p.rglob('*.parquet')) if p.exists() else []; "
                "print(f'Diretório Silver: {p}'); "
                "print(f'Arquivos Parquet: {len(arquivos)}'); "
                "print('\\n'.join(str(a) for a in arquivos)); "
                "raise SystemExit(0 if arquivos else 1)"
            ),
        ]

        conferenca = subprocess.run(
            verificar_silver,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=120,
            check=False,
        )

        if conferenca.stdout.strip():
            st.code(conferenca.stdout, language="text")

        if conferenca.returncode == 0:
            st.success("Pipeline Silver executado com sucesso!")
        else:
            st.error(
                "O script terminou sem erro, mas não foram encontrados "
                "arquivos Parquet no diretório Silver esperado."
            )

            if conferenca.stderr.strip():
                st.code(conferenca.stderr, language="text")

    except subprocess.TimeoutExpired:
        st.error("O Pipeline Silver excedeu o limite de tempo.")

    except FileNotFoundError:
        st.error(
            "Docker não foi encontrado. Verifique se o Docker Desktop "
            "está em execução."
        )

    except OSError as exc:
        st.error(f"Erro ao executar o Pipeline Silver: {exc}")
# ============================================================
# INTERFACE DOS PIPELINES
# ============================================================

def pipeline():
    """Exibe os botões dos pipelines Bronze, Silver e Gold."""

    st.divider()
    st.markdown("#### 🗐 Pipeline dos Dados")

    aplicar_estilos()

    coluna_bronze, coluna_silver, coluna_gold = st.columns(3)

    with coluna_bronze:
        if st.button(
            "Pipeline Bronze 📦",
            key="pipeline_bronze",
            use_container_width=True,
        ):
            executar_pipeline_bronze()

    with coluna_silver:
        if st.button(
            "Pipeline Silver 📦",
            key="pipeline_silver",
            use_container_width=True,
        ):
            executar_pipeline_silver()

    with coluna_gold:
        if st.button(
            "Pipeline Gold 📊",
            key="pipeline_gold",
            use_container_width=True,
        ):
            st.info("Pipeline Gold ainda não foi conectado.")