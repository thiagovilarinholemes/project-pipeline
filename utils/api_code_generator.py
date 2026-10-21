"""
Gerador de código PySpark para APIs REST.

Este módulo recebe um DataFrame produzido pelo DataLens e gera
trechos de código que podem ser copiados para o pipeline da API:

1. campos_selecionados
2. SCHEMA_BRONZE
3. df_silver

O módulo NÃO contém regras de negócio da Silver/Gold.
Ele apenas descreve a estrutura encontrada na API.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd


# ============================================================================
# UTILITÁRIOS
# ============================================================================

def _is_null(value: Any) -> bool:
    """Verifica se um valor deve ser considerado nulo."""

    if value is None:
        return True

    try:
        result = pd.isna(value)

        if isinstance(result, bool):
            return result

        return False

    except (TypeError, ValueError):
        return False


def _normalizar_valor(value: Any) -> Any:
    """
    Converte valores específicos do pandas/numpy para tipos Python.
    """

    if _is_null(value):
        return None

    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            pass

    return value


def _obter_amostras(series: pd.Series) -> list[Any]:
    """Obtém valores não nulos de uma coluna."""

    valores = []

    for value in series.tolist():
        value = _normalizar_valor(value)

        if value is not None:
            valores.append(value)

    return valores


# ============================================================================
# INFERÊNCIA DE TIPOS
# ============================================================================

def infer_spark_type(values: list[Any]) -> str:
    """
    Infere o tipo Spark a partir dos valores encontrados.

    Retorna o código Spark correspondente.
    """

    values = [
        _normalizar_valor(value)
        for value in values
        if _normalizar_valor(value) is not None
    ]

    if not values:
        return "StringType()"

    # ------------------------------------------------------------------------
    # Boolean
    # ------------------------------------------------------------------------

    if all(isinstance(value, bool) for value in values):
        return "BooleanType()"

    # ------------------------------------------------------------------------
    # Inteiros
    # ------------------------------------------------------------------------

    if all(
        isinstance(value, int)
        and not isinstance(value, bool)
        for value in values
    ):
        return "LongType()"

    # ------------------------------------------------------------------------
    # Números decimais
    # ------------------------------------------------------------------------

    if all(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        for value in values
    ):
        return "DoubleType()"

    # ------------------------------------------------------------------------
    # Strings
    # ------------------------------------------------------------------------

    if all(isinstance(value, str) for value in values):
        return "StringType()"

    # ------------------------------------------------------------------------
    # Listas
    # ------------------------------------------------------------------------

    if all(isinstance(value, (list, tuple)) for value in values):
        elementos = []

        for lista in values:
            elementos.extend(lista)

        elementos = [
            _normalizar_valor(value)
            for value in elementos
            if _normalizar_valor(value) is not None
        ]

        if not elementos:
            return "ArrayType(StringType())"

        tipo_elemento = infer_spark_type(elementos)

        return f"ArrayType({tipo_elemento})"

    # ------------------------------------------------------------------------
    # Objetos / Structs
    # ------------------------------------------------------------------------

    if all(isinstance(value, dict) for value in values):
        return generate_struct_type(values)

    # ------------------------------------------------------------------------
    # Fallback
    # ------------------------------------------------------------------------

    return "StringType()"


def generate_struct_type(values: list[dict]) -> str:
    """
    Gera um StructType recursivo para uma lista de objetos JSON.
    """

    campos: dict[str, list[Any]] = {}

    for obj in values:
        if not isinstance(obj, dict):
            continue

        for key, value in obj.items():
            campos.setdefault(str(key), []).append(value)

    linhas = []

    for nome_campo, valores in campos.items():
        tipo = infer_spark_type(valores)

        linhas.append(
            f'        StructField("{nome_campo}", {tipo}, True),'
        )

    if not linhas:
        return "StructType([])"

    return "StructType([\n" + "\n".join(linhas) + "\n    ])"


# ============================================================================
# CAMPOS
# ============================================================================

def generate_selected_fields(fields: list[str]) -> str:
    """
    Gera o código da variável campos_selecionados.
    """

    linhas = [
        "campos_selecionados = ["
    ]

    for field in fields:
        linhas.append(f'    "{field}",')

    linhas.append("]")

    return "\n".join(linhas)


# ============================================================================
# SCHEMA BRONZE
# ============================================================================

def generate_schema_bronze(df: pd.DataFrame) -> str:
    """
    Gera um SCHEMA_BRONZE baseado no DataFrame analisado pelo DataLens.

    Para estruturas simples, gera StructField diretamente.

    Para listas de objetos, gera ArrayType(StructType(...)).

    Para objetos, gera StructType(...).
    """

    linhas = [
        "SCHEMA_BRONZE = StructType(["
    ]

    for column in df.columns:
        nome = str(column)
        valores = _obter_amostras(df[column])
        tipo = infer_spark_type(valores)

        if tipo.startswith("StructType"):
            linhas.append(f'    StructField("{nome}", {tipo}, True),')
        elif tipo.startswith("ArrayType"):
            linhas.append(f'    StructField("{nome}", {tipo}, True),')
        else:
            linhas.append(
                f'    StructField("{nome}", {tipo}, True),'
            )

    linhas.append("])")

    return "\n".join(linhas)


# ============================================================================
# DF SILVER
# ============================================================================

def _gerar_coluna_silver(nome: str, tipo: str) -> str:
    """
    Gera uma expressão col(...) para uma coluna simples.
    """

    if tipo == "StringType()":
        return (
            f'    limpar_texto(col("{nome}")).alias("{nome}"),'
        )

    if tipo == "LongType()":
        return (
            f'    col("{nome}").cast("long").alias("{nome}"),'
        )

    if tipo == "DoubleType()":
        return (
            f'    col("{nome}").cast("double").alias("{nome}"),'
        )

    if tipo == "BooleanType()":
        return (
            f'    col("{nome}").cast("boolean").alias("{nome}"),'
        )

    return (
        f'    col("{nome}").alias("{nome}"),'
    )


def generate_df_silver(
    df: pd.DataFrame,
    selected_fields: list[str],
) -> str:
    """
    Gera o bloco df_silver.

    O gerador preserva os nomes originais da API.

    Ele NÃO tenta criar nomes de negócio como:
        title -> titulo
        price -> preco
        rating -> avaliacao

    Esses aliases continuam sendo responsabilidade do pipeline
    específico da API.
    """

    linhas = [
        "df_silver = df_bronze.select("
    ]

    for field in selected_fields:

        if field not in df.columns:
            continue

        valores = _obter_amostras(df[field])
        tipo = infer_spark_type(valores)

        linhas.append(
            _gerar_coluna_silver(field, tipo)
        )

    linhas.append(")")

    return "\n".join(linhas)

# ============================================================================
# GERAÇÃO PRIMEIRO A GRAVAR
# ============================================================================
def generate_first_records(df, selected_fields):
    """
    Gera o bloco PRIMEIROS REGISTROS para o pipeline Bronze.
    """

    linhas = [
        'print(f"\\n{AZUL}PRIMEIROS REGISTROS{RESET}")',
        'print("="*70)',
        'df_field.select(',
    ]

    for field in selected_fields:

        # Campo simples:
        #
        # id
        # title
        # price
        #
        # vira:
        #
        # col("id").alias("id")

        if field in df.columns:

            alias = field.split(".")[-1]

            linhas.append(
                f'    col("{field}").alias("{alias}"),'
            )

    linhas.extend(
        [
            ').show(2, truncate=20)',
        ]
    )

    return "\n".join(linhas)

# ============================================================================
# GERAÇÃO COMPLETA
# ============================================================================

def generate_api_code(
    df: pd.DataFrame,
    selected_fields: list[str],
) -> dict[str, str]:
    """
    Gera todos os blocos de código do DataLens.
    """

    selected_fields = [
        str(field)
        for field in selected_fields
        if str(field) in df.columns
    ]

    return {
        "campos_selecionados": generate_selected_fields(
            selected_fields
        ),
        "schema_bronze": generate_schema_bronze(
            df[selected_fields]
        ),
        "df_silver": generate_df_silver(
            df,
            selected_fields,
        ),
        "first_records": generate_first_records(
            df,
            selected_fields,
        ),
    }