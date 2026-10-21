"""
Camada Gold: indicadores genéricos sobre qualquer API REST.

A camada Gold NÃO assume nomes específicos de uma API.

Ela funciona com qualquer conjunto de colunas existente
na camada Silver.

Exemplos de Silver aceitos:

JSONPlaceholder:
    id
    name
    email
    address_street
    address_city
    company_name

DummyJSON:
    id
    title
    category
    price
    rating
    dimensions_width
    dimensions_height

Variáveis de ambiente:
- SILVER_PATH
- GOLD_PATH
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    lit,
    sum as spark_sum,
)
from pyspark.sql.types import NumericType


# ============================================================
# 1. CORES DO TERMINAL
# ============================================================

VERDE = "\033[92m"
RESET = "\033[0m"
AZUL = "\033[94m"
AMARELO = "\033[93m"


# ============================================================
# 2. CONFIGURAÇÕES
# ============================================================

SILVER_PATH = os.getenv("SILVER_PATH")
GOLD_PATH = os.getenv("GOLD_PATH")


if not SILVER_PATH:
    raise ValueError(
        "A variável de ambiente SILVER_PATH não foi definida."
    )


if not GOLD_PATH:
    raise ValueError(
        "A variável de ambiente GOLD_PATH não foi definida."
    )


SILVER_DIR = Path(SILVER_PATH)
GOLD_DIR = Path(GOLD_PATH)


# Cria o diretório Gold caso não exista.
GOLD_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


# ============================================================
# 3. INDICADORES GERAIS
# ============================================================

def criar_indicadores_gerais(df):
    """
    Cria indicadores gerais da camada Silver.

    Os indicadores são independentes dos nomes das colunas
    existentes na API.
    """

    total_registros = df.count()

    total_colunas = len(df.columns)

    # --------------------------------------------------------
    # Indicadores relacionados a registros
    # --------------------------------------------------------

    df_indicadores = df.agg(
        count("*").alias("total_registros"),
    )

    # --------------------------------------------------------
    # Adiciona quantidade de colunas
    # --------------------------------------------------------

    df_indicadores = df_indicadores.withColumn(
        "total_colunas",
        lit(total_colunas),
    )

    # --------------------------------------------------------
    # Quantidade de IDs
    # --------------------------------------------------------

    if "id" in df.columns:

        df_ids = df.agg(
            count("id").alias("ids_preenchidos"),
            countDistinct("id").alias("ids_distintos"),
        )

        df_indicadores = df_indicadores.crossJoin(
            df_ids
        )

    else:

        df_indicadores = (
            df_indicadores
            .withColumn(
                "ids_preenchidos",
                lit(None).cast("long"),
            )
            .withColumn(
                "ids_distintos",
                lit(None).cast("long"),
            )
        )

    # --------------------------------------------------------
    # Timestamp
    # --------------------------------------------------------

    df_indicadores = df_indicadores.withColumn(
        "processado_em",
        current_timestamp(),
    )

    return df_indicadores


# ============================================================
# 4. INDICADORES POR COLUNA
# ============================================================

def criar_indicadores_colunas(df):
    """
    Cria uma tabela de indicadores para cada coluna da Silver.

    Funciona independentemente dos nomes das colunas da API.

    Para cada coluna calcula:

    - nome da coluna
    - tipo
    - quantidade total de registros
    - valores preenchidos
    - valores nulos
    - valores distintos
    - média para colunas numéricas

    Para colunas de texto, a média fica NULL.
    """

    total_registros = df.count()

    resultados = []

    for campo in df.schema.fields:

        nome = campo.name
        tipo = campo.dataType.simpleString()

        # ====================================================
        # INDICADORES BÁSICOS
        # ====================================================

        agregacoes = [
            count("*").alias("total_registros"),
            count(col(nome)).alias("valores_preenchidos"),
            countDistinct(col(nome)).alias("valores_distintos"),
        ]

        # ====================================================
        # MÉDIA
        # ====================================================

        # A média precisa ser calculada no DataFrame original.
        # Não podemos calcular avg() depois do agg(), porque
        # a coluna original já não existirá no resultado.
        if isinstance(campo.dataType, NumericType):

            agregacoes.append(
                avg(col(nome)).alias("media")
            )

        # Executa todas as agregações diretamente sobre df.
        linha = df.agg(*agregacoes)

        # ====================================================
        # VALORES NULOS
        # ====================================================

        linha = linha.withColumn(
            "valores_nulos",
            col("total_registros")
            - col("valores_preenchidos"),
        )

        # ====================================================
        # PARA COLUNAS NÃO NUMÉRICAS
        # ====================================================

        if not isinstance(campo.dataType, NumericType):

            linha = linha.withColumn(
                "media",
                lit(None).cast("double"),
            )

        # ====================================================
        # METADADOS DA COLUNA
        # ====================================================

        linha = (
            linha
            .withColumn(
                "coluna",
                lit(nome),
            )
            .withColumn(
                "tipo",
                lit(tipo),
            )
        )

        # ====================================================
        # ORGANIZAÇÃO DAS COLUNAS
        # ====================================================

        linha = linha.select(
            "coluna",
            "tipo",
            "total_registros",
            "valores_preenchidos",
            "valores_nulos",
            "valores_distintos",
            "media",
        )

        resultados.append(linha)

    # ========================================================
    # VERIFICAÇÃO
    # ========================================================

    if not resultados:

        raise ValueError(
            "A camada Silver não possui colunas."
        )

    # ========================================================
    # UNE OS RESULTADOS
    # ========================================================

    df_resultado = resultados[0]

    for dataframe in resultados[1:]:

        df_resultado = df_resultado.unionByName(
            dataframe
        )

    # ========================================================
    # TIMESTAMP
    # ========================================================

    df_resultado = df_resultado.withColumn(
        "processado_em",
        current_timestamp(),
    )

    return df_resultado

# ============================================================
# 5. QUALIDADE DOS DADOS
# ============================================================

def criar_indicadores_qualidade(df):
    """
    Cria indicadores genéricos de qualidade.

    Não depende de nomes específicos de colunas.
    """

    total_registros = df.count()

    resultados = []

    for campo in df.schema.fields:

        nome = campo.name
        tipo = campo.dataType.simpleString()

        # ----------------------------------------------------
        # Valores preenchidos
        # ----------------------------------------------------

        valores_preenchidos = df.filter(
            col(nome).isNotNull()
        ).count()

        # ----------------------------------------------------
        # Valores nulos
        # ----------------------------------------------------

        valores_nulos = (
            total_registros
            - valores_preenchidos
        )

        # ----------------------------------------------------
        # Valores distintos
        # ----------------------------------------------------

        valores_distintos = df.select(
            col(nome)
        ).distinct().count()

        # ----------------------------------------------------
        # Percentual preenchido
        # ----------------------------------------------------

        if total_registros > 0:

            percentual_preenchido = (
                valores_preenchidos
                / total_registros
            ) * 100

        else:

            percentual_preenchido = 0.0

        # ----------------------------------------------------
        # DataFrame da coluna
        # ----------------------------------------------------

        linha = df.sparkSession.createDataFrame(
            [
                (
                    nome,
                    tipo,
                    total_registros,
                    valores_preenchidos,
                    valores_nulos,
                    valores_distintos,
                    percentual_preenchido,
                )
            ],
            [
                "coluna",
                "tipo",
                "total_registros",
                "valores_preenchidos",
                "valores_nulos",
                "valores_distintos",
                "percentual_preenchido",
            ],
        )

        resultados.append(linha)

    if not resultados:
        raise ValueError(
            "Não foi possível gerar indicadores de qualidade."
        )

    df_resultado = resultados[0]

    for dataframe in resultados[1:]:
        df_resultado = df_resultado.unionByName(
            dataframe
        )

    return df_resultado.withColumn(
        "processado_em",
        current_timestamp(),
    )


# ============================================================
# 6. PROCESSAMENTO PRINCIPAL
# ============================================================

def main():
    """
    Executa as transformações da camada Gold.
    """

    spark = (
        SparkSession.builder
        .appName("TutorialPySpark-API-Gold")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        # ====================================================
        # ETAPA A - LEITURA SILVER
        # ====================================================

        print(
            f"\n=== {AZUL}"
            "ETAPA 1: LEITURA SILVER"
            f"{RESET} ==="
        )

        if not SILVER_DIR.exists():

            raise FileNotFoundError(
                f"Diretório Silver não encontrado: "
                f"{SILVER_DIR}"
            )

        df_silver = spark.read.parquet(
            str(SILVER_DIR)
        )

        total_registros = df_silver.count()

        if total_registros == 0:

            raise ValueError(
                "A camada Silver está vazia."
            )

        print(
            f"Total de registros Silver: "
            f"{VERDE}{total_registros}{RESET}"
        )

        print(
            f"Total de colunas Silver: "
            f"{VERDE}{len(df_silver.columns)}{RESET}"
        )

        print("\nColunas encontradas:")

        for coluna in df_silver.columns:
            print(f"- {coluna}")

        print(
            f"\n{AZUL}"
            "SCHEMA SILVER"
            f"{RESET}"
        )

        df_silver.printSchema()

        # ====================================================
        # ETAPA B - INDICADORES GERAIS
        # ====================================================

        print(
            f"\n=== {AZUL}"
            "ETAPA 2: INDICADORES GERAIS"
            f"{RESET} ==="
        )

        df_indicadores = criar_indicadores_gerais(
            df_silver
        )

        df_indicadores.show(
            truncate=False
        )

        # ====================================================
        # ETAPA C - INDICADORES POR COLUNA
        # ====================================================

        print(
            f"\n=== {AZUL}"
            "ETAPA 3: INDICADORES POR COLUNA"
            f"{RESET} ==="
        )

        df_por_coluna = criar_indicadores_colunas(
            df_silver
        )

        df_por_coluna.show(
            truncate=False
        )

        # ====================================================
        # ETAPA D - QUALIDADE
        # ====================================================

        print(
            f"\n=== {AZUL}"
            "ETAPA 4: QUALIDADE DOS DADOS"
            f"{RESET} ==="
        )

        df_qualidade = criar_indicadores_qualidade(
            df_silver
        )

        df_qualidade.show(
            truncate=False
        )

        # ====================================================
        # ETAPA E - GRAVAÇÃO GOLD
        # ====================================================

        print(
            f"\n=== {AZUL}"
            "ETAPA 5: GRAVAÇÃO GOLD"
            f"{RESET} ==="
        )

        caminho_indicadores = (
            GOLD_DIR / "indicadores_gerais"
        )

        caminho_colunas = (
            GOLD_DIR / "indicadores_por_coluna"
        )

        caminho_qualidade = (
            GOLD_DIR / "qualidade_dados"
        )

        # ----------------------------------------------------
        # Indicadores gerais
        # ----------------------------------------------------

        (
            df_indicadores.write
            .mode("overwrite")
            .parquet(
                str(caminho_indicadores)
            )
        )

        # ----------------------------------------------------
        # Indicadores por coluna
        # ----------------------------------------------------

        (
            df_por_coluna.write
            .mode("overwrite")
            .parquet(
                str(caminho_colunas)
            )
        )

        # ----------------------------------------------------
        # Qualidade
        # ----------------------------------------------------

        (
            df_qualidade.write
            .mode("overwrite")
            .parquet(
                str(caminho_qualidade)
            )
        )

        print(
            f"\nGold gravada em: "
            f"{VERDE}{GOLD_DIR}{RESET}"
        )

        # ====================================================
        # ETAPA F - CONFERÊNCIA
        # ====================================================

        print(
            f"\n=== {AZUL}"
            "ETAPA 6: CONFERÊNCIA FINAL"
            f"{RESET} ==="
        )

        tabelas_gold = {

            "indicadores_gerais":
                caminho_indicadores,

            "indicadores_por_coluna":
                caminho_colunas,

            "qualidade_dados":
                caminho_qualidade,
        }

        for nome, caminho in tabelas_gold.items():

            df_salvo = spark.read.parquet(
                str(caminho)
            )

            quantidade = df_salvo.count()

            print(
                f"{VERDE}{nome}{RESET}: "
                f"{VERDE}{quantidade}{RESET} "
                f"linha(s)"
            )

        # ====================================================
        # RESULTADO FINAL
        # ====================================================

        print(
            f"\n{VERDE}"
            "CAMADA GOLD CONCLUÍDA COM SUCESSO!"
            f"{RESET}\n"
        )

    finally:

        spark.stop()


# ============================================================
# 7. EXECUÇÃO
# ============================================================

if __name__ == "__main__":
    main()