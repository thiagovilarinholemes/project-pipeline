
"""
Camada Silver: transformação genérica de arquivos usando PySpark.

Formatos de entrada suportados:
- CSV
- JSON
- Parquet

Funcionalidades:
- Localização automática dos dados da Bronze.
- Leitura do Parquet gerado pela camada Bronze.
- Leitura alternativa do arquivo RAW.
- Achatamento recursivo de estruturas StructType.
- Limpeza de textos vazios.
- Seleção opcional de colunas.
- Validação de identificadores, quando disponíveis.
- Gravação da camada Silver em Parquet.

Variáveis de ambiente:
- BRONZE_PATH
- SILVER_PATH
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when
from pyspark.sql.types import StructType, StringType


# ==============================================================================
# 1. CORES ANSI
# ==============================================================================

VERDE = "\033[92m"
RESET = "\033[0m"
AZUL = "\033[94m"
AMARELO = "\033[93m"
VERMELHO = "\033[91m"


# ==============================================================================
# 2. CONFIGURAÇÕES
# ==============================================================================

BRONZE_ENV = os.getenv("BRONZE_PATH")
SILVER_ENV = os.getenv("SILVER_PATH")

if not BRONZE_ENV or not BRONZE_ENV.strip():
    raise ValueError("A variável BRONZE_PATH não foi definida.")

if not SILVER_ENV or not SILVER_ENV.strip():
    raise ValueError("A variável SILVER_PATH não foi definida.")

BRONZE_DIR = Path(BRONZE_ENV.strip())
SILVER_DIR = Path(SILVER_ENV.strip())

# Diretórios criados pela camada Bronze.
BRONZE_PARQUET_DIR = BRONZE_DIR / "parquet"
BRONZE_RAW_DIR = BRONZE_DIR / "raw"

# Formatos suportados para arquivos RAW.
FORMATOS_SUPORTADOS = {
    ".csv": "csv",
    ".json": "json",
    ".parquet": "parquet",
}

# Colunas opcionais para seleção na Silver.
# Lista vazia significa preservar todas as colunas.
#
# Exemplos:
# CAMPOS_SELECIONADOS = ["id", "name", "price"]
# CAMPOS_SELECIONADOS = ["fuel-type", "aspiration", "price"]
# Como winners_category está alinhado com winners ele vem junto é com colocar winners_category para identificar
CAMPOS_SELECIONADOS = [
    ""
    "name"
    "city",
    "country",
]


# ==============================================================================
# 3. SESSÃO SPARK
# ==============================================================================

def criar_sessao_spark() -> SparkSession:
    """Cria ou recupera uma sessão Spark."""

    return (
        SparkSession.builder
        .appName("TutorialPySpark-File-Silver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ==============================================================================
# 4. LOCALIZAÇÃO AUTOMÁTICA DA BRONZE
# ==============================================================================

def localizar_entrada_bronze():
    """
    Localiza os dados da Bronze.

    Prioridade:
    1. Diretório Parquet produzido pela Bronze.
    2. Arquivos originais dentro de BRONZE_PATH/raw/.

    Retorna:
        Uma tupla (caminho, formato).
    """

    print(f"\n{AZUL}LOCALIZAÇÃO DA CAMADA BRONZE{RESET}")
    print("=" * 70)
    print(f"Diretório Bronze: {BRONZE_DIR}")

    # --------------------------------------------------------------------------
    # Opção 1: utilizar o Parquet gerado pela Bronze.
    # --------------------------------------------------------------------------

    if (
        BRONZE_PARQUET_DIR.is_dir()
        and any(BRONZE_PARQUET_DIR.iterdir())
    ):
        print(f"Origem encontrada: {BRONZE_PARQUET_DIR}")
        print(f"Formato identificado: {VERDE}PARQUET{RESET}")
        print("=" * 70)

        return BRONZE_PARQUET_DIR, "parquet"

    # --------------------------------------------------------------------------
    # Opção 2: procurar arquivos RAW.
    # --------------------------------------------------------------------------

    if BRONZE_RAW_DIR.is_dir():

        arquivos = sorted(
            arquivo
            for arquivo in BRONZE_RAW_DIR.rglob("*")
            if arquivo.is_file()
            and arquivo.suffix.lower() in FORMATOS_SUPORTADOS
        )

        if len(arquivos) == 1:
            arquivo = arquivos[0]
            formato = FORMATOS_SUPORTADOS[
                arquivo.suffix.lower()
            ]

            print(f"Arquivo RAW encontrado: {arquivo}")
            print(f"Formato identificado: {VERDE}{formato.upper()}{RESET}")
            print("=" * 70)

            return arquivo, formato

        if len(arquivos) > 1:
            lista = "\n".join(
                f"  - {arquivo}" for arquivo in arquivos
            )

            raise ValueError(
                "Foram encontrados vários arquivos RAW na Bronze.\n"
                "Não é possível escolher um arquivo com segurança.\n"
                f"Arquivos encontrados:\n{lista}"
            )

    raise FileNotFoundError(
        "Nenhuma entrada válida foi encontrada na Bronze.\n"
        f"Verifique os diretórios:\n"
        f"- {BRONZE_PARQUET_DIR}\n"
        f"- {BRONZE_RAW_DIR}\n"
        "Execute primeiro a camada 01_file_bronze.py."
    )


# ==============================================================================
# 5. LEITURA DOS DADOS BRONZE
# ==============================================================================

def ler_bronze(spark, caminho: Path, formato: str):
    """Lê os dados da Bronze conforme o formato identificado."""

    print(f"\n{AZUL}ETAPA 1: LEITURA BRONZE{RESET}")
    print("=" * 70)
    print(f"Origem: {caminho}")
    print(f"Formato: {formato.upper()}")

    if formato == "csv":
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("mode", "FAILFAST")
            .csv(str(caminho))
        )

    elif formato == "json":
        df = (
            spark.read
            .option("multiLine", True)
            .option("mode", "FAILFAST")
            .json(str(caminho))
        )

    elif formato == "parquet":
        df = spark.read.parquet(str(caminho))

    else:
        raise ValueError(
            f"Formato de entrada não suportado: {formato}"
        )

    if not df.columns:
        raise ValueError(
            "O arquivo Bronze não contém colunas reconhecidas."
        )

    print(f"\n{AZUL}SCHEMA BRONZE{RESET}")
    df.printSchema()

    print(f"\n{AZUL}AMOSTRA BRONZE{RESET}")
    df.show(5, truncate=30)

    quantidade = df.count()

    print(f"Registros Bronze: {VERDE}{quantidade}{RESET}")

    if quantidade == 0:
        raise ValueError(
            "A Bronze não possui registros para transformar."
        )

    return df, quantidade


# ==============================================================================
# 6. LIMPEZA DE TEXTO
# ==============================================================================

def limpar_texto(coluna):
    """
    Remove espaços no início e no final.

    Transforma textos vazios ou compostos apenas por espaços em NULL.
    """

    texto = trim(coluna)

    return when(
        coluna.isNotNull() & (texto != ""),
        texto,
    ).otherwise(None)


# ==============================================================================
# 7. ACHATAMENTO DE ESTRUTURAS ANINHADAS
# ==============================================================================

def referencia_coluna(nome: str):
    """
    Cria uma referência Spark segura para nomes com espaços,
    pontos, hífens ou outros caracteres especiais.
    """

    nome_escapado = nome.replace("`", "``")

    return col(f"`{nome_escapado}`")


def achatar_dataframe(df):
    """
    Achata recursivamente colunas StructType.

    Exemplos:
        dimensions.width -> dimensions_width
        address.city     -> address_city
        company.name     -> company_name

    Arrays não são expandidos nesta etapa.
    """

    while True:

        campos_struct = [
            campo.name
            for campo in df.schema.fields
            if isinstance(campo.dataType, StructType)
        ]

        if not campos_struct:
            break

        colunas = []

        for campo in df.schema.fields:

            nome = campo.name

            if isinstance(campo.dataType, StructType):

                for subcampo in campo.dataType.fields:

                    novo_nome = f"{nome}_{subcampo.name}"

                    nome_pai = nome.replace("`", "``")
                    nome_filho = subcampo.name.replace("`", "``")

                    colunas.append(
                        col(f"`{nome_pai}`.`{nome_filho}`")
                        .alias(novo_nome)
                    )

            else:
                colunas.append(
                    referencia_coluna(nome)
                )

        df = df.select(*colunas)

    return df


# ==============================================================================
# 8. TRANSFORMAÇÃO SILVER
# ==============================================================================

def transformar_silver(df):
    """
    Executa as transformações genéricas da camada Silver.

    - Achata estruturas aninhadas.
    - Limpa valores textuais.
    - Seleciona as colunas configuradas, se existirem.
    """

    print(f"\n{AZUL}ETAPA 2: TRANSFORMAÇÃO SILVER{RESET}")

    # --------------------------------------------------------------------------
    # Achatamento de estruturas aninhadas.
    # --------------------------------------------------------------------------

    df = achatar_dataframe(df)

    print("Achatamento de estruturas concluído.")

    # --------------------------------------------------------------------------
    # Limpeza de todas as colunas textuais.
    # --------------------------------------------------------------------------

    colunas_tratadas = []

    for campo in df.schema.fields:

        coluna = referencia_coluna(campo.name)

        if isinstance(campo.dataType, StringType):
            colunas_tratadas.append(
                limpar_texto(coluna).alias(campo.name)
            )
        else:
            colunas_tratadas.append(coluna)

    df = df.select(*colunas_tratadas)

    print("Limpeza de textos concluída.")

    # --------------------------------------------------------------------------
    # Seleção opcional de colunas.
    # --------------------------------------------------------------------------

    if CAMPOS_SELECIONADOS:

        colunas_existentes = [
            nome
            for nome in CAMPOS_SELECIONADOS
            if nome in df.columns
        ]

        colunas_ausentes = [
            nome
            for nome in CAMPOS_SELECIONADOS
            if nome not in df.columns
        ]

        if colunas_ausentes:
            print(
                f"{AMARELO}Colunas configuradas não encontradas: "
                f"{colunas_ausentes}{RESET}"
            )

        if not colunas_existentes:
            print(
                f"{AMARELO}Nenhuma coluna configurada foi encontrada. "
                f"Todas as colunas serão preservadas.{RESET}"
            )
        else:
            df = df.select(
                *[
                    referencia_coluna(nome)
                    for nome in colunas_existentes
                ]
            )

            print("Colunas selecionadas:")

            for nome in colunas_existentes:
                print(f"  - {nome}")

    else:
        print("Todas as colunas serão preservadas.")

    print(f"\n{AZUL}SCHEMA SILVER{RESET}")
    df.printSchema()

    print(f"\n{AZUL}AMOSTRA SILVER{RESET}")
    df.show(5, truncate=30)

    return df


# ==============================================================================
# 9. VALIDAÇÃO SILVER
# ==============================================================================

def validar_dataframe(df):
    """Valida registros e identificadores quando disponíveis."""

    print(f"\n{AZUL}ETAPA 3: VALIDAÇÃO SILVER{RESET}")

    quantidade = df.count()

    print(f"Registros Silver: {VERDE}{quantidade}{RESET}")

    if quantidade == 0:
        raise ValueError(
            "A camada Silver não possui registros."
        )

    # --------------------------------------------------------------------------
    # Validação de IDs.
    # --------------------------------------------------------------------------

    if "id" in df.columns:

        coluna_id = referencia_coluna("id")

        ids_nulos = df.filter(
            coluna_id.isNull()
        ).count()

        if ids_nulos > 0:
            raise ValueError(
                f"A coluna 'id' possui {ids_nulos} valores nulos."
            )

        ids_unicos = df.select(
            coluna_id
        ).distinct().count()

        if ids_unicos != quantidade:
            raise ValueError(
                "Foram encontrados identificadores duplicados."
            )

        print(f"Validação de IDs: {VERDE}[OK]{RESET}")

    else:
        print(
            f"{AMARELO}Coluna 'id' não encontrada. "
            f"Validação de IDs ignorada.{RESET}"
        )

    # --------------------------------------------------------------------------
    # Verificação de textos vazios.
    # --------------------------------------------------------------------------

    colunas_texto = [
        campo.name
        for campo in df.schema.fields
        if isinstance(campo.dataType, StringType)
    ]

    for nome in colunas_texto:

        coluna = referencia_coluna(nome)

        quantidade_vazios = df.filter(
            coluna.isNotNull() & (trim(coluna) == "")
        ).count()

        if quantidade_vazios > 0:
            raise ValueError(
                f"A coluna '{nome}' possui "
                f"{quantidade_vazios} textos vazios."
            )

    print(f"Validação Silver: {VERDE}[OK]{RESET}")

    return quantidade


# ==============================================================================
# 10. GRAVAÇÃO SILVER
# ==============================================================================

def salvar_silver(df):
    """Grava os dados Silver em formato Parquet."""

    print(f"\n{AZUL}ETAPA 4: GRAVAÇÃO SILVER{RESET}")

    SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    (
        df.write
        .mode("overwrite")
        .parquet(str(SILVER_DIR))
    )

    print(f"Destino: {VERDE}{SILVER_DIR}{RESET}")
    print("Formato de saída: PARQUET")


# ==============================================================================
# 11. CONFERÊNCIA FINAL
# ==============================================================================

def conferir_silver(spark, quantidade_esperada):
    """Relê o Parquet e verifica a quantidade de registros."""

    print(f"\n{AZUL}ETAPA 5: CONFERÊNCIA FINAL{RESET}")

    df = spark.read.parquet(str(SILVER_DIR))

    quantidade_final = df.count()

    if quantidade_final != quantidade_esperada:
        raise ValueError(
            "A quantidade de registros após a gravação diverge: "
            f"esperado={quantidade_esperada}, "
            f"encontrado={quantidade_final}."
        )

    print(f"Destino conferido: {SILVER_DIR}")
    print(f"Registros gravados: {VERDE}{quantidade_final}{RESET}")

    df.show(10, truncate=False)

    print(f"\n{VERDE}CAMADA SILVER CONCLUÍDA COM SUCESSO! [OK]{RESET}")


# ==============================================================================
# 12. EXECUÇÃO PRINCIPAL
# ==============================================================================

def main():
    """Executa o pipeline Bronze -> Silver."""

    spark = criar_sessao_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n" + "=" * 70)
        print(f"{VERDE}INICIANDO PIPELINE FILE SILVER{RESET}")
        print("=" * 70)

        # 1. Localiza automaticamente a entrada Bronze.
        caminho, formato = localizar_entrada_bronze()

        # 2. Lê os dados com Spark.
        df_bronze, _ = ler_bronze(
            spark,
            caminho,
            formato,
        )

        # 3. Transforma os dados.
        df_silver = transformar_silver(df_bronze)

        # 4. Valida o resultado após as transformações.
        quantidade = validar_dataframe(df_silver)

        # 5. Exibe o plano de execução.
        print(f"\n{AZUL}PLANO DA TRANSFORMAÇÃO{RESET}")
        df_silver.explain(mode="formatted")

        # 6. Grava o Parquet Silver.
        salvar_silver(df_silver)

        # 7. Relê e confere os dados gravados.
        conferir_silver(
            spark,
            quantidade,
        )

        print("\n" + "=" * 70)
        print(f"{VERDE}PIPELINE FILE SILVER FINALIZADO [OK]{RESET}")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
