"""
Camada Bronze: ingestão genérica de arquivos usando PySpark.

Formatos suportados:
- JSON
- CSV
- Parquet

Variáveis de ambiente obrigatórias:
- FILE_PATH
- BRONZE_PATH
"""

import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ==============================================================================
# CORES ANSI
# ==============================================================================

VERDE = "\033[92m"
RESET = "\033[0m"
AZUL = "\033[94m"
AMARELO = "\033[93m"
VERMELHO = "\033[91m"


# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================

FILE_PATH = os.getenv("FILE_PATH")
BRONZE_PATH = os.getenv("BRONZE_PATH")


if not FILE_PATH:
    raise ValueError(
        "A variável de ambiente FILE_PATH não foi definida."
    )

if not BRONZE_PATH:
    raise ValueError(
        "A variável de ambiente BRONZE_PATH não foi definida."
    )


FILE = Path(FILE_PATH)
BRONZE_DIR = Path(BRONZE_PATH)


if not FILE.exists():
    raise FileNotFoundError(
        f"Arquivo de entrada não encontrado: {FILE}"
    )


BRONZE_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


# ==============================================================================
# IDENTIFICAÇÃO DO FORMATO
# ==============================================================================

def identificar_formato(arquivo: Path) -> str:
    """
    Identifica o formato do arquivo pela extensão.
    """

    extensao = arquivo.suffix.lower()

    formatos = {
        ".json": "json",
        ".csv": "csv",
        ".parquet": "parquet",
    }

    if extensao not in formatos:
        raise ValueError(
            f"Formato de arquivo não suportado: {extensao}. "
            f"Formatos aceitos: {', '.join(formatos.keys())}"
        )

    return formatos[extensao]


# ==============================================================================
# SESSÃO SPARK
# ==============================================================================

def criar_sessao_spark() -> SparkSession:
    """
    Cria ou recupera uma sessão Spark.
    """

    return (
        SparkSession.builder
        .appName("PySpark-File-Bronze")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ==============================================================================
# CÓPIA RAW
# ==============================================================================

def salvar_raw():
    """
    Preserva uma cópia do arquivo original na camada Bronze.
    """

    raw_dir = BRONZE_DIR / "raw"

    raw_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    destino = raw_dir / FILE.name

    shutil.copy2(
        FILE,
        destino,
    )

    print("=" * 70)
    print(
        f"Arquivo RAW salvo em: "
        f"{VERDE}{destino}{RESET}"
    )
    print("=" * 70)
    print()

    return destino


# ==============================================================================
# LEITURA DO ARQUIVO
# ==============================================================================

def ler_arquivo_com_spark(
    spark: SparkSession,
    formato: str,
):
    """
    Lê o arquivo utilizando PySpark.
    """

    print()
    print("=" * 70)
    print(
        f"Lendo arquivo: "
        f"{VERDE}{FILE}{RESET}"
    )
    print(
        f"Formato: "
        f"{VERDE}{formato.upper()}{RESET}"
    )
    print("=" * 70)
    print()

    if formato == "json":

        df = (
            spark.read
            .option("multiLine", True)
            .json(str(FILE))
        )

    elif formato == "csv":

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("mode", "PERMISSIVE")
            .csv(str(FILE))
        )

    elif formato == "parquet":

        df = spark.read.parquet(
            str(FILE)
        )

    else:
        raise ValueError(
            f"Formato não suportado: {formato}"
        )

    return df


# ==============================================================================
# VALIDAÇÃO
# ==============================================================================

def validar_dataframe(df):
    """
    Valida o DataFrame carregado pelo Spark.
    """

    quantidade = df.count()

    if quantidade == 0:
        raise ValueError(
            "O arquivo não possui registros."
        )

    print(f"{AZUL}ESTRUTURA DO ARQUIVO{RESET}")
    print("=" * 70)

    df.printSchema()

    print()

    print(f"{AZUL}PRIMEIROS REGISTROS{RESET}")
    print("=" * 70)

    df.show(
        5,
        truncate=20,
        vertical=False,
    )

    print()

    print(f"{AZUL}COLUNAS IDENTIFICADAS{RESET}")
    print("=" * 70)

    for indice, coluna in enumerate(
        df.columns,
        start=1,
    ):
        print(
            f"{indice:02d}. {coluna}"
        )

    print()

    print("=" * 70)
    print(
        f"Registros carregados: "
        f"{VERDE}{quantidade} [OK]{RESET}"
    )
    print("=" * 70)
    print()

    # Validação genérica de ID.
    if "id" in df.columns:

        ids_nulos = (
            df
            .filter(col("id").isNull())
            .count()
        )

        if ids_nulos > 0:

            raise ValueError(
                f"Foram encontrados "
                f"{ids_nulos} registros sem ID."
            )

        print("=" * 70)
        print(
            f"Todos os registros possuem ID "
            f"{VERDE}[OK]{RESET}"
        )
        print("=" * 70)
        print()

    else:

        print(
            f"{AMARELO}"
            f"Aviso: coluna 'id' não encontrada. "
            f"Validação de ID ignorada."
            f"{RESET}"
        )

    return df


# ==============================================================================
# PERSISTÊNCIA BRONZE
# ==============================================================================

def salvar_bronze(df):
    """
    Salva os dados processados pela Bronze em Parquet.

    A Bronze não realiza transformação de negócio.
    """

    destino = BRONZE_DIR / "parquet"

    (
        df.write
        .mode("overwrite")
        .parquet(str(destino))
    )

    print("=" * 70)
    print(
        f"Bronze salva em: "
        f"{VERDE}{destino}{RESET}"
    )
    print("=" * 70)
    print()


# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================

def main():
    """
    Executa a ingestão do arquivo para a camada Bronze.
    """

    spark = criar_sessao_spark()

    try:

        print()
        print("=" * 70)
        print(
            f"{VERDE}Iniciando ingestão Bronze{RESET}"
        )
        print("=" * 70)
        print()

        # 1. Identifica o formato.
        formato = identificar_formato(FILE)

        # 2. Preserva o arquivo original.
        salvar_raw()

        # 3. Lê o arquivo com Spark.
        df = ler_arquivo_com_spark(
            spark,
            formato,
        )

        # 4. Valida os dados.
        df = validar_dataframe(df)

        # 5. Salva a camada Bronze.
        salvar_bronze(df)

        print("=" * 70)
        print(
            f"Ingestão Bronze concluída "
            f"com sucesso! {VERDE}[OK]{RESET}"
        )
        print("=" * 70)
        print()

    finally:

        spark.stop()


if __name__ == "__main__":
    main()