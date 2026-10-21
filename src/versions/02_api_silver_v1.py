import os
from pathlib import Path
import requests
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, when, when, explode, element_at
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    DoubleType,
    DecimalType,
    ArrayType
)

# Cores ANSI para formatar as mensagens no terminal.
VERDE = "\033[92m"
RESET = "\033[0m"
AZUL  = "\033[94m"

# Endpoint público da API.
API_URL = os.getenv("API_URL")

# Verifica se a variável API_URL foi definida.
if not API_URL:
    raise ValueError("A variável API_URL não foi definida.")

# ===============================================================================

# Caminho da resposta bruta dentro do contêiner.
BRONZE_PATH = os.getenv("BRONZE_PATH")

# Verifica se a variável BRONZE_PATH foi definida.
if not BRONZE_PATH:
    raise ValueError("A variável de ambiente BRONZE_PATH não foi definida.")

# Converte o caminho para um objeto Path.
BRONZE_DIR = Path(os.getenv("BRONZE_PATH", "/app/datasets/bronze"))
''
# Cria o diretório Bronze caso ele não exista.
# Se já existir, não gera erro.
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

BRONZE_PATH = BRONZE_DIR / "dummyjson.json"

# ===============================================================================

# Caminho da resposta bruta dentro do contêiner.
SILVER_PATH = os.getenv("SILVER_PATH")

# Verifica se a variável BRONZE_PATH foi definida.
if not SILVER_PATH:
    raise ValueError("A variável de ambiente SILVER_PATH não foi definida.")

# Converte o caminho para um objeto Path.
SILVER_DIR = Path(os.getenv("SILVER_PATH", "/app/datasets/silver"))
# Cria o diretório Bronze caso ele não exista.
# Se já existir, não gera erro.
SILVER_DIR.mkdir(parents=True, exist_ok=True)

SILVER_PATH = SILVER_DIR

# ============================================================
# 1. CONFIGURAÇÕES
# ============================================================

COLUNAS_OBRIGATORIAS = [
    "id",
    "title",
    "description",
    "category",
    "price",
    "rating",
    "stock",
    "dimensions",
    "images"
]

# Criando Schema
SCHEMA_BRONZE = StructType([
    StructField("id", LongType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("rating", DoubleType(), True),
    StructField("stock", DecimalType(), True),
    
    StructField(
        "dimensions",
        StructType([
            StructField("width", DoubleType(), True),
            StructField("height", DoubleType(), True),
            StructField("depth", DoubleType(), True),
        ]),
        True,
    ),
    # A API retorna uma lista de URLs de imagens.
    StructField(
        "images",
        ArrayType(StringType()),
        True
    )
])

SCHEMA_BRONZE = StructType([
    StructField(
        "products",
        ArrayType(SCHEMA_BRONZE),
        True,
    ),
    StructField("total", LongType(), True),
    StructField("skip", LongType(), True),
    StructField("limit", LongType(), True),
])

# ============================================================
# 2. FUNÇÕES AUXILIARES
# ============================================================

def limpar_texto(coluna):
    """Remove espaços externos e converte textos vazios em NULL."""

    texto = trim(coluna)

    return when(
        texto.isNotNull() & (texto != ""),
        texto,
    ).otherwise(None)


def validar_dataframe(df):
    """Valida preenchimento, unicidade e formato básico dos e-mails."""

    quantidade = df.count()

    if quantidade == 0:
        raise ValueError("A camada Silver não pode ser vazia.")

    # Verifica campos obrigatórios que não podem ser nulos.
    colunas_obrigatorias = [
        "id",
        "titulo",
        "descricao",
        "categoria",
        "preco",
        "avaliacao",
        "estoque",
        "largura",
        "altura",
        "profundidade",
        "imagens"
    ]

    for nome_coluna in colunas_obrigatorias:
        quantidade_nulos = df.filter(
            col(nome_coluna).isNull()
        ).count()

        if quantidade_nulos > 0:
            raise ValueError(
                f"A coluna '{nome_coluna}' possui "
                f"{quantidade_nulos} valores nulos."
            )

    # Verifica textos vazios ou compostos apenas por espaços.
    colunas_texto = ["titulo", "descricao", "categoria"]

    for nome_coluna in colunas_texto:
        quantidade_vazios = df.filter(
            trim(col(nome_coluna)) == ""
        ).count()

        if quantidade_vazios > 0:
            raise ValueError(
                f"A coluna '{nome_coluna}' possui "
                f"{quantidade_vazios} valores vazios."
            )

    # Verifica identificadores duplicados.
    quantidade_ids = df.select("id").distinct().count()

    if quantidade_ids != quantidade:
        raise ValueError("Foram encontrados identificadores duplicados.")

    print(
        f"Registros validados: {VERDE}{quantidade}{RESET}\n"
    )

    return quantidade

# ============================================================
# 3. PROCESSAMENTO PRINCIPAL
# ============================================================

def main():
    """Executa a transformação Bronze → Silver."""

    spark = (
        SparkSession.builder
        .appName("TutorialPySpark-API-Silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # ----------------------------------------------------
        # ETAPA A — LEITURA BRONZE
        # ----------------------------------------------------

        print(f"\n=== {VERDE}ETAPA 1: LEITURA BRONZE{RESET} ===")

        # Lê a resposta completa da API.
        df_resposta = (
            spark.read
            .schema(SCHEMA_BRONZE)
            .option("multiline", "true")
            .json(str(BRONZE_PATH))
        )

        print(f"\n{VERDE}Schema da resposta:{RESET}")
        df_resposta.printSchema()

        # Extrai cada produto da lista para formar uma linha.
        df_bronze = (
            df_resposta
            .select(explode(col("products")).alias("produto"))
            .select("produto.*")
        )

        
        print(f"\n{VERDE}Schema utilizado na leitura:{RESET}")
        df_bronze.printSchema()

        # Exibe alguns registros para verificar a leitura.
        print(f"\n{VERDE}AMOSTRA DOS DADOS BRONZE{RESET}")
        df_bronze.show(5, truncate=20)

        # Exibe a quantidade de produtos lidos.
        quantidade_bronze = df_bronze.count()

        print(f"Produtos lidos da Bronze: {VERDE}{quantidade_bronze}{RESET}")

        # ----------------------------------------------------
        # ETAPA B — SELEÇÃO E LIMPEZA
        # ----------------------------------------------------

        print(f"\n=== {VERDE}ETAPA 2: TRANSFORMAÇÃO SILVER{RESET} ===")

        df_silver = df_bronze.select(
            col("id").cast("long").alias("id"),
            limpar_texto(col("title")).alias("titulo"),
            limpar_texto(col("description")).alias("descricao"),
            limpar_texto(col("category")).alias("categoria"),

            col("price").cast(DecimalType(10, 2)).alias("preco"),
            col("rating").cast(DecimalType(10, 2)).alias("avaliacao"),
            col("stock").cast(DecimalType(10, 2)).alias("estoque"),

            col("dimensions.width").cast(DoubleType()).alias("largura"),
            col("dimensions.height").cast(DoubleType()).alias("altura"),
            col("dimensions.depth").cast(DoubleType()).alias("profundidade"),

            col("images").alias("imagens")
        ).filter(
            col("id").isNotNull()
        )

        # Mostra o plano lógico e físico da transformação.
        print(f"\n=== {VERDE}PLANO DA TRANSFORMAÇÃO SILVER{RESET} ===")
        df_silver.explain(mode="formatted")

        # Exibe uma amostra.
        df_silver.show(5, truncate=20)

        # ----------------------------------------------------
        # ETAPA C — VALIDAÇÃO
        # ----------------------------------------------------

        print(f"\n=== {VERDE}ETAPA 3: VALIDAÇÃO{RESET} ===")
        
        quantidade = validar_dataframe(df_silver)

        # ----------------------------------------------------
        # ETAPA D — GRAVAÇÃO PARQUET
        # ----------------------------------------------------

        print(f"=== {VERDE}ETAPA 4: GRAVAÇÃO PARQUET{RESET} ===")

        (
            df_silver
            .orderBy("id")
            .write
            .mode("overwrite")
            .parquet(str(SILVER_DIR))
        )

        print(f"Destino: {VERDE}{SILVER_PATH}{RESET}")
        print(f"Registros gravados: {VERDE}{quantidade}{RESET}")

        # ----------------------------------------------------
        # ETAPA E — EXPERIMENTO DE OTIMIZAÇÃO PARQUET
        # ----------------------------------------------------

        print(f"\n=== {VERDE}ETAPA 5: COLUMN PRUNING E PREDICATE PUSHdown{RESET} ===")

        df_parquet = spark.read.parquet(str(SILVER_DIR))

        # A consulta usa apenas três colunas e filtra uma cidade.
        # A cidade de Gwenborough faz parte dos dados da API.
        consulta = (
            df_parquet
            .filter(col("categoria") == "beauty")
            .select("id", "titulo", "categoria", "preco")
        )

        print(f"\n=== {VERDE}PLANO DA CONSULTA PARQUET{RESET} ===")
        consulta.explain(mode="formatted")

        print(f"\n=== {VERDE}RESULTADO DA CONSULTA{RESET} ===")
        consulta.show(10, truncate=False)

        # ----------------------------------------------------
        # ETAPA F — CONFERÊNCIA FINAL
        # ----------------------------------------------------

        quantidade_final = df_parquet.count()

        if quantidade_final != quantidade:
            raise ValueError(
                "A quantidade de registros mudou após "
                "a gravação em Parquet."
            )

        print(f"Registros conferidos: {VERDE}{quantidade_final}{RESET}")
        print()
        print(f"{VERDE}CAMADA SILVER CONCLUÍDA COM SUCESSO!{RESET}")
        print()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
   