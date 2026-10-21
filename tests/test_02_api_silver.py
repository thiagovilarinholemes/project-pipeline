"""
Testes automatizados para src/02_api_silver.py.

Executar no ambiente do projeto com:
    python -m pytest -v tests/test_02_api_silver.py
"""

import importlib.util
import os
import tempfile
from pathlib import Path
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ============================================================
# 1. CONFIGURAÇÃO DO AMBIENTE DE TESTE
# ============================================================

# Cria diretórios temporários para não modificar os dados reais.
DIRETORIO_TESTE = Path(tempfile.mkdtemp(prefix="teste_api_silver_"))

os.environ.setdefault(
    "API_URL",
    "https://dummyjson.com/products?limit=1",
)
os.environ["BRONZE_PATH"] = str(DIRETORIO_TESTE / "bronze")
os.environ["SILVER_PATH"] = str(DIRETORIO_TESTE / "silver")


# ============================================================
# 2. IMPORTAÇÃO DO SCRIPT TESTADO
# ============================================================

# O nome do arquivo começa com números; por isso usamos importlib.
RAIZ_PROJETO = Path(__file__).resolve().parents[1]
CAMINHO_SCRIPT = RAIZ_PROJETO / "src" / "02_api_silver.py"

spec = importlib.util.spec_from_file_location(
    "api_silver",
    CAMINHO_SCRIPT,
)

if spec is None or spec.loader is None:
    raise ImportError(
        f"Não foi possível carregar o script: {CAMINHO_SCRIPT}"
    )

api_silver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api_silver)


# ============================================================
# 3. FIXTURES
# ============================================================

@pytest.fixture(scope="session")
def spark():
    """Cria uma sessão Spark compartilhada pelos testes."""

    sessao = (
        SparkSession.builder
        .master("local[2]")
        .appName("Testes-API-Silver")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    sessao.sparkContext.setLogLevel("ERROR")

    yield sessao

    sessao.stop()


@pytest.fixture
def schema_silver():
    """Define o esquema esperado para os testes Silver."""

    return StructType([
        StructField("id", LongType(), False),
        StructField("titulo", StringType(), True),
        StructField("descricao", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("preco", DecimalType(10, 2), True),
        StructField("avaliacao", DecimalType(10, 2), True),
        StructField("estoque", DecimalType(10, 2), True),
        StructField("largura", DoubleType(), True),
        StructField("altura", DoubleType(), True),
        StructField("profundidade", DoubleType(), True),
        StructField("imagens", ArrayType(StringType()), True),
    ])


@pytest.fixture
def produtos_validos():
    """Fornece produtos fictícios compatíveis com as 11 colunas."""

    return [
        (
            1,                                  # id
            "Produto A",                        # titulo
            "Descrição do produto A",            # descricao
            "beauty",                           # categoria
            Decimal("29.90"),                   # preco
            Decimal("4.50"),                    # avaliacao
            Decimal("10.00"),                   # estoque
            20.0,                               # largura
            5.0,                                # altura
            10.0,                               # profundidade
            ["https://example.com/a.jpg"],       # imagens
        ),
        (
            2,                                  # id
            "Produto B",                        # titulo
            "Descrição do produto B",            # descricao
            "beauty",                           # categoria
            Decimal("49.90"),                   # preco
            Decimal("4.80"),                    # avaliacao
            Decimal("20.00"),                   # estoque
            15.0,                               # largura
            25.0,                               # altura
            8.0,                                # profundidade
            ["https://example.com/b.jpg"],       # imagens
        ),
    ]


def criar_dataframe(spark, schema, registros):
    """Cria DataFrames de teste com um esquema explícito."""

    return spark.createDataFrame(registros, schema=schema)


# ============================================================
# 4. TESTES DA FUNÇÃO limpar_texto()
# ============================================================

def test_limpar_texto_remove_espacos(spark):
    """Confirma que os espaços no início e no fim são removidos."""

    df = spark.createDataFrame(
        [("  Produto A  ",)],
        ["texto"],
    )

    resultado = (
        df.select(api_silver.limpar_texto(col("texto")).alias("texto"))
        .first()["texto"]
    )

    assert resultado == "Produto A"


def test_limpar_texto_converte_string_vazia_em_nulo(spark):
    """Confirma que uma string vazia vira None."""

    df = spark.createDataFrame(
        [("",), ("   ",)],
        ["texto"],
    )

    resultados = (
        df.select(api_silver.limpar_texto(col("texto")).alias("texto"))
        .collect()
    )

    assert resultados[0]["texto"] is None
    assert resultados[1]["texto"] is None


def test_limpar_texto_preserva_nulo(spark):
    """Confirma que valores nulos continuam nulos."""

    df = spark.createDataFrame(
        [(None,)],
        StructType([
            StructField("texto", StringType(), True)
        ]),
    )

    resultado = (
        df.select(api_silver.limpar_texto(col("texto")).alias("texto"))
        .first()["texto"]
    )

    assert resultado is None


# ============================================================
# 5. TESTES DA FUNÇÃO validar_dataframe()
# ============================================================

def test_validar_dataframe_aceita_dados_validos(
    spark,
    schema_silver,
    produtos_validos,
):
    """Um DataFrame válido deve retornar sua quantidade de linhas."""

    df = criar_dataframe(
        spark,
        schema_silver,
        produtos_validos,
    )

    quantidade = api_silver.validar_dataframe(df)

    assert quantidade == 2


def test_validar_dataframe_rejeita_dataframe_vazio(
    spark,
    schema_silver,
):
    """Um DataFrame sem registros deve gerar ValueError."""

    df = criar_dataframe(
        spark,
        schema_silver,
        [],
    )

    with pytest.raises(
        ValueError,
        match="A camada Silver não pode ser vazia",
    ):
        api_silver.validar_dataframe(df)


def test_validar_dataframe_rejeita_valor_nulo(
    spark,
    schema_silver,
    produtos_validos,
):
    """A validação deve identificar valores nulos em campos obrigatórios."""

    registros = list(produtos_validos)

    # O índice 4 corresponde à coluna preco.
    primeiro = list(registros[0])
    primeiro[4] = None
    registros[0] = tuple(primeiro)

    df = criar_dataframe(
        spark,
        schema_silver,
        registros,
    )

    with pytest.raises(
        ValueError,
        match="A coluna 'preco' possui 1 valores nulos",
    ):
        api_silver.validar_dataframe(df)


def test_validar_dataframe_rejeita_texto_vazio(
    spark,
    schema_silver,
    produtos_validos,
):
    """A validação deve rejeitar títulos compostos apenas por espaços."""

    registros = list(produtos_validos)

    primeiro = list(registros[0])
    primeiro[1] = "   "
    registros[0] = tuple(primeiro)

    df = criar_dataframe(
        spark,
        schema_silver,
        registros,
    )

    with pytest.raises(
        ValueError,
        match="A coluna 'titulo' possui 1 valores vazios",
    ):
        api_silver.validar_dataframe(df)


def test_validar_dataframe_rejeita_ids_duplicados(
    spark,
    schema_silver,
    produtos_validos,
):
    """Dois produtos com o mesmo ID devem ser rejeitados."""

    registros = [
        produtos_validos[0],
        (
            1,                                  # ID duplicado
            "Produto C",                        # titulo
            "Descrição do produto C",            # descricao
            "beauty",                           # categoria
            Decimal("19.90"),                   # preco
            Decimal("4.20"),                    # avaliacao
            Decimal("5.00"),                    # estoque
            8.0,                                # largura
            12.0,                               # altura
            4.0,                                # profundidade
            ["https://example.com/c.jpg"],       # imagens
        ),
    ]

    df = criar_dataframe(
        spark,
        schema_silver,
        registros,
    )

    with pytest.raises(
        ValueError,
        match="identificadores duplicados",
    ):
        api_silver.validar_dataframe(df)