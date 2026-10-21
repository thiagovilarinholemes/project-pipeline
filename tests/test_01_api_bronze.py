import importlib.util
import json
import os
from pathlib import Path

import pytest


# ============================================================
# CONFIGURAÇÃO
# ============================================================

# Localiza o arquivo 01_api_bronze.py
BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_PIPELINE = BASE_DIR /"src"/"01_api_bronze.py"


def carregar_pipeline():
    """
    Carrega o arquivo 01_api_bronze.py mesmo o nome
    contendo números.
    """

    spec = importlib.util.spec_from_file_location(
        "api_bronze",
        ARQUIVO_PIPELINE
    )

    modulo = importlib.util.module_from_spec(spec)

    spec.loader.exec_module(modulo)

    return modulo


# ============================================================
# FIXTURE
# ============================================================

@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    """
    Configura as variáveis de ambiente utilizadas pelo pipeline
    antes de carregar o arquivo.
    """

    monkeypatch.setenv(
        "API_URL",
        "https://dummyjson.com/products"
    )

    monkeypatch.setenv(
        "BRONZE_PATH",
        str(tmp_path / "bronze")
    )

    return carregar_pipeline()


# ============================================================
# TESTE 01
# ============================================================

def test_api_url_configurada(pipeline):

    assert pipeline.API_URL is not None
    assert pipeline.API_URL != ""

    assert pipeline.API_URL == (
        "https://dummyjson.com/products"
    )


# ============================================================
# TESTE 02
# ============================================================

def test_bronze_path_configurado(pipeline):

    assert pipeline.BRONZE_PATH is not None
    assert pipeline.BRONZE_PATH != ""

    assert pipeline.BRONZE_DIR.exists()


# ============================================================
# TESTE 03
# ============================================================

def test_buscar_dados_api(pipeline):

    dados, quantidade = pipeline.buscar_dados_api()

    # Verifica estrutura principal
    assert isinstance(dados, dict)

    # Verifica products
    assert "products" in dados
    assert isinstance(dados["products"], list)

    # Deve existir pelo menos um produto
    assert len(dados["products"]) > 0

    # A quantidade retornada pela função deve
    # ser igual à quantidade existente em products.
    assert quantidade == len(dados["products"])


# ============================================================
# TESTE 04
# ============================================================

def test_campos_obrigatorios_api(pipeline):

    dados, quantidade = pipeline.buscar_dados_api()

    produtos = dados["products"]

    campos_obrigatorios = [
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

    for produto in produtos:

        for campo in campos_obrigatorios:

            assert campo in produto, (
                f"O campo '{campo}' não existe "
                f"no produto {produto.get('id')}"
            )


# ============================================================
# TESTE 05
# ============================================================

def test_salvar_json_bronze(pipeline):

    dados, quantidade = pipeline.buscar_dados_api()

    pipeline.salvar_json_bronze(dados)

    # O arquivo deve existir
    assert pipeline.RAW_JSON_PATH.exists()

    # Deve ser um arquivo
    assert pipeline.RAW_JSON_PATH.is_file()

    # Deve possuir conteúdo
    assert pipeline.RAW_JSON_PATH.stat().st_size > 0


# ============================================================
# TESTE 06
# ============================================================

def test_json_bronze_valido(pipeline):

    dados, quantidade = pipeline.buscar_dados_api()

    pipeline.salvar_json_bronze(dados)

    with open(
        pipeline.RAW_JSON_PATH,
        "r",
        encoding="utf-8"
    ) as arquivo:

        dados_bronze = json.load(arquivo)

    assert isinstance(dados_bronze, dict)

    assert "products" in dados_bronze

    assert isinstance(
        dados_bronze["products"],
        list
    )

    assert len(dados_bronze["products"]) == quantidade


# ============================================================
# TESTE 07
# ============================================================

def test_bronze_contem_somente_campos_selecionados(
    pipeline
):

    dados, _ = pipeline.buscar_dados_api()

    pipeline.salvar_json_bronze(dados)

    with open(
        pipeline.RAW_JSON_PATH,
        "r",
        encoding="utf-8"
    ) as arquivo:

        dados_bronze = json.load(arquivo)

    campos_permitidos = {
        "id",
        "title",
        "description",
        "category",
        "price",
        "rating",
        "stock",
        "dimensions",
        "images"
    }

    for produto in dados_bronze["products"]:

        campos_produto = set(produto.keys())

        assert campos_produto.issubset(
            campos_permitidos
        ), (
            "O produto possui campos que não deveriam "
            f"estar na camada Bronze: "
            f"{campos_produto - campos_permitidos}"
        )


# ============================================================
# TESTE 08
# ============================================================

def test_processamento_spark(pipeline):

    dados, quantidade_esperada = (
        pipeline.buscar_dados_api()
    )

    pipeline.salvar_json_bronze(dados)

    spark = pipeline.criar_sessao_spark()

    try:

        df = pipeline.processar_com_spark(
            spark,
            quantidade_esperada
        )

        # O DataFrame retornado deve existir
        assert df is not None

        # Deve possuir a coluna products
        assert "products" in df.columns

    finally:

        spark.stop()


# ============================================================
# TESTE 09
# ============================================================

def test_quantidade_api_igual_spark(pipeline):

    dados, quantidade_api = (
        pipeline.buscar_dados_api()
    )

    pipeline.salvar_json_bronze(dados)

    spark = pipeline.criar_sessao_spark()

    try:

        df = (
            spark.read
            .option("multiline", "true")
            .json(str(pipeline.RAW_JSON_PATH))
        )

        from pyspark.sql.functions import explode

        df_produtos = (
            df
            .select(
                explode("products").alias("produto")
            )
            .select("produto.*")
        )

        quantidade_spark = df_produtos.count()

        assert quantidade_spark == quantidade_api

    finally:

        spark.stop()


# ============================================================
# TESTE 10
# ============================================================

def test_ids_nao_sao_nulos(pipeline):

    dados, quantidade = (
        pipeline.buscar_dados_api()
    )

    pipeline.salvar_json_bronze(dados)

    spark = pipeline.criar_sessao_spark()

    try:

        df = (
            spark.read
            .option("multiline", "true")
            .json(str(pipeline.RAW_JSON_PATH))
        )

        from pyspark.sql.functions import explode

        df_produtos = (
            df
            .select(
                explode("products").alias("produto")
            )
            .select("produto.*")
        )

        quantidade_ids_nulos = (
            df_produtos
            .filter(
                df_produtos["id"].isNull()
            )
            .count()
        )

        assert quantidade_ids_nulos == 0

    finally:

        spark.stop()


# ============================================================
# TESTE 11 - TESTE COMPLETO
# ============================================================

def test_pipeline_completo(pipeline):

    # --------------------------------------------------------
    # 1. API
    # --------------------------------------------------------

    dados, quantidade_api = (
        pipeline.buscar_dados_api()
    )

    assert isinstance(dados, dict)

    assert "products" in dados

    assert isinstance(
        dados["products"],
        list
    )

    assert quantidade_api > 0

    # --------------------------------------------------------
    # 2. Bronze
    # --------------------------------------------------------

    pipeline.salvar_json_bronze(dados)

    assert pipeline.RAW_JSON_PATH.exists()

    # --------------------------------------------------------
    # 3. JSON
    # --------------------------------------------------------

    with open(
        pipeline.RAW_JSON_PATH,
        "r",
        encoding="utf-8"
    ) as arquivo:

        bronze = json.load(arquivo)

    assert len(
        bronze["products"]
    ) == quantidade_api

    # --------------------------------------------------------
    # 4. Spark
    # --------------------------------------------------------

    spark = pipeline.criar_sessao_spark()

    try:

        pipeline.processar_com_spark(
            spark,
            quantidade_api
        )

    finally:

        spark.stop()