# tests/test_api_gold.py

"""
Testes automatizados da camada Gold do projeto PySpark.

Testa:
    1. Leitura dos dados Silver.
    2. Indicadores gerais.
    3. Produtos por categoria.
    4. Indicadores de qualidade.
    5. Gravação e leitura dos arquivos Parquet.
    6. Execução completa do script 03_api_gold.py.
    7. Verificação de campos obrigatórios.

Execução:
    pytest -v tests/test_api_gold.py
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
)


# ============================================================
# 1. CONFIGURAÇÃO DOS TESTES
# ============================================================

@pytest.fixture(scope="module")
def spark():
    """Cria uma sessão Spark local para os testes."""

    sessao = (
        SparkSession.builder
        .master("local[2]")
        .appName("Testes-API-Gold")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    sessao.sparkContext.setLogLevel("ERROR")

    yield sessao

    sessao.stop()


# ============================================================
# 2. CRIAÇÃO DOS DADOS DE TESTE
# ============================================================

@pytest.fixture
def ambiente_teste(spark, tmp_path):
    """
    Cria dados Silver fictícios e diretórios temporários.

    Os dados são isolados dos dados reais do projeto.
    """

    caminho_silver = tmp_path / "silver"
    caminho_gold = tmp_path / "gold"

    caminho_silver.mkdir(parents=True)
    caminho_gold.mkdir(parents=True)

    dados = [
        (1, "Notebook", "Eletronicos", 5),
        (2, "Mouse", "Eletronicos", 4),
        (3, "Teclado", "Eletronicos", 5),
        (4, "Cadeira", "Moveis", 4),
        (5, "Mesa", "Moveis", 3),
    ]

    colunas = [
        "id",
        "titulo",
        "categoria",
        "avaliacao",
    ]

    dataframe = spark.createDataFrame(dados, colunas)

    dataframe.write.mode("overwrite").parquet(
        str(caminho_silver)
    )

    return {
        "silver": caminho_silver,
        "gold": caminho_gold,
        "dados": dataframe,
    }


# ============================================================
# 3. TESTE DA LEITURA SILVER
# ============================================================

def test_leitura_silver(spark, ambiente_teste):
    """Confere a quantidade de registros e as colunas da Silver."""

    dataframe = spark.read.parquet(
        str(ambiente_teste["silver"])
    )

    assert dataframe.count() == 5

    assert set(dataframe.columns) == {
        "id",
        "titulo",
        "categoria",
        "avaliacao",
    }


# ============================================================
# 4. TESTE DOS INDICADORES GERAIS
# ============================================================

def test_indicadores_gerais(spark, ambiente_teste):
    """Confere os valores calculados nos indicadores gerais."""

    dataframe = spark.read.parquet(
        str(ambiente_teste["silver"])
    )

    resultado = dataframe.select(
        "id",
        "titulo",
        "categoria",
        "avaliacao",
    ).agg(
        count("*").alias("total_produtos"),
        countDistinct("titulo").alias("total_titulos"),
        countDistinct("categoria").alias("total_categorias"),
        avg(col("id").cast("double")).alias("media_id_produto"),
    ).first()

    assert resultado["total_produtos"] == 5
    assert resultado["total_titulos"] == 5
    assert resultado["total_categorias"] == 2
    assert resultado["media_id_produto"] == pytest.approx(3.0)


# ============================================================
# 5. TESTE DOS PRODUTOS POR CATEGORIA
# ============================================================

def test_produtos_por_categoria(spark, ambiente_teste):
    """Confere o agrupamento por categoria e suas avaliações."""

    dataframe = spark.read.parquet(
        str(ambiente_teste["silver"])
    )

    resultado = (
        dataframe
        .groupBy("categoria")
        .agg(
            count("*").alias("total_categoria"),
            countDistinct("avaliacao").alias("total_avaliacao"),
        )
        .collect()
    )

    categorias = {
        linha["categoria"]: linha.asDict()
        for linha in resultado
    }

    assert categorias["Eletronicos"]["total_categoria"] == 3
    assert categorias["Eletronicos"]["total_avaliacao"] == 2

    assert categorias["Moveis"]["total_categoria"] == 2
    assert categorias["Moveis"]["total_avaliacao"] == 2


# ============================================================
# 6. TESTE DOS INDICADORES DE QUALIDADE
# ============================================================

def test_qualidade_dados(spark, ambiente_teste):
    """Confere os indicadores de preenchimento e unicidade."""

    dataframe = spark.read.parquet(
        str(ambiente_teste["silver"])
    )

    resultado = dataframe.select(
        "id",
        "titulo",
        "categoria",
        "avaliacao",
    ).agg(
        count("*").alias("total_registros"),
        count("id").alias("ids_preenchidos"),
        countDistinct("id").alias("ids_distintos"),
        count("titulo").alias("titulos_preenchidos"),
        count("categoria").alias("categorias_preenchidas"),
        count("avaliacao").alias("avaliacao_preenchidas"),
    ).first()

    assert resultado["total_registros"] == 5
    assert resultado["ids_preenchidos"] == 5
    assert resultado["ids_distintos"] == 5
    assert resultado["titulos_preenchidos"] == 5
    assert resultado["categorias_preenchidas"] == 5
    assert resultado["avaliacao_preenchidas"] == 5


# ============================================================
# 7. TESTE DOS CAMPOS OBRIGATÓRIOS
# ============================================================

def test_campos_obrigatorios(spark, ambiente_teste):
    """Verifica se os campos obrigatórios estão preenchidos."""

    dataframe = spark.read.parquet(
        str(ambiente_teste["silver"])
    )

    campos = [
        "id",
        "titulo",
        "categoria",
        "avaliacao",
    ]

    for campo in campos:
        quantidade_nula = (
            dataframe
            .filter(col(campo).isNull())
            .count()
        )

        assert quantidade_nula == 0, (
            f"O campo '{campo}' contém "
            f"{quantidade_nula} valor(es) nulo(s)."
        )


# ============================================================
# 8. TESTE DE GRAVAÇÃO E LEITURA PARQUET
# ============================================================

def test_gravacao_parquet_gold(spark, ambiente_teste):
    """Grava indicadores em Parquet e verifica sua leitura."""

    dataframe = spark.read.parquet(
        str(ambiente_teste["silver"])
    )

    indicadores = dataframe.agg(
        count("*").alias("total_produtos"),
        countDistinct("titulo").alias("total_titulos"),
        countDistinct("categoria").alias("total_categorias"),
        avg(col("id").cast("double")).alias("media_id_produto"),
    )

    caminho = (
        ambiente_teste["gold"]
        / "indicadores_gerais"
    )

    indicadores.write.mode("overwrite").parquet(
        str(caminho)
    )

    assert caminho.exists()

    gravado = spark.read.parquet(str(caminho))

    assert gravado.count() == 1

    resultado = gravado.first()

    assert resultado["total_produtos"] == 5
    assert resultado["total_titulos"] == 5
    assert resultado["total_categorias"] == 2
    assert resultado["media_id_produto"] == pytest.approx(3.0)


# ============================================================
# 9. TESTE DA EXECUÇÃO COMPLETA DO SCRIPT
# ============================================================

def test_execucao_pipeline_gold(ambiente_teste):
    """
    Executa src/03_api_gold.py em um processo separado.

    O teste utiliza diretórios temporários e não sobrescreve
    os dados reais do projeto.
    """

    raiz_projeto = Path(__file__).resolve().parents[1]

    script = raiz_projeto / "src" / "03_api_gold.py"

    assert script.exists(), (
        f"Script não encontrado: {script}"
    )

    ambiente = os.environ.copy()

    ambiente["SILVER_PATH"] = str(
        ambiente_teste["silver"]
    )

    ambiente["GOLD_PATH"] = str(
        ambiente_teste["gold"]
    )

    resultado = subprocess.run(
        [sys.executable, str(script)],
        env=ambiente,
        capture_output=True,
        text=True,
        timeout=180,
    )

    assert resultado.returncode == 0, (
        "A execução da camada Gold falhou.\n\n"
        f"SAÍDA:\n{resultado.stdout}\n\n"
        f"ERRO:\n{resultado.stderr}"
    )

    # Confere se os três destinos foram criados.
    tabelas = [
        "indicadores_gerais",
        "produtos_por_categoria",
        "qualidade_dados",
    ]

    for tabela in tabelas:
        caminho = ambiente_teste["gold"] / tabela

        assert caminho.exists(), (
            f"A tabela Gold '{tabela}' não foi criada."
        )
