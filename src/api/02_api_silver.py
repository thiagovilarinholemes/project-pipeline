
"""
Camada Silver: transformação genérica de APIs REST com PySpark.

Suporta:
- JSON em formato de lista: [...]
- JSON em formato de objeto com lista: {"products": [...]}
- JSON em formato de objeto com lista: {"users": [...]}
- JSON representando um único registro: {...}

Variáveis de ambiente:
- API_URL
- BRONZE_PATH
- SILVER_PATH
"""

import os
import json

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when
from pyspark.sql.types import StructType, StringType


# ==============================================================================
# 1. CONFIGURAÇÕES
# ==============================================================================

VERDE = "\033[92m"
RESET = "\033[0m"
AZUL = "\033[94m"
AMARELO = "\033[93m"

API_URL = os.getenv("API_URL")

if not API_URL:
    raise ValueError("A variável API_URL não foi definida.")

BRONZE_ENV = os.getenv("BRONZE_PATH")
SILVER_ENV = os.getenv("SILVER_PATH")

if not BRONZE_ENV:
    raise ValueError("A variável BRONZE_PATH não foi definida.")

if not SILVER_ENV:
    raise ValueError("A variável SILVER_PATH não foi definida.")

BRONZE_DIR = Path(BRONZE_ENV)
SILVER_DIR = Path(SILVER_ENV)

BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# CAMPOS SELECIONADOS PARA A CAMADA SILVER
# ============================================================
"""
Os campos estensivos ficam assim:
    dimensions.width -> dimensions_width  
    dimensions.height -> dimensions_height
"""
campos_selecionados = [
    "year",
    "winners_category",
]

# Nome do arquivo gerado pela camada Bronze.
RAW_JSON_PATH = BRONZE_DIR / "api_response.json"


# ==============================================================================
# 2. EXTRAÇÃO DOS REGISTROS
# ==============================================================================

def extrair_registros(conteudo_json):
    """Normaliza listas e objetos JSON para uma lista de registros."""

    # API que retorna diretamente uma lista.
    if isinstance(conteudo_json, list):
        if not all(isinstance(item, dict) for item in conteudo_json):
            raise ValueError(
                "A resposta contém itens que não são objetos JSON."
            )

        return conteudo_json

    # API que retorna um objeto JSON.
    if isinstance(conteudo_json, dict):

        # Chaves mais comuns que armazenam listas de registros.
        chaves_prioritarias = [
            "products",
            "users",
            "data",
            "results",
            "items",
            "records",
            "posts",
            "comments",
            "todos",
        ]

        for chave in chaves_prioritarias:
            valor = conteudo_json.get(chave)

            if isinstance(valor, list):
                if not all(isinstance(item, dict) for item in valor):
                    raise ValueError(
                        f"A chave '{chave}' contém itens incompatíveis."
                    )

                return valor

        # Procura listas de registros com outros nomes.
        for valor in conteudo_json.values():
            if isinstance(valor, list) and all(
                isinstance(item, dict) for item in valor
            ):
                return valor

        # Um objeto sem listas de registros é tratado como um registro.
        return [conteudo_json]

    raise ValueError(
        "A resposta da API deve ser uma lista ou um objeto JSON."
    )


# ==============================================================================
# 3. LIMPEZA DE TEXTO
# ==============================================================================

def limpar_texto(coluna):
    """Remove espaços externos e transforma textos vazios em NULL."""

    texto = trim(coluna)

    return when(
        coluna.isNotNull() & (texto != ""),
        texto,
    ).otherwise(None)


# ==============================================================================
# 4. EXPANSÃO DE CAMPOS ANINHADOS
# ==============================================================================


from pyspark.sql.functions import col
from pyspark.sql.types import StructType


def achatar_dataframe(df):
    """
    Achata recursivamente campos StructType.

    Exemplos:
        dimensions.width -> dimensions_width
        address.city     -> address_city
        company.name     -> company_name

    Mantém os demais tipos de coluna, incluindo arrays.
    """

    while True:
        # Localiza todas as colunas que ainda são structs.
        campos_struct = [
            campo.name
            for campo in df.schema.fields
            if isinstance(campo.dataType, StructType)
        ]

        # Se não houver structs, o achatamento terminou.
        if not campos_struct:
            break

        # Expande os campos internos dos structs encontrados.
        colunas = []

        for campo in df.schema.fields:
            nome = campo.name

            if isinstance(campo.dataType, StructType):
                for subcampo in campo.dataType.fields:
                    novo_nome = f"{nome}_{subcampo.name}"

                    colunas.append(
                        col(f"`{nome}`.`{subcampo.name}`")
                        .alias(novo_nome)
                    )
            else:
                colunas.append(col(f"`{nome}`"))

        df = df.select(*colunas)

    return df


# ==============================================================================
# 5. VALIDAÇÃO DO DATAFRAME
# ==============================================================================

def validar_dataframe(df):
    """Valida a existência de registros e a qualidade dos identificadores."""

    quantidade = df.count()

    print(f"Quantidade de registros: {VERDE}{quantidade}{RESET}")

    # Uma Silver vazia continua sendo considerada um erro.
    if quantidade == 0:
        raise ValueError(
            "A camada Silver não pode ser vazia. "
            "Verifique a resposta da API, os dados Bronze "
            "e as transformações aplicadas."
        )

    # Verifica IDs somente quando a API fornece essa coluna.
    if "id" in df.columns:

        quantidade_ids_nulos = df.filter(
            col("id").isNull()
        ).count()

        if quantidade_ids_nulos > 0:
            raise ValueError(
                f"A coluna 'id' possui {quantidade_ids_nulos} valores nulos."
            )

        quantidade_ids_unicos = df.select("id").distinct().count()

        if quantidade_ids_unicos != quantidade:
            raise ValueError(
                "Foram encontrados identificadores duplicados."
            )

        print(f"Validação dos IDs: {VERDE}[OK]{RESET}")

    else:
        print(
            f"{AMARELO}A API não possui coluna 'id'. "
            f"Validação de IDs ignorada.{RESET}"
        )

    # Valida os campos textuais sem exigir nomes específicos.
    colunas_texto = [
        campo.name
        for campo in df.schema.fields
        if isinstance(campo.dataType, StringType)
    ]

    for nome_coluna in colunas_texto:
        quantidade_vazios = df.filter(
            col(nome_coluna).isNotNull()
            & (trim(col(nome_coluna)) == "")
        ).count()

        if quantidade_vazios > 0:
            raise ValueError(
                f"A coluna '{nome_coluna}' possui "
                f"{quantidade_vazios} textos vazios."
            )

    print(f"Validação Silver: {VERDE}[OK]{RESET}")
    print()

    return quantidade


# ==============================================================================
# 6. PROCESSAMENTO PRINCIPAL
# ==============================================================================

def main():
    """Executa a transformação Bronze -> Silver."""

    spark = (
        SparkSession.builder
        .appName("TutorialPySpark-API-Silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        # ----------------------------------------------------------------------
        # ETAPA A - LEITURA BRONZE
        # ----------------------------------------------------------------------

        print(f"\n=== {AZUL}ETAPA 1: LEITURA BRONZE{RESET} ===")

        if not RAW_JSON_PATH.is_file():
            raise FileNotFoundError(
                f"Arquivo Bronze não encontrado: {RAW_JSON_PATH}. "
                "Execute primeiro a camada Bronze."
            )

        with RAW_JSON_PATH.open("r", encoding="utf-8") as arquivo:
            conteudo_json = json.load(arquivo)

        registros = extrair_registros(conteudo_json)

        if not registros:
            raise ValueError(
                "A Bronze não possui registros para processar."
            )

        print(f"Origem: {RAW_JSON_PATH}")
        print(f"Registros identificados: {len(registros)}")

        # Converte cada registro para uma linha JSON.
        registros_json = [
            json.dumps(registro, ensure_ascii=False)
            for registro in registros
        ]

        # O Spark infere as colunas a partir dos registros recebidos.
        rdd_json = spark.sparkContext.parallelize(registros_json)
        df_bronze = spark.read.json(rdd_json)

        print(f"\n{AZUL}SCHEMA BRONZE{RESET}")
        df_bronze.printSchema()

        print(f"\n{AZUL}AMOSTRA BRONZE{RESET}")
        df_bronze.show(5, truncate=30)

        quantidade_bronze = df_bronze.count()

        print(f"Registros Bronze: {VERDE}{quantidade_bronze}{RESET}")

        # ----------------------------------------------------------------------
        # ETAPA B - TRANSFORMAÇÃO SILVER
        # ----------------------------------------------------------------------

        print(f"\n=== {AZUL}ETAPA 2: TRANSFORMAÇÃO SILVER{RESET} ===")

        # Converte objetos aninhados em colunas simples.
        df_silver = achatar_dataframe(df_bronze)

        # Limpa todas as colunas de texto existentes.
        colunas_tratadas = []

        for campo in df_silver.schema.fields:

            if isinstance(campo.dataType, StringType):
                colunas_tratadas.append(
                    limpar_texto(col(campo.name)).alias(campo.name)
                )
            else:
                colunas_tratadas.append(col(campo.name))

        df_silver = df_silver.select(*colunas_tratadas)

        # Remove registros sem ID apenas se a coluna existir.
        if "id" in df_silver.columns:
            df_silver = df_silver.filter(col("id").isNotNull())

        print(f"\n{AZUL}SCHEMA SILVER{RESET}")
        df_silver.printSchema()

        print(f"\n{AZUL}AMOSTRA SILVER{RESET}")
        df_silver.show(5, truncate=20)

        print(f"\n{AZUL}PLANO DA TRANSFORMAÇÃO{RESET}")
        df_silver.explain(mode="formatted")

        # ============================================================
        # SELECIONAR SOMENTE OS CAMPOS CONFIGURADOS
        # ============================================================

        # Mantém apenas os campos que existem no DataFrame.
        campos_existentes = [
            campo
            for campo in campos_selecionados
            if campo in df_silver.columns
        ]

        if not campos_existentes:
            raise ValueError(
                "Nenhum dos campos_selecionados foi encontrado no DataFrame."
            )

        df_silver = df_silver.select(*campos_existentes)

        print(f"=== {AZUL}CAMPOS SELECIONADOS PARA A CAMADA SILVER{RESET} ===")
        print("=" * 60)

        for campo in campos_existentes:
            print(f"- {campo}")
            
        # ----------------------------------------------------------------------
        # ETAPA C - VALIDAÇÃO
        # ----------------------------------------------------------------------

        print(f"\n=== {AZUL}ETAPA 3: VALIDAÇÃO{RESET} ===")

        quantidade = validar_dataframe(df_silver)

        # ----------------------------------------------------------------------
        # ETAPA D - GRAVAÇÃO PARQUET
        # ----------------------------------------------------------------------

        print(f"\n=== {AZUL}ETAPA 4: GRAVAÇÃO PARQUET{RESET} ===")

        df_gravacao = (
            df_silver.orderBy("id")
            if "id" in df_silver.columns
            else df_silver
        )

        (
            df_gravacao.write
            .mode("overwrite")
            .parquet(str(SILVER_DIR))
        )

        print(f"Destino: {VERDE}{SILVER_DIR}{RESET}")
        print(f"Registros gravados: {VERDE}{quantidade}{RESET}")

        # ----------------------------------------------------------------------
        # ETAPA E - CONSULTA PARQUET
        # ----------------------------------------------------------------------

        print(f"\n=== {AZUL}ETAPA 5: CONSULTA PARQUET{RESET} ===")

        df_parquet = spark.read.parquet(str(SILVER_DIR))

        # Seleciona somente colunas que realmente existem.
        colunas_disponiveis = df_parquet.columns
        colunas_consulta = colunas_disponiveis[
            :min(4, len(colunas_disponiveis))
        ]

        if not colunas_consulta:
            raise ValueError(
                "Não há colunas disponíveis para consultar o Parquet."
            )

        consulta_base = df_parquet

        # Demonstra um filtro compatível com as APIs que possuem ID.
        if "id" in colunas_disponiveis:
            consulta_base = consulta_base.filter(col("id").isNotNull())

        consulta = consulta_base.select(*colunas_consulta)

        print(f"Colunas selecionadas: {colunas_consulta}")

        print(f"\n{AZUL}PLANO DA CONSULTA{RESET}")
        consulta.explain(mode="formatted")

        print(f"\n{AZUL}RESULTADO DA CONSULTA{RESET}")
        consulta.show(10, truncate=False)

        # ----------------------------------------------------------------------
        # ETAPA F - CONFERÊNCIA FINAL
        # ----------------------------------------------------------------------

        quantidade_final = df_parquet.count()

        if quantidade_final != quantidade:
            raise ValueError(
                "A quantidade de registros mudou após a gravação Parquet."
            )

        print(f"Registros conferidos: {VERDE}{quantidade_final}{RESET}")
        print(f"\n{VERDE}CAMADA SILVER CONCLUÍDA COM SUCESSO!{RESET}\n")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
