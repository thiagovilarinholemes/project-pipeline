"""
Camada Bronze: ingestão genérica de APIs REST usando PySpark.

Suporta:
- APIs que retornam uma lista JSON: [...]
- APIs que retornam um objeto com uma lista de registros: {"products": [...]}
- APIs que retornam um objeto com uma lista de usuários: {"users": [...]}
- APIs que retornam um único objeto JSON: {...}

Variáveis de ambiente obrigatórias:
- API_URL
- BRONZE_PATH
"""

import os
import json
import requests

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ==============================================================================
# CORES ANSI PARA AS MENSAGENS DO TERMINAL
# ==============================================================================

VERDE = "\033[92m"
RESET = "\033[0m"
AZUL = "\033[94m"
AMARELO = "\033[93m"


# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================

API_URL = os.getenv("API_URL")
BRONZE_PATH = os.getenv("BRONZE_PATH")

if not API_URL:
    raise ValueError("A variável de ambiente API_URL não foi definida.")

if not BRONZE_PATH:
    raise ValueError("A variável de ambiente BRONZE_PATH não foi definida.")

BRONZE_DIR = Path(BRONZE_PATH)
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

RAW_JSON_PATH = BRONZE_DIR / "api_response.json"


# ==============================================================================
# SESSÃO SPARK
# ==============================================================================

def criar_sessao_spark() -> SparkSession:
    """Cria ou recupera uma sessão Spark."""

    return (
        SparkSession.builder
        .appName("PySpark-API-Bronze")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ==============================================================================
# NORMALIZAÇÃO DA RESPOSTA DA API
# ==============================================================================

def extrair_registros(conteudo_json):
    """
    Converte diferentes formatos de resposta em uma lista de registros.

    Lista:
        [{"id": 1}, {"id": 2}]

    Objeto com lista:
        {"products": [{"id": 1}, {"id": 2}]}

    Objeto individual:
        {"id": 1, "name": "Thiago"}

    Retorna:
        Uma lista de dicionários.
    """

    # Caso 1: a API retorna diretamente uma lista.
    if isinstance(conteudo_json, list):

        if not all(isinstance(item, dict) for item in conteudo_json):
            raise ValueError(
                "A lista retornada pela API contém itens que não são objetos JSON."
            )

        return conteudo_json

    # Caso 2: a API retorna um objeto JSON.
    if isinstance(conteudo_json, dict):

        # Chaves comuns que armazenam listas de registros.
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

        # Procura primeiro as chaves mais comuns.
        for chave in chaves_prioritarias:
            valor = conteudo_json.get(chave)

            if isinstance(valor, list):
                if not all(isinstance(item, dict) for item in valor):
                    raise ValueError(
                        f"A chave '{chave}' contém itens que não são objetos JSON."
                    )

                return valor

        # Procura outras listas de objetos sem depender do nome da chave.
        for chave, valor in conteudo_json.items():
            if (
                isinstance(valor, list)
                and all(isinstance(item, dict) for item in valor)
            ):
                return valor

        # Caso 3: o próprio objeto representa um único registro.
        return [conteudo_json]

    raise ValueError(
        "Formato JSON não suportado. Esperava-se uma lista ou um objeto."
    )


# ==============================================================================
# BUSCA DOS DADOS NA API
# ==============================================================================

def buscar_dados_api():
    """Consulta a API e identifica a quantidade de registros recebidos."""

    print()
    print("=" * 70)
    print(f"Consultando API: {VERDE}{API_URL}{RESET}")
    print("=" * 70)
    print()

    response = requests.get(
        API_URL,
        timeout=30,
        headers={"Accept": "application/json"},
    )

    # Interrompe a execução caso a API retorne erro HTTP.
    response.raise_for_status()

    # Interpreta a resposta como JSON.
    try:
        conteudo_json = response.json()
    except ValueError as erro:
        raise ValueError(
            "A API não retornou um JSON válido."
        ) from erro

    # Normaliza listas e objetos para contar os registros.
    registros = extrair_registros(conteudo_json)

    print("=" * 70)
    print(f"Formato recebido: {VERDE}{type(conteudo_json).__name__}{RESET}")
    print(f"Registros recebidos: {VERDE}{len(registros)}{RESET}")

    # Exibe os metadados quando existirem.
    if isinstance(conteudo_json, dict):
        for campo in ("total", "skip", "limit"):
            if campo in conteudo_json:
                print(
                    f"{campo.capitalize()} informado pela API: "
                    f"{VERDE}{conteudo_json[campo]}{RESET}"
                )

    print("=" * 70)
    print()

    return conteudo_json, len(registros)


# ==============================================================================
# PERSISTÊNCIA DA CAMADA BRONZE
# ==============================================================================

def salvar_json_bronze(conteudo_json):

    # Extrai os registros independentemente do formato da API.
    registros = extrair_registros(conteudo_json)

    # Define os campos que serão preservados, quando existirem.
    """
    Os campos estensivos ficam assim, ele irá pegar tudo que estiver no primeiro campo,
    a seleção é feita na Camada Silver:
        address.street -> address
        company.name -> company
    """
    campos_selecionados = [
        "id",
        "model",
        "text",
    ]

    # Mantém somente os campos selecionados que existem em cada registro.
    registros_filtrados = [
        {
            campo: registro[campo]
            for campo in campos_selecionados
            if campo in registro
        }
        for registro in registros
    ]

    # Preserva a lista diretamente ou a estrutura original com metadados.
    if isinstance(conteudo_json, list):
        conteudo_filtrado = registros_filtrados

    else:
        conteudo_filtrado = conteudo_json.copy()

        # Identifica a chave que contém os registros.
        for chave, valor in conteudo_json.items():
            if isinstance(valor, list) and valor == registros:
                conteudo_filtrado[chave] = registros_filtrados
                break
        else:
            conteudo_filtrado = registros_filtrados[0]

    # Salva os dados filtrados.
    with RAW_JSON_PATH.open("w", encoding="utf-8") as arquivo:
        json.dump(
            conteudo_filtrado,
            arquivo,
            ensure_ascii=False,
            indent=2,
        )

    print("=" * 70)
    print(f"JSON salvo em: {VERDE}{RAW_JSON_PATH}{RESET}")
    print("=" * 70)

# ==============================================================================
# PROCESSAMENTO COM PYSPARK
# ==============================================================================

def processar_com_spark(spark, quantidade_esperada):
    """Lê, exibe e valida os registros da camada Bronze."""

    # Carrega o JSON original preservado na camada Bronze.
    with RAW_JSON_PATH.open("r", encoding="utf-8") as arquivo:
        conteudo_json = json.load(arquivo)

    # Identifica automaticamente a lista de registros.
    registros = extrair_registros(conteudo_json)

    if not registros:
        raise ValueError(
            "A API não retornou registros. Não é possível inferir "
            "automaticamente o esquema do DataFrame."
        )

    # Converte os registros em JSON para o Spark inferir o esquema.
    # Isso permite trabalhar com campos diferentes entre APIs.
    registros_json = [
        json.dumps(registro, ensure_ascii=False)
        for registro in registros
    ]

    rdd_json = spark.sparkContext.parallelize(registros_json)

    df = spark.read.json(rdd_json)

    # Exibe o esquema detectado automaticamente.
    print(f"\n{AZUL}ESTRUTURA DO JSON{RESET}")
    print("=" * 70)
    df.printSchema()

    # Exibe os primeiros registros sem selecionar colunas fixas.
    # Assim, não ocorre erro se price, rating ou dimensions não existirem.
    print(f"\n{AZUL}PRIMEIROS REGISTROS{RESET}")
    print("=" * 70)
    df.show(2, truncate=20, vertical=False)

    # Exibe as colunas identificadas pelo Spark.
    print(f"\n{AZUL}COLUNAS IDENTIFICADAS{RESET}")
    print("=" * 70)

    for indice, nome_coluna in enumerate(df.columns, start=1):
        print(f"{indice:02d}. {nome_coluna}")

    print("=" * 70)
    print()

    # Conta os registros efetivamente carregados pelo Spark.
    quantidade_lida = df.count()

    assert quantidade_lida == quantidade_esperada, (
        f"Quantidade divergente: API={quantidade_esperada}, "
        f"Spark={quantidade_lida}."
    )

    print("=" * 70)
    print(
        f"Registros lidos pelo Spark: "
        f"{VERDE}{quantidade_lida} [OK]{RESET}"
    )
    print(
        f"Registros processados: "
        f"{VERDE}{quantidade_lida} [OK]{RESET}"
    )
    print(f"Processamento Spark concluído! {VERDE}[OK]{RESET}")
    print("=" * 70)
    print()

    # Valida identificadores apenas quando a API fornece a coluna id.
    if "id" in df.columns:

        quantidade_ids_nulos = (
            df.filter(col("id").isNull()).count()
        )

        assert quantidade_ids_nulos == 0, (
            "Foram encontrados registros sem identificador."
        )

        print("=" * 70)
        print(
            f"Todos os registros possuem identificador. "
            f"{VERDE}[OK]{RESET}"
        )
        print("=" * 70)
        print()

    else:
        print(
            f"{AMARELO}Aviso: a API não possui a coluna 'id'. "
            f"Validação de identificadores ignorada.{RESET}"
        )

    return df


# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================

def main():
    """Executa a ingestão da API para a camada Bronze."""

    spark = criar_sessao_spark()

    try:
        # 1. Consulta a API.
        conteudo_json, quantidade_esperada = buscar_dados_api()

        # 2. Salva a resposta completa.
        salvar_json_bronze(conteudo_json)

        # 3. Lê e valida os registros com PySpark.
        processar_com_spark(spark, quantidade_esperada)

        print("=" * 70)
        print(f"Ingestão Bronze concluída com sucesso! {VERDE}[OK]{RESET}")
        print("=" * 70)
        print()

    finally:
        # Encerra a sessão mesmo se ocorrer um erro.
        spark.stop()


if __name__ == "__main__":
    main()