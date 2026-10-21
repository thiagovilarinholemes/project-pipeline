import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import os
import json
from pathlib import Path

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
BRONZE_DIR = Path(BRONZE_PATH)

# Cria o diretório Bronze caso ele não exista.
# Se já existir, não gera erro.
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# Define o caminho completo do arquivo JSON bruto.
RAW_JSON_PATH = BRONZE_DIR / "dummyjson.json"

# Cria a sessão PySpark
def criar_sessao_spark() -> SparkSession:

    return (
        SparkSession.builder
        .appName("PySpark-API-Bronze")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def buscar_dados_api() -> str:
    print()
    print("=" * 70)
    print(f"Consultando API: {VERDE}{API_URL}{RESET}")
    print("=" * 70)
    print()

    # Define um tempo limite para evitar espera indefinida.
    response = requests.get(
        API_URL,
        timeout=30,
        headers={"Accept": "application/json"},
    )

    # Gera uma exceção para respostas HTTP de erro.
    response.raise_for_status()

# Primeira verificação dos dados
# ===============================================================================
    # Confirma que a resposta pode ser interpretada como JSON.
    dados_api = response.json()
    
    if not isinstance(dados_api, dict):
        raise ValueError(
            "Formato inesperado: a API deveria retornar uma lista."
        )
# Segunda verificação dos dados
# ===============================================================================
    # Obtém a lista de produtos dentro do objeto.
    produtos = dados_api.get("products")

    if not isinstance(produtos, list):
        raise ValueError(
            "A resposta da API não contém uma lista válida em 'products'."
        )
# ===============================================================================
    print("=" * 70)
    print(f"Produtos recebidos: {VERDE}{len(produtos)}{RESET}")
    print(f"Total informado pela API: {VERDE}{dados_api.get('total')}{RESET}")
    print(f"Produtos ignorados antes desta página: {VERDE}{dados_api.get('skip')}{RESET}")
    print("=" * 70)
    print()
    # Retorna a resposta completa e a quantidade recebida.
    return dados_api, len(produtos)

def salvar_json_bronze(conteudo_json):

    # Extrai a lista de produtos da resposta da API.
    produtos = conteudo_json.get("products", [])

    # Define os campos que serão preservados.
    campos_selecionados = [
        "id",
        "model",
        "text",
    ]

    # Mantém somente os campos selecionados em cada produto.
    produtos_filtrados = [
        {
            campo: produto[campo]
            for campo in campos_selecionados
            if campo in produto
        }
        for produto in produtos
    ]

    # Preserva os metadados da resposta da API.
    conteudo_filtrado = {
        "total": conteudo_json.get("total"),
        "skip": conteudo_json.get("skip"),
        "limit": conteudo_json.get("limit"),
        "products": produtos_filtrados
    }

    os.makedirs(BRONZE_PATH, exist_ok=True)

    with open(RAW_JSON_PATH, "w", encoding="utf-8") as arquivo:
        json.dump(
            conteudo_filtrado, # conteudo_json - coloca todos os campos
            arquivo,
            ensure_ascii=False,
            indent=2
        )
    print("=" * 70)
    print(f"Resposta original salva em: {VERDE}{RAW_JSON_PATH}{RESET}")
    print("=" * 70)
    print()

def processar_com_spark(spark, quantidade_esperada):
    
    """Lê o JSON bruto e realiza verificações iniciais."""

    # A resposta da API é um array JSON.
    # multiline=True permite ler esse formato diretamente.
    df = (
        spark.read
        .option("multiline", "true")
        .json(str(RAW_JSON_PATH))
    )

    print(f"\n{AZUL}ESTRUTURA DO JSON{RESET}")
    print("="*70)
    df.printSchema()

    # Expande a lista de produtos em registros individuais.
    df_produtos = (
        df
        .select(explode(col("products")).alias("produto"))
        .select("produto.*")
    )

    print(f"\n{AZUL}PRIMEIROS REGISTROS{RESET}")
    print("="*70)
    df_produtos.select(
        col("id").alias('id'),
        col("title").alias('title'),
        col("description").alias('description'),
        col("category").alias('category'),
        col("price").alias('price'),
        col("rating").alias('rating'),
        col("dimensions.width").alias("width"),
        col("address.suite").alias("suite"),
    ).show(2, truncate=20)

    # Ação Spark: conta os registros efetivamente lidos.
    quantidade_lida = df_produtos.count()
    
    assert quantidade_lida == quantidade_esperada, (
        f"Quantidade divergente: API={quantidade_esperada}, "
        f"Spark={quantidade_lida}."
    )

    print()
    print("="*70)
    print(f"\nRegistros lidos pelo Spark: {VERDE}{quantidade_lida} [OK]{RESET}")
    print(f"Produtos processados pelo Spark: {VERDE}{quantidade_lida} [OK]{RESET}")
    print(f"Processamento Spark concluído com sucesso!{VERDE} [OK]{RESET}")
    print("="*70)
    print()

    # Verifica se o identificador está preenchido.
    quantidade_ids_nulos = (
        df_produtos.filter(df_produtos["id"].isNull()).count()
    )

    assert quantidade_ids_nulos == 0, (
        "Foram encontrados usuários sem identificador."
    )
    
    print("="*70)
    print(f"Todos os registros possuem identificador. {VERDE}[OK]{RESET}")
    print("="*70)
    print()
    return df


def main():
    """Executa a ingestão da API para a camada Bronze."""

    spark = criar_sessao_spark()

    try:
        # 1. Busca os dados da API.
        conteudo_json, quantidade_esperada = buscar_dados_api()

        # 2. Persiste a resposta original.
        salvar_json_bronze(conteudo_json)

        # 3. Lê e verifica os dados usando Spark.
        processar_com_spark(spark, quantidade_esperada)
        print("="*70)
        print(f"Ingestão Bronze concluída com sucesso! {VERDE}[OK]{RESET}")
        print("="*70)
        print()
    finally:
        # Encerra a sessão mesmo quando ocorre uma falha.
        spark.stop()


if __name__ == "__main__":
    main()