
"""Funções para carregar dados de arquivos, APIs REST e bancos SQL."""

import io
import json

import pandas as pd
import requests


def normalize_dataframe(df):
    """Retorna uma cópia com os nomes das colunas em formato string."""
    df = df.copy()
    df.columns = [str(column) for column in df.columns]
    return df


def dataframe_from_json(data):
    """Converte uma resposta JSON em DataFrame."""

    # Caso específico da DummyJSON: extrai somente products.
    if isinstance(data, dict) and isinstance(data.get("products"), list):
        return pd.json_normalize(data["products"], sep=".")

    # A API retornou diretamente uma lista de registros.
    if isinstance(data, list):
        return pd.json_normalize(data, sep=".")

    # Procura uma lista de registros dentro de outro objeto JSON.
    if isinstance(data, dict):
        for value in data.values():
            if isinstance(value, list) and all(
                isinstance(item, dict) for item in value
            ):
                return pd.json_normalize(value, sep=".")

        # Caso seja um único objeto, cria uma linha.
        return pd.json_normalize(data, sep=".")

    raise ValueError("A resposta não contém um objeto ou lista JSON válido.")


def read_uploaded_file(uploaded_file):
    """Lê CSV, TXT, Excel, Parquet ou JSON enviado pelo usuário."""

    filename = uploaded_file.name.lower()
    raw_bytes = uploaded_file.getvalue()

    if filename.endswith((".csv", ".txt")):
        try:
            return pd.read_csv(
                io.BytesIO(raw_bytes),
                sep=None,
                engine="python",
                encoding="utf-8-sig",
            )
        except UnicodeDecodeError:
            return pd.read_csv(
                io.BytesIO(raw_bytes),
                sep=None,
                engine="python",
                encoding="latin-1",
            )

    if filename.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(raw_bytes))

    if filename.endswith(".parquet"):
        return pd.read_parquet(io.BytesIO(raw_bytes))

    if filename.endswith(".json"):
        data = json.loads(raw_bytes.decode("utf-8-sig"))
        return dataframe_from_json(data)

    raise ValueError(f"Formato não suportado: {filename}")


def load_api(
    url,
    method="GET",
    headers=None,
    body=None,
    timeout=30,
):
    """Consulta uma API REST e converte sua resposta JSON em DataFrame."""

    if not url or not url.strip():
        raise ValueError("Informe a URL da API.")

    method = method.upper()
    headers = headers or {}

    if not isinstance(headers, dict):
        raise ValueError("Os headers precisam ser um objeto JSON.")

    if method == "GET":
        response = requests.get(
            url,
            headers=headers,
            timeout=timeout,
        )
    elif method == "POST":
        response = requests.post(
            url,
            headers=headers,
            json=body or {},
            timeout=timeout,
        )
    else:
        raise ValueError(f"Método HTTP não suportado: {method}")

    response.raise_for_status()

    try:
        data = response.json()
    except ValueError as exc:
        raise ValueError("A API não retornou JSON válido.") from exc

    return normalize_dataframe(dataframe_from_json(data))


def load_sql_query(connection_url, query):
    """Executa uma consulta SQL usando SQLAlchemy."""

    from sqlalchemy import create_engine, text

    if not connection_url.strip():
        raise ValueError("Informe a string de conexão do banco.")

    if not query.strip():
        raise ValueError("Informe a consulta SQL.")

    engine = create_engine(connection_url)

    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(query), connection)
    finally:
        engine.dispose()

    return normalize_dataframe(df)
