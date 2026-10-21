
"""Funções de perfil e qualidade dos dados."""

import json
from datetime import datetime
from decimal import Decimal

import pandas as pd


def json_safe(value):
    """Transforma estruturas complexas em valores comparáveis."""

    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )

    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    return value


def profile_dataframe(df):
    """Retorna um resumo de cada coluna do DataFrame."""

    profiles = []

    for column in df.columns:
        series = df[column]

        try:
            unique_count = series.map(json_safe).nunique(dropna=True)
        except (TypeError, ValueError):
            unique_count = None

        examples = []

        for value in series:
            safe_value = json_safe(value)

            if safe_value is None:
                continue

            example = str(safe_value)

            if example not in examples:
                examples.append(example)

            if len(examples) == 3:
                break

        profiles.append(
            {
                "Coluna": str(column),
                "Tipo Pandas": str(series.dtype),
                "Valores não nulos": int(series.notna().sum()),
                "Valores ausentes": int(series.isna().sum()),
                "Valores únicos": unique_count,
                "Exemplos": " | ".join(examples),
            }
        )

    return pd.DataFrame(profiles)


def count_duplicate_rows(df):
    """Conta linhas duplicadas, inclusive com valores complexos."""

    if df.empty:
        return 0

    comparable = df.copy()

    for column in comparable.columns:
        comparable[column] = comparable[column].map(json_safe)

    return int(comparable.duplicated().sum())


def dataframe_for_export(df):
    """Converte listas e dicionários em texto para exportação."""

    result = df.copy()

    for column in result.columns:
        result[column] = result[column].map(
            lambda value: (
                json.dumps(value, ensure_ascii=False, default=str)
                if isinstance(value, (dict, list, tuple, set))
                else value
            )
        )

    return result
