
"""Funções para preparar arquivos de exportação."""

import io

from utils.data_profile import dataframe_for_export


def create_csv(df):
    """Retorna o conteúdo CSV codificado em UTF-8 com BOM."""
    export_df = dataframe_for_export(df)
    return export_df.to_csv(index=False).encode("utf-8-sig")


def create_json(df):
    """Retorna o conteúdo JSON em UTF-8."""
    export_df = dataframe_for_export(df)

    return export_df.to_json(
        orient="records",
        force_ascii=False,
        indent=2,
    ).encode("utf-8")


def create_parquet(df):
    """Retorna o conteúdo Parquet em bytes."""
    export_df = dataframe_for_export(df)

    buffer = io.BytesIO()
    export_df.to_parquet(buffer, index=False)

    return buffer.getvalue()
