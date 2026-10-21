import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


# ============================================================
# 1. CORES DO TERMINAL
# ============================================================

VERDE = "\033[92m"
RESET = "\033[0m"
AZUL = "\033[94m"
AMARELO = "\033[93m"


# ============================================================
# 2. VARIÁVEIS DE AMBIENTE
# ============================================================

PATH_FILE = os.getenv("PATH_FILE")
PATH_INDICADORES = os.getenv("PATH_INDICADORES")


if not PATH_FILE:
    raise ValueError(
        "A variável de ambiente PATH_FILE não foi definida."
    )

if not PATH_INDICADORES:
    raise ValueError(
        "A variável de ambiente PATH_INDICADORES não foi definida."
    )


PATH_FILE = Path(PATH_FILE)
PATH_INDICADORES = Path(PATH_INDICADORES)


# ============================================================
# 3. VALIDAÇÃO DOS CAMINHOS
# ============================================================

if not PATH_FILE.exists():
    raise FileNotFoundError(
        f"O arquivo/diretório informado em PATH_FILE não existe: "
        f"{PATH_FILE}"
    )


# Cria o diretório dos indicadores caso não exista
PATH_INDICADORES.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# 4. DETECÇÃO DO TIPO DA COLUNA
# ============================================================

def detectar_tipo_serie(serie):
    """
    Detecta um tipo mais amigável para apresentação dos indicadores.
    """

    if pd.api.types.is_bool_dtype(serie):
        return "booleano"

    if pd.api.types.is_integer_dtype(serie):
        return "inteiro"

    if pd.api.types.is_float_dtype(serie):
        return "decimal"

    if pd.api.types.is_datetime64_any_dtype(serie):
        return "data"

    return "texto"


# ============================================================
# 5. CÁLCULO DOS INDICADORES
# ============================================================

def calcular_indicadores_csv(caminho_csv):

    caminho = Path(caminho_csv)

    # ---------------------------------------------------------
    # Leitura do CSV
    # ---------------------------------------------------------

    df = pd.read_csv(
        caminho,
        true_values=[
            "true",
            "True",
            "TRUE"
        ],
        false_values=[
            "false",
            "False",
            "FALSE"
        ],
        na_values=[
            "",
            "null",
            "NULL",
            "None",
            "none",
            "NaN",
            "nan"
        ]
    )

    total_registros = len(df)
    total_colunas = len(df.columns)

    # ---------------------------------------------------------
    # Timestamp
    # ---------------------------------------------------------

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    # ---------------------------------------------------------
    # Indicadores relacionados a ID
    # ---------------------------------------------------------

    coluna_id = None

    for coluna in df.columns:

        if coluna.strip().lower() == "id":
            coluna_id = coluna
            break

    if coluna_id:

        ids_preenchidos = int(
            df[coluna_id].notna().sum()
        )

        ids_distintos = int(
            df[coluna_id].nunique(
                dropna=True
            )
        )

        ids_duplicados = max(
            ids_preenchidos - ids_distintos,
            0
        )

    else:

        ids_preenchidos = 0
        ids_distintos = 0
        ids_duplicados = 0

    # ---------------------------------------------------------
    # Indicadores gerais
    # ---------------------------------------------------------

    total_celulas = (
        total_registros *
        total_colunas
    )

    total_celulas_preenchidas = int(
        df.notna().sum().sum()
    )

    total_celulas_nulas = int(
        df.isna().sum().sum()
    )

    percentual_preenchido_arquivo = (
        total_celulas_preenchidas /
        total_celulas *
        100
        if total_celulas > 0
        else 0
    )

    percentual_nulo_arquivo = (
        total_celulas_nulas /
        total_celulas *
        100
        if total_celulas > 0
        else 0
    )

    indicadores_gerais = {

        "arquivo": caminho.name,

        "timestamp": timestamp,

        "total_registros": total_registros,

        "total_colunas": total_colunas,

        "quantidade_ids": ids_preenchidos,

        "ids_preenchidos": ids_preenchidos,

        "ids_distintos": ids_distintos,

        "ids_duplicados": ids_duplicados,

        "total_celulas": total_celulas,

        "total_celulas_preenchidas":
            total_celulas_preenchidas,

        "total_celulas_nulas":
            total_celulas_nulas,

        "percentual_preenchido":
            round(
                percentual_preenchido_arquivo,
                2
            ),

        "percentual_nulo":
            round(
                percentual_nulo_arquivo,
                2
            )
    }

    # ---------------------------------------------------------
    # Indicadores por coluna
    # ---------------------------------------------------------

    indicadores_por_coluna = []

    for coluna in df.columns:

        serie = df[coluna]

        total = len(serie)

        preenchidos = int(
            serie.notna().sum()
        )

        nulos = int(
            serie.isna().sum()
        )

        distintos = int(
            serie.nunique(
                dropna=True
            )
        )

        percentual_preenchido = (
            preenchidos /
            total *
            100
            if total > 0
            else 0
        )

        percentual_nulo = (
            nulos /
            total *
            100
            if total > 0
            else 0
        )

        percentual_distinto = (
            distintos /
            preenchidos *
            100
            if preenchidos > 0
            else 0
        )

        tipo = detectar_tipo_serie(
            serie
        )

        indicador = {

            "nome_coluna": coluna,

            "tipo": tipo,

            "quantidade_total_registros":
                total,

            "valores_preenchidos":
                preenchidos,

            "valores_nulos":
                nulos,

            "valores_distintos":
                distintos,

            "valores_duplicados":
                max(
                    preenchidos - distintos,
                    0
                ),

            "percentual_preenchido":
                round(
                    percentual_preenchido,
                    2
                ),

            "percentual_nulo":
                round(
                    percentual_nulo,
                    2
                ),

            "percentual_distinto":
                round(
                    percentual_distinto,
                    2
                )
        }

        # -----------------------------------------------------
        # Indicadores numéricos
        # -----------------------------------------------------

        if pd.api.types.is_numeric_dtype(
            serie
        ):

            valores = serie.dropna()

            if len(valores) > 0:

                indicador["media"] = round(
                    float(
                        valores.mean()
                    ),
                    4
                )

                indicador["soma"] = round(
                    float(
                        valores.sum()
                    ),
                    4
                )

                indicador["minimo"] = float(
                    valores.min()
                )

                indicador["maximo"] = float(
                    valores.max()
                )

                indicador["mediana"] = round(
                    float(
                        valores.median()
                    ),
                    4
                )

                indicador["desvio_padrao"] = round(
                    float(
                        valores.std()
                    ),
                    4
                )

            else:

                indicador["media"] = None
                indicador["soma"] = None
                indicador["minimo"] = None
                indicador["maximo"] = None
                indicador["mediana"] = None
                indicador["desvio_padrao"] = None

        # -----------------------------------------------------
        # Indicadores booleanos
        # -----------------------------------------------------

        if pd.api.types.is_bool_dtype(
            serie
        ):

            quantidade_true = int(
                (serie == True).sum()
            )

            quantidade_false = int(
                (serie == False).sum()
            )

            indicador["quantidade_true"] = (
                quantidade_true
            )

            indicador["quantidade_false"] = (
                quantidade_false
            )

            indicador["percentual_true"] = (
                round(
                    quantidade_true /
                    preenchidos *
                    100,
                    2
                )
                if preenchidos
                else 0
            )

            indicador["percentual_false"] = (
                round(
                    quantidade_false /
                    preenchidos *
                    100,
                    2
                )
                if preenchidos
                else 0
            )

        indicadores_por_coluna.append(
            indicador
        )

    # ---------------------------------------------------------
    # Resultado
    # ---------------------------------------------------------

    resultado = {

        "indicadores_gerais":
            indicadores_gerais,

        "indicadores_por_coluna":
            indicadores_por_coluna
    }

    return resultado, df


# ============================================================
# 6. SALVAR JSON
# ============================================================

def salvar_json(
    resultado,
    caminho_saida
):

    with open(
        caminho_saida,
        "w",
        encoding="utf-8"
    ) as arquivo:

        json.dump(
            resultado,
            arquivo,
            ensure_ascii=False,
            indent=4
        )


# ============================================================
# 7. SALVAR EXCEL
# ============================================================

def salvar_excel(
    resultado,
    caminho_saida
):

    indicadores_gerais = pd.DataFrame(
        [
            resultado[
                "indicadores_gerais"
            ]
        ]
    )

    indicadores_colunas = pd.DataFrame(
        resultado[
            "indicadores_por_coluna"
        ]
    )

    with pd.ExcelWriter(
        caminho_saida,
        engine="openpyxl"
    ) as writer:

        indicadores_gerais.to_excel(
            writer,
            sheet_name="Indicadores Gerais",
            index=False
        )

        indicadores_colunas.to_excel(
            writer,
            sheet_name="Por Coluna",
            index=False
        )


# ============================================================
# 8. EXECUÇÃO
# ============================================================

if __name__ == "__main__":

    print(
        f"{AZUL}========================================{RESET}"
    )

    print(
        f"{AZUL}      ANÁLISE DE INDICADORES CSV       {RESET}"
    )

    print(
        f"{AZUL}========================================{RESET}"
    )

    print()

    print(
        f"Arquivo: {PATH_FILE}"
    )

    print(
        f"Saída:   {PATH_INDICADORES}"
    )

    print()

    # --------------------------------------------------------
    # Calcula indicadores
    # --------------------------------------------------------

    resultado, df = calcular_indicadores_csv(
        PATH_FILE
    )

    # --------------------------------------------------------
    # Define os arquivos de saída
    # --------------------------------------------------------

    arquivo_json = (
        PATH_INDICADORES /
        "indicadores.json"
    )

    arquivo_excel = (
        PATH_INDICADORES /
        "indicadores.xlsx"
    )

    # --------------------------------------------------------
    # Salva JSON
    # --------------------------------------------------------

    salvar_json(
        resultado,
        arquivo_json
    )

    print(
        f"{VERDE}✓ JSON gerado:{RESET} "
        f"{arquivo_json}"
    )

    # --------------------------------------------------------
    # Salva Excel
    # --------------------------------------------------------

    try:

        salvar_excel(
            resultado,
            arquivo_excel
        )

        print(
            f"{VERDE}✓ Excel gerado:{RESET} "
            f"{arquivo_excel}"
        )

    except ModuleNotFoundError as erro:

        if "openpyxl" in str(erro):

            print()

            print(
                f"{AMARELO}⚠ Excel não foi gerado.{RESET}"
            )

            print(
                f"{AMARELO}"
                "O pacote 'openpyxl' não está instalado."
                f"{RESET}"
            )

            print()

            print(
                "Instale com:"
            )

            print(
                f"{AZUL}"
                "pip install openpyxl"
                f"{RESET}"
            )

        else:

            raise

    # --------------------------------------------------------
    # Resumo
    # --------------------------------------------------------

    print()

    print(
        f"{VERDE}========================================{RESET}"
    )

    print(
        f"{VERDE}          ANÁLISE CONCLUÍDA            {RESET}"
    )

    print(
        f"{VERDE}========================================{RESET}"
    )

    print()

    print(
        f"Total de registros: "
        f"{resultado['indicadores_gerais']['total_registros']}"
    )

    print(
        f"Total de colunas: "
        f"{resultado['indicadores_gerais']['total_colunas']}"
    )

    print(
        f"IDs preenchidos: "
        f"{resultado['indicadores_gerais']['ids_preenchidos']}"
    )

    print(
        f"IDs distintos: "
        f"{resultado['indicadores_gerais']['ids_distintos']}"
    )

    print(
        f"IDs duplicados: "
        f"{resultado['indicadores_gerais']['ids_duplicados']}"
    )

    print()

    print(
        json.dumps(
            resultado,
            ensure_ascii=False,
            indent=4
        )
    )