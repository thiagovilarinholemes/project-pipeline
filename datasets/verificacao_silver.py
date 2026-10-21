from pathlib import Path
import pandas as pd

# ==============================================================================
# CORES ANSI PARA AS MENSAGENS DO TERMINAL
# ==============================================================================

VERDE = "\033[92m"
RESET = "\033[0m"
AZUL = "\033[94m"
AMARELO = "\033[93m"


# ==============================================================================
# CAMINHO DO ARQUIVO PARQUET
# ==============================================================================

ARQUIVO_PARQUET = (
    Path(__file__).parent
    / "file"
    / "silver"
    / "part-00000-6203c8d6-d614-4beb-8b0c-4e44c4891248-c000.snappy.parquet"
)


# ==============================================================================
# LEITURA DO PARQUET
# ==============================================================================

df = pd.read_parquet(
    ARQUIVO_PARQUET,
    engine="pyarrow"
)


# ==============================================================================
# ESQUEMA
# ==============================================================================

print("\n")
print(f"{AZUL}{'=' * 80}{RESET}")
print(f"{AZUL}ESQUEMA{RESET}")
print(f"{AZUL}{'=' * 80}{RESET}")

print(df.dtypes)


# ==============================================================================
# PRIMEIROS REGISTROS
# ==============================================================================

print("\n")
print(f"{AZUL}{'=' * 80}{RESET}")
print(f"{AZUL}PRIMEIROS REGISTROS{RESET}")
print(f"{AZUL}{'=' * 80}{RESET}")

# Quantidade de registros que serão exibidos
quantidade_registros = min(20, len(df))

# Seleciona os primeiros registros
df_visualizacao = df.head(quantidade_registros).copy()

# ==============================================================================
# AJUSTE DA LARGURA DAS COLUNAS
# ==============================================================================

# Configura o Pandas para mostrar todas as colunas
pd.set_option("display.max_columns", None)

# Evita quebrar a tabela horizontalmente
pd.set_option("display.width", 200)

# Evita cortar textos muito cedo
pd.set_option("display.max_colwidth", 40)

# Exibe a tabela
print(
    df_visualizacao.to_string(
        index=False
    )
)

print(
    f"\n{AZUL}Exibindo {quantidade_registros} de {len(df)} registros.{RESET}"
)


# ==============================================================================
# COLUNAS
# ==============================================================================

print("\n")
print(f"{AZUL}{'=' * 80}{RESET}")
print(f"{AZUL}COLUNAS{RESET}")
print(f"{AZUL}{'=' * 80}{RESET}")

for numero, coluna in enumerate(df.columns, start=1):
    print(f"{numero:02d}. {coluna}")


# ==============================================================================
# TOTAL DE REGISTROS
# ==============================================================================

print("\n")
print(f"{AZUL}{'=' * 80}{RESET}")
print(
    f"TOTAL DE REGISTROS: "
    f"{AZUL}{len(df)}{RESET}"
)
print(f"{AZUL}{'=' * 80}{RESET}")