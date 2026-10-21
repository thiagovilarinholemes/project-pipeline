
from pathlib import Path
import pandas as pd

# ==============================================================================
# CORES ANSI PARA AS MENSAGENS DO TERMINAL
# ==============================================================================

VERDE = "\033[92m"
RESET = "\033[0m"
AZUL = "\033[94m"
AMARELO = "\033[93m"

# Caminho do arquivo Parquet
ARQUIVO_PARQUET = (
    Path(__file__).parent
    # /"gold"/"indicadores_gerais"/"part-00000-2ef16adc-dda5-43b4-b500-facaaf3536f7-c000.snappy.parquet"
    # /"gold"/"indicadores_por_coluna"/"part-00000-7036b678-818d-4f8c-82b5-3fca4d2205b3-c000.snappy.parquet"
    # /"gold"/"qualidade_dados"/"part-00000-7036b678-818d-4f8c-82b5-3fca4d2205b3-c000.snappy.parquet"
    /"file"/"gold"/"qualidade_dados"/"part-00007-9fdb3347-858f-4ded-bdf0-3ba73dcf0605-c000.snappy.parquet"
)

# Lê o arquivo
df = pd.read_parquet(ARQUIVO_PARQUET, engine="pyarrow")

# Mostra os tipos das colunas
print(f"{AZUL}=== ESQUEMA ==={RESET}")
print(df.dtypes)

# Mostra os primeiros 20 registros
print(f"{AZUL}=== PRIMEIROS REGISTROS ==={RESET}")
print(df.head(20).to_string(index=False))

# Mostra as colunas
print(f"{AZUL}=== COLUNAS ==={RESET}")
print(df.columns.tolist())

# Mostra a quantidade de registros
print(f"\nTOTAL DE REGISTROS: {AZUL}{len(df)}{RESET}")
