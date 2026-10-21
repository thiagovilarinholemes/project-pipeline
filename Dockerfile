
# Imagem base com Python 3.12.
FROM python:3.12-slim-bookworm

# Evita arquivos .pyc e mantém os logs visíveis no terminal.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instala Java 17, necessário para executar o Spark,
# e procps, que contém ferramentas de diagnóstico.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
       procps \
    && rm -rf /var/lib/apt/lists/*

# Configura o Java para uso pelo PySpark.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Preserva os diretórios necessários para Python, pip e Java.
ENV PATH="/bin:/usr/local/bin:/usr/bin:/bin"

# Define o diretório de trabalho do contêiner.
WORKDIR /app

# Copia e instala as dependências Python.
COPY requirements.txt /app/requirements.txt

RUN python -m pip install --no-cache-dir -r /app/requirements.txt

# Copia o código-fonte para a imagem.
COPY src/ /app/src/

# Cria os diretórios de entrada e saída.
RUN mkdir -p /app/datasets/samples /app/output

# Comando padrão do contêiner.
CMD ["python", "src/01_fundamentos.py"]
