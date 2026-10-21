# DataLens Studio
![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
[![Status](https://img.shields.io/badge/Status-Concluído-green.svg)]()

<img src="./docs/01.png">

Aplicação de exploração de dados com Python e Streamlit. Permite carregar arquivos, consultar APIs REST e ler dados de bancos SQL; depois apresenta prévia, schema inferido, qualidade, estatísticas e opções de exportação.

## Funcionalidades do MVP

- **Arquivos:** CSV, TXT, XLSX, XLS, Parquet e JSON.
- **API REST:** método GET, headers e parâmetros em JSON, caminho opcional dentro da resposta.
- **Bancos:** SQLite, PostgreSQL, MySQL e SQL Server via SQLAlchemy e drivers.
- **Descoberta:** nomes das colunas, tipos Pandas, valores ausentes, percentual de nulos, valores distintos e exemplo.
- **Qualidade:** linhas duplicadas, colunas com nulos e distribuição de tipos.

## Requisitos

- Python 3.11 ou superior recomendado.
- PowerShell no Windows.
- Drivers de banco instalados quando exigidos pelo banco escolhido. SQL Server requer o driver ODBC compatível.

## Conexões com bancos

### SQLite

Informe o caminho do arquivo, por exemplo `./dados.db`.

### PostgreSQL

Exemplo de URL SQLAlchemy:

```text
postgresql+psycopg://usuario:senha@localhost:5432/banco
```

### MySQL

```text
mysql+pymysql://usuario:senha@localhost:3306/banco
```

### SQL Server

```text
mssql+pyodbc://usuario:senha@localhost/banco?driver=ODBC+Driver+18+for+SQL+Server
```

Use credenciais de baixo privilégio, preferencialmente somente leitura. Não grave senhas no código, em arquivos versionados ou em prints.

## Observações importantes

- Esta é uma primeira versão funcional, não um serviço multiusuário pronto para produção.
- A inferência de tipos é feita pelo Pandas e pode precisar de regras adicionais para datas, decimais, identificadores e colunas com valores mistos.
- JSON aninhado é normalizado quando possível. Respostas de APIs paginadas ainda precisam de implementação específica para cada padrão.
- O limite de linhas controla o tamanho carregado quando suportado pelo leitor. Parquet é carregado antes de aplicar o limite nesta versão.
- Para tabelas muito grandes, a próxima etapa recomendada é leitura por lotes e amostragem, além de DuckDB/Polars para reduzir consumo de memória.
- Não exponha a aplicação à internet sem autenticação, gestão segura de segredos e controles de acesso.

## Estrutura

```text
datalens-studio/
├── .streamlit/
│   └── config.toml
├── app.py
├── requirements.txt
├── README.md
└── .gitignore
```

## Como funciona 

A interface gráfica é para selecionar os campos dos dados. Após pegar os campos rode o arquivo `api` ou `file` `01`
para criar a camada `Bronze`.<br> Logo em seguida abra o arquivo `02` onde irá criar a camada 
`Silver`, na parte gráfica do `Streamlit` há como selecionar e copiar os campos que deseja.<br> Em seguida cole no arquivo `02` em `campos_selecionados`, caso o campo seja estendido em vez do `.` ponto utilize o `_`, nos arquivos das pastas `file/04_file_indicadores.py` e `src/03_api_gold.py` possui indicadores para criar a camada `Gold`.<br>

O arquivo `API` na raiz do projeto contém algumas `APIs` para teste.
<br>
Um detalhe a acrescentar, toda vez que for utilizar uma `API` ou um `Arquivo` SEMPRE ALTERE NO `.env`, pois os scripts irá buscar os dados onde está setado o caminho.

## 👨‍💻 Autor

**Thiago Vilarinho Lemes**
👤 Autor: Thiago Vilarinho Lemes <br>
💻 Engenheiro e Analista de Dados.\
🏠 Home: https://thiagolemes.netlify.app/ \
🔗 LinkedIn: <a href="https://www.linkedin.com/in/thiago-v-lemes-b1232727" target="_blank">Thiago Lemes</a><br>
✉️ e-mail:contatothiagolemes@gmail.com | lemes_vilarinho@yahoo.com.br


Principais áreas:

```text
Data Engineering
Docker
Kafka
Python
SQL
Apache Airflow
Docker
PostgreSQL
Data Lake
ETL
APIs
Cloud
IoT
Machine Learning
Generative AI
```