
# CDC to S3 (SQL Server Change Data Capture para S3)

Este projeto captura os dados de mudanças (CDC - Change Data Capture) no SQL Server e os envia para um bucket S3 da AWS em formato **Parquet**. O processo é automatizado e os dados são salvos em uma estrutura de diretórios organizada no S3.

## Funcionalidade

- Conecta-se a um SQL Server com CDC habilitado.
- Captura as mudanças realizadas em uma tabela com CDC.
- Envia as alterações para o S3 em formato **Parquet**.
- Os arquivos Parquet são organizados em pastas no S3 de acordo com o nome do banco de dados, tabela e timestamp.
- O script é executado continuamente, capturando dados em intervalos configuráveis.

## Estrutura do S3

Os dados serão salvos no bucket S3 seguindo a seguinte estrutura de pastas:

```
<nome do banco>/<nome da tabela>/cdc/<file_timestamp>.parquet
```

Exemplo de arquivo no S3:

```
AdventureWorks/Person/cdc/Person_cdc_20250109_143015.parquet
```

## Pré-requisitos

Antes de executar o projeto, certifique-se de ter as seguintes ferramentas instaladas:

- **Python 3.x**.
- **pip** (gerenciador de pacotes do Python).
- **SQL Server** com CDC habilitado.
- **AWS S3** (com permissões de acesso para o envio de arquivos).
  
### Dependências

Instale as dependências necessárias:

```bash
pip install -r requirements.txt
```

O arquivo `requirements.txt` deve conter as seguintes bibliotecas:

```
pyodbc
pandas
boto3
pyarrow
python-dotenv
```

## Configuração

### Passo 1: Habilitar CDC no SQL Server

Certifique-se de que o CDC está habilitado no seu banco de dados e nas tabelas que deseja monitorar. Consulte a [documentação oficial do SQL Server](https://docs.microsoft.com/en-us/sql/relational-databases/performance/change-data-capture?view=sql-server-ver15) para mais detalhes sobre como configurar o CDC.

### Passo 2: Arquivo `.env`

Crie um arquivo `.env` no diretório raiz do projeto e adicione as variáveis de configuração, conforme exemplo abaixo:

```env
# Configurações do SQL Server
SQL_SERVER_HOST=localhost
SQL_SERVER_DATABASE=AdventureWorks2017
SQL_SERVER_USER=sa
SQL_SERVER_PASSWORD=AdventureWorks2017
SQL_SERVER_DRIVER={ODBC Driver 17 for SQL Server}

# Configurações do S3
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_REGION=us-east-1
S3_BUCKET=your-bucket-name
```

### Passo 3: Configurar Credenciais AWS

Crie um **usuário IAM** com permissões para acessar o S3 (permissões `s3:PutObject`) e gere uma chave de acesso (Access Key ID e Secret Access Key). 

Adicione estas credenciais no arquivo `.env`.

### Passo 4: Rodar o Script

Após configurar o `.env`, você pode rodar o script Python:

```bash
python cdc_to_s3.py
```

O script ficará rodando continuamente, capturando os dados de CDC da tabela especificada e enviando-os para o bucket S3 em intervalos configuráveis.