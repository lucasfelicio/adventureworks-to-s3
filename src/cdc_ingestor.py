import os
import pyodbc
import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações do SQL Server (carregadas do .env)
sql_server_config = {
    "server": os.getenv("SQL_SERVER_HOST"),
    "database": os.getenv("SQL_SERVER_DATABASE"),
    "username": os.getenv("SQL_SERVER_USER"),
    "password": os.getenv("SQL_SERVER_PASSWORD"),
    "driver": os.getenv("SQL_SERVER_DRIVER")
}

# Configurações do bucket S3 (carregadas do .env)
s3_bucket = os.getenv("S3_BUCKET")
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

# Consulta para capturar dados de CDC
def fetch_cdc_data(schema, table):
    """
    Captura as mudanças realizadas em uma tabela com CDC habilitado.
    """
    capture_instance = f"{schema}_{table}"
    query = f"""
        SELECT *
        FROM cdc.{capture_instance}_CT
        WHERE __$operation IN (1, 2, 3, 4)
        ORDER BY __$start_lsn;
    """
    connection_string = (
        f"DRIVER={sql_server_config['driver']};"
        f"SERVER={sql_server_config['server']};"
        f"DATABASE={sql_server_config['database']};"
        f"UID={sql_server_config['username']};"
        f"PWD={sql_server_config['password']}"
    )
    conn = pyodbc.connect(connection_string)
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

# Enviar os dados para o S3 em formato Parquet
def upload_to_s3(dataframe, bucket, database, table):
    """
    Envia um DataFrame como Parquet para um bucket S3, organizando em pastas: database/table/cdc/file_timestamp.parquet.
    """
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    
    # Criar a estrutura do caminho no S3
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    key = f"{database}/{table}/cdc/{table}_cdc_{timestamp}.parquet"
    
    # Converter DataFrame para Parquet em memória
    parquet_buffer = BytesIO()
    dataframe.to_parquet(parquet_buffer, engine='pyarrow', index=False)
    
    # Fazer upload para o S3
    s3_client.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())
    print(f"Arquivo enviado para S3: {key}")

# Fluxo principal
def process_cdc(schema, table):
    """
    Captura os dados CDC e os envia para o S3.
    """
    print(f"Buscando dados CDC para a tabela {schema}.{table}...")
    cdc_data = fetch_cdc_data(schema, table)
    if not cdc_data.empty:
        print(f"{len(cdc_data)} alterações capturadas.")
        upload_to_s3(cdc_data, s3_bucket, sql_server_config["database"], table)
    else:
        print("Nenhuma alteração encontrada.")

# Agendar execução (com intervalo)
def schedule_task(schema, table, interval=300):
    """
    Agenda a captura de CDC a cada intervalo especificado.
    """
    import time
    while True:
        try:
            process_cdc(schema, table)
        except Exception as e:
            print(f"Erro: {e}")
        time.sleep(interval)

# Executar script
if __name__ == "__main__":
    # Especifique o esquema e a tabela
    schema_name = "Person"
    table_name = "Person"
    
    # Agendar a tarefa (5 minutos de intervalo)
    schedule_task(schema_name, table_name, interval=300)
