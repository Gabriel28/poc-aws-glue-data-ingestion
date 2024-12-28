import gc
import os
import pandas as pd
import json
import awswrangler as wr
import boto3 as boto3
from dotenv import load_dotenv
from pandas import json_normalize, DataFrame
from concurrent.futures import ThreadPoolExecutor

print("""
    POC para criação de um dataframe a partir de vários arquivos .json coletados simulando um bucketS3. 
""")

# Configurar região no Wrangler
wr.config.region = 'us-east-1'
load_dotenv()
database = os.getenv('DATABASE')
path_bucket = os.getenv('PATH_BUCKET')
table_name = os.getenv('TABLE_NAME')

class Boto3Client:
    @staticmethod
    def boto_client(service_name: str):
        session = boto3.Session()
        credentials = session.get_credentials()
        return session.client(
            service_name=service_name,
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            aws_session_token=credentials.token,
            region_name='us-east-1'
        )

# Processa os JSON de cada diretoria e normaliza sem passar pela df do wr
def process_file(file_path) -> DataFrame:
    try:
        with open(file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
            data['additionals'] = json.dumps(data['additionals'])
            df = json_normalize(data, max_level=0)
            return df
    except (json.JSONDecodeError, OSError) as e:
        print(f"Aqui deu ruim {file_path}: {e}")
        return DataFrame()

def get_all_files(base_path):
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.lower().endswith('.json'):
                yield os.path.join(root, file)

def process_json_files(base_path) -> DataFrame:
    file_paths = get_all_files(base_path)
    with ThreadPoolExecutor() as executor:
        dataframes = executor.map(process_file, file_paths)

    df_united = DataFrame()
    for df in dataframes:
        if not df.empty:
            df_united = pd.concat([df_united, df], ignore_index=True)

    if df_united.empty:
        print("Aqui acabou")
    return df_united

def save_dataframe(df):
    wr.s3.to_parquet(
        df=df,
        path=path_bucket,
        dataset=True,
        database=database,
        table=table_name,
        compression="snappy",
        partition_cols=['year', 'month', 'day'],
        mode="overwrite_partitions"
    )
    gc.collect()

# Verifica se a tabela no Glue existe
def check_glue_table():
    glue_client = Boto3Client.boto_client("glue")
    try:
        response = glue_client.get_table(DatabaseName=database, Name=table_name)
        print("TODA A INFO:", response)
    except glue_client.exceptions.EntityNotFoundException:
        print("NADA ENCONTRADO.. ")
        # Aqui você pode adicionar lógica para criar a tabela no Glue, se necessário.
    except glue_client.exceptions.AccessDeniedException:
        print("VOCÊ NÃO TEM PERMISSÃO, VOLTE DUAS CASAS")
    except Exception as e:
        print(f"AQUI ACABOU GAROTINHO {e}")

# Inicia o processo
if __name__ == "__main__":
    check_glue_table()
    df = process_json_files('resources')
    if not df.empty:
        save_dataframe(df)
        print(f"Dataframe saved successfully")