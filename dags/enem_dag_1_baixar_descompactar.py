from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from dag_config import default_args
import os
import requests
import zipfile
import time

def salvar_arquivo(response, caminho_arquivo):
    """Salva o arquivo no caminho informado"""
    
    with open(caminho_arquivo, "wb") as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            file.write(chunk)
    print(f"📥 Download concluído e salvo em: {caminho_arquivo}")

def baixar_enem_2023():
    """Faz o download da base de dados do ENEM 2023"""
    
    url = Variable.get("url_enem_2023")
    caminho_destino = "/opt/airflow/data/enem_2023.zip"
    os.makedirs("/opt/airflow/data/", exist_ok=True)

    for tentativa in range(5):  
        try:
            print(f"🔄 Tentativa {tentativa + 1}: Baixando {url}")
            response = requests.get(url, stream=True, timeout=30, verify=True)  

            if response.status_code == 200:
                salvar_arquivo(response, caminho_destino)
                return caminho_destino
            else:
                print(f"❌ Erro ao baixar: Status {response.status_code}")
        
        except requests.exceptions.SSLError as e:
            print(f"⚠️ Erro SSL: {e} - Tentando novamente sem verificação SSL.")
            response = requests.get(url, stream=True, timeout=30, verify=False)  

            if response.status_code == 200:
                salvar_arquivo(response, caminho_destino)
                return caminho_destino
        
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na tentativa {tentativa + 1}: {e}")
            time.sleep(5)  
    
    return "❌ Falha ao baixar o arquivo após várias tentativas."

def descompacta_bd_enem():
    zip_path = "/opt/airflow/data/enem_2023.zip"
    extract_to = "/opt/airflow/data/enem_2023"
    print(f"📂 Descompactando {zip_path} para {extract_to}...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print("✅ Descompactação concluída.")

dag = DAG(
    'etl_enem_2023_p1_baixar_descompactar',
    default_args=default_args,
    description='DAG para baixar e descompactar os dados do ENEM 2023',
    schedule_interval='@once',
    catchup=False,
)

tarefa_baixar_bd = PythonOperator(
    task_id='baixar_enem_2023',
    python_callable=baixar_enem_2023,
    op_kwargs={'pasta_destino': "/opt/airflow/data/"},
    dag=dag,
)

tarefa_descompactar = PythonOperator(
    task_id='descompacta_bd_enem',
    python_callable=descompacta_bd_enem,
    dag=dag,
)

# Ordem de execução
tarefa_baixar_bd >> tarefa_descompactar
