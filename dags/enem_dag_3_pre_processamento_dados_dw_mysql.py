from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
import pandas as pd
import os
import time

# Definição dos argumentos da DAG
default_args = {
    "owner": "airflow",
    'start_date': datetime.now(),
    "catchup": False
}

# Definição do tamanho do chunk e limite de processamento
CHUNK_TAMANHO = int(Variable.get("CHUNK_SIZE"))
CHUNK_QTD = int(Variable.get("CHUNK_PROCESS_QTD"))

# Caminhos dos arquivos
ARQUIVO_ENTRADA = "/opt/airflow/data/enem_2023/DADOS/MICRODADOS_ENEM_2023.csv"
ARQUIVO_SAIDA = "/opt/airflow/staging/enem_2023_cleaned.csv"

def verificar_arquivo():
    """Verifica se o arquivo de entrada existe antes de iniciar o processamento."""
    if not os.path.exists(ARQUIVO_ENTRADA):
        print(f"❌ Erro: O arquivo {ARQUIVO_ENTRADA} não foi encontrado!\nCertifique-se de que a DAG de download e descompactação rodou corretamente.")
        time.sleep(3)  
        raise FileNotFoundError(f"❌ Erro: O arquivo {ARQUIVO_ENTRADA} não foi encontrado!\nCertifique-se de que a DAG de download e descompactação rodou corretamente.")

    print(f"✅ Arquivo encontrado: {ARQUIVO_ENTRADA}")

def carregar_pre_processar_dados():
    """Processa e limpa os dados do ENEM 2023, salvando em um arquivo formatado."""

    # 🔹 A verificação do arquivo já foi feita na DAG, então aqui só processamos os dados

    # Garante que o diretório de saída existe
    os.makedirs("/opt/airflow/staging", exist_ok=True)

    chunk_idx = 0  # Contador de chunks processados
    total_chunks = sum(1 for _ in pd.read_csv(ARQUIVO_ENTRADA, delimiter=";", encoding="latin-1", chunksize=CHUNK_TAMANHO))  # Conta chunks totais

    # Lê o arquivo em chunks e processa um número limitado de chunks
    with pd.read_csv(ARQUIVO_ENTRADA, delimiter=';', encoding="latin-1", chunksize=CHUNK_TAMANHO) as reader:
        for i, chunk in enumerate(reader):
            
            print(f"📂 Processamento TOTAL da Base => Chunk {i + 1}...")

            chunk_idx += 1
            progresso = (chunk_idx / total_chunks) * 100
            
            print(f"✅ Processamento TOTAL da Base => Chunk {chunk_idx}/{total_chunks} ({progresso:.2f}%) - {len(chunk)} registros...")      
                  
            if chunk_idx > CHUNK_QTD:
                print(f"🔹 Limite de {CHUNK_QTD} chunks atingido. Parando processamento.")
                break  # Para o loop após atingir o limite

            # 🔹 Seleciona apenas as colunas necessárias
            cols = [
                'TP_FAIXA_ETARIA',  # Faixa etária do candidato
                'SG_UF_PROVA',       # Estado onde fez a prova
                'NU_NOTA_MT',        # Nota Matemática
                'NU_NOTA_CN',        # Nota Ciências da Natureza
                'NU_NOTA_LC',        # Nota Linguagens e Códigos
                'NU_NOTA_CH',        # Nota Ciências Humanas
                'NU_NOTA_REDACAO',   # Nota Redação
                'TP_SEXO'            # Sexo do candidato
            ]
            
            # Mantém apenas as colunas necessárias
            chunk = chunk[cols]

            # 🔹 Trata valores nulos
            chunk = chunk.fillna({
                'TP_SEXO': 'N/I',   # Define 'N/I' para sexo não informado
                'TP_FAIXA_ETARIA': -1,  # Define -1 para faixa etária não informada
                'NU_NOTA_MT': 0,    # Substitui notas ausentes por 0
                'NU_NOTA_CN': 0,
                'NU_NOTA_LC': 0,
                'NU_NOTA_CH': 0,
                'NU_NOTA_REDACAO': 0
            })

            # 🔹 Converte tipos de dados
            chunk['TP_SEXO'] = chunk['TP_SEXO'].astype(str)
            chunk['TP_FAIXA_ETARIA'] = chunk['TP_FAIXA_ETARIA'].astype(int)

            # 🔹 Salva o chunk processado no arquivo de saída
            chunk.to_csv(
                ARQUIVO_SAIDA, 
                index=False, 
                mode='w' if chunk_idx == 1 else 'a',  # 'w' para o primeiro chunk, 'a' para os seguintes
                header=True if chunk_idx == 1 else False  # Apenas o primeiro chunk deve ter cabeçalho
            )
            
            print(f"✅ Chunk {chunk_idx} salvo com sucesso.")

        print("🚀 Processamento finalizado!")

# Criando a DAG
dag = DAG(
    "etl_enem_2023_p3_pre_processamento_dados_dw_mysql",
    default_args=default_args,
    schedule_interval="@once"
)

# Criando a tarefa de verificação do arquivo antes do processamento
tarefa_verificar_arquivo_existe = PythonOperator(
    task_id="verificar_arquivo",
    python_callable=verificar_arquivo,
    dag=dag
)

# Criando a tarefa de carga e limpeza
tarefa_pre_processamento = PythonOperator(
    task_id="carregar_pre_processar_dados",
    python_callable=carregar_pre_processar_dados,
    dag=dag
)

tarefa_verificar_arquivo_existe >> tarefa_pre_processamento

