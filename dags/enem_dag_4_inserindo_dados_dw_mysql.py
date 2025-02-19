from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from dag_config import default_args, get_conexao_mysql
from mysql_queries import (INSERT_DIM_ESTADO, INSERT_DIM_CANDIDATO, 
                         SELECT_ID_ESTADO, SELECT_ID_CANDIDATO, INSERT_FATO_NOTAS)
import pandas as pd

# Caminho para o arquivo pré-processado
caminho_bd_tratado = "/opt/airflow/staging/enem_2023_cleaned.csv"

# Configuração da DAG
default_args_default = default_args.copy()
default_args_default['retries'] = 2

conn = get_conexao_mysql(db="enem_dw", autocommit=True)

# Tamanho dos chunks para processamento
CHUNK_TAMANHO = int(Variable.get("CHUNK_SIZE"))
CHUNK_QTD = int(Variable.get("CHUNK_PROCESS_QTD"))

def inserir_dim_estado(cursor, chunk):
    """Insere siglas de estados (SG_UF_PROVA) na tabela dim_estado, removendo valores duplicados."""
    estados_inseridos = 0
    for estado in chunk['SG_UF_PROVA'].drop_duplicates():
        cursor.execute(INSERT_DIM_ESTADO, (estado,))
        estados_inseridos += cursor.rowcount
    print(f"✅ {estados_inseridos} estados inseridos na `dim_estado`.")

def inserir_dim_candidato(cursor, chunk):
    """Insere combinações únicas de (TP_FAIXA_ETARIA, TP_SEXO) na tabela dim_candidato."""
    candidatos_inseridos = 0
    for _, row in chunk[['TP_FAIXA_ETARIA', 'TP_SEXO']].drop_duplicates().iterrows():
        cursor.execute(INSERT_DIM_CANDIDATO, (row['TP_FAIXA_ETARIA'], row['TP_SEXO']))
        candidatos_inseridos += cursor.rowcount
    print(f"✅ {candidatos_inseridos} candidatos inseridos na `dim_candidato`.")

def inserir_fato_notas(cursor, chunk):
    """Insere registros na tabela fato_notas, associando estado e candidato."""
    registros_sucesso = 0
    registros_falha = 0
    for _, row in chunk.iterrows():
        try:
            # Buscar ID do estado
            cursor.execute(SELECT_ID_ESTADO, (row['SG_UF_PROVA'],))
            id_estado = cursor.fetchone()
            id_estado = id_estado[0] if id_estado else None

            # Buscar ID do candidato
            cursor.execute(SELECT_ID_CANDIDATO, (row['TP_FAIXA_ETARIA'], row['TP_SEXO']))
            id_candidato = cursor.fetchone()
            id_candidato = id_candidato[0] if id_candidato else None

            # Inserir registro na tabela fato_notas
            if id_estado and id_candidato:
                cursor.execute(INSERT_FATO_NOTAS, (
                    id_estado, id_candidato, 
                    row['NU_NOTA_MT'], row['NU_NOTA_CN'], 
                    row['NU_NOTA_LC'], row['NU_NOTA_CH'], row['NU_NOTA_REDACAO']
                ))
                registros_sucesso += 1
        except Exception as e:
            registros_falha += 1
            print(f"⚠️ Erro ao inserir registro: {row.to_dict()} - Erro: {e}")

    print(f"✅ {registros_sucesso} registros inseridos com sucesso! ⚠️ {registros_falha} falhas.")

def inserir_dados_dw_mysql():
    """Lê o arquivo CSV em chunks e insere os dados no Data Warehouse."""
    print("🔹 Iniciando transformação e carga de dados no DW...")

    cursor = conn.cursor()

    print("📂 Carregando dados do arquivo CSV em chunks...")

    chunk_idx = 0
    total_chunks = sum(1 for _ in pd.read_csv(caminho_bd_tratado, delimiter=",", encoding="latin1", chunksize=CHUNK_TAMANHO))

    for chunk in pd.read_csv(caminho_bd_tratado, delimiter=",", encoding="latin1", chunksize=CHUNK_TAMANHO):
        if chunk_idx >= CHUNK_QTD:
            print(f"🔹 Limite de {CHUNK_QTD} chunks atingido. Interrompendo.")
            break

        chunk_idx += 1
        progresso = (chunk_idx / total_chunks) * 100
        print(f"✅ Processando chunk {chunk_idx}/{total_chunks} ({progresso:.2f}%) - {len(chunk)} registros...")

        # Inserir dimensões primeiro, pois precisamos dos IDs
        inserir_dim_estado(cursor, chunk)
        inserir_dim_candidato(cursor, chunk)

        # Inserir tabela fato
        inserir_fato_notas(cursor, chunk)

    cursor.close()
    conn.close()
    print("🚀 Transformação e carga concluídas!")

# Criando a DAG
dag = DAG(
    "etl_enem_2023_p4_inserindo_dados_dw_mysql",
    default_args=default_args_default,
    schedule_interval="@once"
)

# Criando a tarefa de transformação e carga
transform_task = PythonOperator(
    task_id="inserir_dados_dw_mysql",
    python_callable=inserir_dados_dw_mysql,
    dag=dag
)
