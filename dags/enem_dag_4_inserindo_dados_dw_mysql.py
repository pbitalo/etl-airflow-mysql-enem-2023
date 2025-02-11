from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
import pandas as pd
import MySQLdb

caminho_bd_tratado = "/opt/airflow/staging/enem_2023_cleaned.csv"

# Configuração da DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 5),
    "catchup": False,
    "retries": 2,
}

# Configs para conexão com Mysql
conn = MySQLdb.connect(
    host="mysql",
    port=3306,
    user="airflow",
    passwd="airflow",
    db="enem_dw",
    autocommit=True
)

# Tamanho dos chunk's para processamento
CHUNK_SIZE = int(Variable.get("CHUNK_SIZE"))
CHUNK_PROCESS_QTD = int(Variable.get("CHUNK_PROCESS_QTD"))

def inserir_dim_estado(cursor, chunk):
    """Insere estados únicos na tabela dim_estado."""
    estados_inseridos = 0
    for estado in chunk['SG_UF_PROVA'].drop_duplicates():
        cursor.execute("INSERT IGNORE INTO dim_estado (SG_UF_PROVA) VALUES (%s)", (estado,))
        estados_inseridos += cursor.rowcount
    print(f"✅ {estados_inseridos} estados inseridos na `dim_estado`.")

def inserir_dim_candidato(cursor, chunk):
    """Insere candidatos únicos na tabela dim_candidato."""
    candidatos_inseridos = 0
    for _, row in chunk[['TP_FAIXA_ETARIA', 'TP_SEXO']].drop_duplicates().iterrows():
        cursor.execute("""
            INSERT IGNORE INTO dim_candidato (TP_FAIXA_ETARIA, TP_SEXO) 
            VALUES (%s, %s)
        """, (row['TP_FAIXA_ETARIA'], row['TP_SEXO']))
        candidatos_inseridos += cursor.rowcount
    print(f"✅ {candidatos_inseridos} candidatos inseridos na `dim_candidato`.")

def inserir_fato_notas(cursor, chunk):
    """Insere registros na tabela fato_notas."""
    registros_sucesso = 0
    registros_falha = 0
    for _, row in chunk.iterrows():
        try:
            # Buscar ID do estado
            cursor.execute("SELECT id FROM dim_estado WHERE SG_UF_PROVA = %s", (row['SG_UF_PROVA'],))
            id_estado = cursor.fetchone()
            id_estado = id_estado[0] if id_estado else None

            # Buscar ID do candidato
            cursor.execute("SELECT id FROM dim_candidato WHERE TP_FAIXA_ETARIA = %s AND TP_SEXO = %s",
                           (row['TP_FAIXA_ETARIA'], row['TP_SEXO']))
            id_candidato = cursor.fetchone()
            id_candidato = id_candidato[0] if id_candidato else None

            # Inserir registro na tabela fato
            if id_estado and id_candidato:
                cursor.execute("""
                    INSERT INTO fato_notas (id_estado, id_candidato, NU_NOTA_MT, NU_NOTA_CN, NU_NOTA_LC, NU_NOTA_CH, NU_NOTA_REDACAO) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (id_estado, id_candidato, row['NU_NOTA_MT'], row['NU_NOTA_CN'], row['NU_NOTA_LC'], row['NU_NOTA_CH'], row['NU_NOTA_REDACAO']))
                registros_sucesso += 1
        except Exception as e:
            registros_falha += 1
            print(f"⚠️ Erro ao inserir registro: {row.to_dict()} - Erro: {e}")

    print(f"✅ {registros_sucesso} registros inseridos com sucesso! ⚠️ {registros_falha} falhas.")

def inserir_dados_dw_mysql():
    print("🔹 Iniciando transformação e carga de dados no DW...")

    cursor = conn.cursor()

    print("📂 Carregando dados do arquivo CSV em chunks...")
    
    chunk_idx = 0
    total_chunks = sum(1 for _ in pd.read_csv(caminho_bd_tratado, delimiter=",", encoding="latin1", chunksize=CHUNK_SIZE))

    for chunk in pd.read_csv(caminho_bd_tratado, delimiter=",", encoding="latin1", chunksize=CHUNK_SIZE):
        if chunk_idx >= CHUNK_PROCESS_QTD:
            break

        chunk_idx += 1
        progress = (chunk_idx / total_chunks) * 100
        print(f"✅ Processando chunk {chunk_idx}/{total_chunks} ({progress:.2f}%) - {len(chunk)} registros...")

        # Chamando as funções modulares
        inserir_dim_estado(cursor, chunk)
        inserir_dim_candidato(cursor, chunk)
        inserir_fato_notas(cursor, chunk)

    cursor.close()
    conn.close()
    print("🚀 Transformação e carga concluídas!")

# Criando a DAG
dag = DAG(
    "etl_enem_2023_p4_inserindo_dados_dw_mysql",
    default_args=default_args,
    schedule_interval="@once"
)

# Criando a tarefa de transformação e carga
transform_task = PythonOperator(
    task_id="inserir_dados_dw_mysql",
    python_callable=inserir_dados_dw_mysql,
    dag=dag
)