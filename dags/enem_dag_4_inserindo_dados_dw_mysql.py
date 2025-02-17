from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
import pandas as pd
import MySQLdb

# Caminho para o arquivo pré-processado
caminho_bd_tratado = "/opt/airflow/staging/enem_2023_cleaned.csv"

# Configuração da DAG
default_args = {
    "owner": "airflow",
    'start_date': datetime.now(),
    "catchup": False,
    "retries": 2,
}

# Configs para conexão com MySQL (banco enem_dw)
conn = MySQLdb.connect(
    host="mysql",
    port=3306,
    user="airflow",
    passwd="airflow",
    db="enem_dw",
    autocommit=True
)

# Tamanho dos chunk's para processamento
CHUNK_TAMANHO = int(Variable.get("CHUNK_SIZE"))
CHUNK_QTD = int(Variable.get("CHUNK_PROCESS_QTD"))

def inserir_dim_estado(cursor, chunk):
    """Insere siglas de estados (SG_UF_PROVA) na tabela dim_estado, removendo valores duplicados."""
    estados_inseridos = 0
    for estado in chunk['SG_UF_PROVA'].drop_duplicates():
        cursor.execute(
            "INSERT IGNORE INTO dim_estado (SG_UF_PROVA) VALUES (%s)", 
            (estado,)
        )
        estados_inseridos += cursor.rowcount
    print(f"✅ {estados_inseridos} estados inseridos na `dim_estado`.")

def inserir_dim_candidato(cursor, chunk):
    """
    Insere combinações únicas de (TP_FAIXA_ETARIA, TP_SEXO) na tabela dim_candidato,
    removendo valores duplicados.
    """
    candidatos_inseridos = 0
    for _, row in chunk[['TP_FAIXA_ETARIA', 'TP_SEXO']].drop_duplicates().iterrows():
        cursor.execute("""
            INSERT IGNORE INTO dim_candidato (TP_FAIXA_ETARIA, TP_SEXO) 
            VALUES (%s, %s)
        """, (row['TP_FAIXA_ETARIA'], row['TP_SEXO']))
        candidatos_inseridos += cursor.rowcount
    print(f"✅ {candidatos_inseridos} candidatos inseridos na `dim_candidato`.")

def inserir_fato_notas(cursor, chunk):
    """
    Insere registros na tabela fato_notas, relacionando cada linha aos IDs de
    estado e candidato correspondentes.
    """
    registros_sucesso = 0
    registros_falha = 0
    for _, row in chunk.iterrows():
        try:
            # Buscar ID do estado
            cursor.execute(
                "SELECT id FROM dim_estado WHERE SG_UF_PROVA = %s", 
                (row['SG_UF_PROVA'],)
            )
            id_estado = cursor.fetchone()
            id_estado = id_estado[0] if id_estado else None

            # Buscar ID do candidato
            cursor.execute("""
                SELECT id 
                FROM dim_candidato 
                WHERE TP_FAIXA_ETARIA = %s 
                  AND TP_SEXO = %s
            """, (row['TP_FAIXA_ETARIA'], row['TP_SEXO']))
            id_candidato = cursor.fetchone()
            id_candidato = id_candidato[0] if id_candidato else None

            # Inserir registro na tabela fato_notas
            if id_estado and id_candidato:
                cursor.execute("""
                    INSERT INTO fato_notas 
                        (id_estado, id_candidato, 
                         NU_NOTA_MT, NU_NOTA_CN, NU_NOTA_LC, NU_NOTA_CH, NU_NOTA_REDACAO) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_estado, 
                    id_candidato, 
                    row['NU_NOTA_MT'], 
                    row['NU_NOTA_CN'], 
                    row['NU_NOTA_LC'],
                    row['NU_NOTA_CH'], 
                    row['NU_NOTA_REDACAO']
                ))
                registros_sucesso += 1

        except Exception as e:
            registros_falha += 1
            print(f"⚠️ Erro ao inserir registro: {row.to_dict()} - Erro: {e}")

    print(f"✅ {registros_sucesso} registros inseridos com sucesso! ⚠️ {registros_falha} falhas.")

def inserir_dados_dw_mysql():
    """
    Função principal: lê o arquivo cleaned CSV em chunks,
    insere dados nas dimensões e depois na tabela fato_notas.
    """
    print("🔹 Iniciando transformação e carga de dados no DW...")

    cursor = conn.cursor()

    print("📂 Carregando dados do arquivo CSV em chunks...")
    
    chunk_idx = 0
    # Conta quantos chunks existem ao todo
    total_chunks = sum(1 for _ in pd.read_csv(caminho_bd_tratado, delimiter=",", encoding="latin1", chunksize=CHUNK_TAMANHO))

    # Lê e processa cada chunk
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

# Criação da DAG
dag = DAG(
    "etl_enem_2023_p4_inserindo_dados_dw_mysql",
    default_args=default_args,
    schedule_interval="@once"
)

# Tarefa de transformação e carga
tarefa_inserir_dados_mysql = PythonOperator(
    task_id="inserir_dados_dw_mysql",
    python_callable=inserir_dados_dw_mysql,
    dag=dag
)
