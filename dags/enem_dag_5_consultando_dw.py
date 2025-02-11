from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import MySQLdb

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

conn = MySQLdb.connect(
    host="mysql",
    port=3306,
    user="airflow",
    passwd="airflow",
    db="enem_dw"
)

def consultar_dw_mysql(sql_query, task_name):
    print(f"🔹 Conectando ao MySQL para {task_name}...")
    cursor = conn.cursor()
    cursor.execute(sql_query)
    results = cursor.fetchall()
    for row in results:
        print(f"{task_name}: {row}")
    cursor.close()
    conn.close()
    print(f"✅ {task_name} concluído!")

query_faixa_etaria = """
    SELECT 
        c.TP_FAIXA_ETARIA,
        COUNT(*) AS total_candidatos,
        ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) AS percentual
    FROM fato_notas f
    JOIN dim_candidato c ON f.id_candidato = c.id
    GROUP BY c.TP_FAIXA_ETARIA
    ORDER BY c.TP_FAIXA_ETARIA;
"""

query_proporcao_genero = """
    SELECT 
        e.SG_UF_PROVA,
        c.TP_SEXO,
        COUNT(*) AS total_candidatos,
        (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY e.SG_UF_PROVA)) AS percentual
    FROM fato_notas f
    JOIN dim_candidato c ON f.id_candidato = c.id
    JOIN dim_estado e ON f.id_estado = e.id
    GROUP BY e.SG_UF_PROVA, c.TP_SEXO
    ORDER BY e.SG_UF_PROVA, c.TP_SEXO;
"""

query_media_por_disciplina = """
    SELECT 
        e.SG_UF_PROVA,
        AVG(f.NU_NOTA_MT) AS media_matematica,
        AVG(f.NU_NOTA_CN) AS media_ciencias_natureza,
        AVG(f.NU_NOTA_LC) AS media_linguagens_codigos,
        AVG(f.NU_NOTA_CH) AS media_ciencias_humanas,
        AVG(f.NU_NOTA_REDACAO) AS media_redacao
    FROM fato_notas f
    JOIN dim_estado e ON f.id_estado = e.id
    GROUP BY e.SG_UF_PROVA
    ORDER BY e.SG_UF_PROVA;
"""

# Definição da DAG
dag = DAG(
    'etl_enem_2023_p5_consultando_dw',
    default_args=default_args,
    description='Executa consultas no MySQL e imprime os resultados nos logs',
    schedule_interval='@daily',
    catchup=False,
)

consulta_faixa_etaria = PythonOperator(
    task_id='consulta_faixa_etaria',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': query_faixa_etaria, 'task_name': 'Faixa Etária'},
    dag=dag
)

consulta_proporcao_genero = PythonOperator(
    task_id='consulta_proporcao_genero',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': query_proporcao_genero, 'task_name': 'Proporção de Gênero'},
    dag=dag
)

consulta_media_por_disciplina = PythonOperator(
    task_id='consulta_media_por_disciplina',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': query_media_por_disciplina, 'task_name': 'Média por Disciplina'},
    dag=dag
)

[consulta_faixa_etaria, consulta_proporcao_genero, consulta_media_por_disciplina]
