from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_config import default_args, get_conexao_mysql
from sql_queries import (QUERY_PORCENTAGEM_GENERO_ESTADO, QUERY_MEDIA_DISCIPLINA_SEXO_ESTADO, 
                         QUERY_MEDIA_GERAL_SEXO_ESTADO, QUERY_ESTADO_EXTREMOS, 
                         QUERY_TOTAL_CANDIDATOS, QUERY_MEDIA_GERAL)

conn = get_conexao_mysql(db="enem_dw")

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

# Definição da DAG
dag = DAG(
    'etl_enem_2023_p5_consultando_dw',
    default_args=default_args,
    description='Executa consultas no MySQL e imprime os resultados nos logs',
    schedule_interval='@daily',
    catchup=False,
)

# Criando as tarefas da DAG
consulta_porcentagem_genero_estado = PythonOperator(
    task_id='consulta_porcentagem_genero_estado',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': QUERY_PORCENTAGEM_GENERO_ESTADO, 'task_name': 'Porcentagem de Gênero por Estado'},
    dag=dag
)

consulta_media_disciplina_sexo_estado = PythonOperator(
    task_id='consulta_media_disciplina_sexo_estado',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': QUERY_MEDIA_DISCIPLINA_SEXO_ESTADO, 'task_name': 'Média por Disciplina e Sexo por Estado'},
    dag=dag
)

consulta_media_geral_sexo_estado = PythonOperator(
    task_id='consulta_media_geral_sexo_estado',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': QUERY_MEDIA_GERAL_SEXO_ESTADO, 'task_name': 'Média Geral por Sexo e Estado'},
    dag=dag
)

consulta_estado_extremos = PythonOperator(
    task_id='consulta_estado_extremos',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': QUERY_ESTADO_EXTREMOS, 'task_name': 'Estado com Maior e Menor Média Geral'},
    dag=dag
)

consulta_total_candidatos = PythonOperator(
    task_id='consulta_total_candidatos',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': QUERY_TOTAL_CANDIDATOS, 'task_name': 'Total de Candidatos'},
    dag=dag
)

consulta_media_geral = PythonOperator(
    task_id='consulta_media_geral',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': QUERY_MEDIA_GERAL, 'task_name': 'Média Geral do ENEM 2023'},
    dag=dag
)

#Executando as consultas em paralelo.
[consulta_porcentagem_genero_estado, consulta_media_disciplina_sexo_estado, 
 consulta_media_geral_sexo_estado, consulta_estado_extremos, 
 consulta_total_candidatos, consulta_media_geral]
