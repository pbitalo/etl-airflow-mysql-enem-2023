from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_config import default_args
from dag_config import get_conexao_mysql

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

# Query 1: Porcentagem de participantes por sexo e estado
query_porcentagem_genero_estado = """
    SELECT 
        e.SG_UF_PROVA,
        c.TP_SEXO,
        COUNT(*) AS total_candidatos,
        ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY e.SG_UF_PROVA)), 2) AS percentual
    FROM fato_notas f
    JOIN dim_candidato c ON f.id_candidato = c.id
    JOIN dim_estado e ON f.id_estado = e.id
    GROUP BY e.SG_UF_PROVA, c.TP_SEXO
    ORDER BY e.SG_UF_PROVA, c.TP_SEXO;
"""

# Query 2: Média por disciplina por estado
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

# Query 2.1: Estado com maior e menor média geral
query_estado_extremos = """
    WITH media_estados AS (
        SELECT 
            e.SG_UF_PROVA,
            AVG((f.NU_NOTA_MT + f.NU_NOTA_CN + f.NU_NOTA_LC + f.NU_NOTA_CH + f.NU_NOTA_REDACAO) / 5) AS media_geral
        FROM fato_notas f
        JOIN dim_estado e ON f.id_estado = e.id
        GROUP BY e.SG_UF_PROVA
    )
    SELECT SG_UF_PROVA, media_geral
    FROM media_estados
    WHERE media_geral = (SELECT MAX(media_geral) FROM media_estados)
       OR media_geral = (SELECT MIN(media_geral) FROM media_estados)
    ORDER BY media_geral;
"""

# Query 3: Total de candidatos contabilizados
query_total_candidatos = """
    SELECT COUNT(DISTINCT id_candidato) AS total_candidatos FROM fato_notas;
"""

# Definição da DAG
dag = DAG(
    'etl_enem_2023_p5_consultando_dw',
    default_args=default_args,
    description='Executa consultas no MySQL e imprime os resultados nos logs',
    schedule_interval='@daily',
    catchup=False,
)

consulta_porcentagem_genero_estado = PythonOperator(
    task_id='consulta_porcentagem_genero_estado',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': query_porcentagem_genero_estado, 'task_name': 'Porcentagem de Gênero por Estado'},
    dag=dag
)

consulta_media_por_disciplina = PythonOperator(
    task_id='consulta_media_por_disciplina',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': query_media_por_disciplina, 'task_name': 'Média por Disciplina'},
    dag=dag
)

consulta_estado_extremos = PythonOperator(
    task_id='consulta_estado_extremos',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': query_estado_extremos, 'task_name': 'Estado com Maior e Menor Média Geral'},
    dag=dag
)

consulta_total_candidatos = PythonOperator(
    task_id='consulta_total_candidatos',
    python_callable=consultar_dw_mysql,
    op_kwargs={'sql_query': query_total_candidatos, 'task_name': 'Total de Candidatos'},
    dag=dag
)

[consulta_porcentagem_genero_estado, consulta_media_por_disciplina, consulta_estado_extremos, consulta_total_candidatos]

