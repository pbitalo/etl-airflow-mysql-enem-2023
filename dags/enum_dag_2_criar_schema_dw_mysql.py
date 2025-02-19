from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_config import default_args, get_conexao_mysql
from sql_queries import CREATE_DATABASES, DROP_TABLES, CREATE_TABLES

def create_schemas():
    """Cria os bancos de dados e suas tabelas no MySQL, removendo dados antigos antes da recriação."""
    print("🔹 Conectando ao MySQL...")
    conn = get_conexao_mysql()
    cursor = conn.cursor()

    # Criar bancos de dados
    print("🚀 Criando bancos de dados...")
    for query in CREATE_DATABASES:
        cursor.execute(query)

    # Resetar tabelas
    for db, queries in DROP_TABLES.items():
        cursor.execute(f"USE {db};")
        print(f"⚡ Resetando tabelas no banco {db}...")
        for query in queries:
            cursor.execute(query)

    # Criar tabelas
    for db, queries in CREATE_TABLES.items():
        cursor.execute(f"USE {db};")
        print(f"📌 Criando tabelas no banco {db}...")
        if isinstance(queries, list):
            for query in queries:
                cursor.execute(query)
        else:
            cursor.execute(queries)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Bancos de dados e tabelas recriados com sucesso!")

# Criando a DAG
dag = DAG(
    'etl_enem_2023_p2_criar_schema_dw_mysql',
    default_args=default_args,
    description='Cria bancos de dados e schemas no MySQL, resetando os dados a cada execução',
    schedule_interval='@once',
    catchup=False,
)

# Criando a tarefa para criar os bancos e tabelas
tarefa_criar_bd_mysql = PythonOperator(
    task_id='create_schemas',
    python_callable=create_schemas,
    dag=dag,
)
