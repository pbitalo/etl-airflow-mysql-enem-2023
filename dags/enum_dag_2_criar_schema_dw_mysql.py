from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_config import default_args
from dag_config import get_conexao_mysql

conn = get_conexao_mysql()

def create_schemas():
    """Cria os bancos de dados e suas tabelas no MySQL, removendo dados antigos antes da recriação."""
    print("🔹 Conectando ao MySQL...")

    cursor = conn.cursor()

    print("🚀 Criando bancos de dados...")
    cursor.execute("CREATE DATABASE IF NOT EXISTS enem_producao;")
    cursor.execute("CREATE DATABASE IF NOT EXISTS enem_dw;")

    # Seleciona o banco de staging e recria a tabela de produção
    cursor.execute("USE enem_producao;")

    print("⚡ Resetando a tabela `staging_enem`...")
    cursor.execute("DROP TABLE IF EXISTS staging_enem;")
    cursor.execute("""
        CREATE TABLE staging_enem (
            id INT AUTO_INCREMENT PRIMARY KEY,
            TP_FAIXA_ETARIA INT,
            SG_UF_PROVA VARCHAR(2),
            NU_NOTA_MT FLOAT,
            NU_NOTA_CN FLOAT,
            NU_NOTA_LC FLOAT,
            NU_NOTA_CH FLOAT,
            NU_NOTA_REDACAO FLOAT,
            TP_SEXO CHAR(1)
        );
    """)
    print("✅ Tabela `staging_enem` recriada!")

    # Seleciona o banco do Data Warehouse e recria as tabelas
    cursor.execute("USE enem_dw;")
    print("⚡ Resetando tabelas no Data Warehouse...")

    # **Desativando restrições de chave estrangeira temporariamente**
    # cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    cursor.execute("DROP TABLE IF EXISTS fato_notas;")
    cursor.execute("DROP TABLE IF EXISTS dim_candidato;")
    cursor.execute("DROP TABLE IF EXISTS dim_estado;")

    # **Reativando restrições de chave estrangeira**
    # cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    # Criando dimensões e tabelas fato
    print("📌 Criando tabelas do DW...")

    # dim_estado
    cursor.execute("""
        CREATE TABLE dim_estado (
            id INT AUTO_INCREMENT PRIMARY KEY,
            SG_UF_PROVA VARCHAR(2) UNIQUE
        );
    """)

    # dim_candidato
    cursor.execute("""
        CREATE TABLE dim_candidato (
            id INT AUTO_INCREMENT PRIMARY KEY,
            TP_FAIXA_ETARIA INT,
            TP_SEXO CHAR(1)
        );
    """)

    # fato_notas
    cursor.execute("""
        CREATE TABLE fato_notas (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_estado INT,
            id_candidato INT,
            NU_NOTA_MT FLOAT,
            NU_NOTA_CN FLOAT,
            NU_NOTA_LC FLOAT,
            NU_NOTA_CH FLOAT,
            NU_NOTA_REDACAO FLOAT,
            FOREIGN KEY (id_estado) REFERENCES dim_estado(id),
            FOREIGN KEY (id_candidato) REFERENCES dim_candidato(id)
        );
    """)

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
