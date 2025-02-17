from datetime import datetime, timedelta
import MySQLdb

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

default_args_default = {
    "owner": "airflow",
    'start_date': datetime.now(),
    "catchup": False
}

def get_conexao_mysql(**kwargs):
    """
    Cria e retorna uma conexão com o MySQL.
    Permite passar parâmetros adicionais dinamicamente.
    """
    config_conexao_mysql = {
        "host": "mysql",
        "port": 3306,
        "user": "airflow",
        "passwd": "airflow"
    }
    
    # Adiciona os parâmetros extras fornecidos via kwargs ao dicionário de conexão
    config_conexao_mysql.update(kwargs)

    return MySQLdb.connect(**config_conexao_mysql)