import MySQLdb

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
