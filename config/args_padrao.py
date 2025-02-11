import MySQLdb
from datetime import datetime

def get_args_padrao(**kwargs):
    """
    Retorna os argumentos default para criação da DAG.
    """
    config_args_padrao = {
        "owner": "airflow",
        "start_date": datetime.now(),
    }
    
    # Adiciona os parâmetros extras fornecidos via kwargs ao dicionário
    config_args_padrao.update(kwargs)

    return MySQLdb.connect(**config_args_padrao)
