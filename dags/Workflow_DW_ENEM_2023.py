from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# 📌 Configuração padrão
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 📌 Criando a DAG Master
dag = DAG(
    "workflow_dw_enem_2023",  # Nome da DAG principal
    default_args=default_args,
    schedule_interval="@once",  # Pode ser ajustado conforme necessário
    catchup=False,
)

# 📌 Encapsula a DAG 1
fluxo_1_baixar_descompactar_bd = TriggerDagRunOperator(
    task_id="etl_enem_2023_p1_baixar_descompactar",
    trigger_dag_id="etl_enem_2023_p1_baixar_descompactar",  
    wait_for_completion=True,
    trigger_rule="all_success",
    dag=dag,
)

# 📌 Encapsula a DAG 2
fluxo_2_criar_schema_dw_mysql = TriggerDagRunOperator(
    task_id="etl_enem_2023_p2_criar_schema_dw_mysql",
    trigger_dag_id="etl_enem_2023_p2_criar_schema_dw_mysql",  
    wait_for_completion=True,
    trigger_rule="all_success",
    dag=dag,
)

# 📌 Encapsula a DAG 3
fluxo_3_pre_processamento_dados_dw_mysql = TriggerDagRunOperator(
    task_id="etl_enem_2023_p3_pre_processamento_dados_dw_mysql",
    trigger_dag_id="etl_enem_2023_p3_pre_processamento_dados_dw_mysql",  
    wait_for_completion=True,
    trigger_rule="all_success",
    dag=dag,
)

# 📌 Encapsula a DAG 4
fluxo_4_inserindo_dados_dw_mysql = TriggerDagRunOperator(
    task_id="etl_enem_2023_p4_inserindo_dados_dw_mysql",
    trigger_dag_id="etl_enem_2023_p4_inserindo_dados_dw_mysql",  
    wait_for_completion=True,
    trigger_rule="all_success",
    dag=dag,
)

# 📌 Encapsula a DAG 5
fluxo_5_consultando_dw = TriggerDagRunOperator(
    task_id="etl_enem_2023_p5_consultando_dw",
    trigger_dag_id="etl_enem_2023_p5_consultando_dw",  
    wait_for_completion=True,
    trigger_rule="all_success",
    dag=dag,
)

# 📌 A dag1 e dag2 rodaram em paralelo, porém as demais são executadas após o sucesso da anterior.
[fluxo_1_baixar_descompactar_bd , fluxo_2_criar_schema_dw_mysql] >> fluxo_3_pre_processamento_dados_dw_mysql >> fluxo_4_inserindo_dados_dw_mysql >> fluxo_5_consultando_dw
