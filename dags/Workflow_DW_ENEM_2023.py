from airflow import DAG
from dag_config import default_args
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dag = DAG(
    "workflow_dw_enem_2023",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
)

fluxo_1_baixar_descompactar_bd = TriggerDagRunOperator(
    task_id="etl_enem_2023_p1_baixar_descompactar",
    trigger_dag_id="etl_enem_2023_p1_baixar_descompactar",  
    wait_for_completion=True,
    trigger_rule="all_success",
    dag=dag,
)

fluxo_2_criar_schema_dw_mysql = TriggerDagRunOperator(
    task_id="etl_enem_2023_p2_criar_schema_dw_mysql",
    trigger_dag_id="etl_enem_2023_p2_criar_schema_dw_mysql",  
    wait_for_completion=True,
    trigger_rule="all_success",
    dag=dag,
)

fluxo_3_pre_processamento_dados_dw_mysql = TriggerDagRunOperator(
    task_id="etl_enem_2023_p3_pre_processamento_dados_dw_mysql",
    trigger_dag_id="etl_enem_2023_p3_pre_processamento_dados_dw_mysql",  
    wait_for_completion=True,
    trigger_rule="all_success",
    dag=dag,
)

fluxo_4_inserindo_dados_dw_mysql = TriggerDagRunOperator(
    task_id="etl_enem_2023_p4_inserindo_dados_dw_mysql",
    trigger_dag_id="etl_enem_2023_p4_inserindo_dados_dw_mysql",  
    wait_for_completion=True,
    trigger_rule="all_success",
    dag=dag,
)

fluxo_5_consultando_dw = TriggerDagRunOperator(
    task_id="etl_enem_2023_p5_consultando_dw",
    trigger_dag_id="etl_enem_2023_p5_consultando_dw",  
    wait_for_completion=True,
    trigger_rule="all_success",
    dag=dag,
)

# Ordem de executação do workflow
[fluxo_1_baixar_descompactar_bd , fluxo_2_criar_schema_dw_mysql] >> fluxo_3_pre_processamento_dados_dw_mysql >> fluxo_4_inserindo_dados_dw_mysql >> fluxo_5_consultando_dw
