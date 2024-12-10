from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='pipeline_mlops',  
    description='Pipeline que corre los 4 scripts diariamente',
    schedule_interval='0 9 * * *',  # Cron para ejecutar a las 9 AM todos los días
    start_date=datetime(2024, 12, 6),
    catchup=False,  
) as dag:

    # Tarea 1: Ejecutar el primer script (filtrar datos)
    filtrar_datos = BashOperator(
        task_id='filtrar_datos',
        bash_command='python3 ~/airflow/scripts/filtrar_Datos.py',
    )

    # Tarea 2: Ejecutar el segundo script (procesar CTR)
    procesar_ctr = BashOperator(
        task_id='procesar_ctr',
        bash_command='python3 ~/airflow/scripts/top_CTR.py',
    )

    # Tarea 3: Ejecutar el tercer script (procesar productos más vistos)
    procesar_top_product = BashOperator(
        task_id='procesar_top_product',
        bash_command='python3 ~/airflow/scripts/top_Products.py',
    )

    # Tarea 4: Insertar datos en PostgreSQL
    insertar_datos_postgres = BashOperator(
        task_id='insertar_datos_postgres',
        bash_command='python3 ~/airflow/scripts/DBWriting.py',
    )

    # Definir la secuencia de ejecución
    filtrar_datos >> procesar_ctr >> procesar_top_product >> insertar_datos_postgres
