from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import limpieza
import conv

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

dag = DAG(
    'movies_database_processing',
    default_args=default_args,
    description='Cleanup and format change for movies info',
    start_date = datetime(year=2020, month=12, day=18),
    schedule_interval='1 3 * * *'  # Every day at 3:01 AM
)

cleanup = PythonOperator(
    task_id='clean_data',
    python_callable= limpieza.clean,
    op_kwargs = {'p1': "movies.csv", 'p2': "ratings.csv"},
    dag=dag,
)

formatting = PythonOperator(
    task_id='change_format',
    python_callable= conv.conv,
    op_kwargs = {'p1': "movies_procesadas.csv", 'p2': "ratings_procesados.csv"},
    dag=dag,
)

# First clean data, then change format
cleanup >> formatting
