from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_spark():
    result = subprocess.run(
        ['python', '/opt/airflow/pipeline/spark/spark_batch.py'],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(result.stderr)

with DAG(
    dag_id='sensor_pipeline',
    default_args=default_args,
    description='펌프 센서 데이터 파이프라인',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sensor', 'pipeline'],
) as dag:

    run_spark_task = PythonOperator(
        task_id='run_spark_batch',
        python_callable=run_spark,
    )

    notify_done = PythonOperator(
        task_id='notify_done',
        python_callable=lambda: print("파이프라인 완료!"),
    )

    run_spark_task >> notify_done