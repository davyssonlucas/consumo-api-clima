from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import boto3

# Configurações do MinIO
minio_host = ""
bucket_name = "scripts-deltalake"
folder_path = "raw"
script_name = "script_tempo_raw.py"
access_key = ""
secret_key = ""
minio_file_key = f"{folder_path}/{script_name}"

# Configurações da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'raw_dados_tempo',
    default_args=default_args,
    schedule_interval=timedelta(days=5),
    catchup=False,
)

def download_from_s3(**kwargs):
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    s3.download_file(bucket_name, minio_file_key, '/tmp/script_tempo.py')

downloadscript_task = PythonOperator(
    task_id='download_script',
    python_callable=download_from_s3,
    provide_context=True,
    dag=dag,
)

execute_task = BashOperator(
    task_id='execute_script',
    bash_command='python3 /tmp/script_tempo.py',
    dag=dag,
)

delete_task = BashOperator(
    task_id='delete_script',
    bash_command='rm /tmp/script_tempo.py',
    dag=dag,
)

# Define a ordem de execução das tarefas
downloadscript_task >> execute_task >> delete_task