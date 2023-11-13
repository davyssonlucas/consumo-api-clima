from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Configurações do MinIO
minio_host = "140.238.237.205:9001"
bucket_name = "scripts"
folder_path = "trusted"
script_name = "script_tempo_trusted.py"
minio_file_key = f"{folder_path}/{script_name}"
minio_url = f"http://{minio_host}/{bucket_name}/{minio_file_key}"

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
    'trusted_dados_tempo',
    default_args=default_args,
    schedule_interval=timedelta(days=10),
    catchup=False,
)

downloadscript_task = BashOperator(
    task_id='download_script',
    bash_command=f'wget {minio_url} -O /tmp/script_trusted.py',
    dag=dag,
)

execute_task = BashOperator(
    task_id='execute_script',
    bash_command='python3 /tmp/script_trusted.py',
    dag=dag,
)

delete_task = BashOperator(
    task_id='delete_script',
    bash_command='rm /tmp/script_trusted.py',
    dag=dag,
)

# Define a ordem de execução das tarefas
downloadscript_task >> execute_task >> delete_task
