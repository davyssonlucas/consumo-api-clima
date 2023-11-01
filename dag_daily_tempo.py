from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define o nome da DAG
dag_id = 'weather_data_daily_etl'

# Define os argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 31, 00, 0),  # Data de início da DAG
    'retries': 3,  # Número de tentativas em caso de falha
    'retry_delay': timedelta(minutes=2),  # Intervalo entre tentativas
}

# Cria a DAG com os argumentos padrão
dag = DAG(dag_id, default_args=default_args, schedule_interval=timedelta(days=1), catchup=False)#schedule_interval='@daily'days=5

# Importa as bibliotecas e configurações necessárias uma vez
import os
from pyspark.sql import SparkSession
import requests
from pyspark.sql.functions import col, explode

# Configura a variável de ambiente PYSPARK_SUBMIT_ARGS
print("Configurando a variável de ambiente PYSPARK_SUBMIT_ARGS")
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" pyspark-shell'

global API_KEY
API_KEY = ""


# Função para coletar e processar os dados da API e salva em delta
def collect_weather_data(city):  
    state_code = "PR"
    country_code = "BR"
    units = "metric"
    lang = "pt"
    minio_bucket_name = "desafiotempo"
    minio_path = "transient/daily_historic"
    format = "delta"
    mode = "append"
    
    # Cria uma sessão Spark
    print("Iniciando a sessão Spark")
    spark = SparkSession.builder.appName("DailyData").getOrCreate()
    # Configura as configurações do MinIO (caso não estejam definidas em PYSPARK_SUBMIT_ARGS)
    print("Configurando as configurações do MinIO WeatherData")
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "qiBoC6oxvaAdlFCR9I9p")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "vjHjYMCp6fbWyHjO3iFHewSggT0lWiCBFNGVKiu2")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://172.17.59.191:9000")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    url_tempo = f"https://api.openweathermap.org/data/2.5/weather?q={city},{state_code},{country_code}&appid={API_KEY}&units={units}&lang={lang}"
    response = requests.get(url_tempo)
    if response.status_code == 200:
        data1 = response.json()
        rdd = spark.sparkContext.parallelize([data1], numSlices=1)
        # Passa o RDD para o método spark.read.json()
        df_city_temp = spark.read.json(rdd, multiLine=True)
        #df_city_temp.printSchema()
        df_city_temp = df_city_temp.withColumn("name", col('name'))\
                                    .withColumn("temp", col('main.temp'))\
                                    .withColumn("temp_max", col('main.temp_max'))\
                                    .withColumn("temp_min", col('main.temp_min'))\
                                    .withColumn("clouds", col('clouds.all'))\
                                    .withColumn("cod", col('cod'))\
                                    .withColumn("dt", col('dt'))\
                                    .withColumn("timezone", col('timezone'))\
                                    .withColumn("main_feels_like", col('main.feels_like'))\
                                    .withColumn("humidity", col('main.humidity'))\
                                    .withColumn("pressure", col('main.pressure'))\
                                    .withColumn("country", col('sys.country'))\
                                    .withColumn("sys_id", col('sys.id'))\
                                    .withColumn("sunrise", col('sys.sunrise'))\
                                    .withColumn("sunset", col('sys.sunset'))\
                                    .withColumn("deg", col('wind.deg'))\
                                    .withColumn("speed", col('wind.speed'))\
                                    .withColumn("weather_explode", explode('weather'))\
                                    .withColumn("id", col('id'))\
                                    .withColumn("main", col('weather_explode.main'))\
                                    .withColumn("icon", col('weather_explode.icon'))\
                                    .withColumn("weather_id", col('weather_explode.id'))\
                                    .drop("weather_explode")\
                                    .drop("sys")\
                                    .drop("weather")\
                                    .drop("wind")\
                                    .drop("base")
        
        df_city_temp.write.format(format) \
            .mode(mode) \
            .option("path", f"s3a://{minio_bucket_name}/{minio_path}") \
            .save()
        print("Sessão spark encerrada")
        spark.stop()
    else:
        print(f"INFO: {response.text}")


# Tarefas de coleta de dados para cada cidade
list_city_name = ["curitiba", "pinhais", "colombo", "londrina"]
previous_task = None  # Inicialize como None
#collect_data_tasks = []

for city in list_city_name:
    task_id = f'collect_data_{city}'
    collect_data_task = PythonOperator(
        task_id=task_id,
        python_callable=collect_weather_data,
        op_args=[city],
        dag=dag,
    )
    #collect_data_tasks.append(collect_data_task)        
    if previous_task is not None:
        previous_task >> collect_data_task  # Defina a dependência
    previous_task = collect_data_task  # Atualize a tarefa anterior
