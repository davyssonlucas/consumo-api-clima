from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define o nome da DAG
dag_id = 'weather_data_etl'

# Define os argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 31, 00, 0),  # Data de início da DAG
    'retries': 3,  # Número de tentativas em caso de falha
    'retry_delay': timedelta(minutes=2),  # Intervalo entre tentativas
}

# Cria a DAG com os argumentos padrão
dag = DAG(dag_id, default_args=default_args, schedule_interval=timedelta(days=5), catchup=False)#schedule_interval='@daily'days=5

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
    minio_path = "transient/forecast_historic"
    format = "delta"
    mode = "append"
    
    # Cria uma sessão Spark
    print("Iniciando a sessão Spark")
    spark = SparkSession.builder.appName("WeatherData").getOrCreate()
    # Configura as configurações do MinIO (caso não estejam definidas em PYSPARK_SUBMIT_ARGS)
    print("Configurando as configurações do MinIO WeatherData")
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "qiBoC6oxvaAdlFCR9I9p")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "vjHjYMCp6fbWyHjO3iFHewSggT0lWiCBFNGVKiu2")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://172.17.59.191:9000")
    
    url_tempo = f"https://api.openweathermap.org/data/2.5/forecast?q={city},{state_code},{country_code}&appid={API_KEY}&units={units}&lang={lang}"
    print(url_tempo)
    response = requests.get(url_tempo)
    if response.status_code == 200:
        data = response.json()
        
        # Cria um RDD a partir do JSON
        rdd = spark.sparkContext.parallelize([data], numSlices=1)
        # Passa o RDD para o método spark.read.json()
        df = spark.read.json(rdd, multiLine=True)
        df_flat = df.withColumn("list_explode", explode('list'))\
            .withColumn("dt_txt", col('list_explode.dt_txt'))\
            .withColumn("dt", col('list_explode.dt'))\
            .withColumn("feels_like", col('list_explode.main.feels_like'))\
            .withColumn("grnd_level", col('list_explode.main.grnd_level'))\
            .withColumn("humidity", col('list_explode.main.humidity'))\
            .withColumn("pressure", col('list_explode.main.pressure'))\
            .withColumn("sea_level", col('list_explode.main.sea_level'))\
            .withColumn("temp", col('list_explode.main.temp'))\
            .withColumn("temp_kf", col('list_explode.main.temp_kf'))\
            .withColumn("temp_max", col('list_explode.main.temp_max'))\
            .withColumn("temp_min", col('list_explode.main.temp_min'))\
            .withColumn("pop", col('list_explode.pop'))\
            .withColumn("rain", col('list_explode.rain'))\
            .withColumn("sys_pod", col('list_explode.sys.pod'))\
            .withColumn("visibility", col('list_explode.visibility'))\
            .withColumn("clouds", col('list_explode.clouds.all'))\
            .withColumn("deg", col('list_explode.wind.deg'))\
            .withColumn("gust", col('list_explode.wind.gust'))\
            .withColumn("speed", col('list_explode.wind.speed'))\
            .withColumn("weather_explode", explode('list_explode.weather'))\
            .withColumn("description", col('weather_explode.description'))\
            .withColumn("icon", col('weather_explode.icon'))\
            .withColumn("id", col('weather_explode.id'))\
            .withColumn("main", col('weather_explode.main'))\
            .drop("weather_explode")\
            .drop("list_explode")\
            .withColumn("coord", col("city.coord"))\
            .withColumn("country", col("city.country"))\
            .withColumn("city_id", col("city.id"))\
            .withColumn("name", col("city.name"))\
            .withColumn("population", col("city.population"))\
            .withColumn("sunrise", col("city.sunrise"))\
            .withColumn("sunset", col("city.sunset"))\
            .withColumn("timezone", col("city.timezone"))\
            .drop("city")\
            .drop("list")
        
        df_flat.write.format(format) \
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
