import os
from pyspark.sql import SparkSession
import requests
from datetime import datetime
import json

print("Configurando a variável de ambiente PYSPARK_SUBMIT_ARGS")
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell'

global API_KEY
API_KEY = ""

state_code = "PR"
country_code = "BR"
units = "metric"
lang = "pt"
minio_bucket_name = "desafiotempo-deltalake"
minio_path = "transient"

# Cria uma sessão Spark
print("Iniciando a sessão Spark")
spark = SparkSession.builder.appName("DailyData").getOrCreate()
# Cofigurações para salvar no MinIO
print("Cofigurações MinIO")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "T3W1TJgMz6IypvxiCc96")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "PC8jYTK7LifinMPnsgITdI7uZRj3d7v1Eqw0Ablw")
#spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9001")

list_city_name = ["curitiba", "pinhais", "colombo", "londrina"]

for city in list_city_name:
    print(f"Baixando dados metereologicos da cidade de {city}.")
    url_tempo = f"https://api.openweathermap.org/data/2.5/weather?q={city},{state_code},{country_code}&appid={API_KEY}&units={units}&lang={lang}"
    response = requests.get(url_tempo)

    if response.status_code == 200:
        data1 = response.json()
        # Adiciona a data e hora ao nome do arquivo
        timestamp = datetime.now().strftime("%Y%m%d")
        file_name = f"{city}_{timestamp}.json"
        # Aqui, você pode salvar diretamente o JSON no seu bucket
        with open(file_name, "w") as json_file:
            json.dump(data1, json_file)

                # Lê o JSON no Spark DataFrame
        df_city_temp = spark.read.json(file_name)

        # Escreve no bucket S3
        df_city_temp.write.format("json") \
            .mode("append") \
            .option("path", f"s3a://{minio_bucket_name}/{minio_path}/{json_file}") \
            .save()
        command = f"rm {file_name}"
        os.system(command)
        print(f"Dados de {city} salvo.")
    else:
        print(f"INFO: {response.text}")

print("Sessão spark encerrada")
spark.stop()