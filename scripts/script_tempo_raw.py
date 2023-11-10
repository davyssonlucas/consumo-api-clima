import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

print("Configurando a variável de ambiente PYSPARK_SUBMIT_ARGS")
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" pyspark-shell'

global API_KEY
API_KEY = "1006d353c8fa20e289f975ff6839b5e4"

state_code = "PR"
country_code = "BR"
units = "metric"
lang = "pt"
minio_bucket_name = "desafiotempo-deltalake"
transient = "transient"
json_path = f"s3a://{minio_bucket_name}/{transient}"
minio_path = "raw"

# Cria uma sessão Spark
print("Iniciando a sessão Spark")
spark = SparkSession.builder.appName("JSONDelta").getOrCreate()
# Cofigurações para salvar no MinIO
print("Configurando as configurações do MinIO WeatherData")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "T3W1TJgMz6IypvxiCc96")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "PC8jYTK7LifinMPnsgITdI7uZRj3d7v1Eqw0Ablw")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://192.168.1.16:9001")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

data1 = spark.read.format("json").load(json_path)

# Ler o JSON
print("lendo dados do minIO")
df_city_temp = spark.read.json(data1.toJSON(), multiLine=True)

print("criando tabela")
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
                            .drop("base")\
                            .drop("<_io.TextIOWrapper name")

print("salvando dados em delta no minIO")
df_city_temp.write.format("delta") \
    .mode("append") \
    .option("path", f"s3a://{minio_bucket_name}/{minio_path}") \
    .save()

print(f"Dados convertidos em Delta e salvos.")

print("Sessão spark encerrada")
spark.stop()