#script da camada trusted
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, concat, lit, when

print("Configurando a variável de ambiente PYSPARK_SUBMIT_ARGS")
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" pyspark-shell'

state_code = "PR"
country_code = "BR"
units = "metric"
lang = "pt"
minio_bucket_name = "desafiotempo-deltalake"
raw = "raw"
delta_path = f"s3a://{minio_bucket_name}/{raw}"
minio_path = "trusted"

# Cria uma sessão Spark
print("Iniciando a sessão Spark")
spark = SparkSession.builder.appName("DeltaTrusted").getOrCreate()
# Cofigurações para salvar no MinIO
print("Configurando as configurações do MinIO DadosTempo")


spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "")

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Ler o JSON
print("lendo dados da camada raw")
df_city_temp = spark.read.format("delta").load(delta_path)
print("tratando dados da tabela")
# Aplicar o esquema diretamente ao DataFrame
df_city_temp = df_city_temp.select(
    col("name").alias("city"),
    col("country").alias("country"),
    col("lat").alias("lat"),
    col("lon").alias("lon"),
    col("temp").alias("temp"),
    col("temp_max").alias("temp_max"),
    col("temp_min").alias("temp_min"),
    col("main").alias("main_weather"),
    col("description").alias("description"),
    col("clouds").alias("clouds"),
    col("cod").alias("cod"),
    from_unixtime(col("dt")).alias("date_hour"),
    col("timezone").alias("timezone"),
    col("main_feels_like").alias("main_feels_like"),
    col("humidity").alias("humidity"),
    col("pressure").alias("pressure"),
    from_unixtime(col("sunrise")).alias("sunrise"),
    from_unixtime(col("sunset")).alias("sunset"),
    col("deg").alias("deg"),
    col("speed").alias("speed"),
    col("id").alias("id"),
    col("icon").alias("icon"),
    col("weather_id").alias("weather_id"),
    col("visibility").alias("visibility")
    )

print("salvando dados tratados na camada trusted")
df_city_temp.write.format("delta") \
    .mode("append") \
    .option("path", f"s3a://{minio_bucket_name}/{minio_path}") \
    .save()
print(f"Dados tratados e salvos na trusted.")

print("Sessão spark encerrada")
spark.stop()