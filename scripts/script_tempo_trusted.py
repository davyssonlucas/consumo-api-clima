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
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "T3W1TJgMz6IypvxiCc96")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "PC8jYTK7LifinMPnsgITdI7uZRj3d7v1Eqw0Ablw")
#spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Ler o JSON
print("lendo dados da camada raw")
df_city_temp = spark.read.format("delta").load(delta_path)
print("tratando dados da tabela")
# Aplicar o esquema diretamente ao DataFrame
df_city_temp = df_city_temp.select(
    col("name").alias("cidade"),
    when(col("country") == "BR", "Brasil")
    .otherwise(col("country")).alias("país"),
    col("lat").alias("latitude"),
    col("lon").alias("longitude"),
    concat(col("temp"), lit("C°")).alias("temperatura_atual"),
    concat(col("temp_max"), lit("C°")).alias("temperatura_max"),
    concat(col("temp_min"), lit("C°")).alias("temperatura_min"),
    when(col("main") == "Clouds", "Nuvens")
    .when(col("main") == "Clear", "Céu Limpo")
    .when(col("main") == "Rain", "Chuva")
    .otherwise(col("main")).alias("condicao_principal_do_clima"),
    col("description").alias("descricao_do_clima"),
    col("clouds").alias("nuvens"),
    col("cod").alias("codigo"),
    from_unixtime(col("dt")).alias("data_hora"),
    col("timezone").alias("fuso_horario"),
    concat(col("main_feels_like"),lit("C°")).alias("sensacao_termica"),
    concat(col("humidity"), lit("%")).alias("umidade"),
    col("pressure").alias("pressao_atmosferica"),
    from_unixtime(col("sunrise")).alias("nascer_do_sol"),
    from_unixtime(col("sunset")).alias("por_do_sol"),
    concat(col("deg"), lit("°")).alias("direção_do_vento"),
    concat(col("speed"), lit("m/s")).alias("velocidade_do_vento"),
    col("id").alias("id"),
    col("icon").alias("icone_do_clima"),
    col("weather_id").alias("id_clima"),
    concat(col("visibility"), lit("m")).alias("visibilidade")
    )

print("salvando dados tratados na camada trusted")
df_city_temp.write.format("delta") \
    .mode("append") \
    .option("path", f"s3a://{minio_bucket_name}/{minio_path}") \
    .save()
print(f"Dados tratados e salvos na trusted.")

print("Sessão spark encerrada")
spark.stop()