#script da camada raw
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType

print("Configurando a variável de ambiente PYSPARK_SUBMIT_ARGS")
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" pyspark-shell'

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
print("Configurando as configurações da AWS WeatherData")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

data1 = spark.read.format("json").load(json_path)

# Ler o JSON
print("lendo dados do S3")
df_city_temp = spark.read.json(data1.toJSON(), multiLine=True)

print("criando tabela")
df_city_temp = df_city_temp.withColumn("name", col('name'))\
                            .withColumn("temp", col('main.temp'))\
                            .withColumn("temp_max", col('main.temp_max'))\
                            .withColumn("temp_min", col('main.temp_min'))\
                            .withColumn("clouds", col('clouds.all'))\
                            .withColumn("cod", col('cod'))\
                            .withColumn("lon", col('coord.lon'))\
                            .withColumn("lat", col('coord.lat'))\
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
                            .withColumn("description", col('weather_explode.description'))\
                            .withColumn("icon", col('weather_explode.icon'))\
                            .withColumn("weather_id", col('weather_explode.id'))\
                            .withColumn("visibility", col('visibility'))\
                            .drop("coord")\
                            .drop("weather_explode")\
                            .drop("sys")\
                            .drop("weather")\
                            .drop("wind")\
                            .drop("base")\
                            .drop("<_io.TextIOWrapper name")

# Definir o esquema do DataFrame
city_temp_schema = StructType([
    StructField("name", StringType(), True),
    StructField("temp", DoubleType(), True),
    StructField("temp_max", DoubleType(), True),
    StructField("temp_min", DoubleType(), True),
    StructField("clouds", IntegerType(), True),
    StructField("cod", IntegerType(), True),
    StructField("dt", LongType(), True),
    StructField("timezone", IntegerType(), True),
    StructField("main_feels_like", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("sunrise", LongType(), True),
    StructField("sunset", LongType(), True),
    StructField("deg", IntegerType(), True),
    StructField("speed", DoubleType(), True),
    StructField("id", IntegerType(), True),
    StructField("main", StringType(), True),
    StructField("description", StringType(), True),
    StructField("icon", StringType(), True),
    StructField("weather_id", IntegerType(), True),
    StructField("visibility", IntegerType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True)
])

# Aplicar o esquema diretamente ao DataFrame
df_city_temp = df_city_temp.select(
    "name", "temp", "temp_max", "temp_min", "clouds", "cod", "dt", "timezone",
    "main_feels_like", "humidity", "pressure", "country", "sunrise", "sunset",
    "deg", "speed", "id", "main", "description", "icon", "weather_id",
    "visibility", "lon", "lat"
)

print("salvando dados em delta no minIO")
df_city_temp.write.format("delta") \
    .mode("append") \
    .option("path", f"s3a://{minio_bucket_name}/{minio_path}") \
    .save()

print(f"Dados convertidos em Delta e salvos.")

print("Sessão spark encerrada")
spark.stop()