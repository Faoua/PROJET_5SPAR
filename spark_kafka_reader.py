from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType

# Définir le schéma du message JSON venant de Kafka
schema = StructType() \
    .add("id", StringType()) \
    .add("timestamp", StringType()) \
    .add("username", StringType()) \
    .add("content", StringType()) \
    .add("hashtags", ArrayType(StringType())) \
    .add("favourites", StringType()) \
    .add("reblogs", StringType())

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("MastodonKafkaStream") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lire le flux Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mastodon_stream") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir les valeurs binaires Kafka → JSON
df_json = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Afficher les toots en direct dans la console
query = df_json.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
