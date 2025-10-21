from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, length
from pyspark.sql.types import StructType, StringType, TimestampType

# --- Création de la session Spark
spark = SparkSession.builder \
    .appName("MastodonStreamProcessor") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/spark-apps/checkpoints") \
    .config("spark.network.timeout", "300s") \
    .config("spark.executor.heartbeatInterval", "20s") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.adaptive.enabled", "false")

# --- Schéma du JSON venant de Kafka
schema = StructType() \
    .add("username", StringType()) \
    .add("content", StringType()) \
    .add("lang", StringType()) \
    .add("created_at", StringType())

# --- Lecture du stream Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "mastodon_stream") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# --- Extraction du JSON
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("created_at", col("created_at").cast(TimestampType()))

# --- Filtrage anglais
df_filtered = df.filter(col("lang") == "en")

# --- Fenêtrage & comptage
df_windowed = df_filtered \
    .withWatermark("created_at", "1 minute") \
    .groupBy(window(col("created_at"), "1 minute"), col("lang")) \
    .count() \
    .selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "COALESCE(lang, 'unknown') as lang",
        "count"
    )

# --- Moyenne de longueur
df_avg_len = df_filtered \
    .withWatermark("created_at", "1 minute") \
    .groupBy(window(col("created_at"), "1 minute"), col("username")) \
    .agg(avg(length(col("content"))).alias("avg_len")) \
    .selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "COALESCE(username, 'unknown') as username",
        "avg_len"
    )

# --- Fonction d'écriture PostgreSQL
def write_to_postgres(df, epoch_id, table_name):
    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/mastodon_db") \
            .option("dbtable", table_name) \
            .option("user", "postgres") \
            .option("password", "root") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"✅ Batch {epoch_id} écrit dans {table_name}")
    except Exception as e:
        print(f"⚠️ Erreur écriture Postgres ({table_name}) : {e}")


def write_to_postgres_window(df, epoch_id):
    write_to_postgres(df, epoch_id, "toot_counts_window")

def write_to_postgres_avg(df, epoch_id):
    write_to_postgres(df, epoch_id, "avg_toot_length")

# --- Lancement des streams
query1 = df_windowed.writeStream \
    .foreachBatch(write_to_postgres_window) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark-apps/checkpoints/windowed_job") \
    .start()

query2 = df_avg_len.writeStream \
    .foreachBatch(write_to_postgres_avg) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark-apps/checkpoints/avglen_job") \
    .start()


print("🚀 Streaming démarré...")
query1.awaitTermination()
query2.awaitTermination()
