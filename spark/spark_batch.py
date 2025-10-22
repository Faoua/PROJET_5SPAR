from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, length, desc, to_date

# --- Création de la session Spark
spark = SparkSession.builder \
    .appName("MastodonBatchProcessor") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Connexion PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/mastodon_db"
connection_properties = {
    "user": "postgres",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

print("=" * 80)
print(" BATCH PROCESSING - ANALYSE DES DONNÉES HISTORIQUES")
print("=" * 80)


# 1. CHARGER LES DONNÉES DEPUIS POSTGRESQL

print("\n Chargement des données depuis PostgreSQL...")

df_counts = spark.read.jdbc(
    url=jdbc_url,
    table="toot_counts_window",
    properties=connection_properties
)

df_avg_len = spark.read.jdbc(
    url=jdbc_url,
    table="avg_toot_length",
    properties=connection_properties
)

print(f" Données chargées :")
print(f"   - toot_counts_window: {df_counts.count()} lignes")
print(f"   - avg_toot_length: {df_avg_len.count()} lignes")

# 2. TRANSFORMATION 1 : Filtrer les utilisateurs actifs

print("\n TRANSFORMATION 1 : Utilisateurs avec plus de 3 toots")

# Compter le nombre de toots par utilisateur
user_activity = df_avg_len.groupBy("username") \
    .agg(count("*").alias("toot_count")) \
    .filter(col("toot_count") > 3) \
    .orderBy(desc("toot_count"))

print(f" Utilisateurs actifs trouvés : {user_activity.count()}")
user_activity.show(10, truncate=False)


# 3. TRANSFORMATION 2 : Toots par jour et langue

print("\n TRANSFORMATION 2 : Nombre de toots par jour et langue")

# Extraire la date et compter
df_counts_with_date = df_counts.withColumn("date", to_date(col("window_start")))

daily_counts = df_counts_with_date.groupBy("date", "lang") \
    .agg(count("*").alias("total_toots")) \
    .orderBy("date", desc("total_toots"))

print(f" Jours analysés : {daily_counts.select('date').distinct().count()}")
daily_counts.show(10, truncate=False)

# 4. AGRÉGATION 1 : Total de toots par jour

print("\n AGRÉGATION 1 : Total de toots par jour (toutes langues)")

total_per_day = df_counts_with_date.groupBy("date") \
    .agg(count("*").alias("total_windows")) \
    .orderBy("date")

total_per_day.show(10, truncate=False)


# 5. AGRÉGATION 2 : Longueur moyenne des toots

print("\n AGRÉGATION 2 : Statistiques sur la longueur des toots")

stats = df_avg_len.select(
    avg("avg_len").alias("moyenne_globale"),
    count("*").alias("nombre_total_mesures")
)

stats.show(truncate=False)

# Top 10 des utilisateurs avec les toots les plus longs
top_long_toots = df_avg_len.orderBy(desc("avg_len")).limit(10)

print("\n TOP 10 utilisateurs avec les toots les plus longs :")
top_long_toots.show(10, truncate=False)

# 6. OPTIMISATION : Cache et repartition

print("\n OPTIMISATION : Application du cache...")

df_counts.cache()
df_avg_len.cache()

print(" Cache appliqué")

# Repartitionner pour de meilleures performances
df_counts_optimized = df_counts.repartition(2, "lang")
df_avg_len_optimized = df_avg_len.repartition(2, "username")

print(" Repartitionnement effectué")

# 7. SAUVEGARDER LES RÉSULTATS
print("\n Sauvegarde des résultats dans PostgreSQL...")

# Sauvegarder les utilisateurs actifs
user_activity.write.jdbc(
    url=jdbc_url,
    table="batch_user_activity",
    mode="overwrite",
    properties=connection_properties
)
print(" Table 'batch_user_activity' créée")

# Sauvegarder les statistiques journalières
daily_counts.write.jdbc(
    url=jdbc_url,
    table="batch_daily_stats",
    mode="overwrite",
    properties=connection_properties
)
print(" Table 'batch_daily_stats' créée")

print("\n" + "=" * 80)
print(" BATCH PROCESSING TERMINÉ AVEC SUCCÈS !")
print("=" * 80)

spark.stop()