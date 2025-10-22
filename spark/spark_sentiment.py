from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when, regexp_replace

print("=" * 80)
print(" SENTIMENT ANALYSIS - SPARK MLLIB")
print("=" * 80)

# 1. CRÉER LA SESSION SPARK
spark = SparkSession.builder \
    .appName("SentimentAnalysis") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# 2. CHARGER LE DATASET SENTIMENT140

print("\n Chargement du dataset Sentiment140...")

# Schéma du CSV : target,ids,date,flag,user,text
# target: 0 = negative, 4 = positive
df_raw = spark.read.csv(
    "/opt/spark-apps/data/sentiment140.csv",
    header=False,
    inferSchema=True,
    encoding="ISO-8859-1"
)

# Renommer les colonnes
df_raw = df_raw.toDF("target", "ids", "date", "flag", "user", "text")

# Convertir target: 0 → 0 (negative), 4 → 1 (positive)
df = df_raw.withColumn("label", when(col("target") == 0, 0).otherwise(1))

print(f" Dataset chargé : {df.count()} tweets")
print("\n Répartition des sentiments :")
df.groupBy("label").count().show()

# 3. PRÉTRAITEMENT DES DONNÉES

print("\n   Prétraitement du texte...")

# Nettoyer le texte
df_clean = df.withColumn(
    "text_clean",
    regexp_replace(
        regexp_replace(
            regexp_replace(col("text"), r"http\S+", ""),  # Supprimer URLs
            r"@\w+", ""  # Supprimer mentions
        ),
        r"[^a-zA-Z\s]", ""  # Garder seulement lettres
    )
).filter(col("text_clean") != "")

print(f" Texte nettoyé : {df_clean.count()} tweets restants")

# Échantillon pour accélérer (prendre 100k tweets)
df_sample = df_clean.select("text_clean", "label").sample(False, 0.0625).limit(100000)

print(f" Échantillon créé : {df_sample.count()} tweets")

# 4. PIPELINE DE MACHINE LEARNING

print("\n Construction du pipeline ML...")

# Étape 1 : Tokenization (découper en mots)
tokenizer = Tokenizer(inputCol="text_clean", outputCol="words")

# Étape 2 : Supprimer les stop words (the, a, is, etc.)
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")

# Étape 3 : CountVectorizer (mots → vecteurs)
cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", vocabSize=10000)

# Étape 4 : TF-IDF (pondération)
idf = IDF(inputCol="raw_features", outputCol="features")

# Étape 5 : Modèle de classification (Logistic Regression)
lr = LogisticRegression(maxIter=20, regParam=0.01, elasticNetParam=0.0)

# Créer le pipeline
pipeline = Pipeline(stages=[tokenizer, remover, cv, idf, lr])

print(" Pipeline créé avec 5 étapes")

# 5. TRAIN/TEST SPLIT

print("\n Séparation Train/Test (80/20)...")

train_data, test_data = df_sample.randomSplit([0.8, 0.2], seed=42)

print(f" Train: {train_data.count()} tweets")
print(f" Test: {test_data.count()} tweets")

# ============================================================================
# 6. ENTRAÎNEMENT DU MODÈLE
# ============================================================================
print("\n Entraînement du modèle (cela peut prendre 2-5 minutes)...")

model = pipeline.fit(train_data)

print(" Modèle entraîné avec succès !")

#
# 7. PRÉDICTIONS SUR LE TEST SET
# ============================================================================
print("\n Prédictions sur le test set...")

predictions = model.transform(test_data)

# Afficher quelques exemples
print("\n Exemples de prédictions :")
predictions.select("text_clean", "label", "prediction", "probability").show(10, truncate=50)

# 8. ÉVALUATION DU MODÈLE

print("\n ÉVALUATION DU MODÈLE")
print("=" * 80)

evaluator_accuracy = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)

accuracy = evaluator_accuracy.evaluate(predictions)
f1_score = evaluator_f1.evaluate(predictions)

print(f" Accuracy: {accuracy:.2%}")
print(f" F1 Score: {f1_score:.4f}")

# 9. SAUVEGARDER LE MODÈLE

print("\n Sauvegarde du modèle...")

model.write().overwrite().save("/opt/spark-apps/models/sentiment_model")

print(" Modèle sauvegardé dans /opt/spark-apps/models/sentiment_model")

# 10. APPLIQUER SUR LES DONNÉES MASTODON=====================================================================

print("\n Application du modèle sur les données Mastodon...")

# Connexion PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/mastodon_db"
connection_properties = {
    "user": "postgres",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

# Charger les données Mastodon
df_mastodon = spark.read.jdbc(
    url=jdbc_url,
    table="avg_toot_length",
    properties=connection_properties
)


print(f" Données Mastodon chargées : {df_mastodon.count()} lignes")

# Créer un DataFrame exemple avec quelques tweets de test
test_tweets = spark.createDataFrame([
    ("I love this amazing product! It's fantastic!",),
    ("This is terrible, worst experience ever.",),
    ("The weather is nice today.",),
    ("I hate waiting in long queues.",),
    ("Happy birthday! Have a great day!",)
], ["text_clean"])

print("\n Test sur des exemples de tweets :")

# Appliquer le modèle
test_predictions = model.transform(test_tweets)

# Afficher les résultats
test_predictions.select(
    "text_clean",
    "prediction",
    "probability"
).withColumn(
    "sentiment",
    when(col("prediction") == 1, "POSITIVE").otherwise("NEGATIVE")
).show(truncate=False)

# 11. SAUVEGARDER LES RÉSULTATS DANS POSTGRESQL

print("\n Sauvegarde des prédictions dans PostgreSQL...")

# Créer une table avec les résultats
results = test_predictions.select(
    col("text_clean").alias("tweet_text"),
    col("prediction").alias("sentiment_label")
).withColumn(
    "sentiment",
    when(col("sentiment_label") == 1, "positive").otherwise("negative")
)

results.write.jdbc(
    url=jdbc_url,
    table="sentiment_predictions",
    mode="overwrite",
    properties=connection_properties
)

print(" Résultats sauvegardés dans la table 'sentiment_predictions'")

print("\n" + "=" * 80)
print(" SENTIMENT ANALYSIS TERMINÉE AVEC SUCCÈS !")
print("=" * 80)

spark.stop()