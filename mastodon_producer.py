from mastodon import Mastodon
from kafka import KafkaProducer
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os, json, time

# Charger les variables d'environnement (.env)
load_dotenv()

# Connexion à l'API Mastodon
mastodon = Mastodon(
    access_token=os.getenv("MASTO_ACCESS_TOKEN"),
    api_base_url=os.getenv("MASTO_BASE_URL")
)

# Connexion au producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "mastodon_stream"

print(" Démarrage du producteur Mastodon → Kafka...")
while True:
    # Récupérer les toots
    toots = mastodon.timeline_hashtag("DataScience", limit=5)
    
    for toot in toots:
        # Nettoyer le contenu HTML
        text = BeautifulSoup(toot["content"], "html.parser").get_text()
        
        # Construire le message JSON
        data = {
            "id": toot["id"],
            "timestamp": str(toot["created_at"]),
            "username": toot["account"]["username"],
            "content": text,
            "hashtags": [tag["name"] for tag in toot["tags"]],
            "favourites": toot["favourites_count"],
            "reblogs": toot["reblogs_count"]
        }
        
        # Envoyer dans Kafka
        producer.send(topic_name, value=data)
        print(f" Envoyé : {data['username']} → {data['content'][:80]}...")

    # Attendre 10 secondes avant le prochain lot
    time.sleep(10)
