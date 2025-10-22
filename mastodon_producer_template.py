from mastodon import Mastodon, StreamListener
from kafka import KafkaProducer
import json
import os

# --- CONFIG ---
MASTODON_ACCESS_TOKEN = os.getenv("MASTODON_TOKEN", "YOUR_TOKEN_HERE")
MASTODON_API_BASE_URL = "https://mastodon.social"
KAFKA_TOPIC = "mastodon_stream"
KAFKA_SERVER = "localhost:29092"

# --- INITIALISATION ---
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

mastodon = Mastodon(
    access_token=MASTODON_ACCESS_TOKEN,
    api_base_url=MASTODON_API_BASE_URL
)

class MyListener(StreamListener):
    def on_update(self, status):
        toot = {
            "username": status["account"]["username"],  # Changé de "user" à "username"
            "content": status["content"],
            "lang": status.get("language", "unknown"),  # AJOUTÉ
            "created_at": str(status["created_at"]),
            "hashtags": [t["name"] for t in status["tags"]]
        }
        print(f" Toot reçu : {toot['username']} - {toot['hashtags']} - lang: {toot['lang']}")
        producer.send(KAFKA_TOPIC, toot)

listener = MyListener()
mastodon.stream_public(listener)