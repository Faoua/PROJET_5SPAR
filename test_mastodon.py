from mastodon import Mastodon
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Charger les variables d'environnement (.env)
load_dotenv()

# Connexion à l'API Mastodon
mastodon = Mastodon(
    access_token=os.getenv("MASTO_ACCESS_TOKEN"),
    api_base_url=os.getenv("MASTO_BASE_URL")
)

# Récupérer les derniers toots du hashtag #DataScience
toots = mastodon.timeline_hashtag("DataScience", limit=5)

# Parcourir et afficher les toots nettoyés
for toot in toots:
    username = toot["account"]["username"]
    
    # Nettoyage du HTML du contenu
    raw_html = toot["content"]
    clean_text = BeautifulSoup(raw_html, "html.parser").get_text()
    
    print(f" {username} → {clean_text.strip()[:150]}...\n")
