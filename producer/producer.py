import logging
import time
from itertools import cycle
import requests
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import *

log = logging.getLogger(__name__)


# 1. EXTRACT -> les  fonctions d'appel API

    #  GET /movies ->retourne la liste des noms de films.
def get_movies():

    url = f"{API_BASE_URL}/movies"
    log.info("Récupération de la liste des films…")
  
    resp = requests.get(url)
    try :
        resp.raise_for_status()
        data = resp.json()
        movies = [m["name"] for m in data["movies"]]
        log.info(f"{data['count']} films disponibles.")
        return movies
    except :
        log.error(f"Http Error status : {resp.status_code}")
             
#GET /comments/new?movie=<movie>
#Retourne le commentaire en cours + next_comment_in 

def get_comment(movie):
   
    url = f"{API_BASE_URL}/comments/new"
   
    resp = requests.get(url, params={"movie": movie})
    try :
        if resp.status_code == 404:
            log.warning(f"Film introuvable : '{movie}' -> ignoré.")
            return {}
        resp.raise_for_status()
        return resp.json()
    except :
         log.error(f"Http Error status : {resp.status_code}")

def publish(producer, comment, movie):
 
    future = producer.send(
        KAFKA_TOPIC,
        key=movie,
        value=comment,
    )
    record_metadata = future.get(timeout=10) 
    log.info(
        f"publié  [{movie}] id={comment.get('id')}  "
        f"topic={record_metadata.topic}  "
        f"partition={record_metadata.partition}  "
        f"offset={record_metadata.offset}"
    )


# 3. PIPELINE : boucle principale

def run():
    # Initialisation
    movies   = get_movies()      
    producer = build_producer()
    film_cycle = cycle(movies)  

    log.info(f"Démarrage du producer -> topic : '{KAFKA_TOPIC}'")
    log.info(f"Films en rotation : {movies}")

    try:
        while True:
            movie = next(film_cycle)

            # Extract 
            data = get_comment(movie)
            if not data:
    # si pas de film trouvé on passe au prochain film
                continue       

            comment = data.get("comment", {})
            next_comment_in = data.get("next_comment_in")
            interval = data.get("interval_seconds")

            if not comment:
                log.warning(f"Réponse vide pour '{movie}', on passe.")
                time.sleep(1)
                continue

            # charger data dans kafka
            publish(producer, comment, movie)

            #  Syncro serveur 
            # On dort exactement le temps indiqué par l'API
            log.info(
                f"Prochain commentaire dans {next_comment_in}s "
                f"(intervalle serveur : {interval}s)"
            )
            time.sleep(next_comment_in)

    except KeyboardInterrupt:
        log.info("Arrêt du producer demandé.")

    finally:
        producer.flush() 
        producer.close()
        log.info("Producer fermé proprement.")




run()