import logging
import time

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import *

from Model.preprocessing import preprocess_step
import joblib

log = logging.getLogger(__name__)

    #Charge le vectorizer TF-IDF et le modèle depuis le disque.

def load_artifacts():

        log.info(f"Chargement du vectorizer : {VECTORIZER_PATH}")
        vectorizer = joblib.load(VECTORIZER_PATH)

        log.info(f"Chargement du modèle : {MODEL_PATH}")
        model = joblib.load(MODEL_PATH)

        return vectorizer, model


        #Preprocessing -> TF-IDF -> Prédiction → label (positive / negative)
def classify(text, vectorizer, model):
    
        cleaned   = preprocess_step(text)          
        features  = vectorizer.transform([cleaned])  
        prediction = model.predict(features)[0] 

        # Normalise le label en chaîne 
        label = str(prediction).lower()
        if label == "1" :
            return "positive"
        else:
            return "negative"

    # PIPELINE PRINCIPAL

def run():
        vectorizer, model = load_artifacts()
        consumer = build_consumer(TOPIC_RAW,"classifier-group")
        producer = build_producer()

        log.info("En attente de commentaires…")

        try:
            for message in consumer:
                comment = message.value
                movie = message.key or comment.get("movie")
                text = comment.get("comment")

                if not text:
                    log.warning(f"Commentaire vide reçu -> ignoré. id={comment.get('id')}")
                    continue

                # Pipeline ML 
                try:
                    label = classify(text, vectorizer, model)
                except Exception as e:
                    log.error(f"Erreur classification id={comment.get('id')} : {e}")
                    continue
                classified_payload = {
                    "id" : comment["id"],
                    "movie": comment['movie'],
                    "comment" : comment["comment"],                         
                    "predicted_sentiment": label,       
                    "classified_at": time.time(),       
                }

                # Publication sur le topic classifié 
                producer.send(
                    TOPIC_CLASSIFIED,
                    key=movie,
                    value=classified_payload,
                )

                log.info(
                    f"[{movie}] id={comment.get('id')}  "
                    f"->predicted_sentiment={label}"
                )

        except KeyboardInterrupt:
            log.info("Arrêt demandé.")
        finally:
            producer.flush()
            producer.close()
            consumer.close()
            log.info("Consumer #1 fermé proprement.")

run()