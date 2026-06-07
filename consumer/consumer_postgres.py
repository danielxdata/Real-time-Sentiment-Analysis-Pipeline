from dotenv import load_dotenv
from kafka import KafkaConsumer
from sqlalchemy import (
    Column, Double, Integer, String, Text,
    create_engine, func,
)
from sqlalchemy.orm import DeclarativeBase, Session

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import *


log = logging.getLogger(__name__)

# ORM – modèle SQLAlchemy

class Base(DeclarativeBase):
    pass


class Comment(Base):
    __tablename__ = "comments"

    id = Column(String,primary_key=True)
    movie = Column(String,nullable=False)
    comment = Column(Text,nullable=False)
    predicted_sentiment = Column(String,nullable=False)   
    classified_at = Column(Double)                                   
    inserted_at = Column(String,default=func.now())

def connection_db():
        
        try:
            engine = create_engine(DATABASE_URL, pool_pre_ping=True)
            log.info(f"PostgreSQL connecté ({PG_HOST}:{PG_PORT}/{PG_DB})")
            return engine
        except :
            log.warning(f"Connection à PostgreSQL imposible")


# PIPELINE PRINCIPAL

def payload_to_comment(payload):
    return Comment(
        id = payload.get("id"),
        movie = payload.get("movie"),
        comment = payload.get("comment"),
        predicted_sentiment = payload.get("predicted_sentiment"),
        classified_at = payload.get("classified_at"),
 
   
    )



def run():
    engine   = connection_db()
    Base.metadata.create_all(engine)      
    consumer = build_consumer(TOPIC_CLASSIFIED,"postgres-sink-group")

    log.info("En attente de commentaires classifiés…")

    try:
        for message in consumer:
            payload = message.value
            comment = payload_to_comment(payload)

            with Session(engine) as session:
                session.merge(comment)
                session.commit()

            log.info(
                f"Inséré [{comment.movie}] "
                f"id={comment.id}  "
                f"sentiment={comment.predicted_sentiment}"
            )

    except KeyboardInterrupt:
        log.info("Arrêt demandé.")
    finally:
        consumer.close()
        engine.dispose()
        log.info("Consumer #2 fermé proprement.")

run()