"""Configuration centralisée."""
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
import logging
import json
from pathlib import Path
load_dotenv()

# config API
API_BASE_URL = os.getenv("API_BASE_URL")
# config KAFKA
KAFKA_BROKER     = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("KAFKA_BROKER")
TOPIC_CLASSIFIED = os.getenv("TOPIC_CLASSIFIED")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC")
TOPIC_RAW        = os.getenv("KAFKA_TOPIC")
# configuration postgres
PG_HOST     = os.getenv("POSTGRES_HOST")
PG_PORT     = os.getenv("POSTGRES_PORT")
PG_DB       = os.getenv("POSTGRES_DB")
PG_USER     = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DATABASE_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
    f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

# CONFIG Model 
MODEL_PATH       = os.getenv("MODEL_PATH")
VECTORIZER_PATH  = os.getenv("VECTORIZER_PATH")


# config logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)



def build_consumer(Topic ,group_id):
    log.info(f"Consumer connecté à {KAFKA_BROKER} — topic : {TOPIC_RAW}")
    return KafkaConsumer(
        Topic,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",    
        enable_auto_commit=True,
        group_id=group_id  
    )


def build_producer():
    log.info(f"Producer connecté à {KAFKA_BROKER} -> topic : {TOPIC_CLASSIFIED}")
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )