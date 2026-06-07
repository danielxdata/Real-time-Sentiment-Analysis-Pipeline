# Projet End-to-End Kafka Sentiment Analysis

Ce projet met en place une chaîne de traitement complète :
- `producer` récupère des commentaires via une API
- `consumer_model` classifie le sentiment
- `consumer_postgres` stocke les résultats dans PostgreSQL
- Kafka orchestre les messages entre les services
- `kafka-ui` fournit une interface web pour explorer les topics

## Contenu du dépôt

- `producer/` : code Python et Dockerfile pour le producteur Kafka
- `consumer/` : code Python et Dockerfiles pour les consommateurs
- `Model/` : modèles ML et artefacts requis
- `config.py` : configuration partagée
- `docker-compose.yml` : orchestration des services
- `Readme.md` : documentation du projet

## Prérequis

- Docker installé
- Docker Compose disponible
- Le dossier `Model/frozen_data` contenant :
  - `ML_model.pkl`
  - `vectoriser.pkl`

## Lancement avec Docker Compose

Depuis le dossier racine du projet :

```bash
docker compose up --build
```

Cela démarre les services suivants :
- `zookeeper`
- `kafka`
- `postgres`
- `producer`
- `consumer_model`
- `consumer_postgres`
- `kafka-ui`

## Vérification

Vérifiez que les services sont bien démarrés :

```bash
docker compose ps
```

## Service Producer

Le service `producer` :
- récupère les films via l'API
- récupère des commentaires
- publie des messages sur le topic Kafka `movie-comments`

Variables d'environnement :
- `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`
- `KAFKA_TOPIC=movie-comments`
- `API_BASE_URL=https://movie-stream-api-bidi.onrender.com`

## Service Consumer Model

Le service `consumer_model` :
- consomme `movie-comments`
- applique le modèle ML
- publie le résultat sur `movie-comments-classified`

Variables d'environnement :
- `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`
- `KAFKA_TOPIC=movie-comments`
- `TOPIC_CLASSIFIED=movie-comments-classified`
- `MODEL_PATH=Model/frozen_data/ML_model.pkl`
- `VECTORIZER_PATH=Model/frozen_data/vectoriser.pkl`

## Service Consumer PostgreSQL

Le service `consumer_postgres` :
- consomme `movie-comments-classified`
- enregistre les résultats dans PostgreSQL

Variables d'environnement :
- `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`
- `TOPIC_CLASSIFIED=movie-comments-classified`
- `POSTGRES_HOST=postgres`
- `POSTGRES_PORT=5432`
- `POSTGRES_DB=sentiment_db`
- `POSTGRES_USER=admin`
- `POSTGRES_PASSWORD=password`

## Kafka UI

L'interface Kafka UI est disponible sur :

```text
http://localhost:8080
```

## Commandes utiles

Arrêter et supprimer les conteneurs :

```bash
docker compose down
```

Recréer un service spécifique :

```bash
docker compose build producer
```

## Note

La configuration Kafka utilise `KAFKA_BOOTSTRAP_SERVERS`, mais `config.py` accepte également `KAFKA_BROKER` pour compatibilité.
