FROM python:3.11-slim

WORKDIR /app

COPY producer/requirement.txt .
RUN pip3 install --no-cache-dir -r requirement.txt

COPY producer/producer.py .
COPY config.py .

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=movie-comments \
    API_BASE_URL=https://movie-stream-api-bidi.onrender.com

CMD ["python", "-u", "producer.py"]