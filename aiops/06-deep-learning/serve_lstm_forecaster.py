"""
Inférence périodique du modèle LSTM entraîné (train_lstm_forecaster.py).
Conçu pour tourner en CronJob (comme le module 02), publie la prévision
sur le topic Kafka "load-forecast-dl" pour ne pas entrer en conflit avec
le forecaster linéaire existant (comparaison possible des deux approches).
"""
import json
import logging
import os
import time

import numpy as np
from influxdb import InfluxDBClient
from kafka import KafkaProducer
from tensorflow.keras.models import load_model

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("lstm-forecaster-serve")

INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST", "influxdb.smo.svc.cluster.local")
INFLUXDB_PORT = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUXDB_DB = os.environ.get("INFLUXDB_DB", "smo")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "nonrtric-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
FORECAST_TOPIC = os.environ.get("FORECAST_TOPIC", "load-forecast-dl")
MODEL_PATH = os.environ.get("MODEL_OUTPUT_PATH", "/data/lstm_forecaster.keras")
SEQUENCE_LENGTH = int(os.environ.get("SEQUENCE_LENGTH", "24"))


def fetch_recent_sequence(client, seq_len, bucket_minutes=5):
    counts = []
    for i in range(seq_len, 0, -1):
        start = f"now() - {i * bucket_minutes}m"
        end = f"now() - {(i - 1) * bucket_minutes}m"
        query = f"""
            SELECT COUNT(DISTINCT("event_commonEventHeader_sourceName"))
            FROM "ues" WHERE time > {start} AND time <= {end}
        """
        result = client.query(query)
        points = list(result.get_points())
        counts.append(points[0].get("count", 0) if points else 0)
    return np.array(counts, dtype=float)


def run_once():
    if not os.path.exists(MODEL_PATH):
        logger.error(f"Modèle introuvable à {MODEL_PATH} — lancez d'abord train_lstm_forecaster.py")
        return

    norm_path = MODEL_PATH.replace(".keras", "_norm.npy")
    series_min, series_max = np.load(norm_path)

    model = load_model(MODEL_PATH)
    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DB)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    sequence = fetch_recent_sequence(client, SEQUENCE_LENGTH)
    if len(sequence) < SEQUENCE_LENGTH:
        logger.warning("Pas assez de points récents pour une prédiction fiable, on complète par padding")
        sequence = np.pad(sequence, (SEQUENCE_LENGTH - len(sequence), 0), mode="edge")

    norm_seq = (sequence - series_min) / max(1e-6, (series_max - series_min))
    x = norm_seq.reshape((1, SEQUENCE_LENGTH, 1))

    pred_norm = float(model.predict(x, verbose=0)[0][0])
    pred = pred_norm * (series_max - series_min) + series_min
    current = sequence[-1]

    result = {
        "model": "lstm",
        "current_ue_count": float(current),
        "forecast_ue_count": round(max(0, pred)),
        "timestamp": time.time(),
    }
    logger.info(f"Prévision LSTM: {result}")
    producer.send(FORECAST_TOPIC, value=result)
    producer.flush()

    # Écriture dans InfluxDB (mesure 'load_forecast_dl', base 'smo') pour affichage direct dans Chronograf
    try:
        client.write_points([{
            "measurement": "load_forecast_dl",
            "tags": {"model": "lstm"},
            "time": int(result["timestamp"] * 1e9),
            "fields": {
                "current_ue_count": result["current_ue_count"],
                "forecast_ue_count": float(result["forecast_ue_count"]),
            },
        }])
    except Exception as e:
        logger.error(f"Échec écriture InfluxDB: {e}")


if __name__ == "__main__":
    run_once()
