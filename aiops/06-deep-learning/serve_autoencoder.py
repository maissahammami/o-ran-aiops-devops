"""
Inférence continue de l'autoencoder entraîné — scanne les mesures récentes
et publie sur Kafka toute mesure dont l'erreur de reconstruction dépasse
le seuil appris (95e percentile des données d'entraînement "normales").
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
logger = logging.getLogger("autoencoder-serve")

INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST", "influxdb.smo.svc.cluster.local")
INFLUXDB_PORT = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUXDB_DB = os.environ.get("INFLUXDB_DB", "smo")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "nonrtric-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
ANOMALY_TOPIC = os.environ.get("ANOMALY_TOPIC_DL", "anomalies-dl")
MODEL_PATH = os.environ.get("AE_MODEL_PATH", "/data/autoencoder.keras")
THRESHOLD_PATH = os.environ.get("AE_THRESHOLD_PATH", "/data/autoencoder_threshold.npy")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))

FEATURE_FIELDS = [
    "event_measurementsForVfScalingFields_additionalObjects_0_objectInstances_0_objectInstance_value",
    "event_measurementsForVfScalingFields_additionalObjects_1_objectInstances_0_objectInstance_value",
    "event_measurementsForVfScalingFields_additionalObjects_2_objectInstances_0_objectInstance_value",
    "event_measurementsForVfScalingFields_additionalObjects_3_objectInstances_0_objectInstance_value",
]


def write_anomaly_to_influxdb(influx_client, anomaly):
    """Écrit l'anomalie dans la mesure 'anomalies_dl' de la base 'smo' — visible directement dans Chronograf."""
    json_body = [{
        "measurement": "anomalies_dl",
        "tags": {"imsi": str(anomaly["imsi"])},
        "time": int(anomaly["detected_at"] * 1e9),  # nanosecondes, précision attendue par InfluxDB
        "fields": {
            "reconstruction_error": anomaly["reconstruction_error"],
            "threshold": anomaly["threshold"],
        },
    }]
    try:
        influx_client.write_points(json_body)
    except Exception as e:
        logger.error(f"Échec écriture InfluxDB: {e}")


def fetch_recent_points(client, minutes=5):
    query = f'SELECT * FROM "ues" WHERE time > now() - {minutes}m'
    result = client.query(query)
    return list(result.get_points())


def main_loop():
    if not os.path.exists(MODEL_PATH):
        logger.error(f"Modèle introuvable à {MODEL_PATH} — lancez d'abord train_autoencoder.py")
        return

    model = load_model(MODEL_PATH)
    params = np.load(THRESHOLD_PATH, allow_pickle=True).item()
    mean, std, threshold = params["mean"], params["std"], params["threshold"]

    influx_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DB)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    logger.info(f"Autoencoder chargé — seuil d'anomalie MSE: {threshold:.5f}")

    while True:
        points = fetch_recent_points(influx_client)
        anomaly_count = 0

        for point in points:
            row = [point.get(f) for f in FEATURE_FIELDS]
            if any(v is None for v in row):
                continue

            x = (np.array(row, dtype=float) - mean) / std
            x = x.reshape(1, -1)
            reconstructed = model.predict(x, verbose=0)
            mse = float(np.mean((x - reconstructed) ** 2))

            if mse > threshold:
                anomaly_count += 1
                anomaly = {
                    "imsi": point.get("event_commonEventHeader_sourceName", "unknown"),
                    "reconstruction_error": round(mse, 5),
                    "threshold": threshold,
                    "detected_at": time.time(),
                }
                logger.warning(f"Anomalie (autoencoder): {anomaly}")
                producer.send(ANOMALY_TOPIC, value=anomaly)
                write_anomaly_to_influxdb(influx_client, anomaly)

        logger.info(f"{len(points)} points analysés, {anomaly_count} anomalie(s) détectée(s)")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main_loop()
