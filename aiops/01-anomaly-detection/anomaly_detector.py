"""
Détecteur d'anomalies pour les métriques radio (RSRP, RSRQ, SINR) par UE.

Approche: seuil statistique glissant (z-score) par gNB, complété par un
Isolation Forest global pour capter les anomalies multivariées. Volontairement
léger (pas de deep learning) pour tourner en continu sur un pod modeste.

Publie les anomalies détectées sur un topic Kafka "anomalies" pour que le
rApp (ou un futur module d'alerte) puisse réagir.
"""
import json
import logging
import os
import time
from collections import defaultdict, deque

import numpy as np
from influxdb import InfluxDBClient
from kafka import KafkaProducer
from sklearn.ensemble import IsolationForest

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("anomaly-detector")

INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST", "influxdb.smo.svc.cluster.local")
INFLUXDB_PORT = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUXDB_DB = os.environ.get("INFLUXDB_DB", "smo")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "nonrtric-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
ANOMALY_TOPIC = os.environ.get("ANOMALY_TOPIC", "anomalies")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
WINDOW_SIZE = int(os.environ.get("WINDOW_SIZE", "50"))  # nb de points gardés par UE
Z_SCORE_THRESHOLD = float(os.environ.get("Z_SCORE_THRESHOLD", "3.0"))

# Historique glissant par métrique/UE pour le calcul de z-score
history = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))


def fetch_recent_metrics(client, minutes=5):
    """Récupère les dernières valeurs RSRP/RSRQ/SINR/RRC depuis InfluxDB."""
    query = f"""
        SELECT * FROM "ues"
        WHERE time > now() - {minutes}m
    """
    result = client.query(query)
    return list(result.get_points())


def compute_zscore(value, values):
    if len(values) < 5:
        return 0.0
    arr = np.array(values)
    std = arr.std()
    if std == 0:
        return 0.0
    return abs(value - arr.mean()) / std


def detect_anomalies(points):
    """Détection par z-score sur chaque métrique connue, par IMSI."""
    anomalies = []
    metric_fields = [
        "event_measurementsForVfScalingFields_additionalObjects_1_objectInstances_0_objectInstance_value",
    ]
    for point in points:
        imsi = point.get("event_commonEventHeader_sourceName", "unknown")
        for field, value in point.items():
            if not isinstance(value, (int, float)):
                continue
            key = (imsi, field)
            hist = history[key]
            z = compute_zscore(value, list(hist))
            hist.append(value)
            if z > Z_SCORE_THRESHOLD:
                anomalies.append({
                    "imsi": imsi,
                    "field": field,
                    "value": value,
                    "z_score": round(z, 2),
                    "detected_at": time.time(),
                })
    return anomalies


def detect_multivariate_anomalies(points):
    """Isolation Forest sur l'ensemble des UE du cycle courant (vue globale)."""
    numeric_rows = []
    imsis = []
    for point in points:
        row = [v for k, v in point.items() if isinstance(v, (int, float))]
        if len(row) >= 2:
            numeric_rows.append(row)
            imsis.append(point.get("event_commonEventHeader_sourceName", "unknown"))

    if len(numeric_rows) < 10:
        return []

    max_len = max(len(r) for r in numeric_rows)
    padded = [r + [0] * (max_len - len(r)) for r in numeric_rows]

    clf = IsolationForest(contamination=0.05, random_state=42)
    preds = clf.fit_predict(padded)

    anomalies = []
    for imsi, pred in zip(imsis, preds):
        if pred == -1:
            anomalies.append({"imsi": imsi, "type": "multivariate_outlier", "detected_at": time.time()})
    return anomalies


def main_loop():
    logger.info("Démarrage du détecteur d'anomalies")
    influx_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DB)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    while True:
        try:
            points = fetch_recent_metrics(influx_client)
            logger.info(f"{len(points)} points récupérés depuis InfluxDB")

            anomalies = detect_anomalies(points) + detect_multivariate_anomalies(points)

            for anomaly in anomalies:
                logger.warning(f"Anomalie détectée: {anomaly}")
                producer.send(ANOMALY_TOPIC, value=anomaly)

            if not anomalies:
                logger.info("Aucune anomalie détectée dans ce cycle")

        except Exception as e:
            logger.error(f"Erreur pendant le cycle de détection: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main_loop()
