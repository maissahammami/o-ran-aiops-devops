"""
Inférence continue du prédicteur de panne — surveille l'état actuel du
cluster et alerte (Kafka + logs) si la probabilité de panne d'un pod dans
les 10 prochaines minutes dépasse un seuil, AVANT que le CrashLoopBackOff
ne survienne réellement.
"""
import json
import logging
import os
import re
import subprocess
import time

import numpy as np
from influxdb import InfluxDBClient
from kafka import KafkaProducer
from tensorflow.keras.models import load_model

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("failure-predictor-serve")

INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST", "influxdb.smo.svc.cluster.local")
INFLUXDB_PORT = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUXDB_DB = os.environ.get("INFLUXDB_DB", "smo")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "nonrtric-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
ALERT_TOPIC = os.environ.get("FAILURE_ALERT_TOPIC", "failure-predictions")
MODEL_PATH = os.environ.get("FAILURE_MODEL_PATH", "/data/failure_predictor.keras")
WATCHED_NAMESPACES = os.environ.get("WATCHED_NAMESPACES", "ricplt,ricrapp,ricxapp,smo,nonrtric,kafka").split(",")
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", "60"))
ALERT_THRESHOLD = float(os.environ.get("FAILURE_ALERT_THRESHOLD", "0.7"))


def get_node_usage():
    result = subprocess.run(["kubectl", "top", "nodes", "--no-headers"], capture_output=True, text=True)
    line = result.stdout.strip().splitlines()
    if not line:
        return 0.0, 0.0
    parts = line[0].split()
    cpu_percent = float(parts[2].rstrip("%")) if len(parts) > 2 else 0.0
    mem_percent = float(parts[4].rstrip("%")) if len(parts) > 4 else 0.0
    return cpu_percent, mem_percent


def get_pods_snapshot(namespaces):
    cmd = ["kubectl", "get", "pods", "-A", "--no-headers"]
    output = subprocess.run(cmd, capture_output=True, text=True).stdout
    pods = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 5:
            continue
        ns, name, ready, status, restarts_raw = parts[0], parts[1], parts[2], parts[3], parts[4]
        if namespaces and ns not in namespaces:
            continue
        m = re.match(r"(\d+)", restarts_raw)
        restarts = int(m.group(1)) if m else 0
        pods.append({"namespace": ns, "pod": name, "status": status, "restarts": restarts})
    return pods


def main_loop():
    if not os.path.exists(MODEL_PATH):
        logger.error(f"Modèle introuvable à {MODEL_PATH} — lancez d'abord train_failure_predictor.py")
        return

    model = load_model(MODEL_PATH)
    mean, std = np.load(MODEL_PATH.replace(".keras", "_norm.npy"))
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    influx_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DB)

    logger.info(f"Prédicteur de panne démarré (seuil d'alerte: {ALERT_THRESHOLD})")

    while True:
        cpu_pct, mem_pct = get_node_usage()
        pods = get_pods_snapshot(WATCHED_NAMESPACES)
        recent_restarts = sum(1 for p in pods if p["restarts"] > 0)

        for pod in pods:
            features = np.array([[cpu_pct, mem_pct, pod["restarts"], 0, recent_restarts]], dtype=float)
            x = (features - mean) / std
            proba = float(model.predict(x, verbose=0)[0][0])

            if proba >= ALERT_THRESHOLD:
                alert = {
                    "namespace": pod["namespace"],
                    "pod": pod["pod"],
                    "failure_probability": round(proba, 3),
                    "node_cpu_percent": cpu_pct,
                    "node_memory_percent": mem_pct,
                    "detected_at": time.time(),
                }
                logger.warning(f"ALERTE PRÉVENTIVE — panne probable: {alert}")
                producer.send(ALERT_TOPIC, value=alert)
                try:
                    influx_client.write_points([{
                        "measurement": "failure_predictions",
                        "tags": {"namespace": alert["namespace"], "pod": alert["pod"]},
                        "time": int(alert["detected_at"] * 1e9),
                        "fields": {
                            "failure_probability": alert["failure_probability"],
                            "node_cpu_percent": alert["node_cpu_percent"],
                            "node_memory_percent": alert["node_memory_percent"],
                        },
                    }])
                except Exception as e:
                    logger.error(f"Échec écriture InfluxDB: {e}")

        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main_loop()
