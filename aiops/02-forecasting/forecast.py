"""
Prévision de charge (nombre d'UE actifs / demande agrégée) à partir de
l'historique InfluxDB, pour permettre au rApp de pré-activer des gNB
avant le pic de charge plutôt que de réagir après coup.

Modèle volontairement simple: moyenne mobile pondérée + tendance linéaire
sur les N dernières fenêtres. Suffisant pour un signal "charge va monter/
descendre dans les 15 prochaines minutes" sans dépendance lourde (pas de
Prophet/statsmodels pour rester léger sur un pod modeste).
"""
import json
import logging
import os
import time

import numpy as np
from influxdb import InfluxDBClient
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("load-forecaster")

INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST", "influxdb.smo.svc.cluster.local")
INFLUXDB_PORT = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUXDB_DB = os.environ.get("INFLUXDB_DB", "smo")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "nonrtric-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
FORECAST_TOPIC = os.environ.get("FORECAST_TOPIC", "load-forecast")
HISTORY_WINDOWS = int(os.environ.get("HISTORY_WINDOWS", "12"))  # fenêtres de 5min = 1h d'historique
WINDOW_MINUTES = int(os.environ.get("WINDOW_MINUTES", "5"))
FORECAST_HORIZON_MINUTES = int(os.environ.get("FORECAST_HORIZON_MINUTES", "15"))
RUN_INTERVAL_SECONDS = int(os.environ.get("RUN_INTERVAL_SECONDS", "300"))


def fetch_ue_count_series(client):
    """Compte le nombre d'UE distincts vus par fenêtre de temps sur l'historique."""
    counts = []
    for i in range(HISTORY_WINDOWS, 0, -1):
        start = f"now() - {i * WINDOW_MINUTES}m"
        end = f"now() - {(i - 1) * WINDOW_MINUTES}m"
        query = f"""
            SELECT COUNT(DISTINCT("event_commonEventHeader_sourceName"))
            FROM "ues"
            WHERE time > {start} AND time <= {end}
        """
        result = client.query(query)
        points = list(result.get_points())
        count = points[0].get("count", 0) if points else 0
        counts.append(count)
    return counts


def linear_trend_forecast(series, horizon_windows):
    """Régression linéaire simple + moyenne mobile pondérée pour lisser le bruit."""
    if len(series) < 3:
        return series[-1] if series else 0

    x = np.arange(len(series))
    y = np.array(series, dtype=float)

    # Pondération: les points récents comptent plus (poids linéaire croissant)
    weights = np.linspace(0.5, 1.5, len(series))
    coeffs = np.polyfit(x, y, deg=1, w=weights)
    slope, intercept = coeffs[0], coeffs[1]

    forecast_x = len(series) + horizon_windows - 1
    forecast_value = slope * forecast_x + intercept
    return max(0, round(forecast_value))


def classify_trend(current, forecast):
    if forecast > current * 1.15:
        return "RISING"
    elif forecast < current * 0.85:
        return "FALLING"
    return "STABLE"


def main_loop():
    logger.info("Démarrage du module de prévision de charge")
    influx_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DB)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    while True:
        try:
            series = fetch_ue_count_series(influx_client)
            logger.info(f"Série historique UE (par fenêtre {WINDOW_MINUTES}min): {series}")

            horizon_windows = max(1, FORECAST_HORIZON_MINUTES // WINDOW_MINUTES)
            forecast = linear_trend_forecast(series, horizon_windows)
            current = series[-1] if series else 0
            trend = classify_trend(current, forecast)

            result = {
                "current_ue_count": current,
                "forecast_ue_count": forecast,
                "horizon_minutes": FORECAST_HORIZON_MINUTES,
                "trend": trend,
                "timestamp": time.time(),
            }
            logger.info(f"Prévision: {result}")
            producer.send(FORECAST_TOPIC, value=result)

            if trend == "RISING":
                logger.warning(
                    f"Charge en hausse prévue (+{forecast - current} UE d'ici {FORECAST_HORIZON_MINUTES}min) "
                    "— suggestion: pré-activer des gNB supplémentaires"
                )

        except Exception as e:
            logger.error(f"Erreur pendant la prévision: {e}")

        time.sleep(RUN_INTERVAL_SECONDS)


def run_once():
    influx_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DB)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    series = fetch_ue_count_series(influx_client)
    horizon_windows = max(1, FORECAST_HORIZON_MINUTES // WINDOW_MINUTES)
    forecast = linear_trend_forecast(series, horizon_windows)
    current = series[-1] if series else 0
    trend = classify_trend(current, forecast)
    result = {
        "current_ue_count": current,
        "forecast_ue_count": forecast,
        "horizon_minutes": FORECAST_HORIZON_MINUTES,
        "trend": trend,
        "timestamp": time.time(),
    }
    logger.info(f"Prévision (run unique): {result}")
    producer.send(FORECAST_TOPIC, value=result)
    producer.flush()


if __name__ == "__main__":
    import sys
    if "--once" in sys.argv:
        run_once()
    else:
        main_loop()
