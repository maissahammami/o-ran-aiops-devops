"""
Prévision de charge par réseau de neurones récurrent (LSTM).

Améliore le module 02 (régression linéaire) en apprenant des motifs
temporels non-linéaires (cycles jour/nuit, pics d'usage) à partir de
l'historique InfluxDB du nombre d'UE actifs et de la demande agrégée.

Architecture: LSTM empilé (2 couches) -> Dense, entraîné sur des fenêtres
glissantes (sequence_length pas de temps -> horizon pas de temps).

Entrée d'entraînement: séries temporelles InfluxDB (mesure "ues"), ré-
échantillonnées en pas de 5 minutes. Nécessite plusieurs jours d'historique
pour capturer un cycle jour/nuit complet — avec peu de données, le modèle
retombera sur une prévision proche de la moyenne (comportement attendu et
sans danger, pas un bug).
"""
import logging
import os

import numpy as np
import tensorflow as tf
from influxdb import InfluxDBClient
from tensorflow.keras import layers, models

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("lstm-forecaster-train")

INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST", "influxdb.smo.svc.cluster.local")
INFLUXDB_PORT = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUXDB_DB = os.environ.get("INFLUXDB_DB", "smo")
SEQUENCE_LENGTH = int(os.environ.get("SEQUENCE_LENGTH", "24"))   # 24 x 5min = 2h de contexte
HORIZON = int(os.environ.get("HORIZON", "3"))                     # prédire 15 min à l'avance (3 x 5min)
MODEL_OUTPUT_PATH = os.environ.get("MODEL_OUTPUT_PATH", "/data/lstm_forecaster.keras")
EPOCHS = int(os.environ.get("EPOCHS", "50"))


def fetch_ue_count_timeseries(client, lookback_hours=72, bucket_minutes=5):
    """Récupère le nombre d'UE distincts par bucket de temps sur tout l'historique disponible."""
    n_buckets = int(lookback_hours * 60 / bucket_minutes)
    counts = []
    for i in range(n_buckets, 0, -1):
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


def build_sequences(series, seq_len, horizon):
    """Découpe la série en fenêtres (X: seq_len pas, y: valeur horizon pas plus tard)."""
    X, y = [], []
    for i in range(len(series) - seq_len - horizon + 1):
        X.append(series[i:i + seq_len])
        y.append(series[i + seq_len + horizon - 1])
    return np.array(X), np.array(y)


def build_model(seq_len):
    model = models.Sequential([
        layers.Input(shape=(seq_len, 1)),
        layers.LSTM(32, return_sequences=True),
        layers.LSTM(16),
        layers.Dense(8, activation="relu"),
        layers.Dense(1),
    ])
    model.compile(optimizer="adam", loss="mse", metrics=["mae"])
    return model


def main():
    logger.info("Récupération de l'historique InfluxDB pour l'entraînement LSTM")
    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DB)
    series = fetch_ue_count_timeseries(client)

    if len(series) < SEQUENCE_LENGTH + HORIZON + 20:
        logger.error(
            f"Historique insuffisant ({len(series)} points) pour entraîner un LSTM fiable. "
            f"Minimum recommandé: {SEQUENCE_LENGTH + HORIZON + 50} points "
            "(laissez le pipeline tourner plus longtemps, idéalement plusieurs jours)."
        )
        return

    # Normalisation min-max (sauvegardée pour l'inférence)
    series_min, series_max = series.min(), series.max()
    norm = (series - series_min) / max(1e-6, (series_max - series_min))

    X, y = build_sequences(norm, SEQUENCE_LENGTH, HORIZON)
    X = X.reshape((*X.shape, 1))

    split = int(len(X) * 0.85)
    X_train, X_val, y_train, y_val = X[:split], X[split:], y[:split], y[split:]

    model = build_model(SEQUENCE_LENGTH)
    logger.info(model.summary())

    history = model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val) if len(X_val) > 0 else None,
        epochs=EPOCHS, batch_size=16, verbose=2,
        callbacks=[tf.keras.callbacks.EarlyStopping(patience=8, restore_best_weights=True)],
    )

    model.save(MODEL_OUTPUT_PATH)
    # Sauvegarde des paramètres de normalisation à côté du modèle
    np.save(MODEL_OUTPUT_PATH.replace(".keras", "_norm.npy"), np.array([series_min, series_max]))

    final_mae = history.history.get("val_mae", history.history["mae"])[-1]
    logger.info(f"Modèle LSTM sauvegardé dans {MODEL_OUTPUT_PATH} — MAE final: {final_mae:.4f} (échelle normalisée)")


if __name__ == "__main__":
    main()
