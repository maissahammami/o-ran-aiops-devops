"""
Détection d'anomalies par autoencoder (deep learning), en complément du
z-score/Isolation Forest du module 01.

Principe: un autoencoder apprend à reconstruire les vecteurs de métriques
"normaux" (RSRP, RSRQ, SINR, RRC state). Une fois entraîné, toute mesure
que le modèle reconstruit mal (erreur de reconstruction élevée) est un
signal d'anomalie — utile pour détecter des combinaisons de métriques
inhabituelles qu'un simple seuil par variable ne verrait pas (ex: RSRP
normal mais SINR incohérent avec ce RSRP).

Avantage sur Isolation Forest: capture des corrélations non-linéaires
entre métriques. Inconvénient: nécessite plus de données d'entraînement
"normales" pour être fiable (recommandé: plusieurs milliers de points).
"""
import logging
import os

import numpy as np
from influxdb import InfluxDBClient
from tensorflow.keras import layers, models

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("autoencoder-train")

INFLUXDB_HOST = os.environ.get("INFLUXDB_HOST", "influxdb.smo.svc.cluster.local")
INFLUXDB_PORT = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUXDB_DB = os.environ.get("INFLUXDB_DB", "smo")
MODEL_OUTPUT_PATH = os.environ.get("AE_MODEL_PATH", "/data/autoencoder.keras")
THRESHOLD_OUTPUT_PATH = os.environ.get("AE_THRESHOLD_PATH", "/data/autoencoder_threshold.npy")
EPOCHS = int(os.environ.get("EPOCHS", "60"))

# Champs numériques exploités — RSRP/RSRQ/SINR/RRC state après flatten VES
FEATURE_FIELDS = [
    "event_measurementsForVfScalingFields_additionalObjects_0_objectInstances_0_objectInstance_value",  # rrc_state
    "event_measurementsForVfScalingFields_additionalObjects_1_objectInstances_0_objectInstance_value",  # rsrp
    "event_measurementsForVfScalingFields_additionalObjects_2_objectInstances_0_objectInstance_value",  # rsrq
    "event_measurementsForVfScalingFields_additionalObjects_3_objectInstances_0_objectInstance_value",  # sinr
]


def fetch_training_points(client, lookback_hours=72):
    query = f'SELECT * FROM "ues" WHERE time > now() - {lookback_hours}h'
    result = client.query(query)
    rows = []
    for point in result.get_points():
        row = [point.get(f, np.nan) for f in FEATURE_FIELDS]
        if not any(np.isnan(row)):
            rows.append(row)
    return np.array(rows, dtype=float)


def build_autoencoder(n_features):
    inputs = layers.Input(shape=(n_features,))
    x = layers.Dense(8, activation="relu")(inputs)
    x = layers.Dense(3, activation="relu")(x)  # goulot d'étranglement (compression)
    x = layers.Dense(8, activation="relu")(x)
    outputs = layers.Dense(n_features, activation="linear")(x)
    model = models.Model(inputs, outputs)
    model.compile(optimizer="adam", loss="mse")
    return model


def main():
    logger.info("Récupération des données d'entraînement InfluxDB pour l'autoencoder")
    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DB)
    data = fetch_training_points(client)

    if len(data) < 200:
        logger.error(
            f"Seulement {len(data)} points valides trouvés — minimum recommandé: 200+. "
            "Laissez le pipeline tourner plus longtemps avant d'entraîner ce modèle."
        )
        return

    # Normalisation par colonne
    mean, std = data.mean(axis=0), data.std(axis=0)
    std[std == 0] = 1.0
    norm_data = (data - mean) / std

    model = build_autoencoder(norm_data.shape[1])
    model.fit(
        norm_data, norm_data,
        epochs=EPOCHS, batch_size=32, validation_split=0.15, verbose=2,
        callbacks=[__import__("tensorflow").keras.callbacks.EarlyStopping(patience=10, restore_best_weights=True)],
    )

    # Seuil d'anomalie: 95e percentile de l'erreur de reconstruction sur les données d'entraînement
    reconstructed = model.predict(norm_data, verbose=0)
    mse_per_sample = np.mean((norm_data - reconstructed) ** 2, axis=1)
    threshold = float(np.percentile(mse_per_sample, 95))

    model.save(MODEL_OUTPUT_PATH)
    np.save(THRESHOLD_OUTPUT_PATH, np.array({"mean": mean, "std": std, "threshold": threshold}, dtype=object))

    logger.info(f"Autoencoder sauvegardé: {MODEL_OUTPUT_PATH} — seuil d'anomalie (MSE): {threshold:.5f}")


if __name__ == "__main__":
    main()
