"""
Prédicteur de panne de pod (deep learning) — anticipe un redémarrage/crash
avant qu'il ne survienne, en s'appuyant sur les patterns réellement observés
sur cette plateforme : redémarrages groupés liés à la pression de ressources
du nœud (CPU/mémoire), touchant en cascade e2sim, Kafka, et le Policy
Management Service.

Approche: réseau de neurones dense (MLP) classifiant, à partir de features
de charge du nœud/cluster à l'instant T, la probabilité qu'un pod donné
redémarre dans les N prochaines minutes. Entraîné sur un historique construit
à partir de `kubectl get events` et des métriques `kubectl top` collectées
en continu (voir collect_training_data.py).

Ce module ferme la boucle avec le module 04 (auto-remediation) : au lieu de
réagir après un CrashLoopBackOff, on peut déclencher une remédiation
préventive (ex: relâcher de la pression mémoire, redémarrer proprement un
pod fragile avant qu'il ne crashe de façon incontrôlée).
"""
import logging
import os

import numpy as np
from tensorflow.keras import layers, models

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("failure-predictor-train")

TRAINING_DATA_PATH = os.environ.get("FAILURE_TRAINING_DATA_PATH", "/data/failure_history.jsonl")
MODEL_OUTPUT_PATH = os.environ.get("FAILURE_MODEL_PATH", "/data/failure_predictor.keras")
EPOCHS = int(os.environ.get("EPOCHS", "80"))

# Features attendues par enregistrement (voir collect_training_data.py):
# node_cpu_percent, node_memory_percent, pod_restart_count_last_hour,
# pod_age_minutes, n_pods_restarted_last_10min (indicateur de redémarrage groupé)
FEATURE_NAMES = [
    "node_cpu_percent", "node_memory_percent", "pod_restart_count_last_hour",
    "pod_age_minutes", "n_pods_restarted_last_10min",
]


def load_training_data(path):
    """
    Format attendu par ligne (JSONL):
    {
      "node_cpu_percent": 78.5, "node_memory_percent": 91.2,
      "pod_restart_count_last_hour": 2, "pod_age_minutes": 340,
      "n_pods_restarted_last_10min": 3,
      "failed_within_10min": 1
    }
    """
    import json
    X, y = [], []
    if not os.path.exists(path):
        logger.warning(f"Pas d'historique trouvé à {path}")
        return None, None

    with open(path) as f:
        for line in f:
            try:
                record = json.loads(line)
                X.append([record[name] for name in FEATURE_NAMES])
                y.append(record["failed_within_10min"])
            except (KeyError, json.JSONDecodeError) as e:
                logger.warning(f"Ligne ignorée: {e}")

    return np.array(X, dtype=float), np.array(y, dtype=float)


def build_model(n_features):
    model = models.Sequential([
        layers.Input(shape=(n_features,)),
        layers.Dense(16, activation="relu"),
        layers.Dropout(0.2),
        layers.Dense(8, activation="relu"),
        layers.Dense(1, activation="sigmoid"),  # probabilité de panne
    ])
    model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy", "AUC"])
    return model


def main():
    X, y = load_training_data(TRAINING_DATA_PATH)

    if X is None or len(X) < 100:
        logger.error(
            f"Historique insuffisant ({0 if X is None else len(X)} exemples) pour entraîner un "
            "classifieur fiable. Minimum recommandé: 100+ exemples, idéalement équilibré entre "
            "pannes et non-pannes. Lancez collect_training_data.py en continu pendant plusieurs jours."
        )
        return

    # Normalisation
    mean, std = X.mean(axis=0), X.std(axis=0)
    std[std == 0] = 1.0
    X_norm = (X - mean) / std

    split = int(len(X) * 0.8)
    X_train, X_val = X_norm[:split], X_norm[split:]
    y_train, y_val = y[:split], y[split:]

    model = build_model(X.shape[1])
    model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val) if len(X_val) > 0 else None,
        epochs=EPOCHS, batch_size=16, verbose=2,
        class_weight={0: 1.0, 1: max(1.0, (y == 0).sum() / max(1, (y == 1).sum()))},  # rééquilibrage classes rares
        callbacks=[__import__("tensorflow").keras.callbacks.EarlyStopping(patience=12, restore_best_weights=True)],
    )

    model.save(MODEL_OUTPUT_PATH)
    np.save(MODEL_OUTPUT_PATH.replace(".keras", "_norm.npy"), np.array([mean, std]))
    logger.info(f"Modèle de prédiction de panne sauvegardé: {MODEL_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
