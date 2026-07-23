"""
Entraînement d'un modèle de substitution (surrogate) pour approximer les
décisions du solveur CPLEX (admission UE + puissance gNB) sans repasser
par 60s d'optimisation à chaque cycle.

Idée: le rApp continue de tourner CPLEX périodiquement (ex: toutes les
heures) pour ré-calibrer un optimum de référence, mais entre deux, ce
modèle de substitution donne une réponse en millisecondes suffisamment
proche pour les décisions à haute fréquence (cf. le throttle de 180s déjà
en place — ce module permettrait de le réduire nettement).

Entrée:  historique des couples (features réseau -> solution CPLEX)
         accumulés dans un fichier JSONL au fil des exécutions du rApp.
Sortie:  un modèle scikit-learn (RandomForest) sérialisé, prêt à charger
         par le module d'inférence (serve_surrogate.py).

Ce script est un point de départ: la qualité de l'approximation dépend
du volume d'historique CPLEX accumulé (recommandé: plusieurs centaines
d'exécutions avant d'envisager de remplacer CPLEX en production).
"""
import json
import logging
import os

import joblib
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("surrogate-trainer")

TRAINING_DATA_PATH = os.environ.get("TRAINING_DATA_PATH", "/data/cplex_history.jsonl")
MODEL_OUTPUT_PATH = os.environ.get("MODEL_OUTPUT_PATH", "/data/surrogate_model.joblib")


def load_training_data(path):
    """
    Format attendu par ligne (JSONL), un enregistrement par exécution CPLEX:
    {
      "n_ues": 256, "n_active_gnb": 4,
      "avg_sinr": 12.3, "avg_demand_mbps": 14.2,
      "per_gnb_ue_count": [64, 68, 60, 64],
      "objective_value": 61.90927,
      "gnb_power": [1, 1, 1, 1]
    }
    """
    features, targets_power, targets_objective = [], [], []
    if not os.path.exists(path):
        logger.warning(f"Pas de données d'historique trouvées à {path} — entraînement impossible")
        return None, None, None

    with open(path) as f:
        for line in f:
            try:
                record = json.loads(line)
                feat = [
                    record["n_ues"],
                    record["n_active_gnb"],
                    record["avg_sinr"],
                    record["avg_demand_mbps"],
                ] + record["per_gnb_ue_count"]
                features.append(feat)
                targets_power.append(record["gnb_power"])
                targets_objective.append(record["objective_value"])
            except (KeyError, json.JSONDecodeError) as e:
                logger.warning(f"Ligne ignorée (format invalide): {e}")

    return np.array(features), np.array(targets_power), np.array(targets_objective)


def train_and_evaluate(X, y_power, y_objective):
    X_train, X_test, yp_train, yp_test, yo_train, yo_test = train_test_split(
        X, y_power, y_objective, test_size=0.2, random_state=42
    )

    power_model = RandomForestRegressor(n_estimators=200, max_depth=10, random_state=42)
    power_model.fit(X_train, yp_train)
    power_mae = mean_absolute_error(yp_test, power_model.predict(X_test))
    logger.info(f"MAE puissance gNB (test set): {power_mae:.4f}")

    objective_model = RandomForestRegressor(n_estimators=200, max_depth=10, random_state=42)
    objective_model.fit(X_train, yo_train)
    objective_mae = mean_absolute_error(yo_test, objective_model.predict(X_test))
    logger.info(f"MAE valeur objectif (test set): {objective_mae:.4f}")

    return {"power_model": power_model, "objective_model": objective_model,
            "power_mae": power_mae, "objective_mae": objective_mae}


def main():
    logger.info(f"Chargement des données d'entraînement depuis {TRAINING_DATA_PATH}")
    X, y_power, y_objective = load_training_data(TRAINING_DATA_PATH)

    if X is None or len(X) < 30:
        logger.error(
            "Pas assez de données pour entraîner un modèle fiable (minimum recommandé: 30 exécutions). "
            "Laissez le rApp tourner plus longtemps pour accumuler de l'historique CPLEX."
        )
        return

    logger.info(f"{len(X)} exemples d'entraînement chargés")
    models = train_and_evaluate(X, y_power, y_objective)

    joblib.dump(models, MODEL_OUTPUT_PATH)
    logger.info(f"Modèle de substitution sauvegardé dans {MODEL_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
