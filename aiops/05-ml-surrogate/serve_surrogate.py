"""
Service d'inférence léger exposant le modèle de substitution entraîné par
train_surrogate.py. À appeler par le rApp entre deux exécutions complètes
de CPLEX pour obtenir une décision approchée en quelques millisecondes.

Usage recommandé: le rApp appelle CPLEX toutes les N minutes (ex: 60) pour
recalibrer, et interroge ce service pour les cycles intermédiaires plus
fréquents où la charge n'a pas assez varié pour justifier une réoptimisation
complète.
"""
import logging
import os

import joblib
import numpy as np
from flask import Flask, jsonify, request

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("surrogate-server")

MODEL_PATH = os.environ.get("MODEL_OUTPUT_PATH", "/data/surrogate_model.joblib")

app = Flask(__name__)
_models = None


def load_models():
    global _models
    if _models is None:
        if not os.path.exists(MODEL_PATH):
            raise FileNotFoundError(f"Modèle introuvable à {MODEL_PATH} — lancez d'abord train_surrogate.py")
        _models = joblib.load(MODEL_PATH)
        logger.info(f"Modèle chargé depuis {MODEL_PATH} (MAE objectif: {_models['objective_mae']:.4f})")
    return _models


@app.route("/health", methods=["GET"])
def health():
    try:
        load_models()
        return jsonify({"status": "OK"}), 200
    except FileNotFoundError as e:
        return jsonify({"status": "NO_MODEL", "detail": str(e)}), 503


@app.route("/predict", methods=["POST"])
def predict():
    """
    Corps attendu:
    {
      "n_ues": 256, "n_active_gnb": 4,
      "avg_sinr": 12.3, "avg_demand_mbps": 14.2,
      "per_gnb_ue_count": [64, 68, 60, 64]
    }
    """
    try:
        models = load_models()
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 503

    data = request.get_json()
    try:
        features = np.array([[
            data["n_ues"],
            data["n_active_gnb"],
            data["avg_sinr"],
            data["avg_demand_mbps"],
        ] + data["per_gnb_ue_count"]])
    except KeyError as e:
        return jsonify({"error": f"Champ manquant: {e}"}), 400

    predicted_power = models["power_model"].predict(features)[0].tolist()
    predicted_objective = float(models["objective_model"].predict(features)[0])

    return jsonify({
        "predicted_gnb_power": predicted_power,
        "predicted_objective_value": predicted_objective,
        "confidence_note": (
            f"Approximation ML (MAE historique: puissance={models['power_mae']:.3f}, "
            f"objectif={models['objective_mae']:.3f}). Ne remplace pas une réoptimisation "
            "CPLEX périodique complète."
        ),
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
