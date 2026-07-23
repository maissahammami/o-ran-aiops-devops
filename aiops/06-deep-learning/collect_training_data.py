"""
Collecte en continu les features nécessaires à l'entraînement du prédicteur
de panne (train_failure_predictor.py), en observant l'état réel du cluster
via kubectl. Écrit un enregistrement JSONL toutes les minutes, et complète
rétroactivement le label `failed_within_10min` une fois la fenêtre de 10
minutes écoulée (technique de "labeling différé").

À faire tourner en continu pendant plusieurs jours pour accumuler un
historique exploitable par train_failure_predictor.py.
"""
import json
import logging
import os
import re
import subprocess
import time
from collections import deque

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("failure-data-collector")

OUTPUT_PATH = os.environ.get("FAILURE_TRAINING_DATA_PATH", "/data/failure_history.jsonl")
WATCHED_NAMESPACES = os.environ.get("WATCHED_NAMESPACES", "ricplt,ricrapp,ricxapp,smo,nonrtric,kafka").split(",")
COLLECT_INTERVAL_SECONDS = int(os.environ.get("COLLECT_INTERVAL_SECONDS", "60"))
LABEL_WINDOW_SECONDS = 600  # 10 minutes


def get_node_usage():
    """Parse 'kubectl top nodes' pour extraire %CPU et %MEM du nœud minikube."""
    result = subprocess.run(["kubectl", "top", "nodes", "--no-headers"], capture_output=True, text=True)
    line = result.stdout.strip().splitlines()
    if not line:
        return 0.0, 0.0
    parts = line[0].split()
    cpu_percent = float(parts[2].rstrip("%")) if len(parts) > 2 else 0.0
    mem_percent = float(parts[4].rstrip("%")) if len(parts) > 4 else 0.0
    return cpu_percent, mem_percent


def get_pods_snapshot(namespaces):
    """Retourne la liste (namespace, pod, restart_count, age_minutes) pour tous les pods surveillés."""
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


def collect_loop():
    logger.info(f"Collecteur de données de panne démarré — surveillance de {WATCHED_NAMESPACES}")
    pending_records = deque()  # (timestamp_creation, record_dict, pod_key, restart_count_at_creation)

    while True:
        now = time.time()
        cpu_pct, mem_pct = get_node_usage()
        pods = get_pods_snapshot(WATCHED_NAMESPACES)

        recent_restarts = sum(1 for p in pods if p["restarts"] > 0)  # proxy simple, affiné ci-dessous

        for pod in pods:
            key = f"{pod['namespace']}/{pod['pod']}"
            record = {
                "node_cpu_percent": cpu_pct,
                "node_memory_percent": mem_pct,
                "pod_restart_count_last_hour": pod["restarts"],  # approximation: total restarts (proxy)
                "pod_age_minutes": 0,  # non disponible facilement sans kubectl get -o json; laissé à 0 (feature faible)
                "n_pods_restarted_last_10min": recent_restarts,
                "failed_within_10min": None,  # à compléter rétroactivement
            }
            pending_records.append((now, record, key, pod["restarts"]))

        # Compléter les enregistrements dont la fenêtre de 10 min est écoulée
        finalized = []
        still_pending = deque()
        current_pods_by_key = {f"{p['namespace']}/{p['pod']}": p for p in pods}

        while pending_records:
            ts, record, key, restarts_then = pending_records.popleft()
            if now - ts >= LABEL_WINDOW_SECONDS:
                current = current_pods_by_key.get(key)
                failed = 1 if (current and (current["restarts"] > restarts_then or current["status"] in
                               ("CrashLoopBackOff", "Error"))) else 0
                record["failed_within_10min"] = failed
                finalized.append(record)
            else:
                still_pending.append((ts, record, key, restarts_then))

        pending_records = still_pending

        if finalized:
            with open(OUTPUT_PATH, "a") as f:
                for r in finalized:
                    f.write(json.dumps(r) + "\n")
            logger.info(f"{len(finalized)} enregistrement(s) finalisé(s) et écrits dans {OUTPUT_PATH}")

        time.sleep(COLLECT_INTERVAL_SECONDS)


if __name__ == "__main__":
    collect_loop()
