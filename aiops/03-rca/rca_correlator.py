"""
Root Cause Analysis (RCA) automatisée pour la chaîne
envman -> e2sim -> xApps -> VES -> Kafka -> InfluxDB -> rApp -> PMS.

Principe: interroge l'API Kubernetes pour lister les pods en échec
(CrashLoopBackOff, Error, restarts récents), récupère leurs derniers logs,
applique une bibliothèque de patterns connus (ceux rencontrés pendant le
débogage manuel de ce projet) et propose un diagnostic + une commande de
correction suggérée.

Ce module encode explicitement les leçons apprises pendant les sessions de
débogage manuel (préfixe /v1 manquant, mauvais hostname Kafka, champs VES
manquants, RIC/controller inexistant, etc.) pour ne pas avoir à refaire
cette investigation à la main la prochaine fois.
"""
import logging
import re
import subprocess
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("rca-correlator")

# Bibliothèque de patterns connus: (regex sur les logs) -> (diagnostic, remédiation suggérée)
KNOWN_PATTERNS = [
    (
        re.compile(r"The requested method does not exist"),
        "Route REST non trouvée — préfixe d'API manquant (ex: /v1) dans la config du client",
        "Vérifier le préfixe de base de l'API REST cible (ex: tester /v1/<route>)",
    ),
    (
        re.compile(r"Failed to resolve '([\w\.\-]+)'"),
        "Hostname Kafka/service introuvable en DNS — mauvais nom de service dans la config",
        "Vérifier `kubectl get svc -A | grep <nom>` et corriger le ConfigMap concerné",
    ),
    (
        re.compile(r"KeyError: 'eventName'|KeyError: 'lastEpochMicrosec'"),
        "Champ VES obligatoire manquant dans l'événement produit (commonEventHeader incomplet)",
        "Ajouter eventName et lastEpochMicrosec au producteur d'événements VES",
    ),
    (
        re.compile(r"Near-RT RIC not found"),
        "ric_id de la policy A1 ne correspond à aucun RIC AVAILABLE dans le PMS",
        "Vérifier `curl .../a1-policy/v2/rics` et aligner ric_id sur un RIC AVAILABLE avec le bon policytype",
    ),
    (
        re.compile(r"Subscribed topic not available.*Unknown topic or partition"),
        "Topic Kafka référencé en config mais jamais créé sur le broker",
        "Créer le topic manquant via kafka-topics.sh --create",
    ),
    (
        re.compile(r"unrecognized option '--\w+'"),
        "Image Docker incompatible avec les arguments attendus (mauvaise version de tag)",
        "Vérifier les tags disponibles sur le registre et tester --help sur le binaire concerné",
    ),
    (
        re.compile(r"Connection refused"),
        "Service cible probablement en redémarrage au moment de l'appel (condition transitoire)",
        "Réessayer après quelques secondes; si persistant, vérifier l'état du pod cible",
    ),
]


def get_unhealthy_pods(namespaces=None):
    """Liste les pods en CrashLoopBackOff/Error dans les namespaces surveillés."""
    cmd = ["kubectl", "get", "pods", "-A", "--no-headers"]
    output = subprocess.run(cmd, capture_output=True, text=True).stdout
    unhealthy = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        ns, name, ready, status = parts[0], parts[1], parts[2], parts[3]
        if namespaces and ns not in namespaces:
            continue
        if status in ("CrashLoopBackOff", "Error", "ImagePullBackOff", "RunContainerError"):
            unhealthy.append((ns, name, status))
    return unhealthy


def get_pod_logs(namespace, pod_name, tail=200, previous=False):
    cmd = ["kubectl", "logs", "-n", namespace, pod_name, f"--tail={tail}"]
    if previous:
        cmd.append("--previous")
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout + result.stderr


def diagnose(logs_text):
    """Applique la bibliothèque de patterns connus au texte de logs."""
    findings = []
    for pattern, diagnosis, remediation in KNOWN_PATTERNS:
        match = pattern.search(logs_text)
        if match:
            findings.append({
                "diagnosis": diagnosis,
                "remediation": remediation,
                "matched_text": match.group(0)[:200],
            })
    return findings


def run_rca_cycle(watched_namespaces):
    unhealthy = get_unhealthy_pods(watched_namespaces)
    if not unhealthy:
        logger.info("Aucun pod en échec détecté — rien à diagnostiquer")
        return

    for ns, pod_name, status in unhealthy:
        logger.warning(f"Pod en échec: {ns}/{pod_name} ({status})")
        logs = get_pod_logs(ns, pod_name, previous=(status == "CrashLoopBackOff"))
        findings = diagnose(logs)

        if findings:
            for f in findings:
                logger.warning(
                    f"  [RCA] Diagnostic probable: {f['diagnosis']}\n"
                    f"        Remédiation suggérée: {f['remediation']}\n"
                    f"        Extrait log: {f['matched_text']}"
                )
        else:
            logger.info(f"  [RCA] Aucun pattern connu ne correspond — investigation manuelle nécessaire")


if __name__ == "__main__":
    import os
    watched = os.environ.get("WATCHED_NAMESPACES", "ricplt,ricrapp,ricxapp,smo,nonrtric,kafka").split(",")
    interval = int(os.environ.get("RCA_INTERVAL_SECONDS", "60"))
    logger.info(f"RCA Correlator démarré — surveillance de: {watched}")
    while True:
        run_rca_cycle(watched)
        time.sleep(interval)
