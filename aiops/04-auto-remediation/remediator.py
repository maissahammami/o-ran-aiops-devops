"""
Auto-remédiation (self-healing) pour les pannes connues du pipeline.

Étend le RCA Correlator (module 03) en passant à l'action pour un
sous-ensemble de pannes dont la remédiation est sûre à automatiser
(idempotente, sans risque de perte de données). Toute action est loguée
avant exécution pour garder une trace auditable.

Actions automatisées prudentes:
- Consumer Kafka bloqué sur un vieux message invalide -> ne PAS purger
  automatiquement (destructif), seulement alerter (voir REMEDIATIONS).
- Pod en CrashLoopBackOff dont les logs matchent un pattern connu ET
  dont on sait que "delete pod" suffit (ex: état transitoire réseau) ->
  redémarrage ciblé.
- Consumer group avec un lag anormalement élevé -> alerte uniquement
  (pas d'action automatique, risque de perte de données).
"""
import logging
import re
import subprocess
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("auto-remediator")

DRY_RUN = True  # mettre à False uniquement après validation en environnement de test


def run_kubectl(args):
    logger.info(f"[{'DRY-RUN' if DRY_RUN else 'EXEC'}] kubectl {' '.join(args)}")
    if DRY_RUN:
        return "(dry-run, commande non exécutée)"
    result = subprocess.run(["kubectl"] + args, capture_output=True, text=True)
    return result.stdout + result.stderr


def restart_pod(namespace, pod_name):
    """Redémarrage ciblé — sûr car Kubernetes recrée le pod automatiquement (Deployment/StatefulSet)."""
    return run_kubectl(["delete", "pod", "-n", namespace, pod_name])


def restart_deployment(namespace, deployment_name):
    return run_kubectl(["rollout", "restart", "deployment", deployment_name, "-n", namespace])


# Chaque règle: (pattern de log, condition namespace/nom optionnelle, action, description)
def recreate_policy_type_5_on_osc_ric(ric_service, policytype_id, schema_path):
    """
    Recrée un policy type sur un simulateur A1 OSC (ex: a1-sim-osc-0) après
    qu'il ait perdu sa définition suite à un redémarrage (stockage en
    mémoire, non persisté côté simulateur).
    """
    logger.warning(f"Recréation du policy type {policytype_id} sur {ric_service} (port-forward local requis)")
    # Le port-forward doit être géré en amont (ou remplacé par un appel direct
    # au ClusterIP du service depuis l'intérieur du cluster si ce script tourne en pod).
    cmd = [
        "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
        "-X", "PUT", f"http://{ric_service}:8085/policytype?id={policytype_id}",
        "-H", "Content-Type: application/json",
        "-d", f"@{schema_path}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info(f"Recréation policy type — code HTTP: {result.stdout.strip()}")
    return result.stdout.strip() in ("200", "201")


def restart_pms_to_resync(namespace="nonrtric", pms_pod="policymanagementservice-0"):
    """Force la resynchronisation du PMS (parfois nécessaire en plus du délai normal)."""
    logger.warning("Redémarrage forcé du PolicyManagementService pour resynchroniser les policy types")
    return restart_pod(namespace, pms_pod)


REMEDIATION_RULES = [
    {
        "pattern": re.compile(r"Failed to resolve '.*kafka.*'"),
        "action": lambda ns, pod, deploy: restart_deployment(ns, deploy) if deploy else None,
        "description": "Résolution DNS Kafka échouée — souvent transitoire au démarrage, un restart aide",
        "requires_deployment_name": True,
    },
    {
        "pattern": re.compile(r"ConnectionRefusedError|Connection refused"),
        "action": lambda ns, pod, deploy: None,  # transitoire, on n'agit pas, on logue seulement
        "description": "Connexion refusée — probablement transitoire (pod cible en redémarrage), pas d'action",
        "requires_deployment_name": False,
    },
    {
        # Détecté dans les logs du rApp (namespace ricrapp), pas dans un pod en CrashLoopBackOff.
        # Nécessite un chemin de détection séparé — voir check_rapp_policy_errors() ci-dessous.
        "pattern": re.compile(r"not supported by RIC"),
        "action": lambda ns, pod, deploy: None,  # géré par check_rapp_policy_errors, pas ce cycle générique
        "description": "Policy type manquant sur le RIC cible (perdu après redémarrage du simulateur A1)",
        "requires_deployment_name": False,
    },
]


def check_rapp_policy_errors(rapp_namespace="ricrapp", rapp_label="app=energy-saver-rapp",
                              ric_service="a1-sim-osc-0.nonrtric", policytype_id="5",
                              schema_path="/config/E2nodeUESchema.json"):
    """
    Vérifie spécifiquement les logs du rApp pour l'erreur 'not supported by RIC'
    (policy type perdu côté simulateur A1) et déclenche la remédiation dédiée:
    1. Recréer le policy type sur le simulateur A1
    2. Redémarrer le PMS pour forcer la resynchronisation
    """
    cmd = ["kubectl", "logs", "-n", rapp_namespace, "-l", rapp_label, "--tail=50"]
    logs = subprocess.run(cmd, capture_output=True, text=True).stdout

    if "not supported by RIC" in logs:
        logger.warning("Détecté: policy type manquant sur le RIC cible — lancement de la remédiation dédiée")
        if not DRY_RUN:
            recreate_policy_type_5_on_osc_ric(ric_service, policytype_id, schema_path)
            time.sleep(65)  # laisser le temps au cycle normal de synchronisation
            restart_pms_to_resync()
        else:
            logger.info(
                "[DRY-RUN] Aurait recréé le policy type via curl PUT sur le simulateur A1, "
                "puis redémarré policymanagementservice-0"
            )
        return True
    return False


def get_crashlooping_pods(namespaces):
    cmd = ["kubectl", "get", "pods", "-A", "--no-headers"]
    output = subprocess.run(cmd, capture_output=True, text=True).stdout
    pods = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        ns, name, ready, status = parts[0], parts[1], parts[2], parts[3]
        if namespaces and ns not in namespaces:
            continue
        if status == "CrashLoopBackOff":
            pods.append((ns, name))
    return pods


def get_deployment_name_for_pod(namespace, pod_name):
    """Déduit le nom du Deployment depuis le nom du pod (heuristique: retire les 2 derniers suffixes de hash)."""
    parts = pod_name.split("-")
    if len(parts) >= 3:
        return "-".join(parts[:-2])
    return None


def get_pod_logs(namespace, pod_name, previous=True):
    cmd = ["kubectl", "logs", "-n", namespace, pod_name, "--tail=100"]
    if previous:
        cmd.append("--previous")
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout + result.stderr


def remediation_cycle(namespaces, action_cooldown_tracker):
    pods = get_crashlooping_pods(namespaces)
    now = time.time()

    for ns, pod_name in pods:
        cooldown_key = f"{ns}/{pod_name}"
        last_action = action_cooldown_tracker.get(cooldown_key, 0)
        if now - last_action < 300:  # évite de spammer des actions toutes les secondes
            continue

        logs = get_pod_logs(ns, pod_name)
        for rule in REMEDIATION_RULES:
            if rule["pattern"].search(logs):
                deploy_name = get_deployment_name_for_pod(ns, pod_name) if rule["requires_deployment_name"] else None
                logger.warning(f"Règle déclenchée pour {ns}/{pod_name}: {rule['description']}")
                result = rule["action"](ns, pod_name, deploy_name)
                if result:
                    logger.info(f"Résultat action: {result}")
                action_cooldown_tracker[cooldown_key] = now
                break


if __name__ == "__main__":
    import os
    watched = os.environ.get("WATCHED_NAMESPACES", "ricplt,ricrapp,ricxapp,smo,nonrtric,kafka").split(",")
    interval = int(os.environ.get("REMEDIATION_INTERVAL_SECONDS", "60"))
    DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"
    RIC_SERVICE = os.environ.get("RIC_SERVICE_FOR_POLICY", "a1-sim-osc-0.nonrtric")
    POLICYTYPE_ID = os.environ.get("POLICYTYPE_ID", "5")
    SCHEMA_PATH = os.environ.get("POLICY_SCHEMA_PATH", "/config/E2nodeUESchema.json")

    logger.info(f"Auto-remediator démarré (DRY_RUN={DRY_RUN}) — surveillance de: {watched}")
    tracker = {}
    policy_check_cooldown = 0
    while True:
        remediation_cycle(watched, tracker)

        # Vérification dédiée: policy type perdu sur le RIC (cooldown 5 min)
        now = time.time()
        if now - policy_check_cooldown > 300:
            if check_rapp_policy_errors(ric_service=RIC_SERVICE, policytype_id=POLICYTYPE_ID, schema_path=SCHEMA_PATH):
                policy_check_cooldown = now

        time.sleep(interval)
