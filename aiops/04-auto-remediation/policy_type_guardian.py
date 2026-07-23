"""
Gardien proactif du policy type A1 sur le(s) simulateur(s) OSC.

Contrairement au module auto-remediation (04) qui réagit après avoir vu une
erreur "not supported by RIC" dans les logs du rApp, ce module vérifie
directement et périodiquement l'état réel du policy type sur le simulateur
et le PMS, et le recrée AVANT que le rApp n'échoue à poster sa policy.

Cause du problème récurrent: le simulateur a1-sim-osc (image nexus3.o-ran-sc)
stocke ses policy types uniquement en mémoire process — tout redémarrage du
pod (OOM, node pressure, upgrade) efface silencieusement le type, sans que
Kubernetes ne le signale comme une panne (le pod redevient Ready normalement).
"""
import json
import logging
import os
import subprocess
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("policy-type-guardian")

RIC_SIMULATOR_HOST = os.environ.get("RIC_SIMULATOR_HOST", "a1-sim-osc-0.nonrtric")
RIC_SIMULATOR_PORT = os.environ.get("RIC_SIMULATOR_PORT", "8085")
PMS_HOST = os.environ.get("PMS_HOST", "policymanagementservice.nonrtric")
PMS_PORT = os.environ.get("PMS_PORT", "8081")
RIC_ID = os.environ.get("RIC_ID", "ric1")
POLICYTYPE_ID = os.environ.get("POLICYTYPE_ID", "5")
SCHEMA_PATH = os.environ.get("POLICY_SCHEMA_PATH", "/config/E2nodeUESchema.json")
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", "120"))


def curl_get(url):
    result = subprocess.run(["curl", "-s", url], capture_output=True, text=True)
    return result.stdout


def curl_put_json(url, json_file):
    result = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
         "-X", "PUT", url, "-H", "Content-Type: application/json", "-d", f"@{json_file}"],
        capture_output=True, text=True,
    )
    return result.stdout.strip()


def is_policytype_present_on_ric():
    """Interroge directement le PMS pour savoir si le RIC expose bien le type attendu."""
    url = f"http://{PMS_HOST}:{PMS_PORT}/a1-policy/v2/rics"
    body = curl_get(url)
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        logger.error(f"Réponse PMS non-JSON, PMS peut-être indisponible: {body[:200]}")
        return None  # état inconnu, ne pas agir à l'aveugle

    for ric in data.get("rics", []):
        if ric["ric_id"] == RIC_ID:
            present = POLICYTYPE_ID in ric.get("policytype_ids", [])
            logger.info(f"{RIC_ID}: state={ric['state']}, policytype_ids={ric['policytype_ids']}, "
                        f"type {POLICYTYPE_ID} présent={present}")
            return present

    logger.warning(f"{RIC_ID} introuvable dans la réponse du PMS")
    return None


def recreate_policytype():
    url = f"http://{RIC_SIMULATOR_HOST}:{RIC_SIMULATOR_PORT}/policytype?id={POLICYTYPE_ID}"
    logger.warning(f"Recréation du policy type {POLICYTYPE_ID} sur {RIC_SIMULATOR_HOST}")
    status = curl_put_json(url, SCHEMA_PATH)
    success = status in ("200", "201")
    logger.info(f"Recréation policy type — code HTTP: {status} ({'OK' if success else 'ÉCHEC'})")
    return success


def main_loop():
    logger.info(
        f"Gardien de policy type démarré — surveillance de {RIC_ID}/{POLICYTYPE_ID} "
        f"toutes les {CHECK_INTERVAL_SECONDS}s"
    )
    while True:
        present = is_policytype_present_on_ric()

        if present is False:
            logger.warning(f"Policy type {POLICYTYPE_ID} MANQUANT sur {RIC_ID} — recréation immédiate")
            if recreate_policytype():
                logger.info("Recréation réussie côté simulateur. "
                             "Le PMS synchronisera automatiquement au prochain cycle (~60s).")
            else:
                logger.error("Échec de la recréation — vérifier manuellement le simulateur A1")
        elif present is True:
            logger.debug(f"Policy type {POLICYTYPE_ID} présent sur {RIC_ID} — rien à faire")
        # present is None: état inconnu (PMS injoignable), on ne fait rien pour éviter d'agir à l'aveugle

        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main_loop()
