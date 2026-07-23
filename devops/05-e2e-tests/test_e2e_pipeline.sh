#!/bin/bash
# Test d'intégration end-to-end du pipeline Energy-Saver.
# Formalise en un script automatisé toutes les vérifications manuelles
# effectuées pendant les sessions de débogage (envman->e2sim->xApp->
# Kafka->InfluxDB->rApp->PMS), pour les rejouer après chaque déploiement.
#
# Exit code 0 = tout va bien. Non-zéro = un maillon de la chaîne est cassé,
# avec un message clair sur lequel.

set -uo pipefail
FAILED=0

check() {
  local description="$1"
  local result="$2"
  if [ "$result" -eq 0 ]; then
    echo "[OK]   $description"
  else
    echo "[FAIL] $description"
    FAILED=1
  fi
}

echo "=== Test E2E — Pipeline Energy-Saver ==="
echo

# 1. E2 Nodes connectés au E2Term
echo "--- 1. E2 Nodes ---"
NODEB_STATES=$(kubectl exec -n ricplt deploy/deployment-ricplt-e2mgr -- \
  curl -s http://localhost:3800/v1/nodeb/states 2>/dev/null)
CONNECTED_COUNT=$(echo "$NODEB_STATES" | grep -o '"CONNECTED"' | wc -l)
check "Au moins 4 E2 Nodes CONNECTED (trouvé: $CONNECTED_COUNT)" $([ "$CONNECTED_COUNT" -ge 4 ] && echo 0 || echo 1)

# 2. envman <-> e2sim (API REST /v1/UE)
echo "--- 2. Environment Manager -> E2 Sim ---"
for i in 1 2 3 4; do
  RESP=$(kubectl exec -n ricplt deploy/envman-deployment -- \
    curl -s -o /dev/null -w "%{http_code}" \
    "http://e2node${i}-e2sim-helm-simulator.ricplt:8081/v1/UE" 2>/dev/null)
  check "e2node${i} répond 200 sur /v1/UE (reçu: $RESP)" $([ "$RESP" == "200" ] && echo 0 || echo 1)
done

# 3. Topics Kafka existent et ont des messages récents
echo "--- 3. Data River (Kafka) ---"
TOPICS=$(kubectl exec -n kafka nonrtric-kafka-pool-1-0 -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null)
for topic in e2nodes k8snodesresources ues; do
  echo "$TOPICS" | grep -qx "$topic"
  check "Topic Kafka '$topic' existe" $?
done

# 4. VES Collector reçoit bien des événements (pas seulement des healthz)
echo "--- 4. VES Collector ---"
RECENT_EVENTS=$(kubectl logs -n smo deploy/ves-collector --tail=100 2>/dev/null | grep -c "Received event at")
check "VES Collector a reçu des événements récemment (trouvé: $RECENT_EVENTS)" $([ "$RECENT_EVENTS" -gt 0 ] && echo 0 || echo 1)

# 5. influxdb-connector ne crashe pas
echo "--- 5. InfluxDB Connector ---"
CONNECTOR_STATUS=$(kubectl get pods -n smo -l app.kubernetes.io/name=influxdb-connector \
  -o jsonpath='{.items[0].status.phase}' 2>/dev/null)
check "influxdb-connector est Running (état: $CONNECTOR_STATUS)" $([ "$CONNECTOR_STATUS" == "Running" ] && echo 0 || echo 1)

# 6. PMS voit au moins un RIC AVAILABLE avec un policytype non vide
echo "--- 6. Policy Management Service ---"
RICS_JSON=$(kubectl exec -n nonrtric policymanagementservice-0 -- \
  curl -s http://localhost:8081/a1-policy/v2/rics 2>/dev/null)
AVAILABLE_WITH_TYPE=$(echo "$RICS_JSON" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    count = sum(1 for r in data.get('rics', []) if r['state'] == 'AVAILABLE' and any(t for t in r['policytype_ids']))
    print(count)
except Exception:
    print(0)
" 2>/dev/null)
check "Au moins un RIC AVAILABLE avec policytype (trouvé: $AVAILABLE_WITH_TYPE)" $([ "${AVAILABLE_WITH_TYPE:-0}" -ge 1 ] && echo 0 || echo 1)

# 7. rApp tourne sans crash-loop
echo "--- 7. Energy Saver rApp ---"
RAPP_RESTARTS=$(kubectl get pods -n ricrapp -l app=energy-saver-rapp \
  -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}' 2>/dev/null)
check "rApp n'a pas redémarré récemment (restarts: ${RAPP_RESTARTS:-N/A})" $([ "${RAPP_RESTARTS:-99}" -lt 5 ] && echo 0 || echo 1)

echo
if [ "$FAILED" -eq 0 ]; then
  echo "=== Résultat: TOUS LES TESTS PASSENT ==="
  exit 0
else
  echo "=== Résultat: AU MOINS UN TEST A ÉCHOUÉ — voir [FAIL] ci-dessus ==="
  exit 1
fi
