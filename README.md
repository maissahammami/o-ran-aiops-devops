# Energy-Saver — Addon AIOps & DevOps

Ce dossier ajoute 11 modules au projet Energy-Saver : 5 modules **AIOps**
(intelligence appliquée à l'exploitation du RAN) et 6 pratiques **DevOps**
(fiabilisation et industrialisation du déploiement). Chaque module est
autonome — activez ce qui vous intéresse sans dépendance obligatoire aux
autres.

## Structure

```
aiops-devops-addon/
├── aiops/
│   ├── 01-anomaly-detection/   # Détection d'anomalies RSRP/RSRQ/SINR (z-score + Isolation Forest)
│   ├── 02-forecasting/         # Prévision de charge UE (régression linéaire pondérée)
│   ├── 03-rca/                 # Root Cause Analysis automatisée (bibliothèque de patterns connus)
│   ├── 04-auto-remediation/    # Auto-remédiation prudente (DRY_RUN par défaut)
│   └── 05-ml-surrogate/        # Modèle de substitution pour approximer CPLEX
├── devops/
│   ├── 01-cicd/                # GitHub Actions: build/lint/push/test E2E
│   ├── 02-gitops/              # Manifests ArgoCD (sync auto + self-heal)
│   ├── 03-observability/       # Loki + dashboard Grafana unifié
│   ├── 04-helm-charts/         # Chart Helm paramétrable pour tous les modules AIOps
│   ├── 05-e2e-tests/           # Script de test end-to-end de tout le pipeline
│   └── 06-chaos/               # Expériences Chaos Mesh ciblant les fragilités connues
└── README.md
```

## Installation rapide (via Helm, recommandé)

```bash
cd devops/04-helm-charts
helm lint energy-saver-addon
helm install energy-saver-addon energy-saver-addon/ -n ricrapp \
  --set global.image.registry=ghcr.io/<votre-org>/energy-saver-tests
```

Activez/désactivez chaque module dans `values.yaml` (tout est `enabled: true/false`).
**`autoRemediation` et `mlSurrogate` sont désactivés par défaut** — à activer
volontairement après validation.

## Construire les images Docker (CI/CD)

Le workflow `.github/workflows/build-and-push.yml` build automatiquement
chaque module vers `ghcr.io` à chaque push sur `main`. Il faut :
1. Copier ce workflow dans `.github/workflows/` à la racine de votre dépôt Git
2. Adapter `IMAGE_PREFIX` et les chemins `context` si votre arborescence diffère

## Observabilité

```bash
helm repo add grafana https://grafana.github.io/helm-charts
helm install loki grafana/loki-stack -f devops/03-observability/loki-stack-values.yaml -n monitoring
kubectl apply -f devops/03-observability/grafana-datasource-loki.yaml
```

Importez ensuite `unified-dashboard.json` dans Grafana (Dashboards → Import).

## Tests E2E

```bash
chmod +x devops/05-e2e-tests/test_e2e_pipeline.sh
./devops/05-e2e-tests/test_e2e_pipeline.sh
```

Retourne un code de sortie non-zéro si un maillon du pipeline est cassé —
idéal en étape finale d'un pipeline CI/CD ou en vérification post-déploiement.

## Chaos Engineering

```bash
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace
kubectl apply -f devops/06-chaos/chaos-mesh-experiments.yaml
```

**Recommandation** : testez d'abord sur un cluster de dev, pas en production
directe — certaines expériences (coupure réseau Kafka, kill du PMS) sont
volontairement perturbatrices.

## Notes importantes

- **`auto-remediation`** tourne en `DRY_RUN=true` par défaut : il logue les
  actions qu'il *aurait* prises sans les exécuter. Ne passez `DRY_RUN=false`
  qu'après avoir observé plusieurs cycles de logs et validé la pertinence
  des règles pour votre environnement.
- **`ml-surrogate`** nécessite un historique d'exécutions CPLEX accumulé
  (recommandé : 30+ exécutions minimum, idéalement plusieurs centaines)
  avant de produire un modèle utile. Le rApp actuel n'exporte pas encore
  cet historique au format attendu (`cplex_history.jsonl`) — il faudra
  ajouter un export de ce format dans `model.py` du rApp (après chaque
  `msol = mdl.solve(...)`, sérialiser features + solution en JSONL).
- Les Dockerfiles des modules AIOps sont fonctionnels mais minimalistes ;
  ajustez les versions de dépendances (`requirements`) selon vos besoins de
  reproductibilité stricte (pinning de versions recommandé en production).
