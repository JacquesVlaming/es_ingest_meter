#!/usr/bin/env bash
set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ID="${GCP_PROJECT_ID:?Please export GCP_PROJECT_ID=your-project-id}"
REGION="us-central1"
CLUSTER="jacques-vlaming-cluster"
REPO="es-ingest"
NAMESPACE="es-tools"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}"
TAG="${1:-latest}"

METER_IMAGE="${REGISTRY}/es-ingest-meter:${TAG}"
SCHEDULER_IMAGE="${REGISTRY}/es-ingest-scheduler:${TAG}"

echo "==> Project  : ${PROJECT_ID}"
echo "==> Registry : ${REGISTRY}"
echo "==> Tag      : ${TAG}"
echo ""

# ── Authenticate Docker to Artifact Registry ───────────────────────────────────
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

# ── Build images ───────────────────────────────────────────────────────────────
echo "==> Building es-ingest-meter..."
docker buildx build --platform linux/amd64 -f Dockerfile.meter -t "${METER_IMAGE}" --push .

echo "==> Building es-ingest-scheduler..."
docker buildx build --platform linux/amd64 -f Dockerfile.scheduler -t "${SCHEDULER_IMAGE}" --push .

# ── Push images ────────────────────────────────────────────────────────────────
echo "==> Pushing images..."
docker push "${METER_IMAGE}"
docker push "${SCHEDULER_IMAGE}"

# ── Patch image references in manifests ────────────────────────────────────────
sed -i.bak "s|us-central1-docker.pkg.dev/YOUR_PROJECT_ID|${REGISTRY%/*}|g" \
    k8s/cronjob-scheduler.yaml k8s/job-meter.yaml
rm -f k8s/cronjob-scheduler.yaml.bak k8s/job-meter.yaml.bak

# ── Get GKE credentials ────────────────────────────────────────────────────────
echo "==> Fetching GKE credentials..."
gcloud container clusters get-credentials "${CLUSTER}" --region="${REGION}" --project="${PROJECT_ID}"

# ── Create namespace ───────────────────────────────────────────────────────────
kubectl get namespace "${NAMESPACE}" &>/dev/null || kubectl create namespace "${NAMESPACE}"

# ── Apply manifests ────────────────────────────────────────────────────────────
echo "==> Applying ConfigMap..."
kubectl apply -f k8s/configmap.yaml

echo "==> Applying Secret..."
kubectl apply -f k8s/secret.yaml

echo "==> Applying CronJob..."
kubectl apply -f k8s/cronjob-scheduler.yaml

echo ""
echo "✓ Done."
echo ""
echo "Useful commands:"
echo "  Watch scheduler runs  :  kubectl get jobs -n ${NAMESPACE} -w"
echo "  Run meter now         :  kubectl delete job es-ingest-meter -n ${NAMESPACE} --ignore-not-found && kubectl apply -f k8s/job-meter.yaml"
echo "  Meter logs            :  kubectl logs -f job/es-ingest-meter -n ${NAMESPACE}"
echo "  Trigger scheduler now :  kubectl create job scheduler-manual-\$(date +%s) --from=cronjob/es-ingest-scheduler -n ${NAMESPACE}"
