#!/bin/zsh
set -euo pipefail
# Uncomment for verbose debugging
# set -x

# Full e2e: build local Docker image, run operator in-cluster,
# load cert-manager images into kind, install via Helm through Tenant flow,
# then delete Tenant and verify cleanup.

CLEANED_UP=0

HOME_CLUSTER=home
REMOTE_CLUSTER=remote
# Operator namespace (where the chart is installed)
OP_NS=crypto-edge-operator
# Tenant namespace (where Tenant CRs live)
TENANT_NS=default
TENANT_NAME=e2e-full-acme
WORKSPACE_NS=e2e-full-acme-workspace
CERTM_REPO=https://charts.jetstack.io
CERTM_NAME=cert-manager
CERTM_VERSION=1.19.1

OP_IMAGE=crypto-edge-operator:dev
OP_CHART_PATH=charts/crypto-edge-operator
OP_RELEASE_NAME=crypto-edge-operator

log() { printf "[e2e-full] %s\n" "$*"; }

cleanup() {
  set +e
  # Prevent multiple concurrent cleanups on repeated signals
  [ "$CLEANED_UP" -eq 1 ] && return
  CLEANED_UP=1

  log "tearing down kind clusters"
  # Delete with timeout when available to avoid hanging
  if command -v timeout >/dev/null 2>&1; then
    timeout 60 kind delete cluster --name "$HOME_CLUSTER" 2>/dev/null || true
    timeout 60 kind delete cluster --name "$REMOTE_CLUSTER" 2>/dev/null || true
  else
    kind delete cluster --name "$HOME_CLUSTER" 2>/dev/null || true
    kind delete cluster --name "$REMOTE_CLUSTER" 2>/dev/null || true
  fi
}
# On Ctrl-C/TERM, run cleanup then exit immediately
trap 'cleanup; exit 130' INT TERM
# On normal exit, run cleanup once
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || { echo "docker is required"; exit 1; }
command -v kind >/dev/null 2>&1 || { echo "kind is required"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required"; exit 1; }
command -v yq >/dev/null 2>&1 || { echo "yq is required"; exit 1; }

log "creating kind clusters"
log "creating home cluster: $HOME_CLUSTER"
kind create cluster --name "$HOME_CLUSTER" || { log "failed to create home cluster"; kind get clusters || true; exit 1; }
log "home cluster created"
sleep 3
log "creating remote cluster: $REMOTE_CLUSTER"
kind create cluster --name "$REMOTE_CLUSTER" || { log "failed to create remote cluster"; kind get clusters || true; exit 1; }
log "remote cluster created"
log "current kind clusters:"; kind get clusters || true

log "extract kubeconfigs (external for host, internal for pods)"
kind get kubeconfig --name "$HOME_CLUSTER" > /tmp/home-kind.kubeconfig
kind get kubeconfig --name "$REMOTE_CLUSTER" > /tmp/remote-kind.kubeconfig
# Internal kubeconfig uses cluster-internal IP/hostname so pods can reach the API
kind get kubeconfig --name "$REMOTE_CLUSTER" --internal > /tmp/remote-kind.internal.kubeconfig

log "wait for home cluster API health"
for i in {1..30}; do
  kubectl --kubeconfig /tmp/home-kind.kubeconfig get --raw=/healthz >/dev/null 2>&1 && break
  sleep 2
  [ $i -eq 30 ] && { log "home cluster API not ready"; exit 1; }
done
log "home cluster API ready"

log "wait for remote cluster API health"
for i in {1..30}; do
  kubectl --kubeconfig /tmp/remote-kind.kubeconfig get --raw=/healthz >/dev/null 2>&1 && break
  sleep 2
  [ $i -eq 30 ] && { log "remote cluster API not ready"; exit 1; }
done
log "remote cluster API ready"

log "kube contexts available:"
kubectl config get-contexts || true

# kubeconfigs already extracted above

log "install Tenant CRD in both clusters (redundant if chart installs CRDs)"
kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -f config/crd/bases/mesh.openkcm.io_tenants.yaml
kubectl --kubeconfig /tmp/remote-kind.kubeconfig apply -f config/crd/bases/mesh.openkcm.io_tenants.yaml

log "build operator Docker image"
docker build -t "$OP_IMAGE" .

log "load operator image into home kind"
kind load docker-image "$OP_IMAGE" --name "$HOME_CLUSTER"

log "install operator Helm chart from local path"
# Ensure helm is available
command -v helm >/dev/null 2>&1 || { echo "helm is required"; exit 1; }

# Install local chart with correct namespace and values
helm --kubeconfig /tmp/home-kind.kubeconfig upgrade -i "$OP_RELEASE_NAME" "$OP_CHART_PATH" \
  --namespace "$OP_NS" \
  --create-namespace \
  --set image.repository=crypto-edge-operator \
  --set image.registry= \
  --set image.tag=dev \
  --set image.pullPolicy=IfNotPresent \
  --set installMode.crdsRbacOnly=false \
  --set autoscaling.enabled=false \
  --wait --timeout 300s

log "create remote kubeconfig secret in home cluster (using internal kubeconfig)"
kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" create secret generic remote-1 \
  --from-file=kubeconfig=/tmp/remote-kind.internal.kubeconfig --dry-run=client -o yaml \
  | yq e '.metadata.labels."sigs.k8s.io/multicluster-runtime-kubeconfig"="true"' - \
  | kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -f -

log "determine operator deployment name"
DEP_NAME="$OP_RELEASE_NAME"
# Fallback: discover by label if name differs from release
if ! kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" get deploy "$DEP_NAME" >/dev/null 2>&1; then
  DEP_NAME=$(kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" get deploy -l app=crypto-edge-operator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
fi
if [ -z "$DEP_NAME" ]; then
  log "could not determine operator deployment name"
  kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" get deploy -o yaml
  exit 1
fi

log "wait for operator deployment rollout: $DEP_NAME"
if ! kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" rollout status deploy "$DEP_NAME" --timeout=180s; then
  log "operator rollout failed; collecting diagnostics"
  kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" get deploy "$DEP_NAME" -o yaml || true
  kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" get rs -o wide || true
  kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" get pods -o wide || true
  kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" describe deploy "$DEP_NAME" || true
  kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" get events --sort-by=.lastTimestamp || true
  # Dump first pod logs if present
  POD=$(kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" get pods -l app=crypto-edge-operator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  if [ -n "$POD" ]; then
    kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" logs "$POD" --tail=200 || true
  fi
  exit 1
fi
log "operator ready"

log "apply tenant resource to remote and home"
kubectl --kubeconfig /tmp/remote-kind.kubeconfig apply -n "$TENANT_NS" -f - <<EOF
apiVersion: mesh.openkcm.io/v1alpha1
kind: Tenant
metadata:
  name: ${TENANT_NAME}
  namespace: ${TENANT_NS}
spec:
  workspace: ${WORKSPACE_NS}
EOF
kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -n "$TENANT_NS" -f - <<EOF
apiVersion: mesh.openkcm.io/v1alpha1
kind: Tenant
metadata:
  name: ${TENANT_NAME}
  namespace: ${TENANT_NS}
spec:
  workspace: ${WORKSPACE_NS}
EOF

log "wait for workspace namespace on remote"
for i in {1..60}; do
  kubectl --kubeconfig /tmp/remote-kind.kubeconfig get ns "$WORKSPACE_NS" >/dev/null 2>&1 && break
  sleep 2
  [ $i -eq 60 ] && { log "namespace not created"; exit 1; }
done
log "namespace present"

log "wait for tenant Ready on remote"
for i in {1..200}; do
  PHASE=$(kubectl --kubeconfig /tmp/remote-kind.kubeconfig get tenant "$TENANT_NAME" -n "$TENANT_NS" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [ "$PHASE" = "Ready" ] && break
  sleep 3
  [ $i -eq 200 ] && { log "tenant not Ready"; kubectl --kubeconfig /tmp/remote-kind.kubeconfig get tenant "$TENANT_NAME" -n "$TENANT_NS" -o yaml; exit 1; }
done
log "tenant Ready"

log "delete tenant on remote and home"
kubectl --kubeconfig /tmp/remote-kind.kubeconfig delete tenant "$TENANT_NAME" -n "$TENANT_NS"
kubectl --kubeconfig /tmp/home-kind.kubeconfig delete tenant "$TENANT_NAME" -n "$TENANT_NS"

log "wait for namespace deletion on remote"
for i in {1..120}; do
  kubectl --kubeconfig /tmp/remote-kind.kubeconfig get ns "$WORKSPACE_NS" >/dev/null 2>&1 || break
  sleep 2
  [ $i -eq 120 ] && { log "namespace not deleted"; exit 1; }
done
log "workspace namespace deleted"

log "e2e-full passed"
