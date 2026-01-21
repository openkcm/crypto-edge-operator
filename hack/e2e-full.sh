#!/bin/zsh
set -euo pipefail
# Uncomment for verbose debugging
# set -x

# Full e2e: build local Docker image, run operator in-cluster,
# load echo-server images into kind, install via Helm through Tenant flow,
# then delete Tenant and verify cleanup.

CLEANED_UP=0

HOME_CLUSTER=home
EDGE_01_CLUSTER=edge01
EDGE_02_CLUSTER=edge02
# Operator namespace (where the chart is installed)
OP_NS=crypto-edge-operator
# Tenant namespace (where Tenant CRs live)
TENANT_NS=default
TENANT_NAME_EDGE01=e2e-full-echo-edge01
TENANT_NAME_EDGE02=e2e-full-echo-edge02
TENANT_SUFFIXES=(a b c)
CERTM_REPO=https://ealenn.github.io/charts
CERTM_NAME=echo-server
CERTM_VERSION=0.5.0

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
    timeout 60 kind delete cluster --name "$EDGE_01_CLUSTER" 2>/dev/null || true
    timeout 60 kind delete cluster --name "$EDGE_02_CLUSTER" 2>/dev/null || true
  else
    kind delete cluster --name "$HOME_CLUSTER" 2>/dev/null || true
    kind delete cluster --name "$EDGE_01_CLUSTER" 2>/dev/null || true
    kind delete cluster --name "$EDGE_02_CLUSTER" 2>/dev/null || true
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

log "creating edge01 cluster: $EDGE_01_CLUSTER"
kind create cluster --name "$EDGE_01_CLUSTER" || { log "failed to create edge01 cluster"; kind get clusters || true; exit 1; }
log "edge01 cluster created"
sleep 3

log "creating edge02 cluster: $EDGE_02_CLUSTER"
kind create cluster --name "$EDGE_02_CLUSTER" || { log "failed to create edge02 cluster"; kind get clusters || true; exit 1; }
log "edge02 cluster created"
log "current kind clusters:"; kind get clusters || true

log "extract kubeconfigs (external for host, internal for pods)"
kind get kubeconfig --name "$HOME_CLUSTER" > /tmp/home-kind.kubeconfig
kind get kubeconfig --name "$EDGE_01_CLUSTER" > /tmp/edge01-kind.kubeconfig
kind get kubeconfig --name "$EDGE_02_CLUSTER" > /tmp/edge02-kind.kubeconfig
# Internal kubeconfigs use cluster-internal IP/hostname so pods can reach the API
kind get kubeconfig --name "$EDGE_01_CLUSTER" --internal > /tmp/edge01-kind.internal.kubeconfig
kind get kubeconfig --name "$EDGE_02_CLUSTER" --internal > /tmp/edge02-kind.internal.kubeconfig

log "wait for home cluster API health"
for i in {1..30}; do
  kubectl --kubeconfig /tmp/home-kind.kubeconfig get --raw=/healthz >/dev/null 2>&1 && break
  sleep 2
  [ $i -eq 30 ] && { log "home cluster API not ready"; exit 1; }
done
log "home cluster API ready"

log "wait for edge01 cluster API health"
for i in {1..30}; do
  kubectl --kubeconfig /tmp/edge01-kind.kubeconfig get --raw=/healthz >/dev/null 2>&1 && break
  sleep 2
  [ $i -eq 30 ] && { log "edge01 cluster API not ready"; exit 1; }
done
log "edge01 cluster API ready"

log "wait for edge02 cluster API health"
for i in {1..30}; do
  kubectl --kubeconfig /tmp/edge02-kind.kubeconfig get --raw=/healthz >/dev/null 2>&1 && break
  sleep 2
  [ $i -eq 30 ] && { log "edge02 cluster API not ready"; exit 1; }
done
log "edge02 cluster API ready"

log "kube contexts available:"
kubectl config get-contexts || true

# kubeconfigs already extracted above

log "install CRDs (Account, CryptoEdgeDeployment) in home cluster"
kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -f config/crd/bases/

log "install CRDs (Account, CryptoEdgeDeployment, Region) in edge clusters"
kubectl --kubeconfig /tmp/edge01-kind.kubeconfig apply -f config/crd/bases/
kubectl --kubeconfig /tmp/edge02-kind.kubeconfig apply -f config/crd/bases/

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
  --skip-crds \
  --wait --timeout 300s

log "create remote kubeconfig secrets in home cluster (using internal kubeconfigs)"
kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" create secret generic kubeconfig-edge01 \
  --from-file=kubeconfig=/tmp/edge01-kind.internal.kubeconfig --dry-run=client -o yaml \
  | yq e '.metadata.labels."sigs.k8s.io/multicluster-runtime-kubeconfig"="true"' - \
  | kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -f -

kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" create secret generic kubeconfig-edge02 \
  --from-file=kubeconfig=/tmp/edge02-kind.internal.kubeconfig --dry-run=client -o yaml \
  | yq e '.metadata.labels."sigs.k8s.io/multicluster-runtime-kubeconfig"="true"' - \
  | kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -f -

log "verify kubeconfig secrets created"
kubectl --kubeconfig /tmp/home-kind.kubeconfig -n "$OP_NS" get secrets -l sigs.k8s.io/multicluster-runtime-kubeconfig=true -o wide

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

log "create Account on home cluster"
kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -n "$TENANT_NS" -f - <<EOF
apiVersion: mesh.openkcm.io/v1alpha1
kind: Account
metadata:
  name: dev-account
  namespace: ${TENANT_NS}
spec:
  displayName: "Development Account"
  owner: "dev-team"
EOF

log "create Region CRs to define kubeconfig secrets"
kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -n "$OP_NS" -f - <<EOF
apiVersion: mesh.openkcm.io/v1alpha1
kind: Region
metadata:
  name: edge01
  namespace: ${OP_NS}
spec:
  kubeconfigSecretName: kubeconfig-edge01
---
apiVersion: mesh.openkcm.io/v1alpha1
kind: Region
metadata:
  name: edge02
  namespace: ${OP_NS}
spec:
  kubeconfigSecretName: kubeconfig-edge02
EOF

log "apply 3 CryptoEdgeDeployments per region to home cluster"
# Create three deployments for edge01 and edge02
TENANT_NAMES_EDGE01=()
TENANT_NAMES_EDGE02=()
for s in ${TENANT_SUFFIXES[@]}; do
  NAME_EDGE01="${TENANT_NAME_EDGE01}-${s}"
  NAME_EDGE02="${TENANT_NAME_EDGE02}-${s}"
  TENANT_NAMES_EDGE01+=("$NAME_EDGE01")
  TENANT_NAMES_EDGE02+=("$NAME_EDGE02")
  # Edge01
  kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -n "$TENANT_NS" -f - <<EOF
apiVersion: mesh.openkcm.io/v1alpha1
kind: CryptoEdgeDeployment
metadata:
  name: ${NAME_EDGE01}
  namespace: ${TENANT_NS}
spec:
  accountRef:
    name: dev-account
  regionRef:
    name: edge01
  targetRegion: edge01
EOF
  # Edge02
  kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -n "$TENANT_NS" -f - <<EOF
apiVersion: mesh.openkcm.io/v1alpha1
kind: CryptoEdgeDeployment
metadata:
  name: ${NAME_EDGE02}
  namespace: ${TENANT_NS}
spec:
  accountRef:
    name: dev-account
  regionRef:
    name: edge02
  targetRegion: edge02
EOF
done

log "wait for all CryptoEdgeDeployments Ready on home cluster (timeout 10m each)"
# Edge01
for name in ${TENANT_NAMES_EDGE01[@]}; do
  for i in {1..200}; do
    PHASE=$(kubectl --kubeconfig /tmp/home-kind.kubeconfig get cryptoedgedeployment "$name" -n "$TENANT_NS" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [ "$PHASE" = "Ready" ] && break
    sleep 3
    [ $i -eq 200 ] && { log "edge01 deployment $name not Ready"; kubectl --kubeconfig /tmp/home-kind.kubeconfig get cryptoedgedeployment "$name" -n "$TENANT_NS" -o yaml; exit 1; }
  done
  log "edge01 deployment $name Ready"
done

# Edge02
for name in ${TENANT_NAMES_EDGE02[@]}; do
  for i in {1..200}; do
    PHASE=$(kubectl --kubeconfig /tmp/home-kind.kubeconfig get cryptoedgedeployment "$name" -n "$TENANT_NS" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [ "$PHASE" = "Ready" ] && break
    sleep 3
    [ $i -eq 200 ] && { log "edge02 deployment $name not Ready"; kubectl --kubeconfig /tmp/home-kind.kubeconfig get cryptoedgedeployment "$name" -n "$TENANT_NS" -o yaml; exit 1; }
  done
  log "edge02 deployment $name Ready"
done

log "verify workloads exist in target namespaces on edge clusters"
# Edge01 pods present in each target namespace
for name in ${TENANT_NAMES_EDGE01[@]}; do
  kubectl --kubeconfig /tmp/edge01-kind.kubeconfig get ns "$name" >/dev/null 2>&1 || { log "edge01 namespace $name missing"; exit 1; }
  POD_COUNT=$(kubectl --kubeconfig /tmp/edge01-kind.kubeconfig get pods -n "$name" --no-headers 2>/dev/null | wc -l | tr -d ' ')
  [ "${POD_COUNT:-0}" -ge 1 ] || { log "edge01 no pods found in namespace $name"; exit 1; }
done
log "edge01 workloads present in target namespaces"

# Edge02 pods present in each target namespace
for name in ${TENANT_NAMES_EDGE02[@]}; do
  kubectl --kubeconfig /tmp/edge02-kind.kubeconfig get ns "$name" >/dev/null 2>&1 || { log "edge02 namespace $name missing"; exit 1; }
  POD_COUNT=$(kubectl --kubeconfig /tmp/edge02-kind.kubeconfig get pods -n "$name" --no-headers 2>/dev/null | wc -l | tr -d ' ')
  [ "${POD_COUNT:-0}" -ge 1 ] || { log "edge02 no pods found in namespace $name"; exit 1; }
done
log "edge02 workloads present in target namespaces"

log "delete all CryptoEdgeDeployments on home cluster"
for name in ${TENANT_NAMES_EDGE01[@]}; do
  kubectl --kubeconfig /tmp/home-kind.kubeconfig delete cryptoedgedeployment "$name" -n "$TENANT_NS"
done
for name in ${TENANT_NAMES_EDGE02[@]}; do
  kubectl --kubeconfig /tmp/home-kind.kubeconfig delete cryptoedgedeployment "$name" -n "$TENANT_NS"
done

log "wait for namespace deletion on edge clusters"
# Edge01: wait until all namespaces gone
for i in {1..120}; do
  remaining=0
  for name in ${TENANT_NAMES_EDGE01[@]}; do
    kubectl --kubeconfig /tmp/edge01-kind.kubeconfig get ns "$name" >/dev/null 2>&1 && remaining=$((remaining+1))
  done
  [ $remaining -eq 0 ] && break
  sleep 2
  [ $i -eq 120 ] && { log "edge01 namespaces not deleted"; exit 1; }
done
log "edge01 tenant namespaces deleted"

# Edge02: wait until all namespaces gone
for i in {1..120}; do
  remaining=0
  for name in ${TENANT_NAMES_EDGE02[@]}; do
    kubectl --kubeconfig /tmp/edge02-kind.kubeconfig get ns "$name" >/dev/null 2>&1 && remaining=$((remaining+1))
  done
  [ $remaining -eq 0 ] && break
  sleep 2
  [ $i -eq 120 ] && { log "edge02 namespaces not deleted"; exit 1; }
done
log "edge02 tenant namespaces deleted"

log "e2e-full passed"
