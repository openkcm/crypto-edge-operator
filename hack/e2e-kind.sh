#!/bin/zsh
set -euo pipefail

OP_PID="" # will hold operator PID

HOME_CLUSTER=home
REMOTE_CLUSTER=remote
NAMESPACE=default
REMOTE_SECRET=demo-remote-1
REMOTE_SECRET_LABEL="sigs.k8s.io/multicluster-runtime-kubeconfig=true"
TENANT_NAME=e2e-acme
WORKSPACE_NS=e2e-acme-workspace
CHART_REPO=https://charts.jetstack.io
CHART_NAME=cert-manager
CHART_VERSION=1.19.1
VALUES='{"replicaCount":1}'

log() { printf "[e2e] %s\n" "$*"; }

cleanup() {
  set +e
  if [ -n "$OP_PID" ] && kill -0 $OP_PID 2>/dev/null; then
    log "sending TERM to operator PID=$OP_PID"
    kill $OP_PID 2>/dev/null || true
    # Wait up to 5s
    for i in {1..10}; do
      kill -0 $OP_PID 2>/dev/null || break
      sleep 0.5
    done
    if kill -0 $OP_PID 2>/dev/null; then
      log "operator not exited; sending KILL"
      kill -9 $OP_PID 2>/dev/null || true
    fi
  fi
  log "tearing down kind clusters"
  kind delete cluster --name "$HOME_CLUSTER" 2>/dev/null || true
  kind delete cluster --name "$REMOTE_CLUSTER" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

log "creating kind clusters"
kind create cluster --name "$HOME_CLUSTER"
# Small delay to reduce likelihood of port allocation race on macOS Docker.
sleep 3
kind create cluster --name "$REMOTE_CLUSTER"

log "wait for home cluster API health"
for i in {1..30}; do
  kubectl --context kind-$HOME_CLUSTER get --raw=/healthz >/dev/null 2>&1 && break
  sleep 2
  [ $i -eq 30 ] && { log "home cluster API not ready"; exit 1; }
done
log "home cluster API ready"

log "wait for remote cluster API health"
for i in {1..30}; do
  kubectl --context kind-$REMOTE_CLUSTER get --raw=/healthz >/dev/null 2>&1 && break
  sleep 2
  [ $i -eq 30 ] && { log "remote cluster API not ready"; exit 1; }
done
log "remote cluster API ready"

log "extract remote kubeconfig"
kind get kubeconfig --name "$REMOTE_CLUSTER" > /tmp/remote-kind.kubeconfig

log "extract home kubeconfig"
kind get kubeconfig --name "$HOME_CLUSTER" > /tmp/home-kind.kubeconfig

log "install Tenant CRD in home cluster"
kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -f config/crd/bases/platform.example.com_tenants.yaml

log "install Tenant CRD in remote cluster"
kubectl --kubeconfig /tmp/remote-kind.kubeconfig apply -f config/crd/bases/platform.example.com_tenants.yaml

log "switch kubectl context to home cluster"
# IMPORTANT: KUBECONFIG must point to a file path, not contain the raw kubeconfig content.
# Previously we assigned the content which caused kubectl/client-go to treat segments of the
# kubeconfig YAML as file names, yielding "file name too long" errors while loading.
export KUBECONFIG=/tmp/home-kind.kubeconfig
if [ ! -s "$KUBECONFIG" ]; then
  log "home kubeconfig file missing or empty at $KUBECONFIG"
  exit 1
fi

log "create remote kubeconfig secret in home cluster"
kubectl -n "$NAMESPACE" create secret generic "$REMOTE_SECRET" --from-file=kubeconfig=/tmp/remote-kind.kubeconfig --dry-run=client -o yaml \
  | yq e '.metadata.labels."sigs.k8s.io/multicluster-runtime-kubeconfig"="true"' - \
  | kubectl apply -f -

log "build operator"
GO111MODULE=on go build -o /tmp/cryptoedge-operator ./main.go

log "start operator (background)"
KUBECONFIG=/tmp/home-kind.kubeconfig /tmp/cryptoedge-operator -namespace "$NAMESPACE" -kubeconfig-label sigs.k8s.io/multicluster-runtime-kubeconfig -kubeconfig-key kubeconfig &
OP_PID=$!
echo $OP_PID > /tmp/cryptoedge-operator.pid
sleep 6

log "apply tenant resource directly to remote cluster (no shadow model)"
kubectl --kubeconfig /tmp/remote-kind.kubeconfig apply -f - <<EOF
apiVersion: platform.example.com/v1alpha1
kind: Tenant
metadata:
  name: ${TENANT_NAME}
  namespace: ${NAMESPACE}
spec:
  workspace: ${WORKSPACE_NS}
  chart:
    repo: ${CHART_REPO}
    name: ${CHART_NAME}
    version: ${CHART_VERSION}
    values:
      replicaCount: 1
EOF

log "wait for workspace namespace on remote cluster"
# Check remote cluster directly by using its kubeconfig
REMOTE_KUBECONFIG=/tmp/remote-kind.kubeconfig
for i in {1..30}; do
  KUBECONFIG=$REMOTE_KUBECONFIG kubectl get ns "$WORKSPACE_NS" >/dev/null 2>&1 && break
  sleep 2
  [ $i -eq 30 ] && { log "namespace not created in remote cluster"; exit 1; }
done
log "namespace present on remote cluster"

log "check tenant phase on remote cluster"
for i in {1..40}; do
  PHASE=$(KUBECONFIG=/tmp/remote-kind.kubeconfig kubectl get tenant "$TENANT_NAME" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [ "$PHASE" = "Ready" ] && break
  sleep 3
  [ $i -eq 40 ] && { log "tenant did not reach Ready phase"; KUBECONFIG=/tmp/remote-kind.kubeconfig kubectl get tenant "$TENANT_NAME" -n "$NAMESPACE" -o yaml; exit 1; }
done
log "tenant phase is Ready on remote cluster"

log "terminate operator (explicit)"
if [ -n "$OP_PID" ] && kill -0 $OP_PID 2>/dev/null; then
  kill $OP_PID || true
  for i in {1..10}; do
    kill -0 $OP_PID 2>/dev/null || break
    sleep 0.3
  done
  if kill -0 $OP_PID 2>/dev/null; then
    log "force killing operator"
    kill -9 $OP_PID 2>/dev/null || true
  fi
fi
rm -f /tmp/cryptoedge-operator.pid

log "e2e passed"
