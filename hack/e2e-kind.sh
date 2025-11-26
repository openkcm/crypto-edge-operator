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

log "install Tenant CRD in home cluster (mesh.openkcm.io)"
kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -f config/crd/bases/mesh.openkcm.io_tenants.yaml

log "install Tenant CRD in remote cluster (mesh.openkcm.io)"
kubectl --kubeconfig /tmp/remote-kind.kubeconfig apply -f config/crd/bases/mesh.openkcm.io_tenants.yaml

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

log "build operator (cmd/crypto-edge-operator)"
GO111MODULE=on go build -o /tmp/cryptoedge-operator ./cmd/crypto-edge-operator

log "start operator (background)"
# Require actual chart install (do not skip on non-fatal repo errors for this test).
unset ALLOW_CHART_SKIP || true
(KUBECONFIG=/tmp/home-kind.kubeconfig /tmp/cryptoedge-operator \
  -namespace "$NAMESPACE" \
  -kubeconfig-label sigs.k8s.io/multicluster-runtime-kubeconfig \
  -kubeconfig-key kubeconfig \
  -chart-repo "$CHART_REPO" \
  -chart-name "$CHART_NAME" \
  -chart-version "$CHART_VERSION") &
OP_PID=$!
echo $OP_PID > /tmp/cryptoedge-operator.pid
sleep 6

log "apply tenant resource directly to remote cluster (no shadow model)"
kubectl --kubeconfig /tmp/remote-kind.kubeconfig apply -f - <<EOF
apiVersion: mesh.openkcm.io/v1alpha1
kind: Tenant
metadata:
  name: ${TENANT_NAME}
  namespace: ${NAMESPACE}
spec:
  workspace: ${WORKSPACE_NS}
EOF

log "apply same tenant resource to home cluster for visibility"
kubectl --kubeconfig /tmp/home-kind.kubeconfig apply -f - <<EOF
apiVersion: mesh.openkcm.io/v1alpha1
kind: Tenant
metadata:
  name: ${TENANT_NAME}
  namespace: ${NAMESPACE}
spec:
  workspace: ${WORKSPACE_NS}
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

log "check tenant phase on remote cluster (timeout 10m)"
# 10 minutes total: 200 iterations * 3s sleep = 600s.
for i in {1..200}; do
  PHASE=$(KUBECONFIG=/tmp/remote-kind.kubeconfig kubectl get tenant "$TENANT_NAME" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [ "$PHASE" = "Ready" ] && break
  sleep 3
  [ $i -eq 200 ] && { log "tenant did not reach Ready phase within 10m"; KUBECONFIG=/tmp/remote-kind.kubeconfig kubectl get tenant "$TENANT_NAME" -n "$NAMESPACE" -o yaml; exit 1; }
done
log "tenant phase is Ready on remote cluster"

log "check tenant phase on home cluster (timeout 10m)"
for i in {1..200}; do
  PHASE_HOME=$(KUBECONFIG=/tmp/home-kind.kubeconfig kubectl get tenant "$TENANT_NAME" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [ "$PHASE_HOME" = "Ready" ] && break
  sleep 3
  [ $i -eq 200 ] && { log "home cluster tenant did not reach Ready phase within 10m"; KUBECONFIG=/tmp/home-kind.kubeconfig kubectl get tenant "$TENANT_NAME" -n "$NAMESPACE" -o yaml; exit 1; }
done
log "tenant phase is Ready on home cluster"

log "verify cert-manager deployments present in workspace namespace on remote cluster"
# Deployment names are prefixed by the Helm release name (tenant-<tenantName>-<secretName>) since we use the cluster Secret name.
RELEASE_PREFIX="tenant-${TENANT_NAME}-${REMOTE_SECRET}"
missingCount=0
for deploy in ${RELEASE_PREFIX}-cert-manager ${RELEASE_PREFIX}-cert-manager-cainjector ${RELEASE_PREFIX}-cert-manager-webhook; do
  for i in {1..60}; do
    OUT=$(KUBECONFIG=/tmp/remote-kind.kubeconfig kubectl get deploy "$deploy" -n "$WORKSPACE_NS" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
    if [ -n "$OUT" ] && [ "$OUT" -ge 1 ]; then
      log "deployment $deploy ready ($OUT replicas)"
      break
    fi
    sleep 2
    [ $i -eq 60 ] && { log "deployment $deploy not ready after 120s"; KUBECONFIG=/tmp/remote-kind.kubeconfig kubectl get deploy "$deploy" -n "$WORKSPACE_NS" -o yaml || { missingCount=$((missingCount+1)); }; break; }
  done
done

if [ $missingCount -gt 0 ]; then
  log "some expected deployments missing or not ready; listing all deployments for diagnostics"
  KUBECONFIG=/tmp/remote-kind.kubeconfig kubectl get deploy -n "$WORKSPACE_NS"
  log "checking for alternative prefix using cluster name"
  ALT_PREFIX="tenant-${TENANT_NAME}-${REMOTE_CLUSTER}"
  KUBECONFIG=/tmp/remote-kind.kubeconfig kubectl get deploy -n "$WORKSPACE_NS" | awk '{print $1}' | grep "${ALT_PREFIX}-cert-manager" >/dev/null 2>&1 && {
    log "alternative prefix ${ALT_PREFIX} appears present; consider updating REMOTE_SECRET or release naming logic"
  }
  [ $missingCount -gt 0 ] && { log "e2e failing due to missing deployments"; exit 1; }
fi

log "verify cert-manager pods running"
KUBECONFIG=/tmp/remote-kind.kubeconfig kubectl get pods -n "$WORKSPACE_NS"

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
