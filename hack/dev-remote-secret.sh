#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <kubeconfig-file> <namespace> <secret-name>" >&2
  exit 1
fi

KUBECONFIG_FILE=$1
NAMESPACE=$2
SECRET_NAME=$3

if [[ ! -f "$KUBECONFIG_FILE" ]]; then
  echo "File not found: $KUBECONFIG_FILE" >&2
  exit 1
fi

kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || kubectl create ns "$NAMESPACE"

ENCODED=$(base64 < "$KUBECONFIG_FILE" | tr -d '\n')
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: ${SECRET_NAME}
  namespace: ${NAMESPACE}
type: Opaque
data:
  kubeconfig: ${ENCODED}
EOF

echo "Secret ${SECRET_NAME} created/updated in namespace ${NAMESPACE}."
