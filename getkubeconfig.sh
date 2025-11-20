export NAMESPACE=garden-kms
export SHOOT_NAME=worker01
export KUBECONFIG=/Users/I753738/Downloads/kubeconfig_cryptoedgeoperator.yaml
kubectl create \
    -f <(printf '{"spec":{"expirationSeconds":3600}}') \
    --raw /apis/core.gardener.cloud/v1beta1/namespaces/${NAMESPACE}/shoots/${SHOOT_NAME}/adminkubeconfig | \
    jq -r ".status.kubeconfig" | \
    base64 -d
