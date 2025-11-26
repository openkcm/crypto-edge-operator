# crypto-edge-operator Helm Chart

Helm chart to deploy the Crypto Edge Operator responsible for reconciling `Tenant` custom resources (CRD: `tenants.mesh.openkcm.io`).

## Features
- Deploys controller manager Deployment
- Installs ServiceAccount, ClusterRole, ClusterRoleBinding
- Installs CRD for `Tenant` resources
- Configurable image registry/repository/tag or digest
- Health and readiness probes
- Optional pod disruption budget & autoscaling stubs

## Prerequisites
- Kubernetes >= 1.26 (CRD v1)
- Helm >= 3.11

## Installation

Add (or directly install from) the local path:

```bash
helm install crypto-edge-operator ./charts/crypto-edge-operator \
  --namespace crypto-edge-system --create-namespace
```

## Values Overview
| Key | Description | Default |
|-----|-------------|---------|
| `image.registry` | Registry hosting the operator image | `docker.io/openkcm` |
| `image.repository` | Image repository name | `crypto-edge-operator` |
| `image.tag` | Image tag (falls back to chart appVersion) | `v0.1.0` |
| `image.digest` | Optional sha256 digest (overrides tag) | `` |
| `replicaCount` | Desired replicas (unless autoscaling enabled) | `1` |
| `serviceAccount.create` | Create SA | `true` |
| `namespace` | Override target namespace for resources | (release namespace) |

See `values.yaml` for full list.

## Upgrading
Increment `Chart.yaml` version when template changes. Update `appVersion` when operator code changes.

## CRD
The chart installs the `Tenant` CRD. If you need to disable CRD installation for GitOps management, you can remove the file under `crds/` or introduce a boolean gate (future enhancement).

## Uninstall
```bash
helm uninstall crypto-edge-operator -n crypto-edge-system
```
The CRD will remain (Helm leaves CRDs by default); delete manually if desired:
```bash
kubectl delete crd tenants.mesh.openkcm.io
```

## Development
Render templates:
```bash
helm template crypto-edge-operator ./charts/crypto-edge-operator
```

Lint chart:
```bash
helm lint ./charts/crypto-edge-operator
```

## Future Improvements
- Add PodDisruptionBudget and HPA toggles to templates
- Gate CRD installation by value (e.g. `crds.install: true`)
- Add leader election config via values
- Add metrics service and ServiceMonitor for Prometheus
