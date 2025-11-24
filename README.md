# crypto-edge-operator

Experimental multicluster operator prototype.

## Central Helm Chart Configuration

Helm chart rollout is now centrally configured by process flags instead of per-Tenant spec fields. All Tenants share the same chart (repository, name, version) and the operator installs/upgrades that chart into each Tenant workspace namespace on every engaged cluster.

### Flags

The multicluster entrypoint (`RunMulticlusterExample`) supports these flags:

```
--chart-repo     Central Helm chart repository URL (default: https://charts.jetstack.io)
--chart-name     Central Helm chart name (default: cert-manager)
--chart-version  Central Helm chart version (default: 1.19.1)
--namespace      Namespace containing kubeconfig secrets (default: default)
--kubeconfig-label  Label selecting kubeconfig secrets (default: sigs.k8s.io/multicluster-runtime-kubeconfig)
--kubeconfig-key    Data key for kubeconfig content in secret (default: kubeconfig)
```

Example run (local):
# OpenKCM: crypto-edge-operator

[![REUSE status](https://api.reuse.software/badge/github.com/openkcm/crypto-edge-operator)](https://api.reuse.software/info/github.com/openkcm/crypto-edge-operator)

```bash
go run ./cmd/multicluster \
  --chart-repo=https://charts.jetstack.io \
  --chart-name=cert-manager \
  --chart-version=1.19.1 \
  --namespace=platform-system
```

### Changing the Chart
Crypto Edge Operator

To roll out a different chart or version across all Tenants, restart (or upgrade) the operator with new flag values. The controller computes a fingerprint from repo|name|version and only performs a Helm upgrade if the fingerprint changed.

### Values Support

Per-Tenant Helm values are currently disabled; values are an empty map. To introduce centralized values, add a new flag (e.g. `--chart-values-file`) and load + merge it before install/upgrade. Per-Tenant overrides would require a design update (e.g. referencing a ConfigMap or reintroducing a controlled subset of spec fields).

### Migration From Previous CRD

Older versions used `spec.chart` within the `Tenant` CRD. That field has been removed. Existing Tenant objects with a `chart` key in their spec must be deleted or re-applied without the field after installing the updated CRD:
This project is open to feature requests/suggestions, bug reports etc. via [GitHub issues](https://github.com/openkcm/crypto-edge-operator/issues). Contribution and feedback are encouraged and always welcome. For more information about how to contribute, the project structure, as well as additional contribution information, see our [Contribution Guidelines](CONTRIBUTING.md).

## Security / Disclosure
If you find any bug that may be a security problem, please follow our instructions at [in our security policy](https://github.com/openkcm/crypto-edge-operator/security/policy) on how to report it. Please do not create GitHub issues for security-related doubts or problems.

```bash
kubectl delete tenant -A --all    # if safe; or selectively recreate
kubectl apply -f config/crd/bases/platform.example.com_tenants.yaml
```

Then create new Tenants with only `spec.workspace` (and optional `spec.clusterRef`). Chart selection is now purely an operator deployment concern.

### Environment Override

Set `ALLOW_CHART_SKIP=true` in the operator environment to treat non-fatal chart load errors (e.g. temporary repo outage) as skippable, allowing the Tenant phase to progress to Ready if other conditions are satisfied.

### Events & Conditions

Lifecycle Events emitted per Tenant per cluster:

* HelmInstallStart / HelmInstalled
* HelmUpgradeStart / HelmUpgraded
* HelmInstallFailed / HelmUpgradeFailed
* HelmSkip (fingerprint unchanged)
* ChartNotLoaded / ChartVersionNotFound / ChartVersionInvalid
* PhaseSet (aggregated phase transitions)

Status Conditions use cluster-scoped types (e.g. `ClusterReady/<cluster>` / `ClusterError/<cluster>` / `ClusterProgress/<cluster>`).

### Fingerprinting

An annotation `platform.example.com/fingerprint-<cluster>` is set on the Tenant after successful install/upgrade. Update logic currently performs a direct object Update; future improvement will switch to a PATCH with retries and emit FingerprintUpdated / FingerprintUpdateFailed events.

### Troubleshooting

* No install occurring? Verify the chart flags and that the repo is reachable from the operator pod.
* Continuous ChartNotLoaded warnings? Check network egress, repo URL correctness, or temporary repository outage.
* VersionNotFound / VersionInvalid events? Confirm the semantic version exists in the repository and is valid (SemVer compliant).
* Missing events entirely? Ensure RBAC includes create permissions for events (the bundled Role does).

---

See `internal/multicluster/example.go` for the reconciliation logic implementing these behaviors.
# CryptoEdge Operator

Multi-cluster Kubernetes operator that deploys a Helm chart for each `Tenant` custom resource present on a cluster. Each Tenant specifies a workspace namespace and chart details; the operator ensures the workspace namespace and performs Helm install / upgrade with idempotent fingerprint skipping. Tenants are now stored directly on the cluster they target (no shadow propagation model).

## Architecture
1. Kubeconfig Provider: Discovers clusters via labeled Secrets (`sigs.k8s.io/multicluster-runtime-kubeconfig`). A self-cluster secret is auto-synthesized on startup (strategy C: validate, embed certs/keys, fallback to full file).
2. Multicluster Manager: One controller-runtime manager orchestrates dynamic cluster engagement; a single controller reconciles `Tenant` objects per cluster.
3. Reconcile Steps (per cluster-local Tenant):
  - Ensure Tenant CRD present on that cluster (embedded manifest applied on demand).
  - Fetch the local Tenant object.
  - Optional cluster targeting: if `spec.clusterRef.secretName` set and does not match cluster name, reconciliation is skipped.
  - Ensure workspace namespace exists (`spec.workspace`).
  - Resolve Helm chart (repo URL + name + version) and values map.
  - Compute fingerprint (sha256 of repo|name|version|sorted key=value pairs) per cluster; skip Helm action if unchanged.
  - Install or upgrade release (`tenant-<name>-<cluster>`), update annotation `platform.example.com/fingerprint-<cluster>`.
  - Upsert per-cluster status conditions and aggregate top-level `status.phase`.

## Tenant CRD
Defined in `api/v1alpha1/tenant_types.go` (embedded YAML in `internal/multicluster/platform.example.com_tenants.yaml`).

Spec example:
```yaml
spec:
  workspace: acme-workspace
  chart:
    repo: https://charts.jetstack.io
    name: cert-manager
    version: 1.19.1
    values:
      replicaCount: 1
```

Status:
```yaml
status:
  phase: Ready|Pending|Error
  conditions:
    - type: ClusterReady/self-cluster
      status: "True"
      reason: Installed|Upgraded|NoChange
    - type: ClusterError/<cluster>
      status: "True"
      reason: InstallFailed|UpgradeFailed|ChartNotLoaded
```
Fingerprint stored per cluster in annotation: `platform.example.com/fingerprint-<cluster>`.

## Build & Run
```bash
make tidy
make build
make run  # uses current KUBECONFIG as home cluster
```

## Adding Remote Cluster
Create a kubeconfig Secret with the discovery label:
```bash
kubectl -n default create secret generic demo-remote-1 \
  --from-file=kubeconfig=/path/to/remote.kubeconfig \
  --label sigs.k8s.io/multicluster-runtime-kubeconfig=true
```

## Apply Tenant
```bash
kubectl apply -f examples/tenant-acme.yaml
kubectl get tenant acme -o yaml
```

## Helm Release Naming
`tenant-<tenantName>-<clusterName>` ensures uniqueness across clusters.

## Fingerprint Idempotency
Avoids unnecessary Helm upgrades. Changing any chart field or values key/value mutates the sha256 hash, triggering an upgrade.

## Self Cluster Kubeconfig Strategy (C)
1. Load user kubeconfig file ($KUBECONFIG first path or $HOME/.kube/config).
2. Inline referenced cert/key/CA files into data fields.
3. Reduce to current context & validate by constructing rest.Config and probing /version.
4. Fallback to full file if validation fails; otherwise synthesize minimal config when no file present.

## Phase Aggregation Rules
- Error if any `ClusterError/*` condition true.
- Ready if at least one `ClusterReady/*` true and no errors.
- Pending otherwise.

## Troubleshooting
| Symptom | Cause | Resolution |
|---------|-------|-----------|
| chart not loaded; skipping helm action | Repo unreachable or invalid chart/version | Check repo URL & version; network access. |
| helm install failed (auth) | Remote cluster credentials invalid | Recreate kubeconfig Secret with valid token or cert/key. |
| CRD ensure failed repeatedly | Insufficient RBAC on remote cluster | Grant create/update on CRDs. |
| Fingerprint not updating | Annotation update failed | Ensure Tenant update RBAC; inspect controller logs. |

## Kind-based E2E Test
A reproducible two-cluster test harness lives in `hack/e2e-kind.sh` and is wired to `make e2e-kind`.

Flow:
1. Creates two kind clusters: `home` and `remote`.
2. Installs the Tenant CRD on both clusters.
3. Creates a kubeconfig Secret for the remote cluster in the home cluster (for discovery by the multicluster manager).
4. Starts the operator against the home cluster (manager discovers both clusters).
5. Applies a `Tenant` directly on the remote cluster (no shadow object creation required).
6. Operator on remote cluster ensures workspace namespace and performs Helm install.
7. Tenant status on remote cluster transitions to `Ready`.
8. Script validates readiness and tears everything down.

Run it:
```bash
make e2e-kind
```
Expected log snippets:
```
workspace namespace created
helm install success
tenant phase is Ready on remote cluster
e2e passed
```

### Process Lifecycle & Cleanup
The e2e harness starts the operator as a background process and now enforces a clean shutdown:
1. Operator PID recorded in `/tmp/cryptoedge-operator.pid`.
2. On normal completion or trap (INT/TERM/EXIT) the script sends SIGTERM, waits up to ~5s, then SIGKILL if still running.
3. `make stop` target is available for manual cleanup if you start with `make run-pid`.

Manual usage:
```bash
make run-pid   # starts operator, writes PID file
make stop      # terminates operator (TERM + optional KILL)
```

This prevents orphaned processes that would otherwise keep ports bound and interfere with subsequent test runs.

Troubleshooting:
| Symptom | Cause | Resolution |
|---------|-------|-----------|
| port bind error during kind remote creation | Docker reused API server port quickly | Re-run; script includes a small sleep to mitigate race |
| engage manager error (kind not registered) | Missing scheme in cluster options | Ensure `ClusterOptions: []cluster.Option{func(o *cluster.Options){o.Scheme = scheme}}` present |
| shadow tenant invalid | Shadow spec omitted required fields | Regenerate shadow with full spec (already implemented) |

## Roadmap
- Finalizer for uninstall.
- (DONE) Removed shadow propagation: Tenants now reside locally on their target cluster.
- Tests (unit & extended e2e).
- Metrics (ready/error counts per cluster).
- Controller-gen integration to auto-generate CRDs.

## License
Apache 2.0 (placeholder)
# CryptoEdge Operator (MVP)

Multi-cluster operator that installs a Helm chart (e.g., cert-manager) into a tenant-specific namespace on a remote customer cluster when a `Tenant` custom resource is created in the home cluster.

## Status
MVP scaffold. Multi-cluster runtime integration and full Helm implementation marked as TODO.

## Tenant CRD (MVP Fields)
See `config/crd/bases/platform.example.com_tenants.yaml` and `api/v1alpha1/tenant_types.go`.

## Quick Dev Loop
```bash
make tidy
make build
make run # runs against your current KUBECONFIG/home cluster
```

Apply example manifests (once CRD installed):
```bash
kubectl apply -f examples/remote-kubeconfig-secret.yaml
kubectl apply -f examples/tenant-acme.yaml
```

## Remote kubeconfig Secret
Must contain a key `kubeconfig` with the remote cluster kubeconfig content. See helper script `hack/dev-remote-secret.sh`.

## Helm Release Naming
Release name format (proposed): `tenant-<tenantName>`.

## TODO / Roadmap
- Implement Helm install/upgrade logic inside reconcile.
- Integrate multicluster-runtime once import path confirmed.
- Add finalizer for uninstall.
- Status conditions reflecting remote deployment state.
- Controller-gen + CRD generation.
- Proper unit/e2e tests.

## Assumptions
Module path assumed: `github.com/cryptoedge/cryptoedgeoperator`. Adjust in `go.mod` if different.

## Kind-based e2e (future)
```bash
# (placeholder instructions)
kind create cluster --name home
kind create cluster --name remote
# Export remote kubeconfig to file
kind get kubeconfig --name remote > /tmp/remote.kubeconfig
./hack/dev-remote-secret.sh /tmp/remote.kubeconfig cryptoedge-operator acme-remote-kubeconfig
kubectl apply -f examples/tenant-acme.yaml
```
Copyright (20xx-)20xx SAP SE or an SAP affiliate company and OpenKCM contributors. Please see our [LICENSE](LICENSE) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/openkcm/crypto-edge-operator).
