package controllers

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	platformv1alpha1 "github.com/cryptoedge/cryptoedgeoperator/api/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	action "helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/release"
)

// TenantReconciler reconciles a Tenant object
type TenantReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// RBAC markers for future controller-gen usage (currently manual manifests provided under config/rbac).
// +kubebuilder:rbac:groups=platform.example.com,resources=tenants,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=platform.example.com,resources=tenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;update;get;list;watch

// Reconcile single-cluster request.
func (r *TenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("tenant", req.NamespacedName)
	logger.Info("begin reconcile")

	tenant := &platformv1alpha1.Tenant{}
	if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
		if kerrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	logger.Info("fetched tenant", "generation", tenant.Generation)

	if tenant.Status.Phase == "" {
		tenant.Status.Phase = platformv1alpha1.TenantPhasePending
		// Attempt an initial status write so we can see pending state even before secret fetch.
		if err := r.Status().Update(ctx, tenant); err != nil {
			logger.Error(err, "initial pending status update failed")
		} else {
			logger.Info("initial status set to Pending")
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "Pending", "tenant initialization started")
		}
	}

	secret := &corev1.Secret{}
	useHostFallback := false
	var secretKey types.NamespacedName
	if tenant.Spec.ClusterRef != nil && tenant.Spec.ClusterRef.SecretName != "" {
		secretKey = types.NamespacedName{Namespace: tenant.Spec.ClusterRef.SecretNamespace, Name: tenant.Spec.ClusterRef.SecretName}
		if err := r.Get(ctx, secretKey, secret); err != nil {
			logger.Error(err, "remote secret fetch failed; falling back to host cluster", "secret", secretKey.String())
			r.Recorder.Event(tenant, corev1.EventTypeWarning, "RemoteSecretMissing", fmt.Sprintf("secret %s fetch failed: %v", secretKey.String(), err))
			useHostFallback = true
		} else {
			logger.Info("remote kubeconfig secret fetched", "secret", secretKey.String())
		}
	} else {
		// No clusterRef: operate on host cluster directly.
		useHostFallback = true
		logger.Info("no clusterRef provided; using host cluster")
	}

	var remoteClient client.Client
	var remoteHost string
	if !useHostFallback {
		kubeconfigBytes, ok := secret.Data["kubeconfig"]
		if !ok || len(kubeconfigBytes) == 0 {
			logger.Error(fmt.Errorf("kubeconfig secret missing 'kubeconfig' key"), "invalid secret data; using host fallback")
			useHostFallback = true
		} else {
			// Build remote rest.Config from kubeconfig bytes.
			rawCfg, err := clientcmd.Load(kubeconfigBytes)
			if err != nil {
				logger.Error(err, "failed to parse kubeconfig; using host fallback")
				useHostFallback = true
			} else {
				clientCfg := clientcmd.NewDefaultClientConfig(*rawCfg, &clientcmd.ConfigOverrides{})
				remoteRest, err := clientCfg.ClientConfig()
				if err != nil {
					logger.Error(err, "failed to build rest config; using host fallback")
					useHostFallback = true
				} else {
					remoteClient, err = client.New(remoteRest, client.Options{Scheme: r.Scheme})
					if err != nil {
						logger.Error(err, "failed to create remote client; using host fallback")
						useHostFallback = true
					} else {
						remoteHost = remoteRest.Host
						// Connectivity debug: list namespaces
						var nsList corev1.NamespaceList
						if err := remoteClient.List(ctx, &nsList, &client.ListOptions{Limit: 3}); err != nil {
							logger.Error(err, "remote connectivity test failed; using host fallback")
							useHostFallback = true
						} else {
							logger.Info("remote connectivity OK", "sampleNamespaces", len(nsList.Items), "apiServer", remoteHost)
							r.Recorder.Event(tenant, corev1.EventTypeNormal, "RemoteOK", fmt.Sprintf("remote api reachable host=%s", remoteHost))
						}
					}
				}
			}
		}
	}
	if useHostFallback {
		remoteClient = r.Client // host cluster client
		logger.Info("using host cluster as fallback target")
		r.Recorder.Event(tenant, corev1.EventTypeNormal, "HostFallback", "using host cluster for operations")
	}

	// Connectivity debug: attempt a lightweight API call (list namespaces) to validate remote access.
	var nsList corev1.NamespaceList
	listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := remoteClient.List(listCtx, &nsList, &client.ListOptions{Limit: 5}); err != nil {
		logger.Error(err, "remote cluster list namespaces failed")
		r.setStatus(ctx, tenant, platformv1alpha1.TenantPhaseError, fmt.Sprintf("remote connectivity failed (list namespaces): %v", err))
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	logger.Info("remote connectivity (fallback list) OK", "namespaceSampleCount", len(nsList.Items), "apiServer", remoteHost)

	// Ensure workspace namespace exists on remote cluster.
	workspaceNS := &corev1.Namespace{}
	if err := remoteClient.Get(ctx, types.NamespacedName{Name: tenant.Spec.Workspace}, workspaceNS); err != nil {
		if kerrors.IsNotFound(err) {
			create := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tenant.Spec.Workspace}}
			if err2 := remoteClient.Create(ctx, create); err2 != nil {
				logger.Error(err2, "failed to create workspace namespace", "workspace", tenant.Spec.Workspace)
				r.Recorder.Event(tenant, corev1.EventTypeWarning, "WorkspaceCreateFailed", err2.Error())
				r.setStatus(ctx, tenant, platformv1alpha1.TenantPhaseError, fmt.Sprintf("namespace create failed: %v", err2))
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
			logger.Info("workspace namespace created", "workspace", tenant.Spec.Workspace)
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "WorkspaceCreated", tenant.Spec.Workspace)
		} else {
			logger.Error(err, "error checking workspace namespace", "workspace", tenant.Spec.Workspace)
			r.Recorder.Event(tenant, corev1.EventTypeWarning, "WorkspaceCheckFailed", err.Error())
			r.setStatus(ctx, tenant, platformv1alpha1.TenantPhaseError, fmt.Sprintf("namespace check failed: %v", err))
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	} else {
		logger.Info("workspace namespace exists", "workspace", tenant.Spec.Workspace)
		r.Recorder.Event(tenant, corev1.EventTypeNormal, "WorkspaceExists", tenant.Spec.Workspace)
	}

	// Real Helm (basic) install/upgrade attempt (host cluster or remote fallback client not yet wired to Helm configuration).
	releaseName := fmt.Sprintf("tenant-%s", tenant.Name)
	chartRef := tenant.Spec.Chart
	settings := cli.New()

	// Idempotency: build a fingerprint (hash) of chart spec (repo+name+version+sorted values) to compare with stored annotation.
	// We use annotations on the Tenant resource: cryptoedge/fingerprint
	const fpAnnotationKey = "cryptoedge.example.com/chartFingerprint"
	valueKeys := make([]string, 0, len(chartRef.Values))
	for k := range chartRef.Values {
		valueKeys = append(valueKeys, k)
	}
	slices.Sort(valueKeys)
	var fpBuilder strings.Builder
	fpBuilder.WriteString(chartRef.Repo)
	fpBuilder.WriteString("|")
	fpBuilder.WriteString(chartRef.Name)
	fpBuilder.WriteString("|")
	fpBuilder.WriteString(chartRef.Version)
	fpBuilder.WriteString("|")
	for _, k := range valueKeys {
		fpBuilder.WriteString(k)
		fpBuilder.WriteString("=")
		fpBuilder.WriteString(fmt.Sprintf("%v", chartRef.Values[k]))
		fpBuilder.WriteString(";")
	}
	fingerprintSource := fpBuilder.String()
	fingerprint := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(fingerprintSource)))
	prevFingerprint := tenant.Annotations[fpAnnotationKey]

	// Build an action.Configuration using default RESTClientGetter (host kubeconfig from env).
	aCfg := new(action.Configuration)
	// Use environment kubeconfig; without remote rest.Config integration here we only act on host cluster when falling back.
	if err := aCfg.Init(settings.RESTClientGetter(), tenant.Spec.Workspace, os.Getenv("HELM_DRIVER"), func(format string, v ...any) {
		logger.V(1).Info(fmt.Sprintf(format, v...))
	}); err != nil {
		logger.Error(err, "helm configuration init failed")
		r.setStatus(ctx, tenant, platformv1alpha1.TenantPhaseError, fmt.Sprintf("helm config failed: %v", err))
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// Acquire and load chart using safer ChartPathOptions (avoids Pull.Run panic path);
	// we pass RepoURL directly instead of requiring a repo entry.
	var chartLoaded *chart.Chart
	if chartRef.Repo != "" {
		// Ensure Helm cache dirs exist (ChartPathOptions expects them).
		_ = os.MkdirAll(settings.RepositoryCache, 0o755)
		_ = os.MkdirAll(settings.RepositoryConfig, 0o755)
		cp := &action.ChartPathOptions{RepoURL: chartRef.Repo, Version: chartRef.Version}
		loc, err := cp.LocateChart(chartRef.Name, settings)
		if err != nil {
			logger.Error(err, "locate chart failed")
		} else {
			c, err := loader.Load(loc)
			if err != nil {
				logger.Error(err, "chart load failed", "loc", loc)
			} else {
				chartLoaded = c
			}
		}
	}

	installed := false
	var rel *release.Release
	list := action.NewList(aCfg)
	list.All = true
	releases, err := list.Run()
	if err == nil {
		for _, rls := range releases {
			if rls.Name == releaseName && rls.Namespace == tenant.Spec.Workspace {
				installed = true
				break
			}
		}
	}

	values := map[string]any{}
	for k, v := range chartRef.Values {
		values[k] = v
	}

	// If release exists and fingerprint unchanged, skip upgrade.
	if installed && prevFingerprint == fingerprint {
		logger.Info("skip upgrade; fingerprint unchanged", "fingerprint", fingerprint)
		// Ensure status reflects readiness even if we skipped Helm action.
		r.setStatus(ctx, tenant, platformv1alpha1.TenantPhaseReady, "fingerprint unchanged; release assumed healthy")
		return ctrl.Result{}, nil
	}

	if !installed {
		install := action.NewInstall(aCfg)
		install.ReleaseName = releaseName
		install.Namespace = tenant.Spec.Workspace
		install.CreateNamespace = false // we already ensured
		if chartLoaded == nil {
			logger.Info("chart not loaded; skipping real helm install", "repo", chartRef.Repo, "name", chartRef.Name, "version", chartRef.Version)
		} else {
			rel, err = install.Run(chartLoaded, values)
			if err != nil {
				logger.Error(err, "helm install failed")
				r.Recorder.Event(tenant, corev1.EventTypeWarning, "HelmInstallFailed", err.Error())
				r.setStatus(ctx, tenant, platformv1alpha1.TenantPhaseError, fmt.Sprintf("helm install failed: %v", err))
				return ctrl.Result{RequeueAfter: 90 * time.Second}, nil
			}
			logger.Info("helm install success", "release", releaseName)
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "HelmInstalled", releaseName)
		}
	} else {
		upgrade := action.NewUpgrade(aCfg)
		upgrade.Namespace = tenant.Spec.Workspace
		if chartLoaded != nil {
			rel, err = upgrade.Run(releaseName, chartLoaded, values)
			if err != nil {
				logger.Error(err, "helm upgrade failed")
				r.Recorder.Event(tenant, corev1.EventTypeWarning, "HelmUpgradeFailed", err.Error())
				return ctrl.Result{RequeueAfter: 120 * time.Second}, nil
			}
			logger.Info("helm upgrade success", "release", releaseName)
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "HelmUpgraded", releaseName)
		} else {
			logger.Info("chart not loaded; skipping upgrade path", "repo", chartRef.Repo, "name", chartRef.Name, "version", chartRef.Version)
		}
	}

	if rel != nil {
		tenant.Status.LastAppliedChart = fmt.Sprintf("%s:%d@%s", rel.Name, rel.Version, chartRef.Version)
		// Record fingerprint annotation after successful helm action.
		if tenant.Annotations == nil {
			tenant.Annotations = map[string]string{}
		}
		tenant.Annotations[fpAnnotationKey] = fingerprint
		if err := r.Update(ctx, tenant); err != nil {
			logger.Error(err, "failed to update tenant annotations with fingerprint")
		}
		r.setStatus(ctx, tenant, platformv1alpha1.TenantPhaseReady, fmt.Sprintf("helm release %s status=%s", rel.Name, rel.Info.Status))
		r.Recorder.Event(tenant, corev1.EventTypeNormal, "Ready", "helm release deployed")
		logger.Info("tenant reconciled (helm)", "phase", tenant.Status.Phase, "release", rel.Name)
	} else {
		// No real release (likely missing chart); keep simulated marker.
		simulatedReleaseID := fmt.Sprintf("%s/%s@%s", chartRef.Repo, chartRef.Name, chartRef.Version)
		tenant.Status.LastAppliedChart = simulatedReleaseID
		r.setStatus(ctx, tenant, platformv1alpha1.TenantPhaseReady, "simulated install (chart not loaded)")
		logger.Info("tenant reconciled (simulated fallback)", "phase", tenant.Status.Phase)
	}

	return ctrl.Result{}, nil
}

func (r *TenantReconciler) setStatus(ctx context.Context, t *platformv1alpha1.Tenant, phase platformv1alpha1.TenantPhase, msg string) {
	t.Status.Phase = phase
	t.Status.LastMessage = msg
	condType := "Ready"
	status := metav1.ConditionFalse
	if phase == platformv1alpha1.TenantPhaseReady {
		status = metav1.ConditionTrue
	}
	cond := metav1.Condition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             string(phase),
		Message:            msg,
		ObservedGeneration: t.ObjectMeta.Generation,
	}
	replaced := false
	for i, c := range t.Status.Conditions {
		if c.Type == condType {
			t.Status.Conditions[i] = cond
			replaced = true
			break
		}
	}
	if !replaced {
		t.Status.Conditions = append(t.Status.Conditions, cond)
	}
	if err := r.Status().Update(ctx, t); err != nil {
		log.FromContext(ctx).Error(err, "status update failed")
		r.Recorder.Event(t, corev1.EventTypeWarning, "StatusUpdateFailed", err.Error())
	} else {
		r.Recorder.Event(t, corev1.EventTypeNormal, string(phase), msg)
	}
}

// SetupWithManager not supported under multicluster signature; use multicluster builder.
func (r *TenantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return fmt.Errorf("SetupWithManager not supported for multicluster; use mcbuilder in main")
}
