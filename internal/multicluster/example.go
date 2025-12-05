package multicluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	action "helm.sh/helm/v3/pkg/action"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	ctrl "sigs.k8s.io/controller-runtime"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"
	kubeconfigprovider "sigs.k8s.io/multicluster-runtime/providers/kubeconfig"

	platformv1alpha1 "github.com/openkcm/crypto-edge-operator/api/v1alpha1"
	helmutil "github.com/openkcm/crypto-edge-operator/internal/helmutil"
)

// RunMulticlusterExample starts a multicluster manager that reconciles Tenants across discovered clusters.
//
//nolint:maintidx,gocyclo // complexity/maintainability accepted short-term; will refactor into helpers later
func RunMulticlusterExample() {
	var namespace string
	var kubeconfigSecretLabel string
	var kubeconfigSecretKey string
	// Central chart configuration (applies to all Tenants)
	var chartRepo string
	var chartName string
	var chartVersion string
	var chartInstallCRDs bool

	flag.StringVar(&namespace, "namespace", "default", "Namespace where kubeconfig secrets are stored")
	flag.StringVar(&kubeconfigSecretLabel, "kubeconfig-label", "sigs.k8s.io/multicluster-runtime-kubeconfig",
		"Label used to identify secrets containing kubeconfig data")
	flag.StringVar(&kubeconfigSecretKey, "kubeconfig-key", "kubeconfig", "Key in the secret data that contains the kubeconfig")
	flag.StringVar(&chartRepo, "chart-repo", "https://charts.jetstack.io", "Central Helm chart repository URL")
	flag.StringVar(&chartName, "chart-name", "cert-manager", "Central Helm chart name")
	flag.StringVar(&chartVersion, "chart-version", "1.19.1", "Central Helm chart version")
	flag.BoolVar(&chartInstallCRDs, "chart-install-crds", true, "Set installCRDs Helm value (cert-manager requires CRDs on first install)")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	// Ensure Helm paths are sane inside our container/runtime.
	// Explicitly set HELM_* envs so cli.New() picks correct locations.
	if os.Getenv("HELM_REPOSITORY_CONFIG") == "" {
		os.Setenv("HELM_REPOSITORY_CONFIG", "/.config/helm/repositories.yaml")
	}
	if os.Getenv("HELM_REPOSITORY_CACHE") == "" {
		os.Setenv("HELM_REPOSITORY_CACHE", "/.cache/helm/repository")
	}
	if os.Getenv("HELM_REGISTRY_CONFIG") == "" {
		os.Setenv("HELM_REGISTRY_CONFIG", "/.config/helm/registry.json")
	}

	ctrllog.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	entryLog := ctrllog.Log.WithName("multicluster-entrypoint")
	ctx := ctrl.SetupSignalHandler()

	entryLog.Info("Starting multicluster example", "namespace", namespace, "kubeconfigSecretLabel", kubeconfigSecretLabel)

	// Ensure a self kubeconfig secret exists so the provider at least manages the local cluster if user hasn't created one.
	// This uses the in-cluster (or local) rest.Config to synthesise a kubeconfig and store it as a labeled secret.
	hostCfg := ctrl.GetConfigOrDie()
	if err := ensureSelfKubeconfigSecret(ctx, hostCfg, namespace, kubeconfigSecretLabel, kubeconfigSecretKey); err != nil {
		entryLog.Error(err, "failed to ensure self kubeconfig secret")
	}

	// Create scheme including core and platform (Tenant) types prior to provider & manager creation.
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = platformv1alpha1.AddToScheme(scheme)

	providerOpts := kubeconfigprovider.Options{
		Namespace:             namespace,
		KubeconfigSecretLabel: kubeconfigSecretLabel,
		KubeconfigSecretKey:   kubeconfigSecretKey,
		// Critical: propagate our scheme (with Tenant type) into every discovered cluster.
		// Without this, cluster engagement fails when the multicluster manager attempts
		// to start watches for platformv1alpha1.Tenant because the cluster's scheme
		// only contains core types.
		ClusterOptions: []cluster.Option{func(o *cluster.Options) { o.Scheme = scheme }},
	}
	provider := kubeconfigprovider.New(providerOpts)

	managerOpts := mcmanager.Options{Metrics: metricsserver.Options{BindAddress: "0"}, Scheme: scheme}
	mgr, err := mcmanager.New(ctrl.GetConfigOrDie(), provider, managerOpts)
	if err != nil {
		entryLog.Error(err, "Unable to create multicluster manager")
		os.Exit(1)
	}
	if err := provider.SetupWithManager(ctx, mgr); err != nil {
		entryLog.Error(err, "Unable to setup provider")
		os.Exit(1)
	}
	// Scheme already contains Tenant; we will fetch and update Tenants directly per engaged cluster (no shadow model).

	// Multicluster Tenant controller – installs/updates Helm release per cluster.
	if err := mcbuilder.ControllerManagedBy(mgr).
		Named("multicluster-tenants").
		For(&platformv1alpha1.Tenant{}).
		Complete(mcreconcile.Func(func(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
			log := ctrllog.FromContext(ctx).WithValues("cluster", req.ClusterName, "tenant", req.NamespacedName)
			log.V(1).Info("reconcile start")
			cl, err := mgr.GetCluster(ctx, req.ClusterName)
			if err != nil {
				log.Error(err, "get cluster failed")
				return ctrl.Result{}, err
			}
			// Defensive: ensure Tenant type registered in scheme (in case of edge re-engagement scenarios).
			_ = platformv1alpha1.AddToScheme(cl.GetScheme())
			// Ensure CRD exists on this cluster before attempting to fetch local Tenant objects.
			if err := EnsureTenantCRD(ctx, cl.GetClient()); err != nil {
				log.Error(err, "ensure Tenant CRD failed")
				return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
			}
			// Fetch Tenant from THIS cluster (model: each cluster stores its own Tenant objects; no home shadow propagation).
			tenant := &platformv1alpha1.Tenant{}
			if err := cl.GetClient().Get(ctx, req.NamespacedName, tenant); err != nil {
				if apierrors.IsNotFound(err) {
					// Cleanup on NotFound: discover release namespace first, then uninstall and delete that namespace (not "default").
					releaseName := fmt.Sprintf("tenant-%s-%s", req.NamespacedName.Name, req.ClusterName)
					remoteCfg := cl.GetConfig()
					getter := helmutil.NewRemoteRESTClientGetter(remoteCfg)
					aCfg := new(action.Configuration)
					if err2 := aCfg.Init(getter, "default", os.Getenv("HELM_DRIVER"), func(format string, v ...any) { log.V(1).Info(fmt.Sprintf(format, v...)) }); err2 == nil {
						wsName := ""
						lst := action.NewList(aCfg)
						lst.All = true
						if rels, lErr := lst.Run(); lErr == nil {
							for _, r := range rels {
								if r.Name == releaseName {
									wsName = r.Namespace
									break
								}
							}
						}
						if wsName != "" {
							un := action.NewUninstall(aCfg)
							un.Timeout = 120 * time.Second
							un.IgnoreNotFound = true
							if _, unErr := un.Run(releaseName); unErr != nil && !strings.Contains(strings.ToLower(unErr.Error()), "release: not found") {
								log.Error(unErr, "helm uninstall on NotFound failed", "release", releaseName)
							} else {
								log.Info("helm uninstall on NotFound success", "release", releaseName)
							}
							if strings.ToLower(wsName) != "default" {
								nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: wsName}}
								if delErr := cl.GetClient().Delete(ctx, nsObj); delErr != nil {
									if apierrors.IsNotFound(delErr) {
										log.V(1).Info("workspace namespace already deleted", "workspace", wsName)
									} else {
										log.Error(delErr, "failed to delete workspace namespace after NotFound", "workspace", wsName)
										return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
									}
								} else {
									log.Info("workspace namespace delete requested after NotFound", "workspace", wsName)
								}
							} else {
								log.V(1).Info("skip deleting protected namespace", "workspace", wsName)
							}
						}
					}
					return ctrl.Result{}, nil
				}
				log.Error(err, "get tenant failed")
				return ctrl.Result{}, err
			}
			// Keep a copy of the original object for patch operations
			original := tenant.DeepCopy()
			log.V(1).Info("tenant fetched", "phase", tenant.Status.Phase, "workspace", tenant.Spec.Workspace)
			// Helper to emit a minimal Event directly into the cluster where the Tenant lives.
			publishEvent := func(eventType, reason, message string) {
				// Skip if tenant namespace empty (should not happen).
				if tenant.Namespace == "" {
					return
				}
				ev := &corev1.Event{}
				ev.Namespace = tenant.Namespace
				ev.Name = fmt.Sprintf("%s.%d", tenant.Name, time.Now().UnixNano())
				ev.InvolvedObject = corev1.ObjectReference{
					Kind:       "Tenant",
					Namespace:  tenant.Namespace,
					Name:       tenant.Name,
					UID:        tenant.UID,
					APIVersion: platformv1alpha1.GroupVersion.String(),
				}
				ev.Type = eventType
				ev.Reason = reason
				ev.Message = message
				ev.FirstTimestamp = metav1.Now()
				ev.LastTimestamp = ev.FirstTimestamp
				ev.Source = corev1.EventSource{Component: "multicluster-tenants"}
				// Populate deprecated fields still required by validation for corev1.Event objects.
				ev.ReportingController = "mesh.openkcm.io/multicluster-tenants"
				instName := os.Getenv("POD_NAME")
				if instName == "" {
					instName = os.Getenv("HOSTNAME")
				}
				if instName == "" {
					instName = "host-" + req.ClusterName
				}
				ev.ReportingInstance = instName
				ev.Action = reason
				ev.EventTime = metav1.MicroTime{Time: time.Now()}
				ev.Count = 1
				// quick unique fingerprint for dedup detection by consumers
				ev.Series = &corev1.EventSeries{Count: 1, LastObservedTime: ev.EventTime}
				// Attempt create with short timeout.
				cCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				if err := cl.GetClient().Create(cCtx, ev); err != nil && !apierrors.IsAlreadyExists(err) {
					log.Info("event create failed", "reason", reason, "err", err)
				}
			}
			// If clusterRef specified (non-nil) and secretName targets a different cluster, skip.
			if tenant.Spec.ClusterRef != nil && tenant.Spec.ClusterRef.SecretName != "" && tenant.Spec.ClusterRef.SecretName != req.ClusterName {
				log.V(1).Info("skipping cluster due to clusterRef", "cluster", req.ClusterName, "target", tenant.Spec.ClusterRef.SecretName)
				return ctrl.Result{}, nil
			}
			wsName := tenant.Spec.Workspace
			// Handle deletion: uninstall Helm release and delete workspace namespace, then exit reconcile early.
			if tenant.DeletionTimestamp != nil {
				releaseName := fmt.Sprintf("tenant-%s-%s", tenant.Name, req.ClusterName)
				// Build helm action.Configuration using remote cluster rest.Config.
				remoteCfg := cl.GetConfig()
				getter := helmutil.NewRemoteRESTClientGetter(remoteCfg)
				aCfg := new(action.Configuration)
				if err := aCfg.Init(getter, wsName, os.Getenv("HELM_DRIVER"), func(format string, v ...any) { log.V(1).Info(fmt.Sprintf(format, v...)) }); err != nil {
					log.Error(err, "helm configuration init failed during deletion")
				} else {
					un := action.NewUninstall(aCfg)
					un.Timeout = 120 * time.Second
					un.IgnoreNotFound = true
					if _, err := un.Run(releaseName); err != nil && !strings.Contains(strings.ToLower(err.Error()), "release: not found") {
						log.Error(err, "helm uninstall failed", "release", releaseName)
					} else {
						log.Info("helm uninstall success", "release", releaseName)
					}
				}
				// Delete workspace namespace
				nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: wsName}}
				if err := cl.GetClient().Delete(ctx, nsObj); err != nil {
					if apierrors.IsNotFound(err) {
						log.V(1).Info("workspace namespace already deleted", "workspace", wsName)
					} else {
						log.Error(err, "failed to delete workspace namespace", "workspace", wsName)
						// Requeue to retry namespace deletion
						return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
					}
				} else {
					log.Info("workspace namespace delete requested", "workspace", wsName)
				}
				// Nothing else to do for a deleting Tenant
				return ctrl.Result{}, nil
			}
			ns := &corev1.Namespace{}
			if err := cl.GetClient().Get(ctx, client.ObjectKey{Name: wsName}, ns); err != nil {
				if apierrors.IsNotFound(err) {
					create := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: wsName}}
					if err2 := cl.GetClient().Create(ctx, create); err2 != nil {
						log.Error(err2, "failed to create workspace namespace", "workspace", wsName)
						publishEvent(corev1.EventTypeWarning, "WorkspaceCreateFailed", err2.Error())
						return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
					}
					log.Info("workspace namespace created", "workspace", wsName)
					publishEvent(corev1.EventTypeNormal, "WorkspaceCreated", wsName)
				} else {
					return ctrl.Result{}, err
				}
			} else {
				log.Info("workspace namespace exists", "workspace", wsName)
				publishEvent(corev1.EventTypeNormal, "WorkspaceExists", wsName)
			}
			releaseName := fmt.Sprintf("tenant-%s-%s", tenant.Name, req.ClusterName)
			// Use central chart configuration instead of per-Tenant spec.
			chartRefRepo := chartRepo
			chartRefName := chartName
			chartRefVersion := chartVersion
			// Fingerprint spec + chart values for idempotency per cluster.
			fpHasher := sha256.New()
			fpHasher.Write([]byte(chartRefRepo))
			fpHasher.Write([]byte("|"))
			fpHasher.Write([]byte(chartRefName))
			fpHasher.Write([]byte("|"))
			fpHasher.Write([]byte(chartRefVersion))
			// Deterministic iteration of values keys
			valKeys := []string{} // no per-tenant values (central management)
			sort.Strings(valKeys)
			// values intentionally empty
			fingerprint := hex.EncodeToString(fpHasher.Sum(nil))
			annoKey := "mesh.openkcm.io/fingerprint-" + req.ClusterName
			prevFP := tenant.Annotations[annoKey]
			if tenant.Annotations == nil {
				tenant.Annotations = map[string]string{}
			}
			settings := cli.New()
			// Debug: log effective Helm settings to analyze path issues
			ctrllog.Log.WithName("helm-settings").Info("helm paths",
				"RepositoryCache", settings.RepositoryCache,
				"RepositoryConfig", settings.RepositoryConfig,
				"RegistryConfig", settings.RegistryConfig,
				"Env_HELM_CACHE_HOME", os.Getenv("HELM_CACHE_HOME"),
				"Env_HELM_CONFIG_HOME", os.Getenv("HELM_CONFIG_HOME"),
				"Env_HELM_DATA_HOME", os.Getenv("HELM_DATA_HOME"),
				"Env_HELM_REPOSITORY_CONFIG", os.Getenv("HELM_REPOSITORY_CONFIG"),
				"Env_HELM_REPOSITORY_CACHE", os.Getenv("HELM_REPOSITORY_CACHE"),
				"Env_HELM_REGISTRY_CONFIG", os.Getenv("HELM_REGISTRY_CONFIG"),
			)
			// Build helm action.Configuration using remote cluster rest.Config.
			remoteCfg := cl.GetConfig()
			getter := helmutil.NewRemoteRESTClientGetter(remoteCfg)
			aCfg := new(action.Configuration)
			if err := aCfg.Init(getter, wsName, os.Getenv("HELM_DRIVER"), func(format string, v ...any) { log.V(1).Info(fmt.Sprintf(format, v...)) }); err != nil {
				log.Error(err, "helm configuration init failed")
				publishEvent(corev1.EventTypeWarning, "HelmConfigInitFailed", err.Error())
				return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
			}
			var loaded *chart.Chart
			versionValid := true
			if chartRefVersion != "" {
				if _, err := semver.NewVersion(chartRefVersion); err != nil {
					versionValid = false
					log.Error(err, "chart version invalid", "version", chartRefVersion)
				}
			}
			var locateErr error
			if chartRefRepo != "" && versionValid {
				_ = os.MkdirAll(settings.RepositoryCache, 0o755)
				// settings.RepositoryConfig is a file path (repositories.yaml). Ensure parent dir and file.
				if settings.RepositoryConfig != "" {
					_ = os.MkdirAll(filepath.Dir(settings.RepositoryConfig), 0o755)
					// Create file if missing to avoid Helm treating a directory-only path as invalid.
					if _, statErr := os.Stat(settings.RepositoryConfig); os.IsNotExist(statErr) {
						_ = os.WriteFile(settings.RepositoryConfig, []byte("{}\n"), 0o644)
					}
				}
				cp := &action.ChartPathOptions{RepoURL: chartRefRepo, Version: chartRefVersion}
				loc, err := cp.LocateChart(chartRefName, settings)
				if err != nil {
					locateErr = err
					log.Error(err, "locate chart failed")
				} else if c, err2 := loader.Load(loc); err2 == nil {
					loaded = c
				} else {
					locateErr = err2
					log.Error(err2, "chart load failed", "loc", loc)
				}
			}
			values := map[string]any{}
			if chartInstallCRDs {
				// cert-manager chart expects 'installCRDs' to be true on initial bootstrap so API types exist before webhook/manager readiness checks.
				values["installCRDs"] = true
			}
			list := action.NewList(aCfg)
			list.All = true
			installed := false
			if rels, err := list.Run(); err == nil {
				for _, r := range rels {
					if r.Name == releaseName && r.Namespace == wsName {
						installed = true
						break
					}
				}
			}
			// Helper to upsert per-cluster condition.
			upsertCondition := func(cond metav1.Condition) {
				found := false
				for i := range tenant.Status.Conditions {
					c := tenant.Status.Conditions[i]
					if c.Type == cond.Type && c.ObservedGeneration == tenant.Generation && c.Message == cond.Message && c.Reason == cond.Reason && c.Status == cond.Status {
						// identical; keep existing LastTransitionTime
						found = true
						break
					}
					if c.Type == cond.Type {
						// replace
						tenant.Status.Conditions[i] = cond
						found = true
						break
					}
				}
				if !found {
					tenant.Status.Conditions = append(tenant.Status.Conditions, cond)
				}
			}
			condReadyType := "ClusterReady/" + req.ClusterName
			condErrorType := "ClusterError/" + req.ClusterName
			if prevFP == fingerprint && installed {
				log.Info("fingerprint unchanged; skipping helm upgrade", "release", releaseName)
				publishEvent(corev1.EventTypeNormal, "HelmSkip", "fingerprint unchanged")
				upsertCondition(metav1.Condition{Type: condReadyType, Status: metav1.ConditionTrue, Reason: "NoChange", Message: "release up-to-date", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
			}
			// Progress condition type (declared before any goto targets to satisfy compiler).
			progressType := "ClusterProgress/" + req.ClusterName
			if !installed && loaded != nil {
				publishEvent(corev1.EventTypeNormal, "HelmInstallStart", releaseName)
				inst := action.NewInstall(aCfg)
				inst.ReleaseName = releaseName
				inst.Namespace = wsName
				// Wait for resources (including hook Jobs) to be ready before we mark cluster ready.
				inst.Wait = true
				inst.Timeout = 180 * time.Second
				// Mark progress condition (True while running)
				upsertCondition(metav1.Condition{Type: progressType, Status: metav1.ConditionTrue, Reason: "Installing", Message: "helm install in progress", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
				if _, err := inst.Run(loaded, values); err != nil {
					log.Error(err, "helm install failed")
					publishEvent(corev1.EventTypeWarning, "HelmInstallFailed", err.Error())
					upsertCondition(metav1.Condition{Type: condErrorType, Status: metav1.ConditionTrue, Reason: "InstallFailed", Message: err.Error(), ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
					upsertCondition(metav1.Condition{Type: progressType, Status: metav1.ConditionFalse, Reason: "InstallFailed", Message: "helm install failed", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
					// install failed; conditions set
				}
				log.Info("helm install success", "release", releaseName)
				publishEvent(corev1.EventTypeNormal, "HelmInstalled", releaseName)
				tenant.Annotations[annoKey] = fingerprint
				if err := cl.GetClient().Patch(ctx, tenant, client.MergeFrom(original)); err != nil {
					if apierrors.IsNotFound(err) {
						log.V(1).Info("tenant disappeared before annotation patch; ignoring")
					} else {
						log.Error(err, "failed to patch tenant annotation with fingerprint")
					}
				}
				upsertCondition(metav1.Condition{Type: progressType, Status: metav1.ConditionFalse, Reason: "InstallComplete", Message: "helm install complete", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
				upsertCondition(metav1.Condition{Type: condReadyType, Status: metav1.ConditionTrue, Reason: "Installed", Message: "release installed", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
			} else if installed && loaded != nil {
				publishEvent(corev1.EventTypeNormal, "HelmUpgradeStart", releaseName)
				up := action.NewUpgrade(aCfg)
				up.Namespace = wsName
				up.Wait = true
				up.Timeout = 180 * time.Second
				upsertCondition(metav1.Condition{Type: progressType, Status: metav1.ConditionTrue, Reason: "Upgrading", Message: "helm upgrade in progress", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
				if _, err := up.Run(releaseName, loaded, values); err != nil {
					log.Error(err, "helm upgrade failed")
					publishEvent(corev1.EventTypeWarning, "HelmUpgradeFailed", err.Error())
					upsertCondition(metav1.Condition{Type: condErrorType, Status: metav1.ConditionTrue, Reason: "UpgradeFailed", Message: err.Error(), ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
					upsertCondition(metav1.Condition{Type: progressType, Status: metav1.ConditionFalse, Reason: "UpgradeFailed", Message: "helm upgrade failed", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
					// upgrade failed; conditions set
				}
				log.Info("helm upgrade success", "release", releaseName)
				publishEvent(corev1.EventTypeNormal, "HelmUpgraded", releaseName)
				// Patch updated annotation using the original as base
				tenant.Annotations[annoKey] = fingerprint
				if err := cl.GetClient().Patch(ctx, tenant, client.MergeFrom(original)); err != nil {
					if apierrors.IsNotFound(err) {
						log.V(1).Info("tenant disappeared before annotation patch; ignoring")
					} else {
						log.Error(err, "failed to patch tenant annotation with fingerprint")
					}
				}
				upsertCondition(metav1.Condition{Type: progressType, Status: metav1.ConditionFalse, Reason: "UpgradeComplete", Message: "helm upgrade complete", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
				upsertCondition(metav1.Condition{Type: condReadyType, Status: metav1.ConditionTrue, Reason: "Upgraded", Message: "release upgraded", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
			} else {
				// Handle scenarios where chart not loaded or invalid without hammering status every loop.
				if !versionValid {
					log.Info("chart version invalid; skipping helm action", "version", chartRefVersion)
					publishEvent(corev1.EventTypeWarning, "ChartVersionInvalid", chartRefVersion)
					upsertCondition(metav1.Condition{Type: condErrorType, Status: metav1.ConditionTrue, Reason: "VersionInvalid", Message: "chart version invalid", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
				} else if locateErr != nil && strings.Contains(strings.ToLower(locateErr.Error()), "invalid_reference") {
					log.Info("chart version not found in repo", "version", chartRefVersion)
					publishEvent(corev1.EventTypeWarning, "ChartVersionNotFound", chartRefVersion)
					upsertCondition(metav1.Condition{Type: condErrorType, Status: metav1.ConditionTrue, Reason: "VersionNotFound", Message: "chart version not found in repository", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
				} else if loaded == nil {
					log.Info("chart not loaded; skipping helm action", "repo", chartRefRepo)
					publishEvent(corev1.EventTypeWarning, "ChartNotLoaded", chartRefRepo)
					// Non-fatal error condition only added once per generation (avoid loop churn).
					alreadySet := false
					for _, c := range tenant.Status.Conditions {
						if c.Type == condErrorType && c.Reason == "ChartNotLoaded" && c.ObservedGeneration == tenant.Generation {
							alreadySet = true
							break
						}
					}
					if !alreadySet {
						upsertCondition(metav1.Condition{Type: condErrorType, Status: metav1.ConditionTrue, Reason: "ChartNotLoaded", Message: "chart not loaded; repo unreachable?", ObservedGeneration: tenant.Generation, LastTransitionTime: metav1.Now()})
					}
				}
			}
			// Phase aggregation (label preserved from previous goto target)
			// phase conditions already set above
			// Aggregate overall Phase with nuanced error severity.
			// Fatal errors: InstallFailed, UpgradeFailed. Non-fatal/spec errors: ChartNotLoaded, VersionNotFound, VersionInvalid.
			allowChartSkip := os.Getenv("ALLOW_CHART_SKIP") == "true"
			phase := platformv1alpha1.TenantPhasePending
			hadFatalError := false
			hadReady := false
			nonFatalErrorPresent := false
			for _, c := range tenant.Status.Conditions {
				if strings.HasPrefix(c.Type, "ClusterError/") && c.Status == metav1.ConditionTrue {
					// Inspect Reason for severity.
					switch c.Reason {
					case "InstallFailed", "UpgradeFailed":
						hadFatalError = true
					case "ChartNotLoaded", "VersionNotFound", "VersionInvalid":
						nonFatalErrorPresent = true
					}
				}
				if strings.HasPrefix(c.Type, "ClusterReady/") && c.Status == metav1.ConditionTrue {
					hadReady = true
				}
			}
			// Determine phase precedence.
			if hadFatalError {
				phase = platformv1alpha1.TenantPhaseError
			} else if hadReady && (allowChartSkip || !nonFatalErrorPresent) {
				// Ready wins if there is a ready condition and either no non-fatal error OR user allows skip.
				phase = platformv1alpha1.TenantPhaseReady
			} else if nonFatalErrorPresent {
				// Remain Pending for non-fatal issues (e.g. repo temporarily unreachable) unless allowChartSkip flips to Ready.
				phase = platformv1alpha1.TenantPhasePending
			}
			tenant.Status.Phase = phase
			// Retry status update (rate limiter/context cancellations can occur under load).
			updateErr := func() error {
				var lastErr error
				for range 3 { // Go 1.25 int range loop
					// Short timeout per attempt to avoid hanging entire reconcile.
					uCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					// Use a status patch based on the latest fetched object to avoid UID/resourceVersion precondition issues
					lastErr = cl.GetClient().Status().Patch(uCtx, tenant, client.MergeFrom(original))
					cancel()
					if lastErr != nil && apierrors.IsNotFound(lastErr) {
						// Object no longer exists; nothing to update.
						return nil
					}
					if lastErr == nil {
						return nil
					}
					if strings.Contains(lastErr.Error(), "context canceled") {
						// Backoff a bit; underlying rate limiter may be throttling.
						time.Sleep(500 * time.Millisecond)
						continue
					}
					// For other errors, still retry but shorter backoff.
					time.Sleep(250 * time.Millisecond)
				}
				return lastErr
			}()
			if updateErr != nil {
				log.Error(updateErr, "failed to update tenant status phase/conditions after retries")
				publishEvent(corev1.EventTypeWarning, "StatusUpdateFailed", updateErr.Error())
				return ctrl.Result{RequeueAfter: 45 * time.Second}, nil
			}
			publishEvent(corev1.EventTypeNormal, "PhaseSet", string(phase))
			// If we only have non-fatal chart load issues and allowChartSkip is true, avoid tight requeue.
			if allowChartSkip && !hadFatalError && !hadReady && nonFatalErrorPresent {
				return ctrl.Result{RequeueAfter: 120 * time.Second}, nil
			}
			return ctrl.Result{}, nil
		})); err != nil {
		entryLog.Error(err, "unable to create multicluster tenant controller")
		os.Exit(1)
	}

	if err := mgr.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		entryLog.Error(err, "unable to start multicluster manager")
		os.Exit(1)
	}
}

// ensureSelfKubeconfigSecret has been moved to operator.go to avoid duplication.
