package multicluster

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	helmrelease "helm.sh/helm/v3/pkg/release"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metautil "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1unstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"

	platformv1alpha1 "github.com/openkcm/krypton-operator/api/v1alpha1"
	helmutil "github.com/openkcm/krypton-operator/internal/helmutil"
	secretprovider "github.com/openkcm/krypton-operator/multicluster/secretprovider"
)

// RunKryptonOperator starts the operator for KryptonDeployment resources.
func RunKryptonOperator() {
	var namespace string
	var kubeconfigSecretLabel string
	var kubeconfigSecretKey string
	var watchKubeconfig string
	var watchContext string
	var ensureWatchCRDs bool
	var watchKubeconfigSecretName string
	var watchKubeconfigSecretNamespace string
	var watchKubeconfigSecretKey string
	var chartRepo string
	var chartName string
	var chartVersion string
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.StringVar(&namespace, "namespace", namespace, "Namespace containing kubeconfig secrets")
	flag.StringVar(&watchContext, "watch-context", watchContext, "Kubeconfig context name for the watch cluster")
	flag.BoolVar(&ensureWatchCRDs, "ensure-watch-crds", ensureWatchCRDs, "Ensure required CRDs exist on the watch cluster at startup")
	flag.StringVar(&watchKubeconfigSecretName, "watch-kubeconfig-secret", watchKubeconfigSecretName, "Name of Secret containing kubeconfig for the watch cluster")
	flag.StringVar(&watchKubeconfigSecretNamespace, "watch-kubeconfig-secret-namespace", watchKubeconfigSecretNamespace, "Namespace of Secret containing kubeconfig for the watch cluster")
	flag.StringVar(&watchKubeconfigSecretKey, "watch-kubeconfig-secret-key", "kubeconfig", "Data key in the Secret containing kubeconfig bytes")
	flag.StringVar(&chartRepo, "chart-repo", "https://charts.jetstack.io", "Central Helm chart repository URL")
	flag.StringVar(&chartName, "chart-name", "cert-manager", "Central Helm chart name")
	flag.StringVar(&chartVersion, "chart-version", "1.19.1", "Central Helm chart version")
	flag.Parse()

	// Environment fallbacks
	if ns := os.Getenv("WATCH_NAMESPACE"); ns != "" {
		namespace = ns
	} else if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		namespace = ns
	} else if b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if s := string(b); s != "" {
			namespace = s
		}
	}
	if lbl := os.Getenv("KUBECONFIG_SECRET_LABEL"); lbl != "" {
		kubeconfigSecretLabel = lbl
	}
	if key := os.Getenv("KUBECONFIG_SECRET_KEY"); key != "" {
		kubeconfigSecretKey = key
	}
	if v := os.Getenv("WATCH_KUBECONFIG"); v != "" {
		watchKubeconfig = v
	} else if v := os.Getenv("WATCH_CLUSTER_KUBECONFIG"); v != "" { // alias
		watchKubeconfig = v
	}
	if v := os.Getenv("WATCH_CONTEXT"); v != "" {
		watchContext = v
	}
	if v := os.Getenv("ENSURE_WATCH_CRDS"); v == "true" || v == "1" || v == "yes" {
		ensureWatchCRDs = true
	}
	if v := os.Getenv("WATCH_KUBECONFIG_SECRET"); v != "" {
		watchKubeconfigSecretName = v
	}
	if v := os.Getenv("WATCH_KUBECONFIG_SECRET_NAMESPACE"); v != "" {
		watchKubeconfigSecretNamespace = v
	}
	if v := os.Getenv("WATCH_KUBECONFIG_SECRET_KEY"); v != "" {
		watchKubeconfigSecretKey = v
	}
	if watchKubeconfigSecretNamespace == "" {
		watchKubeconfigSecretNamespace = namespace
	}

	setupHelmEnv()

	ctrllog.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	entryLog := ctrllog.Log.WithName("krypton-operator")
	ctx := ctrl.SetupSignalHandler()

	entryLog.Info("Starting Krypton operator", "namespace", namespace, "chart", fmt.Sprintf("%s/%s:%s", chartRepo, chartName, chartVersion))
	entryLog.Info("Kubeconfig provider config", "namespace", namespace, "secretLabel", kubeconfigSecretLabel, "secretKey", kubeconfigSecretKey)

	watchCfg, watchHost, watchCtxUsed, err := buildWatchConfig(ctx, watchKubeconfig, watchContext, watchKubeconfigSecretNamespace, watchKubeconfigSecretName, watchKubeconfigSecretKey)
	if err != nil {
		entryLog.Error(err, "Failed to build watch cluster config")
		os.Exit(1)
	}
	entryLog.Info("Watch cluster configured", "host", watchHost, "kubeconfig", truncatePath(watchKubeconfig), "context", watchCtxUsed)

	homeScheme, edgeScheme, providerScheme := buildSchemes()

	homeMgr, err := createHomeManager(watchCfg, homeScheme)
	if err != nil {
		entryLog.Error(err, "Unable to create home manager")
		os.Exit(1)
	}

	provider := secretprovider.New(homeMgr.GetClient(), edgeScheme, namespace)

	mgr, err := createMCManager(watchCfg, provider, providerScheme)
	if err != nil {
		entryLog.Error(err, "Unable to create manager")
		os.Exit(1)
	}

	if err := setupProviderAndHealth(ctx, mgr, provider); err != nil {
		entryLog.Error(err, "Unable to setup provider/health")
		os.Exit(1)
	}

	if err := ensureWatchCRDsIfRequested(ctx, watchCfg, homeScheme, ensureWatchCRDs); err != nil {
		entryLog.Error(err, "Ensure watch CRDs failed")
		os.Exit(1)
	}

	if err := registerHomeController(homeMgr, mgr, namespace, chartRepo, chartName, chartVersion); err != nil {
		entryLog.Error(err, "Unable to create home controller")
		os.Exit(1)
	}

	entryLog.Info("Starting managers")
	if err := startManagers(ctx, entryLog, homeMgr, mgr); err != nil {
		entryLog.Error(err, "Managers exited with error")
		os.Exit(1)
	}
}
func setupHelmEnv() {
	if os.Getenv("HELM_REPOSITORY_CONFIG") == "" {
		os.Setenv("HELM_REPOSITORY_CONFIG", "/.config/helm/repositories.yaml")
	}
	if os.Getenv("HELM_REPOSITORY_CACHE") == "" {
		os.Setenv("HELM_REPOSITORY_CACHE", "/.cache/helm/repository")
	}
	if os.Getenv("HELM_REGISTRY_CONFIG") == "" {
		os.Setenv("HELM_REGISTRY_CONFIG", "/.config/helm/registry.json")
	}
	if os.Getenv("HELM_DRIVER") == "" {
		os.Setenv("HELM_DRIVER", "secret")
	}
}

func buildSchemes() (*runtime.Scheme, *runtime.Scheme, *runtime.Scheme) {
	homeScheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(homeScheme)
	_ = platformv1alpha1.AddToScheme(homeScheme)

	edgeScheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(edgeScheme)
	_ = platformv1alpha1.AddToScheme(edgeScheme)

	providerScheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(providerScheme)
	_ = platformv1alpha1.AddToScheme(providerScheme)
	return homeScheme, edgeScheme, providerScheme
}

func createHomeManager(cfg *rest.Config, scheme *runtime.Scheme) (ctrl.Manager, error) {
	return ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "",
	})
}

func createMCManager(cfg *rest.Config, provider *secretprovider.Provider, scheme *runtime.Scheme) (mcmanager.Manager, error) {
	managerOpts := mcmanager.Options{
		Metrics:                metricsserver.Options{BindAddress: ":8080"},
		Scheme:                 scheme,
		HealthProbeBindAddress: ":8081",
		WebhookServer:          webhook.NewServer(webhook.Options{Port: 0}),
	}
	return mcmanager.New(cfg, provider, managerOpts)
}

func setupProviderAndHealth(ctx context.Context, mgr mcmanager.Manager, provider *secretprovider.Provider) error {
	if err := provider.SetupWithManager(ctx, mgr); err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("healthz", func(_ *http.Request) error { return nil }); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", func(_ *http.Request) error { return nil }); err != nil {
		return err
	}
	return nil
}

func ensureWatchCRDsIfRequested(ctx context.Context, cfg *rest.Config, scheme *runtime.Scheme, ensure bool) error {
	if !ensure {
		return nil
	}
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	if err := EnsureKryptonDeploymentCRD(ctx, cl); err != nil {
		return err
	}
	return nil
}

func registerHomeController(homeMgr ctrl.Manager, mgr mcmanager.Manager, namespace, chartRepo, chartName, chartVersion string) error {
	return ctrl.NewControllerManagedBy(homeMgr).
		Named("kryptondeployments-home").
		For(&platformv1alpha1.KryptonDeployment{}).
		Complete(reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
			log := ctrllog.FromContext(ctx).WithValues("deployment", req.NamespacedName)
			log.Info("reconcile start")
			recorder := homeMgr.GetEventRecorderFor("krypton-operator")
			return reconcileCED(ctx, log, homeMgr, mgr, namespace, chartRepo, chartName, chartVersion, recorder, req)
		}))
}

func startManagers(ctx context.Context, entryLog logr.Logger, homeMgr ctrl.Manager, mgr mcmanager.Manager) error {
	done := make(chan error, 2)
	go func() { done <- homeMgr.Start(ctx) }()
	go func() { done <- mgr.Start(ctx) }()
	for range 2 {
		if err := <-done; err != nil {
			return err
		}
	}
	return nil
}

// buildWatchConfig returns a rest.Config for the watch/home cluster, the host it targets, the effective context used, and error.
func buildWatchConfig(ctx context.Context, kubeconfigPath, kubeContext, secretNS, secretName, secretKey string) (*rest.Config, string, string, error) {
	// If a Secret name is provided, load kubeconfig from that Secret in the current cluster.
	if secretName != "" {
		baseCfg := ctrl.GetConfigOrDie()
		sch := runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(sch)
		cl, err := client.New(baseCfg, client.Options{Scheme: sch})
		if err != nil {
			return nil, "", "", fmt.Errorf("construct client failed for secret fetch: %w", err)
		}
		sec := &corev1.Secret{}
		if err := cl.Get(ctx, client.ObjectKey{Namespace: secretNS, Name: secretName}, sec); err != nil {
			return nil, "", "", fmt.Errorf("fetch secret %s/%s failed: %w", secretNS, secretName, err)
		}
		data, ok := sec.Data[secretKey]
		if !ok || len(data) == 0 {
			return nil, "", "", fmt.Errorf("secret %s/%s missing key %q", secretNS, secretName, secretKey)
		}
		raw, err := clientcmd.Load(data)
		if err != nil {
			return nil, "", "", fmt.Errorf("parse kubeconfig from secret failed: %w", err)
		}
		overrides := &clientcmd.ConfigOverrides{}
		usedCtx := raw.CurrentContext
		if kubeContext != "" {
			overrides.CurrentContext = kubeContext
			usedCtx = kubeContext
		}
		cfg, err := clientcmd.NewDefaultClientConfig(*raw, overrides).ClientConfig()
		if err != nil {
			return nil, "", "", fmt.Errorf("build rest config from secret failed: %w", err)
		}
		return cfg, cfg.Host, usedCtx, nil
	}
	// If a kubeconfig path is provided, load from file with optional context.
	if kubeconfigPath != "" {
		loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath}
		overrides := &clientcmd.ConfigOverrides{}
		usedCtx := ""
		if kubeContext != "" {
			overrides.CurrentContext = kubeContext
			usedCtx = kubeContext
		} else {
			// Read the raw config to determine the current-context
			rawCfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).RawConfig()
			if err == nil {
				usedCtx = rawCfg.CurrentContext
			}
		}
		cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).ClientConfig()
		if err != nil {
			return nil, "", "", fmt.Errorf("loading watch kubeconfig failed: %w", err)
		}
		return cfg, cfg.Host, usedCtx, nil
	}
	// Fallback to in-cluster or default kubeconfig via controller-runtime.
	cfg := ctrl.GetConfigOrDie()
	return cfg, cfg.Host, "in-cluster", nil
}

func truncatePath(p string) string {
	if p == "" {
		return ""
	}
	if len(p) > 64 {
		return "…" + p[len(p)-61:]
	}
	return p
}

// (duplicate block removed)

func reconcileCED(
	ctx context.Context,
	log logr.Logger,
	homeMgr ctrl.Manager,
	mgr mcmanager.Manager,
	namespace string,
	chartRepo, chartName, chartVersion string,
	recorder record.EventRecorder,
	req reconcile.Request,
) (reconcile.Result, error) {
	setReadyCondition := func(dep *platformv1alpha1.KryptonDeployment, status metav1.ConditionStatus, reason, message string) {
		metautil.SetStatusCondition(&dep.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             status,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: dep.Generation,
		})
	}

	const finalizerName = "mesh.openkcm.io/kryptondeployment-finalizer"

	deployment := &platformv1alpha1.KryptonDeployment{}
	if err := homeMgr.GetClient().Get(ctx, req.NamespacedName, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("deployment not found, assuming deleted")
			return reconcile.Result{}, nil
		}
		log.Error(err, "failed to fetch deployment")
		return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Validate inline account info is present
	if deployment.Spec.Account.Name == "" {
		deployment.Status.Phase = platformv1alpha1.KryptonDeploymentPhaseError
		deployment.Status.LastMessage = "Spec.account.name must be set"
		setReadyCondition(deployment, metav1.ConditionFalse, "AccountMissing", deployment.Status.LastMessage)
		_ = homeMgr.GetClient().Status().Update(ctx, deployment)
		recorder.Event(deployment, corev1.EventTypeWarning, "AccountMissing", deployment.Status.LastMessage)
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	log.Info("account info validated", "account", deployment.Spec.Account.Name)
	recorder.Event(deployment, corev1.EventTypeNormal, "AccountValidated", "Account "+deployment.Spec.Account.Name+" validated")

	// Handle deletion fast-path
	if !deployment.DeletionTimestamp.IsZero() {
		return rcedHandleDelete(ctx, log, homeMgr, mgr, namespace, recorder, deployment)
	}

	foundFinalizer := slices.Contains(deployment.GetFinalizers(), finalizerName)
	if !foundFinalizer {
		deployment.SetFinalizers(append(deployment.GetFinalizers(), finalizerName))
		if err := homeMgr.GetClient().Update(ctx, deployment); err != nil {
			log.Error(err, "failed to add finalizer")
			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}
		log.Info("finalizer added")
		recorder.Event(deployment, corev1.EventTypeNormal, "FinalizerAdded", "Finalizer added for cleanup on delete")
	}

	targetSecretName, regionName := rcedResolveTarget(ctx, homeMgr, namespace, deployment)
	log.Info("routing to edge cluster", "targetRegion", regionName, "targetSecret", targetSecretName)
	recorder.Event(deployment, corev1.EventTypeNormal, "Routing", "Routing to region "+regionName)

	edgeCluster, err := mgr.GetCluster(ctx, targetSecretName)
	if err != nil {
		log.Error(err, "failed to get edge cluster", "targetRegion", regionName)
		deployment.Status.Phase = platformv1alpha1.KryptonDeploymentPhaseError
		deployment.Status.LastMessage = "Edge cluster " + regionName + " not available"
		setReadyCondition(deployment, metav1.ConditionFalse, "EdgeUnavailable", deployment.Status.LastMessage)
		_ = homeMgr.GetClient().Status().Update(ctx, deployment)
		recorder.Event(deployment, corev1.EventTypeWarning, "EdgeUnavailable", deployment.Status.LastMessage)
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}

	nsName := deployment.Name
	if res, err := rcedEnsureNamespace(ctx, log, edgeCluster, nsName, deployment, recorder); err != nil || res.RequeueAfter > 0 {
		return res, err
	}

	return rcedDeployAndStatus(ctx, log, homeMgr, edgeCluster, nsName, chartRepo, chartName, chartVersion, deployment, recorder, setReadyCondition)
}

func rcedHandleDelete(
	ctx context.Context,
	log logr.Logger,
	homeMgr ctrl.Manager,
	mgr mcmanager.Manager,
	namespace string,
	recorder record.EventRecorder,
	deployment *platformv1alpha1.KryptonDeployment,
) (reconcile.Result, error) {
	const finalizerName = "mesh.openkcm.io/kryptondeployment-finalizer"
	targetSecretName, regionName := rcedResolveTarget(ctx, homeMgr, namespace, deployment)
	log.Info("deletion: routing to edge cluster", "targetRegion", regionName, "targetSecret", targetSecretName)
	recorder.Event(deployment, corev1.EventTypeNormal, "DeleteStarted", "Routing delete to region "+regionName)
	edgeCluster, err := mgr.GetCluster(ctx, targetSecretName)
	if err == nil {
		releaseName := "ced-" + deployment.Name
		if err := uninstallHelmChart(ctx, log, edgeCluster, deployment.Name, releaseName); err != nil {
			log.Error(err, "deletion: helm uninstall failed", "release", releaseName)
			recorder.Event(deployment, corev1.EventTypeWarning, "HelmUninstallFailed", err.Error())
		} else {
			log.Info("deletion: helm uninstall succeeded", "release", releaseName)
			recorder.Event(deployment, corev1.EventTypeNormal, "HelmUninstalled", "Uninstalled release "+releaseName)
		}
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: deployment.Name}}
		if err := edgeCluster.GetClient().Delete(ctx, ns); err != nil {
			if !apierrors.IsNotFound(err) {
				log.Error(err, "deletion: failed to delete namespace", "namespace", deployment.Name)
			}
		} else {
			log.Info("deletion: namespace delete requested", "namespace", deployment.Name)
			recorder.Event(deployment, corev1.EventTypeNormal, "EdgeNamespaceDeleteRequested", "Requested deletion for namespace "+deployment.Name)
		}
	} else {
		log.Error(err, "deletion: failed to get edge cluster", "targetRegion", regionName)
	}
	// remove finalizer
	finalizers := deployment.GetFinalizers()
	newFinalizers := make([]string, 0, len(finalizers))
	for _, f := range finalizers {
		if f != finalizerName {
			newFinalizers = append(newFinalizers, f)
		}
	}
	deployment.SetFinalizers(newFinalizers)
	if err := homeMgr.GetClient().Update(ctx, deployment); err != nil {
		log.Error(err, "deletion: failed to remove finalizer")
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}
	log.Info("deletion: finalizer removed")
	recorder.Event(deployment, corev1.EventTypeNormal, "FinalizerRemoved", "Finalizer removed, deletion complete")
	return reconcile.Result{}, nil
}

func rcedResolveTarget(ctx context.Context, homeMgr ctrl.Manager, namespace string, deployment *platformv1alpha1.KryptonDeployment) (secretKey, regionName string) {
	regionName = deployment.Spec.Region.Name
	// Prefer new kubeconfig ref if provided
	if deployment.Spec.Region.Kubeconfig != nil {
		name := deployment.Spec.Region.Kubeconfig.Secret.Name
		ns := deployment.Spec.Region.Kubeconfig.Secret.Namespace
		if name != "" && ns != "" {
			return ns + "/" + name, regionName
		}
		if name != "" {
			// Namespace not specified; default to operator discovery namespace
			return namespace + "/" + name, regionName
		}
	}
	// Backward compatibility: use deprecated field
	if deployment.Spec.Region.KubeconfigSecretName != "" {
		return namespace + "/" + deployment.Spec.Region.KubeconfigSecretName, regionName
	}
	// Default: derive secret name from region in discovery namespace
	return namespace + "/" + (regionName + "-kubeconfig"), regionName
}

func rcedEnsureNamespace(
	ctx context.Context,
	log logr.Logger,
	edgeCluster cluster.Cluster,
	nsName string,
	deployment *platformv1alpha1.KryptonDeployment,
	recorder record.EventRecorder,
) (reconcile.Result, error) {
	ns := &corev1.Namespace{}
	// Use uncached reader for GET to avoid "cache not started" errors on freshly created clusters.
	if err := edgeCluster.GetAPIReader().Get(ctx, client.ObjectKey{Name: nsName}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			ns = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}
			if err := edgeCluster.GetClient().Create(ctx, ns); err != nil {
				log.Error(err, "failed to create namespace on edge", "namespace", nsName)
				return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
			}
			log.Info("created namespace on edge cluster", "namespace", nsName, "region", deployment.Spec.Region.Name)
			recorder.Event(deployment, corev1.EventTypeNormal, "EdgeNamespaceCreated", "Namespace "+nsName+" created on "+deployment.Spec.Region.Name)
		} else {
			log.Error(err, "failed to check namespace")
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}
	return reconcile.Result{}, nil
}

func rcedDeployAndStatus(
	ctx context.Context,
	log logr.Logger,
	homeMgr ctrl.Manager,
	edgeCluster cluster.Cluster,
	nsName string,
	chartRepo, chartName, chartVersion string,
	deployment *platformv1alpha1.KryptonDeployment,
	recorder record.EventRecorder,
	setReadyCondition func(*platformv1alpha1.KryptonDeployment, metav1.ConditionStatus, string, string),
) (reconcile.Result, error) {
	releaseName := "ced-" + deployment.Name

	// First, check for missing resources/release and repair if needed to capture events.
	repaired, missingList, err := rcedCheckAndHeal(ctx, log, edgeCluster, nsName, releaseName, chartRepo, chartName, chartVersion, recorder, deployment)
	if err != nil {
		log.Error(err, "health check failed")
		deployment.Status.Phase = platformv1alpha1.KryptonDeploymentPhaseError
		deployment.Status.LastMessage = "Health check failed: " + err.Error()
		setReadyCondition(deployment, metav1.ConditionFalse, "HealthCheckFailed", deployment.Status.LastMessage)
		_ = homeMgr.GetClient().Status().Update(ctx, deployment)
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if repaired {
		msg := "Detected missing resources, triggered repair: " + strings.Join(missingList, ", ")
		log.Info(msg)
		recorder.Event(deployment, corev1.EventTypeWarning, "RepairTriggered", msg)
		deployment.Status.Phase = platformv1alpha1.KryptonDeploymentPhasePending
		deployment.Status.LastMessage = msg
		setReadyCondition(deployment, metav1.ConditionFalse, "RepairTriggered", msg)
		_ = homeMgr.GetClient().Status().Update(ctx, deployment)
		return reconcile.Result{RequeueAfter: 20 * time.Second}, nil
	}

	// No repair needed; still run deploy/upgrade to converge drift/config changes idempotently.
	if err := deployHelmChart(ctx, log, edgeCluster, nsName, releaseName, chartRepo, chartName, chartVersion); err != nil {
		log.Error(err, "helm deployment failed")
		deployment.Status.Phase = platformv1alpha1.KryptonDeploymentPhaseError
		deployment.Status.LastMessage = "Helm deployment failed: " + err.Error()
		setReadyCondition(deployment, metav1.ConditionFalse, "HelmFailed", deployment.Status.LastMessage)
		_ = homeMgr.GetClient().Status().Update(ctx, deployment)
		recorder.Event(deployment, corev1.EventTypeWarning, "HelmDeployFailed", deployment.Status.LastMessage)
		return reconcile.Result{RequeueAfter: 60 * time.Second}, nil
	}
	log.Info("helm deploy succeeded", "release", releaseName, "namespace", nsName)
	recorder.Event(deployment, corev1.EventTypeNormal, "HelmDeployed", "Release "+releaseName+" deployed to "+nsName)

	// Verify runtime health (Deployments available) and surface degraded state if not ready yet.
	healthy, notReady := rcedNamespaceWorkloadHealthy(ctx, edgeCluster, nsName)
	if !healthy {
		msg := "Workloads not yet ready: " + strings.Join(notReady, ", ")
		log.Info(msg)
		recorder.Event(deployment, corev1.EventTypeNormal, "NotReady", msg)
		deployment.Status.Phase = platformv1alpha1.KryptonDeploymentPhasePending
		deployment.Status.LastMessage = msg
		setReadyCondition(deployment, metav1.ConditionFalse, "NotReady", msg)
		_ = homeMgr.GetClient().Status().Update(ctx, deployment)
		return reconcile.Result{RequeueAfter: 15 * time.Second}, nil
	}

	deployment.Status.Phase = platformv1alpha1.KryptonDeploymentPhaseReady
	deployment.Status.LastMessage = "Successfully deployed to region " + deployment.Spec.Region.Name
	deployment.Status.LastAppliedChart = chartRepo + "/" + chartName + ":" + chartVersion
	setReadyCondition(deployment, metav1.ConditionTrue, "Deployed", "Successfully deployed to region "+deployment.Spec.Region.Name)
	if err := homeMgr.GetClient().Status().Update(ctx, deployment); err != nil {
		log.Error(err, "failed to update status")
	}
	log.Info("deployment reconciled successfully", "region", deployment.Spec.Region.Name)
	recorder.Event(deployment, corev1.EventTypeNormal, "Ready", "Deployment ready in region "+deployment.Spec.Region.Name)
	// Periodic recheck interval
	return reconcile.Result{RequeueAfter: getCheckInterval()}, nil
}

func deployHelmChart(ctx context.Context, log logr.Logger, edgeCluster cluster.Cluster, namespace, releaseName, chartRepo, chartName, chartVersion string) error {
	// Get REST config for edge cluster
	restCfg := edgeCluster.GetConfig()
	// Ensure Helm uses the target namespace for applying manifests and storing release data
	getter := helmutil.NewRemoteRESTClientGetterForNamespace(restCfg, namespace)

	// Initialize Helm action config
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(getter, namespace, os.Getenv("HELM_DRIVER"), func(format string, v ...any) {
		log.V(1).Info(fmt.Sprintf(format, v...))
	}); err != nil {
		return fmt.Errorf("helm init failed: %w", err)
	}

	// Download chart from direct tarball URL into a temp file, then load
	settings := cli.New()
	_ = settings // reserved for future use if needed
	chartTarURL := chartRepo + "/" + chartName + "-" + chartVersion + ".tgz"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, chartTarURL, nil)
	if err != nil {
		return fmt.Errorf("failed to build download request: %w", err)
	}
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download chart: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download chart: status %d", resp.StatusCode)
	}
	tmpPath := filepath.Join(os.TempDir(), chartName+"-"+chartVersion+".tgz")
	out, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		return fmt.Errorf("failed to write chart file: %w", err)
	}
	out.Close()

	chartObj, err := loader.Load(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to load chart: %w", err)
	}

	// Check if release exists
	histClient := action.NewHistory(actionConfig)
	histClient.Max = 1
	_, err = histClient.Run(releaseName)

	values := map[string]any{
		"replicaCount": 1,
	}

	if err != nil {
		// Release doesn't exist - install
		install := action.NewInstall(actionConfig)
		install.Namespace = namespace
		install.ReleaseName = releaseName
		install.Wait = false
		install.Timeout = 300 * time.Second

		_, err = install.Run(chartObj, values)
		if err != nil {
			return fmt.Errorf("helm install failed: %w", err)
		}
		log.Info("helm chart installed", "release", releaseName)
	} else {
		// Release exists - upgrade
		upgrade := action.NewUpgrade(actionConfig)
		upgrade.Namespace = namespace
		upgrade.Wait = false
		upgrade.Timeout = 300 * time.Second

		_, err = upgrade.Run(releaseName, chartObj, values)
		if err != nil {
			return fmt.Errorf("helm upgrade failed: %w", err)
		}
		log.Info("helm chart upgraded", "release", releaseName)
	}

	return nil
}

func uninstallHelmChart(ctx context.Context, log logr.Logger, edgeCluster cluster.Cluster, namespace, releaseName string) error {
	// Get REST config for edge cluster
	restCfg := edgeCluster.GetConfig()
	getter := helmutil.NewRemoteRESTClientGetterForNamespace(restCfg, namespace)

	// Initialize Helm action config
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(getter, namespace, os.Getenv("HELM_DRIVER"), func(format string, v ...any) {
		log.V(1).Info(fmt.Sprintf(format, v...))
	}); err != nil {
		return fmt.Errorf("helm init failed: %w", err)
	}

	// If release doesn't exist, nothing to uninstall
	histClient := action.NewHistory(actionConfig)
	histClient.Max = 1
	if _, err := histClient.Run(releaseName); err != nil {
		log.V(1).Info("uninstall: release not found, skipping", "release", releaseName)
		return nil
	}

	// Uninstall release
	uninstall := action.NewUninstall(actionConfig)
	uninstall.Timeout = 300 * time.Second
	uninstall.KeepHistory = false
	if _, err := uninstall.Run(releaseName); err != nil {
		return fmt.Errorf("helm uninstall failed: %w", err)
	}
	log.Info("helm uninstall completed", "release", releaseName)
	return nil
}

// getCheckInterval returns the configured health-check reconcile interval.
// It reads KRYPTON_CHECK_INTERVAL env var, accepting Go duration strings (e.g., "60s", "5m")
// or integer seconds. Defaults to 60s.
var (
	checkIntervalOnce sync.Once
	checkIntervalDur  time.Duration
)

func getCheckInterval() time.Duration {
	checkIntervalOnce.Do(func() {
		v := strings.TrimSpace(os.Getenv("KRYPTON_CHECK_INTERVAL"))
		if v == "" {
			checkIntervalDur = 60 * time.Second
			return
		}
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			checkIntervalDur = d
			return
		}
		if s, err := strconv.Atoi(v); err == nil && s > 0 {
			checkIntervalDur = time.Duration(s) * time.Second
			return
		}
		checkIntervalDur = 60 * time.Second
	})
	return checkIntervalDur
}

// rcedCheckAndHeal verifies that all resources rendered by the Helm release manifest exist on the target cluster.
// If any are missing, it triggers a corrective upgrade/install and returns repaired=true with the missing list.
func rcedCheckAndHeal(
	ctx context.Context,
	log logr.Logger,
	edgeCluster cluster.Cluster,
	namespace, releaseName string,
	chartRepo, chartName, chartVersion string,
	recorder record.EventRecorder,
	deployment *platformv1alpha1.KryptonDeployment,
) (repaired bool, missing []string, err error) {
	restCfg := edgeCluster.GetConfig()
	getter := helmutil.NewRemoteRESTClientGetterForNamespace(restCfg, namespace)
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(getter, namespace, os.Getenv("HELM_DRIVER"), func(format string, v ...any) {
		log.V(1).Info(fmt.Sprintf(format, v...))
	}); err != nil {
		return false, nil, fmt.Errorf("helm init failed: %w", err)
	}

	// Determine if release exists
	hist := action.NewHistory(actionConfig)
	hist.Max = 1
	_, histErr := hist.Run(releaseName)
	if histErr != nil {
		// Release missing: trigger redeploy
		recorder.Event(deployment, corev1.EventTypeWarning, "ReleaseMissing", "Helm release missing; re-installing")
		if err := deployHelmChart(ctx, log, edgeCluster, namespace, releaseName, chartRepo, chartName, chartVersion); err != nil {
			return false, nil, fmt.Errorf("repair install failed: %w", err)
		}
		return true, []string{"release:" + releaseName}, nil
	}

	// Fetch current release to get manifest
	get := action.NewGet(actionConfig)
	rel, getErr := get.Run(releaseName)
	if getErr != nil {
		return false, nil, fmt.Errorf("helm get failed: %w", getErr)
	}
	if rel == nil || rel.Info == nil || rel.Info.Status == helmrelease.StatusUninstalled {
		recorder.Event(deployment, corev1.EventTypeWarning, "ReleaseNotInstalled", "Release not installed; re-installing")
		if err := deployHelmChart(ctx, log, edgeCluster, namespace, releaseName, chartRepo, chartName, chartVersion); err != nil {
			return false, nil, fmt.Errorf("repair install failed: %w", err)
		}
		return true, []string{"release:" + releaseName}, nil
	}

	// Parse manifest into objects and verify existence (namespace-scoped preferred).
	objs, parseErr := parseManifestToObjects(rel.Manifest)
	if parseErr != nil {
		// If parsing fails, fall back to no-op (don't hard fail the reconcile)
		log.Error(parseErr, "manifest parse failed; skipping granular checks")
		return false, nil, nil
	}

	apiReader := edgeCluster.GetAPIReader()
	for _, obj := range objs {
		// Only check objects in our namespace or those without namespace (cluster-scoped), best-effort.
		if obj.GetNamespace() != "" && obj.GetNamespace() != namespace {
			continue
		}
		gvk := obj.GroupVersionKind()
		// Limit checks to commonly permitted types to avoid RBAC issues
		allowed := (gvk.Group == "" && gvk.Version == "v1" && (gvk.Kind == "ConfigMap" || gvk.Kind == "Secret" || gvk.Kind == "Service" || gvk.Kind == "ServiceAccount")) ||
			(gvk.Group == "apps" && gvk.Version == "v1" && (gvk.Kind == "Deployment" || gvk.Kind == "ReplicaSet"))
		if !allowed {
			continue
		}
		// Default empty namespace to target reconcile namespace for namespaced kinds
		ns := obj.GetNamespace()
		if ns == "" {
			ns = namespace
		}
		key := client.ObjectKey{Name: obj.GetName(), Namespace: ns}
		u := &metav1unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		if err := apiReader.Get(ctx, key, u); err != nil {
			if apierrors.IsNotFound(err) {
				missing = append(missing, fmt.Sprintf("%s/%s", obj.GetKind(), key.String()))
			} else {
				log.Error(err, "failed to check object", "gkv", gvk.String(), "name", key.String())
			}
		}
	}

	if len(missing) > 0 {
		// Attempt to heal by re-applying via upgrade (idempotent)
		if err := deployHelmChart(ctx, log, edgeCluster, namespace, releaseName, chartRepo, chartName, chartVersion); err != nil {
			return false, missing, fmt.Errorf("repair upgrade failed: %w", err)
		}
		return true, missing, nil
	}
	return false, nil, nil
}

// parseManifestToObjects splits a Helm manifest multi-doc string into Unstructured objects.
func parseManifestToObjects(manifest string) ([]*metav1unstructured.Unstructured, error) {
	dec := yaml.NewDecodingSerializer(metav1unstructured.UnstructuredJSONScheme)
	parts := splitYAMLDocuments(manifest)
	objs := make([]*metav1unstructured.Unstructured, 0, len(parts))
	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			continue
		}
		u := &metav1unstructured.Unstructured{}
		_, gvk, err := dec.Decode([]byte(p), nil, u)
		if err != nil || gvk == nil {
			// skip non-k8s docs (e.g., notes) or parse errors
			continue
		}
		objs = append(objs, u)
	}
	return objs, nil
}

func splitYAMLDocuments(s string) []string {
	// Helm manifests are concatenated with --- separators; also include cases with comments after ---
	// A simple split on "\n---" is sufficient for our purposes.
	// Keep it tolerant to different newline patterns.
	var docs []string
	cur := s
	for {
		idx := strings.Index(cur, "\n---")
		if idx == -1 {
			docs = append(docs, cur)
			break
		}
		docs = append(docs, cur[:idx])
		cur = cur[idx+4:]
	}
	return docs
}

// rcedNamespaceWorkloadHealthy checks Deployments in namespace for availability.
func rcedNamespaceWorkloadHealthy(ctx context.Context, edgeCluster cluster.Cluster, namespace string) (bool, []string) {
	var dlist appsv1.DeploymentList
	if err := edgeCluster.GetAPIReader().List(ctx, &dlist, &client.ListOptions{Namespace: namespace}); err != nil {
		// Conservative: if we cannot list, treat as not healthy once.
		return false, []string{"list-error:" + err.Error()}
	}
	var notReady []string
	for _, d := range dlist.Items {
		desired := int32(1)
		if d.Spec.Replicas != nil {
			desired = *d.Spec.Replicas
		}
		if d.Status.AvailableReplicas < desired {
			notReady = append(notReady, fmt.Sprintf("Deployment/%s (%d/%d available)", d.Name, d.Status.AvailableReplicas, desired))
		}
	}
	return len(notReady) == 0, notReady
}
