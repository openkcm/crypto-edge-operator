package multicluster

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"io"
	"path/filepath"

	"github.com/go-logr/logr"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metautil "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	kubeconfigprovider "sigs.k8s.io/multicluster-runtime/providers/kubeconfig"

	platformv1alpha1 "github.com/openkcm/crypto-edge-operator/api/v1alpha1"
	helmutil "github.com/openkcm/crypto-edge-operator/internal/helmutil"
)

// RunCryptoEdgeOperator starts the operator for CryptoEdgeDeployment resources.
func RunCryptoEdgeOperator() {
	var namespace string
	var kubeconfigSecretLabel string
	var kubeconfigSecretKey string
	var chartRepo string
	var chartName string
	var chartVersion string

	flag.StringVar(&namespace, "namespace", "default", "Namespace where kubeconfig secrets and CRDs are stored")
	flag.StringVar(&kubeconfigSecretLabel, "kubeconfig-label", "sigs.k8s.io/multicluster-runtime-kubeconfig", "Label for kubeconfig secrets (override with KUBECONFIG_SECRET_LABEL)")
	flag.StringVar(&kubeconfigSecretKey, "kubeconfig-key", "kubeconfig", "Key in secret containing kubeconfig data (override with KUBECONFIG_SECRET_KEY)")
	flag.StringVar(&chartRepo, "chart-repo", "https://ealenn.github.io/charts", "Helm chart repository URL")
	flag.StringVar(&chartName, "chart-name", "echo-server", "Helm chart name")
	flag.StringVar(&chartVersion, "chart-version", "0.5.0", "Helm chart version")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	// Environment fallbacks for namespace/secret settings
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

	// Setup Helm environment
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

	ctrllog.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	entryLog := ctrllog.Log.WithName("cryptoedge-operator")
	ctx := ctrl.SetupSignalHandler()

	entryLog.Info("Starting CryptoEdge operator", "namespace", namespace, "chart", fmt.Sprintf("%s/%s:%s", chartRepo, chartName, chartVersion))
	entryLog.Info("Kubeconfig provider config", "namespace", namespace, "secretLabel", kubeconfigSecretLabel, "secretKey", kubeconfigSecretKey)

	// Create scheme for home cluster (includes platform CRDs)
	homeScheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(homeScheme)
	_ = platformv1alpha1.AddToScheme(homeScheme)

	// Create scheme for edge clusters (include platform CRDs so they don't error, but we won't actually use them)
	edgeScheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(edgeScheme)
	_ = platformv1alpha1.AddToScheme(edgeScheme)

	// Setup kubeconfig provider for multicluster
	// Use minimal scheme for provider so edge clusters don't try to list platform CRDs
	providerScheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(providerScheme)
	// Add platform CRDs to the scheme so manager can read them, but edge clusters won't have them
	_ = platformv1alpha1.AddToScheme(providerScheme)

	providerOpts := kubeconfigprovider.Options{
		Namespace:             namespace,
		KubeconfigSecretLabel: kubeconfigSecretLabel,
		KubeconfigSecretKey:   kubeconfigSecretKey,
		ClusterOptions: []cluster.Option{
			func(o *cluster.Options) { o.Scheme = edgeScheme },
		},
	}
	provider := kubeconfigprovider.New(providerOpts)

	// Create multicluster manager with scheme that includes platform CRDs
	managerOpts := mcmanager.Options{
		Metrics:                metricsserver.Options{BindAddress: ":8080"},
		Scheme:                 providerScheme,
		HealthProbeBindAddress: ":8081",
		WebhookServer:          webhook.NewServer(webhook.Options{Port: 0}), // Disable webhooks
	}
	mgr, err := mcmanager.New(ctrl.GetConfigOrDie(), provider, managerOpts)
	if err != nil {
		entryLog.Error(err, "Unable to create manager")
		os.Exit(1)
	}

	if err := provider.SetupWithManager(ctx, mgr); err != nil {
		entryLog.Error(err, "Unable to setup provider")
		os.Exit(1)
	}

	// Add health checks (use a simple ping check instead of webhook server)
	if err := mgr.AddHealthzCheck("healthz", func(_ *http.Request) error { return nil }); err != nil {
		entryLog.Error(err, "Unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", func(_ *http.Request) error { return nil }); err != nil {
		entryLog.Error(err, "Unable to set up ready check")
		os.Exit(1)
	}

	// Controller on home cluster using standard controller-runtime manager
	homeMgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 homeScheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "",
	})
	if err != nil {
		entryLog.Error(err, "Unable to create home manager")
		os.Exit(1)
	}

	if err := ctrl.NewControllerManagedBy(homeMgr).
		Named("cryptoedgedeployments-home").
		For(&platformv1alpha1.CryptoEdgeDeployment{}).
		Complete(reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
			log := ctrllog.FromContext(ctx).WithValues("deployment", req.NamespacedName)
			log.Info("reconcile start")

			recorder := homeMgr.GetEventRecorderFor("cryptoedge-operator")

			setReadyCondition := func(dep *platformv1alpha1.CryptoEdgeDeployment, status metav1.ConditionStatus, reason, message string) {
				metautil.SetStatusCondition(&dep.Status.Conditions, metav1.Condition{
					Type:               "Ready",
					Status:             status,
					Reason:             reason,
					Message:            message,
					ObservedGeneration: dep.Generation,
				})
			}

			const finalizerName = "mesh.openkcm.io/cryptoedgedeployment-finalizer"

			// Fetch CryptoEdgeDeployment from home cluster
			deployment := &platformv1alpha1.CryptoEdgeDeployment{}
			if err := homeMgr.GetClient().Get(ctx, req.NamespacedName, deployment); err != nil {
				if apierrors.IsNotFound(err) {
					log.Info("deployment not found, assuming deleted")
					return reconcile.Result{}, nil
				}
				log.Error(err, "failed to fetch deployment")
				return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
			}

			// Validate Account exists
			account := &platformv1alpha1.Account{}
			accountKey := client.ObjectKey{Name: deployment.Spec.AccountRef.Name, Namespace: deployment.Namespace}
			if err := homeMgr.GetClient().Get(ctx, accountKey, account); err != nil {
				log.Error(err, "account not found", "account", deployment.Spec.AccountRef.Name)
				deployment.Status.Phase = platformv1alpha1.CryptoEdgeDeploymentPhaseError
				deployment.Status.LastMessage = fmt.Sprintf("Account %s not found", deployment.Spec.AccountRef.Name)
				setReadyCondition(deployment, metav1.ConditionFalse, "AccountNotFound", deployment.Status.LastMessage)
				_ = homeMgr.GetClient().Status().Update(ctx, deployment)
				recorder.Event(deployment, corev1.EventTypeWarning, "AccountNotFound", deployment.Status.LastMessage)
				return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
			}
			log.Info("account validated", "account", deployment.Spec.AccountRef.Name)
			recorder.Event(deployment, corev1.EventTypeNormal, "AccountValidated", fmt.Sprintf("Account %s validated", deployment.Spec.AccountRef.Name))

			// Handle deletion with finalizer: uninstall helm release and delete namespace on edge
			if !deployment.ObjectMeta.DeletionTimestamp.IsZero() {
				// Resolve Region and kubeconfig Secret name
				targetSecretName := ""
				region := &platformv1alpha1.Region{}
				if err := homeMgr.GetClient().Get(ctx, client.ObjectKey{Name: deployment.Spec.TargetRegion, Namespace: namespace}, region); err == nil {
					if region.Spec.KubeconfigSecretName != "" {
						targetSecretName = region.Spec.KubeconfigSecretName
					}
				}
				if targetSecretName == "" {
					targetSecretName = fmt.Sprintf("%s-kubeconfig", deployment.Spec.TargetRegion)
				}
				log.Info("deletion: routing to edge cluster", "targetRegion", deployment.Spec.TargetRegion, "targetSecret", targetSecretName)
				recorder.Event(deployment, corev1.EventTypeNormal, "DeleteStarted", fmt.Sprintf("Routing delete to region %s", deployment.Spec.TargetRegion))

				edgeCluster, err := mgr.GetCluster(ctx, targetSecretName)
				if err != nil {
					log.Error(err, "deletion: failed to get edge cluster", "targetRegion", deployment.Spec.TargetRegion)
				} else {
					// Attempt helm uninstall
					releaseName := fmt.Sprintf("ced-%s", deployment.Name)
					if err := uninstallHelmChart(ctx, log, edgeCluster, deployment.Name, releaseName); err != nil {
						log.Error(err, "deletion: helm uninstall failed", "release", releaseName)
						recorder.Event(deployment, corev1.EventTypeWarning, "HelmUninstallFailed", err.Error())
					} else {
						log.Info("deletion: helm uninstall succeeded", "release", releaseName)
						recorder.Event(deployment, corev1.EventTypeNormal, "HelmUninstalled", fmt.Sprintf("Uninstalled release %s", releaseName))
					}
					// Delete namespace on edge cluster
					ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: deployment.Name}}
					if err := edgeCluster.GetClient().Delete(ctx, ns); err != nil {
						if !apierrors.IsNotFound(err) {
							log.Error(err, "deletion: failed to delete namespace", "namespace", deployment.Name)
						}
					} else {
						log.Info("deletion: namespace delete requested", "namespace", deployment.Name)
						recorder.Event(deployment, corev1.EventTypeNormal, "EdgeNamespaceDeleteRequested", fmt.Sprintf("Requested deletion for namespace %s", deployment.Name))
					}
				}

				// Remove finalizer to allow deletion to complete
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

			// Ensure finalizer is set on active objects
			foundFinalizer := false
			for _, f := range deployment.GetFinalizers() {
				if f == finalizerName {
					foundFinalizer = true
					break
				}
			}
			if !foundFinalizer {
				deployment.SetFinalizers(append(deployment.GetFinalizers(), finalizerName))
				if err := homeMgr.GetClient().Update(ctx, deployment); err != nil {
					log.Error(err, "failed to add finalizer")
					return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
				}
				log.Info("finalizer added")
				recorder.Event(deployment, corev1.EventTypeNormal, "FinalizerAdded", "Finalizer added for cleanup on delete")
			}

			// Resolve Region and kubeconfig Secret name (default: <region>-kubeconfig)
			targetSecretName := ""
			region := &platformv1alpha1.Region{}
			if err := homeMgr.GetClient().Get(ctx, client.ObjectKey{Name: deployment.Spec.TargetRegion, Namespace: namespace}, region); err == nil {
				if region.Spec.KubeconfigSecretName != "" {
					targetSecretName = region.Spec.KubeconfigSecretName
				}
			}
			if targetSecretName == "" {
				targetSecretName = fmt.Sprintf("%s-kubeconfig", deployment.Spec.TargetRegion)
			}
			log.Info("routing to edge cluster", "targetRegion", deployment.Spec.TargetRegion, "targetSecret", targetSecretName)
			recorder.Event(deployment, corev1.EventTypeNormal, "Routing", fmt.Sprintf("Routing to region %s", deployment.Spec.TargetRegion))

			// Get target edge cluster from multicluster manager
			edgeCluster, err := mgr.GetCluster(ctx, targetSecretName)
			if err != nil {
				log.Error(err, "failed to get edge cluster", "targetRegion", deployment.Spec.TargetRegion)
				deployment.Status.Phase = platformv1alpha1.CryptoEdgeDeploymentPhaseError
				deployment.Status.LastMessage = fmt.Sprintf("Edge cluster %s not available", deployment.Spec.TargetRegion)
				setReadyCondition(deployment, metav1.ConditionFalse, "EdgeUnavailable", deployment.Status.LastMessage)
				_ = homeMgr.GetClient().Status().Update(ctx, deployment)
				recorder.Event(deployment, corev1.EventTypeWarning, "EdgeUnavailable", deployment.Status.LastMessage)
				return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
			}

			// Use deployment name as namespace in edge cluster
			nsName := deployment.Name

			// Ensure namespace exists on edge cluster
			ns := &corev1.Namespace{}
			if err := edgeCluster.GetClient().Get(ctx, client.ObjectKey{Name: nsName}, ns); err != nil {
				if apierrors.IsNotFound(err) {
					ns = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}
					if err := edgeCluster.GetClient().Create(ctx, ns); err != nil {
						log.Error(err, "failed to create namespace on edge", "namespace", nsName)
						return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
					}
					log.Info("created namespace on edge cluster", "namespace", nsName, "region", deployment.Spec.TargetRegion)
					recorder.Event(deployment, corev1.EventTypeNormal, "EdgeNamespaceCreated", fmt.Sprintf("Namespace %s created on %s", nsName, deployment.Spec.TargetRegion))
				} else {
					log.Error(err, "failed to check namespace")
					return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
				}
			}

			// Deploy Helm chart to edge cluster
			releaseName := fmt.Sprintf("ced-%s", deployment.Name)
			if err := deployHelmChart(ctx, log, edgeCluster, nsName, releaseName, chartRepo, chartName, chartVersion); err != nil {
				log.Error(err, "helm deployment failed")
				deployment.Status.Phase = platformv1alpha1.CryptoEdgeDeploymentPhaseError
				deployment.Status.LastMessage = fmt.Sprintf("Helm deployment failed: %v", err)
				setReadyCondition(deployment, metav1.ConditionFalse, "HelmFailed", deployment.Status.LastMessage)
				_ = homeMgr.GetClient().Status().Update(ctx, deployment)
				recorder.Event(deployment, corev1.EventTypeWarning, "HelmDeployFailed", deployment.Status.LastMessage)
				return reconcile.Result{RequeueAfter: 60 * time.Second}, nil
			}
			log.Info("helm deploy succeeded", "release", releaseName, "namespace", nsName)
			recorder.Event(deployment, corev1.EventTypeNormal, "HelmDeployed", fmt.Sprintf("Release %s deployed to %s", releaseName, nsName))

			// Update status to Ready
			deployment.Status.Phase = platformv1alpha1.CryptoEdgeDeploymentPhaseReady
			deployment.Status.LastMessage = fmt.Sprintf("Successfully deployed to region %s", deployment.Spec.TargetRegion)
			deployment.Status.LastAppliedChart = fmt.Sprintf("%s/%s:%s", chartRepo, chartName, chartVersion)
			setReadyCondition(deployment, metav1.ConditionTrue, "Deployed", fmt.Sprintf("Successfully deployed to region %s", deployment.Spec.TargetRegion))
			if err := homeMgr.GetClient().Status().Update(ctx, deployment); err != nil {
				log.Error(err, "failed to update status")
			}

			log.Info("deployment reconciled successfully", "region", deployment.Spec.TargetRegion)
			recorder.Event(deployment, corev1.EventTypeNormal, "Ready", fmt.Sprintf("Deployment ready in region %s", deployment.Spec.TargetRegion))
			return reconcile.Result{}, nil
		})); err != nil {
		entryLog.Error(err, "Unable to create home controller")
		os.Exit(1)
	}

	entryLog.Info("Starting managers")
	go func() {
		if err := homeMgr.Start(ctx); err != nil {
			entryLog.Error(err, "Home manager exited with error")
		}
	}()
	if err := mgr.Start(ctx); err != nil {
		entryLog.Error(err, "Manager exited with error")
		os.Exit(1)
	}
}

func deployHelmChart(ctx context.Context, log logr.Logger, edgeCluster cluster.Cluster, namespace, releaseName, chartRepo, chartName, chartVersion string) error {
	// Get REST config for edge cluster
	restCfg := edgeCluster.GetConfig()
	getter := helmutil.NewRemoteRESTClientGetter(restCfg)

	// Initialize Helm action config
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(getter, namespace, os.Getenv("HELM_DRIVER"), func(format string, v ...interface{}) {
		log.V(1).Info(fmt.Sprintf(format, v...))
	}); err != nil {
		return fmt.Errorf("helm init failed: %w", err)
	}

	// Download chart from direct tarball URL into a temp file, then load
	settings := cli.New()
	_ = settings // reserved for future use if needed
	chartTarURL := fmt.Sprintf("%s/%s-%s.tgz", chartRepo, chartName, chartVersion)

	resp, err := http.Get(chartTarURL)
	if err != nil {
		return fmt.Errorf("failed to download chart: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download chart: status %d", resp.StatusCode)
	}
	tmpPath := filepath.Join(os.TempDir(), fmt.Sprintf("%s-%s.tgz", chartName, chartVersion))
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

	values := map[string]interface{}{
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
	getter := helmutil.NewRemoteRESTClientGetter(restCfg)

	// Initialize Helm action config
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(getter, namespace, os.Getenv("HELM_DRIVER"), func(format string, v ...interface{}) {
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
