package controllers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	platformv1alpha1 "github.com/openkcm/crypto-edge-operator/api/v1alpha1"
)

// TenantReconciler reconciles a Tenant object
type TenantReconciler struct {
	client.Client

	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// RBAC markers for future controller-gen usage (currently manual manifests provided under config/rbac).
// +kubebuilder:rbac:groups=mesh.openkcm.io,resources=tenants,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mesh.openkcm.io,resources=tenants/status,verbs=get;update;patch
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
			logger.Error(errors.New("kubeconfig secret missing 'kubeconfig' key"), "invalid secret data; using host fallback")
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
							r.Recorder.Event(tenant, corev1.EventTypeNormal, "RemoteOK", "remote api reachable host="+remoteHost)
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
				r.setStatus(ctx, tenant, platformv1alpha1.TenantPhaseError, "namespace create failed: "+err2.Error())
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
			logger.Info("workspace namespace created", "workspace", tenant.Spec.Workspace)
			r.Recorder.Event(tenant, corev1.EventTypeNormal, "WorkspaceCreated", tenant.Spec.Workspace)
		} else {
			logger.Error(err, "error checking workspace namespace", "workspace", tenant.Spec.Workspace)
			r.Recorder.Event(tenant, corev1.EventTypeWarning, "WorkspaceCheckFailed", err.Error())
			r.setStatus(ctx, tenant, platformv1alpha1.TenantPhaseError, "namespace check failed: "+err.Error())
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	} else {
		logger.Info("workspace namespace exists", "workspace", tenant.Spec.Workspace)
		r.Recorder.Event(tenant, corev1.EventTypeNormal, "WorkspaceExists", tenant.Spec.Workspace)
	}

	// Central chart management removed from CRD; single-cluster controller no longer performs helm actions.
	r.setStatus(ctx, tenant, platformv1alpha1.TenantPhaseReady, "workspace ensured; central chart managed externally")
	return ctrl.Result{}, nil

	// If release exists and fingerprint unchanged, skip upgrade.
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
		ObservedGeneration: t.Generation,
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
	return errors.New("SetupWithManager not supported for multicluster; use mcbuilder in main")
}
