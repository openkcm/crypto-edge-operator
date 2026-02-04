// Package secretprovider implements a multicluster Provider that resolves
// edge clusters from kubeconfig Secrets. It lets the operator construct
// controller-runtime Cluster instances on demand, keyed by secret name
// (optionally prefixed with a namespace as "ns/name").
//
// Why this Provider exists:
//   - In multi-cluster scenarios, edge cluster credentials are typically stored
//     as Secrets. The operator needs to turn those into usable clients quickly.
//   - This Provider lazily creates and caches Cluster objects from embedded
//     kubeconfigs, avoiding global kubeconfig files and minimizing startup work.
//   - It supports namespaced keys and a default namespace to keep secret lookup
//     flexible while remaining simple.
package secretprovider

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	crcluster "sigs.k8s.io/controller-runtime/pkg/cluster"
)

// Provider maps cluster names to kubeconfig Secrets (name matches secret name) in a fixed namespace.
// Clusters are created lazily upon first access. Scheme from host manager is used for object decoding.
type Provider struct {
	client    client.Client
	scheme    *runtime.Scheme
	namespace string
	mu        sync.Mutex
	clusters  map[string]crcluster.Cluster
}

var _ multicluster.Provider = &Provider{}

func New(c client.Client, scheme *runtime.Scheme, secretNamespace string) *Provider {
	return &Provider{client: c, scheme: scheme, namespace: secretNamespace, clusters: map[string]crcluster.Cluster{}}
}

func (p *Provider) Get(ctx context.Context, name string) (crcluster.Cluster, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cl, ok := p.clusters[name]; ok {
		return cl, nil
	}
	// Support namespaced keys in the form "ns/name"; default to configured namespace otherwise.
	ns := p.namespace
	secName := name
	if strings.Contains(name, "/") {
		parts := strings.SplitN(name, "/", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			ns = parts[0]
			secName = parts[1]
		}
	}
	sec := &corev1.Secret{}
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: ns, Name: secName}, sec); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, fmt.Errorf("cluster secret %s/%s not found", ns, secName)
		}
		return nil, fmt.Errorf("error getting secret %s/%s: %w", ns, secName, err)
	}
	data, ok := sec.Data["kubeconfig"]
	if !ok || len(data) == 0 {
		return nil, fmt.Errorf("secret %s/%s missing kubeconfig key", ns, secName)
	}
	cfg, err := buildConfig(data)
	if err != nil {
		return nil, fmt.Errorf("failed building rest config: %w", err)
	}
	cl, err := crcluster.New(cfg, func(o *crcluster.Options) { o.Scheme = p.scheme })
	if err != nil {
		return nil, fmt.Errorf("failed creating cluster: %w", err)
	}
	p.clusters[name] = cl
	return cl, nil
}

// IndexField: no-op for MVP.
func (p *Provider) IndexField(ctx context.Context, obj client.Object, fieldName string, indexerFunc client.IndexerFunc) error {
	return nil
}

// List returns names of constructed clusters.
func (p *Provider) List(ctx context.Context) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.clusters))
	for n := range p.clusters {
		out = append(out, n)
	}
	return out, nil
}

func buildConfig(kubeconfig []byte) (*rest.Config, error) {
	raw, err := clientcmd.Load(kubeconfig)
	if err != nil {
		return nil, err
	}
	clientCfg := clientcmd.NewDefaultClientConfig(*raw, &clientcmd.ConfigOverrides{})
	return clientCfg.ClientConfig()
}

// Start implements ProviderRunnable (optional). We don't need background work; just block until context done.
// Start placeholder (no-op)
// Start implements ProviderRunnable (optional). Block until context done.
// Start placeholder (no-op)
// Start implements Provider's background tasks (none for now)
func (p *Provider) Start(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

// SetupWithManager allows the provider to integrate with the multicluster manager.
// No background indexing or informers are required for on-demand secret lookup.
func (p *Provider) SetupWithManager(ctx context.Context, mgr manager.Manager) error { return nil }
