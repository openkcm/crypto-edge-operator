package secretprovider

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	sec := &corev1.Secret{}
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: p.namespace, Name: name}, sec); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, fmt.Errorf("cluster secret %s/%s not found", p.namespace, name)
		}
		return nil, fmt.Errorf("error getting secret %s/%s: %w", p.namespace, name, err)
	}
	data, ok := sec.Data["kubeconfig"]
	if !ok || len(data) == 0 {
		return nil, fmt.Errorf("secret %s/%s missing kubeconfig key", p.namespace, name)
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
