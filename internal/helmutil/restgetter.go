// Package helmutil provides small helpers and adapters for integrating Helm
// into a Kubernetes operator.
//
// Why RemoteRESTClientGetter exists:
//   - Helm's action.Configuration.Init expects a RESTClientGetter interface to
//     construct API clients, discovery, and REST mappers.
//   - In an operator we already hold a fully constructed *rest.Config for the
//     target (often remote/edge) cluster via controller-runtime. Reading a local
//     kubeconfig file is undesirable or impossible inside the operator pod.
//   - This adapter bridges Helm's expectations with the operator's reality:
//     it wraps the existing *rest.Config, sets a sensible default namespace for
//     objects that omit metadata.namespace, and lazily constructs discovery and
//     RESTMapper components.
//   - Practical outcome: The operator can install/upgrade/uninstall Helm charts
//     directly against remote clusters using in-memory configuration, without
//     depending on kubeconfig files. See usage in the deploy/uninstall paths
//     of the multicluster operator.
package helmutil

import (
	"errors"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"

	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// RemoteRESTClientGetter implements helm's RESTClientGetter backed by an existing *rest.Config.
// This allows helm action.Configuration.Init to target remote clusters discovered by multicluster-runtime.
// It lazily constructs discovery and RESTMapper components.
type RemoteRESTClientGetter struct {
	baseConfig *rest.Config
	namespace  string
}

func NewRemoteRESTClientGetter(cfg *rest.Config) *RemoteRESTClientGetter {
	return &RemoteRESTClientGetter{baseConfig: rest.CopyConfig(cfg), namespace: "default"}
}

// NewRemoteRESTClientGetterForNamespace sets a default namespace used by Helm's kube client when
// templated objects do not specify metadata.namespace explicitly.
func NewRemoteRESTClientGetterForNamespace(cfg *rest.Config, namespace string) *RemoteRESTClientGetter {
	rg := &RemoteRESTClientGetter{baseConfig: rest.CopyConfig(cfg), namespace: namespace}
	if rg.namespace == "" {
		rg.namespace = "default"
	}
	return rg
}

func (g *RemoteRESTClientGetter) ToRESTConfig() (*rest.Config, error) {
	if g.baseConfig == nil {
		return nil, errors.New("rest config nil")
	}
	return rest.CopyConfig(g.baseConfig), nil
}

func (g *RemoteRESTClientGetter) ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	rc, err := g.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	// Basic discovery client with in-memory cache.
	disco, err := discovery.NewDiscoveryClientForConfig(rc)
	if err != nil {
		return nil, err
	}
	return memory.NewMemCacheClient(disco), nil
}

func (g *RemoteRESTClientGetter) ToRESTMapper() (meta.RESTMapper, error) {
	disco, err := g.ToDiscoveryClient()
	if err != nil {
		return nil, err
	}
	return restmapper.NewDeferredDiscoveryRESTMapper(disco), nil
}

// ToRawKubeConfigLoader returns a minimal empty kubeconfig loader used by helm for informational warning printing.
func (g *RemoteRESTClientGetter) ToRawKubeConfigLoader() clientcmd.ClientConfig {
	// Provide a loader that returns the desired namespace for Helm's kube client.
	empty := clientcmdapi.NewConfig()
	overrides := &clientcmd.ConfigOverrides{}
	if g.namespace != "" {
		overrides.Context.Namespace = g.namespace
	}
	return clientcmd.NewDefaultClientConfig(*empty, overrides)
}
