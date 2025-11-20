package helmutil

import (
	"fmt"

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
}

func NewRemoteRESTClientGetter(cfg *rest.Config) *RemoteRESTClientGetter {
	return &RemoteRESTClientGetter{baseConfig: rest.CopyConfig(cfg)}
}

func (g *RemoteRESTClientGetter) ToRESTConfig() (*rest.Config, error) {
	if g.baseConfig == nil {
		return nil, fmt.Errorf("rest config nil")
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

// Helm sometimes calls this for printing warnings; we can return a minimal loader.
func (g *RemoteRESTClientGetter) ToRawKubeConfigLoader() clientcmd.ClientConfig {
	// Minimal stub config (empty) for helm informational calls.
	empty := clientcmdapi.NewConfig()
	return clientcmd.NewDefaultClientConfig(*empty, &clientcmd.ConfigOverrides{})
}
