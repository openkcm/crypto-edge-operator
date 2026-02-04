package helmutil

import (
	"testing"

	"k8s.io/client-go/rest"
)

func TestRemoteRESTClientGetter_ToRESTConfig(t *testing.T) {
	base := &rest.Config{Host: "https://example.invalid"}
	g := NewRemoteRESTClientGetter(base)
	cfg, err := g.ToRESTConfig()
	if err != nil {
		t.Fatalf("ToRESTConfig error: %v", err)
	}
	if cfg == nil {
		t.Fatalf("ToRESTConfig returned nil config")
	}
	if cfg == base {
		t.Fatalf("ToRESTConfig should return a copy, got same pointer")
	}
	if cfg.Host != base.Host {
		t.Fatalf("Host mismatch: got %s want %s", cfg.Host, base.Host)
	}
}

func TestRemoteRESTClientGetter_ToRawKubeConfigLoader_Namespace(t *testing.T) {
	base := &rest.Config{Host: "https://example.invalid"}
	g := NewRemoteRESTClientGetterForNamespace(base, "foo")
	loader := g.ToRawKubeConfigLoader()
	ns, _, err := loader.Namespace()
	if err != nil {
		t.Fatalf("Namespace error: %v", err)
	}
	if ns != "foo" {
		t.Fatalf("Namespace mismatch: got %s want %s", ns, "foo")
	}
}
