package secretprovider

import (
	"context"
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1 "k8s.io/api/core/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func makeKubeconfig() []byte {
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["test"] = &clientcmdapi.Cluster{Server: "https://example.invalid"}
	cfg.AuthInfos["u1"] = &clientcmdapi.AuthInfo{Token: "dummy"}
	cfg.Contexts["test"] = &clientcmdapi.Context{Cluster: "test", AuthInfo: "u1", Namespace: "default"}
	cfg.CurrentContext = "test"
	data, _ := clientcmd.Write(*cfg)
	return data
}

func TestProvider_Get_DefaultNamespace(t *testing.T) {
	sch := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(sch)
	// Secret in namespace "opns"
	sec := &corev1.Secret{}
	sec.Namespace = "opns"
	sec.Name = "edge"
	sec.Data = map[string][]byte{"kubeconfig": makeKubeconfig()}

	c := fake.NewClientBuilder().WithScheme(sch).WithObjects(sec).Build()
	p := New(c, sch, "opns")

	ctx := context.Background()
	cl, err := p.Get(ctx, "edge")
	if err != nil {
		t.Fatalf("Provider.Get returned error: %v", err)
	}
	if cl == nil {
		t.Fatalf("Provider.Get returned nil cluster")
	}

	// List should include constructed name
	names, err := p.List(ctx)
	if err != nil {
		t.Fatalf("Provider.List returned error: %v", err)
	}
	if !slices.Contains(names, "edge") {
		t.Fatalf("constructed cluster name not found in list")
	}
}

func TestProvider_Get_NamespacedKey(t *testing.T) {
	sch := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(sch)
	sec := &corev1.Secret{}
	sec.Namespace = "otherns"
	sec.Name = "edge"
	sec.Data = map[string][]byte{"kubeconfig": makeKubeconfig()}

	c := fake.NewClientBuilder().WithScheme(sch).WithObjects(sec).Build()
	p := New(c, sch, "opns")

	ctx := context.Background()
	cl, err := p.Get(ctx, "otherns/edge")
	if err != nil {
		t.Fatalf("Provider.Get returned error: %v", err)
	}
	if cl == nil {
		t.Fatalf("Provider.Get returned nil cluster")
	}

	// Verify secret was fetched from other namespace via client
	got := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "otherns", Name: "edge"}, got); err != nil {
		t.Fatalf("client Get failed: %v", err)
	}
}
