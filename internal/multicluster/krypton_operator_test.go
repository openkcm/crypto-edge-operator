package multicluster

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	platformv1alpha1 "github.com/openkcm/krypton-operator/api/v1alpha1"
)

func TestTruncatePath(t *testing.T) {
	if truncatePath("") != "" {
		t.Fatalf("empty path should return empty string")
	}
	short := "abc"
	if truncatePath(short) != short {
		t.Fatalf("short path mismatch")
	}
	long := "this/is/a/very/long/path/that/should/get/truncated/by/the/helper/function/because/it/exceeds/sixtyfour/chars"
	got := truncatePath(long)
	if len(got) >= len(long) || got[0] == 't' {
		t.Fatalf("expected truncated string starting with ellipsis, got: %s", got)
	}
}

func TestSplitYAMLDocuments(t *testing.T) {
	s := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: b\n"
	parts := splitYAMLDocuments(s)
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}
}

func TestParseManifestToObjects(t *testing.T) {
	manifest := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n  namespace: ns1\n---\n# note\nThis is not k8s\n---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: d1\n  namespace: ns1\nspec:\n  selector:\n    matchLabels:\n      app: x\n  template:\n    metadata:\n      labels:\n        app: x\n    spec:\n      containers:\n      - name: c\n        image: i\n"
	objs, err := parseManifestToObjects(manifest)
	if err != nil {
		t.Fatalf("parseManifestToObjects error: %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("expected 2 k8s objects, got %d", len(objs))
	}
}

func TestGetCheckInterval(t *testing.T) {
	// Reset cached once state
	checkIntervalOnce = sync.Once{}
	t.Setenv("KRYPTON_CHECK_INTERVAL", "5s")
	d := getCheckInterval()
	if d != 5*time.Second {
		t.Fatalf("expected 5s, got %v", d)
	}
}

func TestBuildWatchConfig_File(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "kubeconfig")
	data := []byte(`apiVersion: v1
clusters:
- cluster:
    server: https://example.invalid
  name: c1
contexts:
- context:
    cluster: c1
    user: u1
  name: ctx1
current-context: ctx1
kind: Config
preferences: {}
users:
- name: u1
  user:
    token: dummy
`)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	cfg, host, used, err := buildWatchConfig(context.Background(), p, "", "", "", "")
	if err != nil {
		t.Fatalf("buildWatchConfig error: %v", err)
	}
	if cfg == nil || host == "" || used != "ctx1" {
		t.Fatalf("unexpected results: cfg nil? %v host=%s used=%s", cfg == nil, host, used)
	}
}

func TestRcedResolveTarget(t *testing.T) {
	dep := &platformv1alpha1.KryptonDeployment{}
	dep.Spec.Region.Name = "eu-west1"
	// new kubeconfig ref with explicit ns
	dep.Spec.Region.Kubeconfig = &platformv1alpha1.KubeconfigRef{Secret: platformv1alpha1.SecretRef{Name: "edge", Namespace: "opns"}}
	key, region := rcedResolveTarget(context.Background(), nil, "discover", dep)
	if key != "opns/edge" || region != "eu-west1" {
		t.Fatalf("unexpected key/region: %s %s", key, region)
	}
	// only name provided -> default to discovery namespace
	dep.Spec.Region.Kubeconfig.Secret.Namespace = ""
	key, _ = rcedResolveTarget(context.Background(), nil, "discover", dep)
	if key != "discover/edge" {
		t.Fatalf("unexpected key defaulting: %s", key)
	}
	// deprecated field fallback
	dep.Spec.Region.Kubeconfig = nil
	dep.Spec.Region.KubeconfigSecretName = "legacy"
	key, _ = rcedResolveTarget(context.Background(), nil, "discover", dep)
	if key != "discover/legacy" {
		t.Fatalf("unexpected legacy key: %s", key)
	}
	// default derive from region
	dep.Spec.Region.KubeconfigSecretName = ""
	key, _ = rcedResolveTarget(context.Background(), nil, "discover", dep)
	if key != "discover/eu-west1-kubeconfig" {
		t.Fatalf("unexpected derived key: %s", key)
	}
}
