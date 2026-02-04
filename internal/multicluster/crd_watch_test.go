package multicluster

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// TestEnsureKryptonDeploymentCRD_Create ensures the CRD is created when missing.
func TestEnsureKryptonDeploymentCRD_Create(t *testing.T) {
	sch := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(sch)
	c := fake.NewClientBuilder().WithScheme(sch).Build()

	ctx := context.Background()
	if err := EnsureKryptonDeploymentCRD(ctx, c); err != nil {
		t.Fatalf("EnsureKryptonDeploymentCRD returned error: %v", err)
	}

	// Verify CRD now exists
	// Idempotency: second call should be a no-op and return nil
	if err := EnsureKryptonDeploymentCRD(ctx, c); err != nil {
		t.Fatalf("EnsureKryptonDeploymentCRD second call should be no-op, got error: %v", err)
	}
}
