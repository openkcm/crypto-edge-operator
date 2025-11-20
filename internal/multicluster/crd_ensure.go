package multicluster

import (
	"context"
	_ "embed" // required for go:embed directive
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Embed the Tenant CRD manifest for on-demand apply to discovered clusters.
//
//go:embed platform.example.com_tenants.yaml
var tenantCRDYAML []byte

var decUnstructured = yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)

// EnsureTenantCRD applies the Tenant CRD into the target cluster if it is not present.
// It is intentionally lightweight and only checks for presence by name.
func EnsureTenantCRD(ctx context.Context, c client.Client) error {
	crdName := "tenants.platform.example.com"
	crd := &unstructured.Unstructured{}
	crd.SetAPIVersion("apiextensions.k8s.io/v1")
	crd.SetKind("CustomResourceDefinition")
	crd.SetName(crdName)

	// Retry loop for transient connection / context issues.
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		getErr := c.Get(ctx, client.ObjectKey{Name: crd.GetName()}, crd)
		if getErr == nil {
			return nil // already exists
		}
		if getErr != nil && !errors.IsNotFound(getErr) {
			// Transient network errors often show 'connection refused' or 'context canceled'. Retry those; abort others.
			errStr := getErr.Error()
			if strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "context canceled") || strings.Contains(errStr, "Client.Timeout") {
				lastErr = fmt.Errorf("get CRD transient error (attempt %d): %w", attempt+1, getErr)
				time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
				continue
			}
			return fmt.Errorf("get CRD failed: %w", getErr)
		}
		// NotFound: proceed to create.
		obj := &unstructured.Unstructured{}
		_, gvk, decErr := decUnstructured.Decode(tenantCRDYAML, nil, obj)
		if decErr != nil {
			return fmt.Errorf("decode embedded CRD failed: %w", decErr)
		}
		if gvk.Kind != "CustomResourceDefinition" {
			return fmt.Errorf("embedded manifest kind %s unexpected", gvk.Kind)
		}
		createErr := c.Create(ctx, obj)
		if createErr == nil || errors.IsAlreadyExists(createErr) {
			return nil
		}
		errStr := createErr.Error()
		if strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "context canceled") || strings.Contains(errStr, "Client.Timeout") {
			lastErr = fmt.Errorf("create CRD transient error (attempt %d): %w", attempt+1, createErr)
			time.Sleep(time.Duration(attempt+1) * 300 * time.Millisecond)
			continue
		}
		return fmt.Errorf("create CRD failed: %w", createErr)
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("ensure CRD exhausted retries without success")
}
