package multicluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	_ "embed" // required for go:embed directive

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Embed the KryptonDeployment CRD manifest for optional apply to the watch cluster.
//
//go:embed mesh.openkcm.io_kryptondeployments.yaml
var kryptonDeploymentCRDYAML []byte

var decUnstructuredWatch = yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)

// EnsureKryptonDeploymentCRD applies the CRD into the target (watch) cluster if it is not present.
func EnsureKryptonDeploymentCRD(ctx context.Context, c client.Client) error {
	crdName := "kryptondeployments.mesh.openkcm.io"
	crd := &unstructured.Unstructured{}
	crd.SetAPIVersion("apiextensions.k8s.io/v1")
	crd.SetKind("CustomResourceDefinition")
	crd.SetName(crdName)

	var lastErr error
	for attempt := range 5 {
		getErr := c.Get(ctx, client.ObjectKey{Name: crd.GetName()}, crd)
		if apierrors.IsNotFound(getErr) {
			// proceed to create
		} else if getErr != nil {
			errStr := getErr.Error()
			if strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "context canceled") || strings.Contains(errStr, "Client.Timeout") {
				lastErr = fmt.Errorf("get CRD transient error (attempt %d): %w", attempt+1, getErr)
				time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
				continue
			}
			return fmt.Errorf("get CRD failed: %w", getErr)
		} else {
			return nil // already exists
		}
		// NotFound: proceed to create from embedded YAML.
		obj := &unstructured.Unstructured{}
		_, gvk, decErr := decUnstructuredWatch.Decode(kryptonDeploymentCRDYAML, nil, obj)
		if decErr != nil {
			return fmt.Errorf("decode embedded CRD failed: %w", decErr)
		}
		if gvk.Kind != "CustomResourceDefinition" {
			return fmt.Errorf("embedded manifest kind %s unexpected", gvk.Kind)
		}
		createErr := c.Create(ctx, obj)
		if createErr == nil || apierrors.IsAlreadyExists(createErr) {
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
	return errors.New("ensure CRD exhausted retries without success")
}
