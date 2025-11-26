// Package v1alpha1 contains the API types for the
// mesh.openkcm.io group at version v1alpha1.
//
// +kubebuilder:object:generate=true
// +groupName=mesh.openkcm.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion for mesh.openkcm.io API group
var GroupVersion = schema.GroupVersion{Group: "mesh.openkcm.io", Version: "v1alpha1"}

var (
	// SchemeBuilder registers our API types
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	// AddToScheme adds the types to the scheme
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() { // Register API types
	SchemeBuilder.Register(&Tenant{}, &TenantList{})
}
