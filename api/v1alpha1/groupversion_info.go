package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion for platform.example.com API group
var GroupVersion = schema.GroupVersion{Group: "platform.example.com", Version: "v1alpha1"}

var (
	// SchemeBuilder registers our API types
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	// AddToScheme adds the types to the scheme
	AddToScheme = SchemeBuilder.AddToScheme
)
