package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RegionSpec defines configuration for an edge region/cluster.
type RegionSpec struct {
	// KubeconfigSecretName is the name of the Secret in the operator namespace
	// that contains the kubeconfig for this region. If empty, defaults to
	// "<region-name>-kubeconfig".
	KubeconfigSecretName string `json:"kubeconfigSecretName,omitempty" yaml:"kubeconfigSecretName,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:path=regions,scope=Namespaced,shortName=reg

// Region maps a logical region name to its kubeconfig Secret reference.
type Region struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec RegionSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// RegionList contains a list of Region.
type RegionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Region `json:"items"`
}

// GetObjectKind returns the ObjectKind for Region.
func (r *Region) GetObjectKind() schema.ObjectKind { return &r.TypeMeta }

// GetObjectKind returns the ObjectKind for RegionList.
func (rl *RegionList) GetObjectKind() schema.ObjectKind { return &rl.TypeMeta }
