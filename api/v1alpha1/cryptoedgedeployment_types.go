package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CryptoEdgeDeploymentPhase enumerates simple lifecycle states.
type CryptoEdgeDeploymentPhase string

const (
	CryptoEdgeDeploymentPhasePending CryptoEdgeDeploymentPhase = "Pending"
	CryptoEdgeDeploymentPhaseReady   CryptoEdgeDeploymentPhase = "Ready"
	CryptoEdgeDeploymentPhaseError   CryptoEdgeDeploymentPhase = "Error"
)

// AccountRef references an Account in the same namespace.
type AccountRef struct {
	Name string `json:"name" yaml:"name"`
}

// CryptoEdgeDeploymentSpec defines the desired state.
type CryptoEdgeDeploymentSpec struct {
	// AccountRef is a required reference to the Account that owns this deployment.
	AccountRef AccountRef `json:"accountRef" yaml:"accountRef"`

	// RegionRef references a Region resource (in the operator namespace) that
	// defines the kubeconfig secret for the target cluster.
	// If omitted, TargetRegion is used as a simple region name.
	RegionRef *RegionRef `json:"regionRef,omitempty" yaml:"regionRef,omitempty"`

	// TargetRegion specifies the target edge cluster (e.g., edge01, edge02, etc.).
	// The deployment will be rolled out to this cluster.
	TargetRegion string `json:"targetRegion" yaml:"targetRegion"`
}

// RegionRef references a Region by name.
type RegionRef struct {
	Name string `json:"name" yaml:"name"`
}

// CryptoEdgeDeploymentStatus captures observed state.
type CryptoEdgeDeploymentStatus struct {
	Phase            CryptoEdgeDeploymentPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	LastMessage      string                    `json:"lastMessage,omitempty" yaml:"lastMessage,omitempty"`
	Conditions       []metav1.Condition        `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	LastAppliedChart string                    `json:"lastAppliedChart,omitempty" yaml:"lastAppliedChart,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=cryptoedgedeployments,scope=Namespaced,shortName=ced

// CryptoEdgeDeployment is the Schema for the cryptoedgedeployments API.
// The name of the CryptoEdgeDeployment is used as the namespace name in the target cluster.
type CryptoEdgeDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"` //nolint:modernize

	Spec   CryptoEdgeDeploymentSpec   `json:"spec,omitempty"`   //nolint:modernize
	Status CryptoEdgeDeploymentStatus `json:"status,omitempty"` //nolint:modernize
}

// +kubebuilder:object:root=true

// CryptoEdgeDeploymentList contains a list of CryptoEdgeDeployment.
type CryptoEdgeDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"` //nolint:modernize

	Items []CryptoEdgeDeployment `json:"items"`
}

// GetObjectKind returns the ObjectKind for CryptoEdgeDeployment.
func (c *CryptoEdgeDeployment) GetObjectKind() schema.ObjectKind { return &c.TypeMeta }

// GetObjectKind returns the ObjectKind for CryptoEdgeDeploymentList.
func (cl *CryptoEdgeDeploymentList) GetObjectKind() schema.ObjectKind { return &cl.TypeMeta }
