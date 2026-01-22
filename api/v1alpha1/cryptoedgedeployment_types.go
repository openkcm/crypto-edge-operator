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

// AccountInfo contains inline information about the owning account.
// This replaces the external Account CRD reference.
type AccountInfo struct {
	// Name identifies the logical account owner of this deployment.
	Name string `json:"name" yaml:"name"`
	// DisplayName is an optional human-readable name.
	DisplayName string `json:"displayName,omitempty" yaml:"displayName,omitempty"`
	// Owner is an optional organizational owner/tenant marker.
	Owner string `json:"owner,omitempty" yaml:"owner,omitempty"`
}

// CryptoEdgeDeploymentSpec defines the desired state.
type CryptoEdgeDeploymentSpec struct {
	// Account contains inline information about the owner of this deployment.
	// Replaces the external Account CRD reference.
	Account AccountInfo `json:"account" yaml:"account"`

	// Region contains inline information about the target region/edge cluster.
	// Replaces the external Region CRD reference.
	Region RegionInfo `json:"region" yaml:"region"`
}

// RegionInfo carries inline target region configuration.
type RegionInfo struct {
	// Name specifies the logical region/edge cluster name (e.g., edge01).
	Name string `json:"name" yaml:"name"`
	// KubeconfigSecretName optionally overrides the default kubeconfig secret name.
	// If empty, defaults to "<region-name>-kubeconfig".
	KubeconfigSecretName string `json:"kubeconfigSecretName,omitempty" yaml:"kubeconfigSecretName,omitempty"`
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
	metav1.ObjectMeta `json:"metadata"`

	Spec   CryptoEdgeDeploymentSpec   `json:"spec"`
	Status CryptoEdgeDeploymentStatus `json:"status"`
}

// +kubebuilder:object:root=true

// CryptoEdgeDeploymentList contains a list of CryptoEdgeDeployment.
type CryptoEdgeDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []CryptoEdgeDeployment `json:"items"`
}

// GetObjectKind returns the ObjectKind for CryptoEdgeDeployment.
func (c *CryptoEdgeDeployment) GetObjectKind() schema.ObjectKind { return &c.TypeMeta }

// GetObjectKind returns the ObjectKind for CryptoEdgeDeploymentList.
func (cl *CryptoEdgeDeploymentList) GetObjectKind() schema.ObjectKind { return &cl.TypeMeta }
