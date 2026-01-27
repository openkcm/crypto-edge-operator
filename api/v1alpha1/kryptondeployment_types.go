package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KryptonDeploymentPhase enumerates simple lifecycle states.
type KryptonDeploymentPhase string

const (
	KryptonDeploymentPhasePending KryptonDeploymentPhase = "Pending"
	KryptonDeploymentPhaseReady   KryptonDeploymentPhase = "Ready"
	KryptonDeploymentPhaseError   KryptonDeploymentPhase = "Error"
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

// KryptonDeploymentSpec defines the desired state.
type KryptonDeploymentSpec struct {
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
	// Kubeconfig optionally references a Secret containing the kubeconfig
	// for the target edge cluster. If not set, defaults to a Secret named
	// "<region-name>-kubeconfig" in the operator discovery namespace.
	Kubeconfig *KubeconfigRef `json:"kubeconfig,omitempty" yaml:"kubeconfig,omitempty"`

	// KubeconfigSecretName optionally overrides the default kubeconfig secret name.
	// Deprecated: use Kubeconfig.secretName and Kubeconfig.secretNamespace instead.
	KubeconfigSecretName string `json:"kubeconfigSecretName,omitempty" yaml:"kubeconfigSecretName,omitempty"`
}

// KubeconfigRef points to a namespaced Secret that contains kubeconfig data.
type KubeconfigRef struct {
	// Secret references the namespaced Secret containing the kubeconfig.
	Secret SecretRef `json:"secret,omitempty" yaml:"secret,omitempty"`
}

// SecretRef is a simple namespaced name reference for a Secret.
type SecretRef struct {
	Name      string `json:"name,omitempty" yaml:"name,omitempty"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
}

// KryptonDeploymentStatus captures observed state.
type KryptonDeploymentStatus struct {
	Phase            KryptonDeploymentPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	LastMessage      string                 `json:"lastMessage,omitempty" yaml:"lastMessage,omitempty"`
	Conditions       []metav1.Condition     `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	LastAppliedChart string                 `json:"lastAppliedChart,omitempty" yaml:"lastAppliedChart,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=kryptondeployments,scope=Namespaced,shortName=kd

// KryptonDeployment is the Schema for the kryptondeployments API.
// The name of the KryptonDeployment is used as the namespace name in the target cluster.
type KryptonDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   KryptonDeploymentSpec   `json:"spec"`
	Status KryptonDeploymentStatus `json:"status"`
}

// +kubebuilder:object:root=true

// KryptonDeploymentList contains a list of KryptonDeployment.
type KryptonDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []KryptonDeployment `json:"items"`
}

// GetObjectKind returns the ObjectKind for KryptonDeployment.
func (c *KryptonDeployment) GetObjectKind() schema.ObjectKind { return &c.TypeMeta }

// GetObjectKind returns the ObjectKind for KryptonDeploymentList.
func (cl *KryptonDeploymentList) GetObjectKind() schema.ObjectKind { return &cl.TypeMeta }
