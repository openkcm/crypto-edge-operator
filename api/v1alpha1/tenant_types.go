package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TenantPhase enumerates simple lifecycle states.
type TenantPhase string

const (
	TenantPhasePending TenantPhase = "Pending"
	TenantPhaseReady   TenantPhase = "Ready"
	TenantPhaseError   TenantPhase = "Error"
)

// ClusterRef references a Secret containing a kubeconfig for the remote cluster.
type ClusterRef struct {
	SecretName      string `json:"secretName,omitempty" yaml:"secretName,omitempty"`
	SecretNamespace string `json:"secretNamespace,omitempty" yaml:"secretNamespace,omitempty"`
}

// TenantSpec defines desired state.
type TenantSpec struct {
	// ClusterRef optionally targets a specific discovered cluster; if omitted the Tenant applies to the cluster it resides on.
	ClusterRef *ClusterRef `json:"clusterRef,omitempty" yaml:"clusterRef,omitempty"`
	Workspace  string      `json:"workspace" yaml:"workspace"`
}

// TenantStatus captures observed state.
type TenantStatus struct {
	Phase            TenantPhase        `json:"phase,omitempty" yaml:"phase,omitempty"`
	LastMessage      string             `json:"lastMessage,omitempty" yaml:"lastMessage,omitempty"`
	Conditions       []metav1.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	LastAppliedChart string             `json:"lastAppliedChart,omitempty" yaml:"lastAppliedChart,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Tenant is the Schema for the tenants API.
type Tenant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TenantSpec   `json:"spec,omitempty"`
	Status TenantStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TenantList contains a list of Tenant.
type TenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Tenant `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Tenant{}, &TenantList{})
}

// Implement runtime.Object interfaces for manual scheme registration (controller-gen would normally do this).
func (t *Tenant) GetObjectKind() schema.ObjectKind      { return &t.TypeMeta }
func (tl *TenantList) GetObjectKind() schema.ObjectKind { return &tl.TypeMeta }
func (t *Tenant) DeepCopyObject() runtime.Object {
	if t == nil {
		return nil
	}
	out := new(Tenant)
	*out = *t
	// no map copy needed (Chart removed)
	if t.Status.Conditions != nil {
		out.Status.Conditions = make([]metav1.Condition, len(t.Status.Conditions))
		copy(out.Status.Conditions, t.Status.Conditions)
	}
	return out
}
func (tl *TenantList) DeepCopyObject() runtime.Object {
	if tl == nil {
		return nil
	}
	out := new(TenantList)
	*out = *tl
	if tl.Items != nil {
		out.Items = make([]Tenant, len(tl.Items))
		for i := range tl.Items {
			out.Items[i] = *tl.Items[i].DeepCopyObject().(*Tenant)
		}
	}
	return out
}
