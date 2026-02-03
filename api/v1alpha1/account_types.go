package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AccountSpec defines the desired state of an Account.
type AccountSpec struct {
	// DisplayName is a human-readable name for this account.
	DisplayName string `json:"displayName,omitempty" yaml:"displayName,omitempty"`
	// Owner is the entity responsible for this account (e.g., customer name, org).
	Owner string `json:"owner,omitempty" yaml:"owner,omitempty"`
}

// AccountStatus captures the observed state.
type AccountStatus struct {
	// Ready indicates if the account is in operational state.
	Ready bool `json:"ready,omitempty" yaml:"ready,omitempty"`
	// LastSyncTime records when the account was last reconciled.
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty" yaml:"lastSyncTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=accounts,scope=Namespaced,shortName=acct

// Account is the Schema for the accounts API.
// An Account can own multiple kryptondeployments.
type Account struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   AccountSpec   `json:"spec"`
	Status AccountStatus `json:"status"`
}

// +kubebuilder:object:root=true

// AccountList contains a list of Account.
type AccountList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Account `json:"items"`
}

// GetObjectKind returns the ObjectKind for Account.
func (a *Account) GetObjectKind() schema.ObjectKind { return &a.TypeMeta }

// GetObjectKind returns the ObjectKind for AccountList.
func (al *AccountList) GetObjectKind() schema.ObjectKind { return &al.TypeMeta }
