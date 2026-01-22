package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *TenantSpec) DeepCopyInto(out *TenantSpec) {
	*out = *in
	if in.ClusterRef != nil {
		out.ClusterRef = new(ClusterRef)
		*out.ClusterRef = *in.ClusterRef
	}
}

// DeepCopy creates a new deep-copied TenantSpec.
func (in *TenantSpec) DeepCopy() *TenantSpec {
	if in == nil {
		return nil
	}
	out := new(TenantSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *TenantStatus) DeepCopyInto(out *TenantStatus) {
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			// metav1.Condition has DeepCopyInto; fall back to assignment if absent.
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}

// DeepCopy creates a new deep-copied TenantStatus.
func (in *TenantStatus) DeepCopy() *TenantStatus {
	if in == nil {
		return nil
	}
	out := new(TenantStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *AccountSpec) DeepCopyInto(out *AccountSpec) {
	*out = *in
}

// DeepCopy creates a new deep-copied AccountSpec.
func (in *AccountSpec) DeepCopy() *AccountSpec {
	if in == nil {
		return nil
	}
	out := new(AccountSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *AccountStatus) DeepCopyInto(out *AccountStatus) {
	*out = *in
	if in.LastSyncTime != nil {
		out.LastSyncTime = (*in.LastSyncTime).DeepCopy()
	}
}

// DeepCopy creates a new deep-copied AccountStatus.
func (in *AccountStatus) DeepCopy() *AccountStatus {
	if in == nil {
		return nil
	}
	out := new(AccountStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *AccountInfo) DeepCopyInto(out *AccountInfo) { *out = *in }

// DeepCopy creates a new deep-copied AccountInfo.
func (in *AccountInfo) DeepCopy() *AccountInfo {
	if in == nil {
		return nil
	}
	out := new(AccountInfo)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *CryptoEdgeDeploymentSpec) DeepCopyInto(out *CryptoEdgeDeploymentSpec) {
	*out = *in
	out.Account = in.Account
	out.Region = in.Region
}

// DeepCopy creates a new deep-copied CryptoEdgeDeploymentSpec.
func (in *CryptoEdgeDeploymentSpec) DeepCopy() *CryptoEdgeDeploymentSpec {
	if in == nil {
		return nil
	}
	out := new(CryptoEdgeDeploymentSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *CryptoEdgeDeploymentStatus) DeepCopyInto(out *CryptoEdgeDeploymentStatus) {
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}

// DeepCopy creates a new deep-copied CryptoEdgeDeploymentStatus.
func (in *CryptoEdgeDeploymentStatus) DeepCopy() *CryptoEdgeDeploymentStatus {
	if in == nil {
		return nil
	}
	out := new(CryptoEdgeDeploymentStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *RegionInfo) DeepCopyInto(out *RegionInfo) {
	*out = *in
	if in.Kubeconfig != nil {
		out.Kubeconfig = new(KubeconfigRef)
		// Deep copy of nested SecretRef (value type)
		out.Kubeconfig.Secret = in.Kubeconfig.Secret
	}
}

// DeepCopy creates a new deep-copied RegionInfo.
func (in *RegionInfo) DeepCopy() *RegionInfo {
	if in == nil {
		return nil
	}
	out := new(RegionInfo)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *KubeconfigRef) DeepCopyInto(out *KubeconfigRef) { *out = *in }

// DeepCopy creates a new deep-copied KubeconfigRef.
func (in *KubeconfigRef) DeepCopy() *KubeconfigRef {
	if in == nil {
		return nil
	}
	out := new(KubeconfigRef)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *SecretRef) DeepCopyInto(out *SecretRef) { *out = *in }

// DeepCopy creates a new deep-copied SecretRef.
func (in *SecretRef) DeepCopy() *SecretRef {
	if in == nil {
		return nil
	}
	out := new(SecretRef)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject implements runtime.Object for Account
func (in *Account) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(Account)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *Account) DeepCopyInto(out *Account) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopyObject implements runtime.Object for AccountList
func (in *AccountList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(AccountList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *AccountList) DeepCopyInto(out *AccountList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Account, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

// DeepCopyObject implements runtime.Object for CryptoEdgeDeployment
func (in *CryptoEdgeDeployment) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(CryptoEdgeDeployment)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *CryptoEdgeDeployment) DeepCopyInto(out *CryptoEdgeDeployment) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopyObject implements runtime.Object for CryptoEdgeDeploymentList
func (in *CryptoEdgeDeploymentList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(CryptoEdgeDeploymentList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *CryptoEdgeDeploymentList) DeepCopyInto(out *CryptoEdgeDeploymentList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]CryptoEdgeDeployment, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *RegionSpec) DeepCopyInto(out *RegionSpec) {
	*out = *in
}

// DeepCopy creates a new deep-copied RegionSpec.
func (in *RegionSpec) DeepCopy() *RegionSpec {
	if in == nil {
		return nil
	}
	out := new(RegionSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject implements runtime.Object for Region
func (in *Region) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(Region)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *Region) DeepCopyInto(out *Region) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
}

// DeepCopyObject implements runtime.Object for RegionList
func (in *RegionList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(RegionList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out. in must be non-nil.
func (in *RegionList) DeepCopyInto(out *RegionList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Region, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}
