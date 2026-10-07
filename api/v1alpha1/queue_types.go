/*
Copyright 2026 The FairShareGpuScheduler Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// QueueSpec defines the desired state of Queue.
type QueueSpec struct {
	// Parent is the parent queue name in the hierarchy. Empty means root.
	// +optional
	Parent string `json:"parent,omitempty"`

	// Weight is the relative weight of the queue for DRF fair sharing.
	// +kubebuilder:default=1
	Weight int32 `json:"weight,omitempty"`

	// Guaranteed is the guaranteed resource reservation (must carry nvidia.com/gpu).
	// +optional
	Guaranteed metav1.ResourceList `json:"guaranteed,omitempty"`

	// BorrowLimit is the maximum additional resources the queue can borrow beyond guaranteed.
	// +optional
	BorrowLimit metav1.ResourceList `json:"borrowLimit,omitempty"`

	// PriorityClass is the default priority class value for pods in this queue.
	// +kubebuilder:default=100
	PriorityClass int32 `json:"priorityClass,omitempty"`

	// Reclaimable indicates whether borrowed resources can be reclaimed when parent/ancestor needs them.
	// +kubebuilder:default=true
	Reclaimable bool `json:"reclaimable,omitempty"`

	// Preemptible indicates whether pods in this queue can be preempted by higher priority/starving queues.
	// +kubebuilder:default=true
	Preemptible bool `json:"preemptible,omitempty"`
}

// QueueStatus defines the observed state of Queue.
type QueueStatus struct {
	// Allocated is the total resources currently allocated to pods in this queue.
	// +optional
	Allocated metav1.ResourceList `json:"allocated,omitempty"`

	// Borrowed is the resources currently borrowed beyond guaranteed.
	// +optional
	Borrowed metav1.ResourceList `json:"borrowed,omitempty"`

	// Lent is the guaranteed resources currently lent out to sibling/descendant queues.
	// +optional
	Lent metav1.ResourceList `json:"lent,omitempty"`

	// PendingPods is the number of pods currently pending in this queue.
	// +optional
	PendingPods int32 `json:"pendingPods,omitempty"`

	// ShareRatio is the dominant resource share ratio string.
	// +optional
	ShareRatio string `json:"shareRatio,omitempty"`

	// LastReclaimTime is the timestamp of the last resource reclaim operation.
	// +optional
	LastReclaimTime *metav1.Time `json:"lastReclaimTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=q
type Queue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   QueueSpec   `json:"spec,omitempty"`
	Status QueueStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type QueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Queue `json:"items"`
}

// QueueBindingSpec defines the desired state of QueueBinding.
type QueueBindingSpec struct {
	// Queue is the target Queue name.
	Queue string `json:"queue"`

	// NamespaceSelector matches namespaces for this binding.
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`

	// Namespace is a direct namespace assignment.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// PodSelector matches pods within target namespaces.
	// +optional
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty"`
}

// QueueBindingStatus defines the observed state of QueueBinding.
type QueueBindingStatus struct {
	// Conditions represent the latest available observations of binding state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=qb
type QueueBinding struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   QueueBindingSpec   `json:"spec,omitempty"`
	Status QueueBindingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type QueueBindingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QueueBinding `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Queue{}, &QueueList{}, &QueueBinding{}, &QueueBindingList{})
}
