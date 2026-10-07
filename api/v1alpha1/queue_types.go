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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:generate=true
type QueueSpec struct {
	Parent        string             `json:"parent,omitempty"`
	Weight        int32              `json:"weight,omitempty"`
	Guaranteed    v1.ResourceList    `json:"guaranteed,omitempty"`
	BorrowLimit   v1.ResourceList    `json:"borrowLimit,omitempty"`
	PriorityClass int32              `json:"priorityClass,omitempty"`
	Reclaimable   bool               `json:"reclaimable,omitempty"`
	Preemptible   bool               `json:"preemptible,omitempty"`
}

// +kubebuilder:object:generate=true
type QueueStatus struct {
	Allocated       v1.ResourceList `json:"allocated,omitempty"`
	Borrowed        v1.ResourceList `json:"borrowed,omitempty"`
	Lent            v1.ResourceList `json:"lent,omitempty"`
	PendingPods     int32           `json:"pendingPods,omitempty"`
	ShareRatio      string          `json:"shareRatio,omitempty"`
	LastReclaimTime *metav1.Time    `json:"lastReclaimTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=q
// +kubebuilder:object:generate=true
type Queue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   QueueSpec   `json:"spec,omitempty"`
	Status QueueStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:object:generate=true
type QueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Queue `json:"items"`
}

// +kubebuilder:object:generate=true
type QueueBindingSpec struct {
	Queue             string                  `json:"queue"`
	NamespaceSelector *metav1.LabelSelector   `json:"namespaceSelector,omitempty"`
	Namespace         string                  `json:"namespace,omitempty"`
	PodSelector       *metav1.LabelSelector   `json:"podSelector,omitempty"`
}

// +kubebuilder:object:generate=true
type QueueBindingStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:scope=Namespaced,shortName=qb
// +kubebuilder:object:generate=true
type QueueBinding struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   QueueBindingSpec   `json:"spec,omitempty"`
	Status QueueBindingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:object:generate=true
type QueueBindingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QueueBinding `json:"items"`
}
