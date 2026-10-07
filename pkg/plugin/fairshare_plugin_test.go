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

package plugin

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPluginPreFilterAndFilter(t *testing.T) {
	ctx := context.TODO()
	p, err := New(ctx, nil, nil)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	fsPlugin := p.(*FairShareGPU)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels: map[string]string{
				"fairshare.io/queue": "default-queue",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "c1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceName("nvidia.com/gpu"): mustQuantity("2"),
						},
					},
				},
			},
		},
	}

	_, preErr := fsPlugin.PreFilter(ctx, pod)
	if preErr != nil {
		t.Errorf("expected PreFilter success, got %v", preErr)
	}

	fits := fsPlugin.Filter(ctx, pod, "worker-1", 4)
	if !fits {
		t.Error("expected Filter to pass with 4 free GPUs")
	}

	score := fsPlugin.Score(ctx, pod, "worker-1")
	if score <= 0 {
		t.Errorf("expected positive score, got %d", score)
	}
}

func mustQuantity(s string) resource.Quantity {
	q, _ := resource.ParseQuantity(s)
	return q
}
