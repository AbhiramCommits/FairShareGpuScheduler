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
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fairsharev1alpha1 "github.com/abhiramkasireddi/fairshare-gpu-scheduler/api/v1alpha1"
	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/fairshare"
	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/topology"
)

func scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = fairsharev1alpha1.AddToScheme(s)
	return s
}

// StartRegistrySync periodically loads Queue, QueueBinding and Node state from
// the API server into the shared registry so the plugin enforces real quota.
// It is a no-op if the config is nil.
func StartRegistrySync(ctx context.Context, cfg *rest.Config, interval time.Duration) error {
	if cfg == nil {
		return nil
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	var c client.Client
	ensure := func() (client.Client, error) {
		if c != nil {
			return c, nil
		}
		nc, err := client.New(cfg, client.Options{Scheme: scheme()})
		if err != nil {
			return nil, err
		}
		c = nc
		return c, nil
	}
	sync := func() {
		cl, err := ensure()
		if err != nil {
			klog.Warningf("fairshare: could not create sync client: %v", err)
			return
		}
		if err := syncOnce(ctx, cl); err != nil {
			// Drop the client so the next tick rebuilds its RESTMapper; this
			// recovers from a discovery/CRD race at startup.
			klog.Warningf("fairshare: registry sync failed, will retry: %v", err)
			c = nil
			return
		}
		tree := defaultRegistry.Tree()
		n := 0
		if tree != nil {
			n = len(tree.Children)
		}
		klog.V(2).Infof("fairshare: registry synced (%d queues)", n)
	}
	sync()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sync()
			}
		}
	}()
	return nil
}

// SyncOnce performs a single registry refresh. Exported for tests and tooling.
func SyncOnce(ctx context.Context, c client.Client) error {
	return syncOnce(ctx, c)
}

func syncOnce(ctx context.Context, c client.Client) error {
	var queues fairsharev1alpha1.QueueList
	if err := c.List(ctx, &queues); err != nil {
		return err
	}
	specs := make([]fairshare.QueueSpec, 0, len(queues.Items))
	for _, q := range queues.Items {
		specs = append(specs, fairshare.QueueSpec{
			Name:          q.Name,
			Parent:        q.Spec.Parent,
			Weight:        int64(q.Spec.Weight),
			Guaranteed:    resourceVec(q.Spec.Guaranteed),
			BorrowLimit:   resourceVec(q.Spec.BorrowLimit),
			PriorityClass: q.Spec.PriorityClass,
			Reclaimable:   q.Spec.Reclaimable,
			Preemptible:   q.Spec.Preemptible,
		})
	}
	if len(specs) > 0 {
		_ = defaultRegistry.SetQueues(specs)
	}

	var bindings fairsharev1alpha1.QueueBindingList
	if err := c.List(ctx, &bindings); err == nil {
		bs := make([]BindingSpec, 0, len(bindings.Items))
		for _, b := range bindings.Items {
			bs = append(bs, BindingSpec{
				Queue:             b.Spec.Queue,
				Namespace:         b.Spec.Namespace,
				NamespaceSelector: selectorMap(b.Spec.NamespaceSelector),
				PodSelector:       selectorMap(b.Spec.PodSelector),
			})
		}
		defaultRegistry.SetBindings(bs)
	}

	var nodes v1.NodeList
	if err := c.List(ctx, &nodes); err == nil {
		states := map[string]topology.NodeGPUState{}
		capacity := 0.0
		for _, n := range nodes.Items {
			gpu := 0
			if val, ok := n.Status.Allocatable[v1.ResourceName(GPUResourceName)]; ok {
				gpu = int(val.Value())
			}
			capacity += float64(gpu)
			states[n.Name] = topology.NodeGPUState{
				NodeName:    n.Name,
				FreeGPUs:    gpu,
				TotalGPUs:   gpu,
				NVLinkGroup: n.Labels["fairshare.io/nvlink-group"],
				NUMANode:    n.Labels["topology.kubernetes.io/numa-node"],
			}
		}
		defaultRegistry.SetNodeStates(states)
		defaultRegistry.SetCapacity(fairshare.ResourceVec{GPUResourceName: capacity})
	}
	return nil
}

func resourceVec(rl v1.ResourceList) fairshare.ResourceVec {
	rv := fairshare.ResourceVec{}
	for name, q := range rl {
		rv[string(name)] = float64(q.Value())
	}
	return rv
}

func selectorMap(sel *metav1.LabelSelector) map[string]string {
	if sel == nil {
		return nil
	}
	m, err := metav1.LabelSelectorAsMap(sel)
	if err != nil {
		return nil
	}
	return m
}
