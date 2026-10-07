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

// Command scheduler runs the out-of-tree FairShareGPU scheduler plugin on top
// of the upstream kube-scheduler framework.
package main

import (
	"context"
	"os"
	"time"

	"k8s.io/component-base/cli"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	// Register the default upstream scheduler plugins.
	_ "k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/plugin"
)

func main() {
	// Keep the shared queue registry in sync with the Queue/QueueBinding CRDs
	// and node GPU labels. Failure is non-fatal (the plugin falls back to the
	// default queue), so the scheduler still starts without cluster access.
	go func() {
		cfg, err := ctrl.GetConfig()
		if err != nil {
			klog.Warningf("fairshare: no kube config, queue sync disabled: %v", err)
			return
		}
		if err := plugin.StartRegistrySync(context.Background(), cfg, 5*time.Second); err != nil {
			klog.Warningf("fairshare: registry sync disabled: %v", err)
		}
	}()

	command := app.NewSchedulerCommand(
		app.WithPlugin(plugin.Name, plugin.New),
	)
	code := cli.Run(command)
	os.Exit(code)
}
