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
	"os"

	"k8s.io/component-base/cli"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	// Register the default upstream scheduler plugins.
	_ "k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"

	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/plugin"
)

func main() {
	command := app.NewSchedulerCommand(
		app.WithPlugin(plugin.Name, plugin.New),
	)
	code := cli.Run(command)
	os.Exit(code)
}
