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

// Command controller runs the FairShare quota controller: it reconciles Queue
// status, performs elastic lend/reclaim and exposes FinOps metrics on :9090.
package main

import (
	"flag"
	"net/http"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	fairsharev1alpha1 "github.com/abhiramkasireddi/fairshare-gpu-scheduler/api/v1alpha1"
	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/controller"
	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/finops"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(fairsharev1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var gpuHourlyRate float64
	flag.StringVar(&metricsAddr, "metrics-addr", ":9090", "The address the FinOps metrics endpoint binds to.")
	flag.Float64Var(&gpuHourlyRate, "gpu-hourly-rate", 2.50, "USD cost per GPU-hour for chargeback.")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Serve the Prometheus FinOps metrics registry on the configured address.
	mux := http.NewServeMux()
	mux.Handle("/metrics", finops.MetricsHandler())
	go func() {
		setupLog.Info("starting metrics endpoint", "addr", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			setupLog.Error(err, "metrics server stopped")
		}
	}()

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err := (&controller.QueueReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		GpuHourlyRate: gpuHourlyRate,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Queue")
		os.Exit(1)
	}

	setupLog.Info("starting FairShare quota controller")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
