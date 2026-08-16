package main

import (
	"context"
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
	"github.com/kruntimes/kruntimes/internal/gateway"
)

const runtimeIndexField = "spec.runtime"

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr string
		probeAddr   string
		httpAddr    string
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8085", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8086", "The address the health probe endpoint binds to.")
	flag.StringVar(&httpAddr, "http-bind-address", ":8084", "The address the Runtime gateway HTTP API binds to.")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	config := ctrl.GetConfigOrDie()
	manager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to create Runtime gateway manager")
		os.Exit(1)
	}
	if err := manager.GetFieldIndexer().IndexField(context.Background(), &v1alpha1.Run{}, runtimeIndexField, func(object client.Object) []string {
		run, ok := object.(*v1alpha1.Run)
		if !ok || run.Spec.Runtime == "" {
			return nil
		}
		return []string{run.Spec.Runtime}
	}); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to index Runs by Runtime")
		os.Exit(1)
	}
	if err := manager.AddHealthzCheck("ping", healthz.Ping); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to register health check")
		os.Exit(1)
	}
	if err := manager.AddReadyzCheck("ping", healthz.Ping); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to register readiness check")
		os.Exit(1)
	}
	if err := manager.Add(&gateway.Server{
		Runs:       manager.GetCache(),
		Authorizer: gateway.KubernetesAuthorizer{Client: kubernetes.NewForConfigOrDie(config)},
		Dialer:     gateway.GRPCDialer{},
		Address:    httpAddr,
	}); err != nil {
		ctrl.Log.WithName("setup").Error(err, "unable to add Runtime gateway server")
		os.Exit(1)
	}

	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.WithName("setup").Error(err, "Runtime gateway stopped")
		os.Exit(1)
	}
}
