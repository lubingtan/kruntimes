package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
	"github.com/kruntimes/kruntimes/internal/dashboard"
)

func main() {
	var (
		address         string
		certificateFile string
		privateKeyFile  string
	)
	flag.StringVar(&address, "bind-address", ":8443", "The HTTPS address the Dashboard binds to.")
	flag.StringVar(&certificateFile, "tls-certificate-file", "", "PEM TLS certificate file for the Dashboard. Required.")
	flag.StringVar(&privateKeyFile, "tls-private-key-file", "", "PEM TLS private key file for the Dashboard. Required.")
	flag.Parse()
	if certificateFile == "" || privateKeyFile == "" {
		fmt.Fprintln(os.Stderr, "both --tls-certificate-file and --tls-private-key-file are required")
		os.Exit(2)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	factory, err := dashboard.NewRequestClientFactory(ctrl.GetConfigOrDie(), scheme)
	if err != nil {
		fmt.Fprintf(os.Stderr, "configure Dashboard Kubernetes client: %v\n", err)
		os.Exit(1)
	}

	server := &http.Server{
		Addr:              address,
		Handler:           &dashboard.Server{Clients: factory},
		ReadHeaderTimeout: 5 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}()

	if err := server.ListenAndServeTLS(certificateFile, privateKeyFile); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(os.Stderr, "serve Dashboard: %v\n", err)
		os.Exit(1)
	}
}
