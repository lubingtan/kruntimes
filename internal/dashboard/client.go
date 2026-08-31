// Package dashboard implements the read-only Dashboard backend.
package dashboard

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var ErrMissingBearerToken = errors.New("dashboard request requires a Kubernetes bearer token")

// RequestClientFactory creates Kubernetes clients that act only as the
// credential supplied with one Dashboard request. The base configuration
// contributes transport details such as the in-cluster API endpoint and CA; it
// never contributes an API credential.
type RequestClientFactory struct {
	baseConfig *rest.Config
	scheme     *runtime.Scheme
	newClient  func(*rest.Config, client.Options) (client.Client, error)
}

func NewRequestClientFactory(baseConfig *rest.Config, scheme *runtime.Scheme) (*RequestClientFactory, error) {
	if baseConfig == nil {
		return nil, errors.New("dashboard Kubernetes base configuration is required")
	}
	if baseConfig.Host == "" {
		return nil, errors.New("dashboard Kubernetes base configuration requires an API host")
	}
	if scheme == nil {
		return nil, errors.New("dashboard Kubernetes scheme is required")
	}
	return &RequestClientFactory{
		baseConfig: rest.CopyConfig(baseConfig),
		scheme:     scheme,
		newClient:  client.New,
	}, nil
}

// ClientForRequest returns a new client whose credential is the request bearer
// token. A client is intentionally not cached because a Dashboard request must
// never reuse another user's Kubernetes identity.
func (f *RequestClientFactory) ClientForRequest(request *http.Request) (client.Client, error) {
	if f == nil {
		return nil, errors.New("dashboard request client factory is required")
	}
	token, err := bearerToken(request)
	if err != nil {
		return nil, err
	}
	config := f.configForToken(token)
	kubernetesClient, err := f.newClient(config, client.Options{Scheme: f.scheme})
	if err != nil {
		return nil, fmt.Errorf("create request-scoped Kubernetes client: %w", err)
	}
	return kubernetesClient, nil
}

func bearerToken(request *http.Request) (string, error) {
	if request == nil {
		return "", ErrMissingBearerToken
	}
	parts := strings.Fields(request.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", ErrMissingBearerToken
	}
	return parts[1], nil
}

func (f *RequestClientFactory) configForToken(token string) *rest.Config {
	// AnonymousClientConfig copies only client-go's known-safe transport fields.
	// In particular, it removes bearer-file, basic-auth, auth-provider,
	// exec-plugin, TLS client-certificate, custom transport, and impersonation
	// credentials before the caller token becomes the complete request identity.
	config := rest.AnonymousClientConfig(f.baseConfig)
	config.BearerToken = token
	return config
}
