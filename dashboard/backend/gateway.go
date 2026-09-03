package dashboard

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// RunLogGateway reads the logs for one Run through the Runtime Gateway. The
// Dashboard uses this narrow interface so its Kubernetes request client never
// needs pods/log permission.
type RunLogGateway interface {
	RunLogs(context.Context, string, string, string, string, int64, bool) (*http.Response, error)
}

// HTTPRunLogGateway is the in-cluster Runtime Gateway client used by the
// Dashboard. A request-scoped Kubernetes bearer token is forwarded only to the
// Gateway, never to frontend JavaScript.
type HTTPRunLogGateway struct {
	baseURL *url.URL
	client  *http.Client
}

func NewHTTPRunLogGateway(rawURL, caFile string) (*HTTPRunLogGateway, error) {
	endpoint, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, errors.New("Dashboard Runtime Gateway URL must be an absolute HTTP URL")
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, errors.New("Dashboard Runtime Gateway URL must use http or https")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read Dashboard Runtime Gateway CA bundle: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("Dashboard Runtime Gateway CA bundle contains no certificates")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	return &HTTPRunLogGateway{baseURL: endpoint, client: &http.Client{Transport: transport}}, nil
}

func (g *HTTPRunLogGateway) RunLogs(ctx context.Context, token, namespace, runtimeName, runUID string, tailLines int64, follow bool) (*http.Response, error) {
	if g == nil || g.baseURL == nil || g.client == nil {
		return nil, errors.New("Dashboard Runtime Gateway client is not configured")
	}
	endpoint := g.baseURL.JoinPath("v1", "namespaces", namespace, "runtimes", runtimeName, "runs", runUID, "logs")
	query := endpoint.Query()
	query.Set("tailLines", fmt.Sprintf("%d", tailLines))
	if follow {
		query.Set("follow", "true")
	}
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create Runtime Gateway log request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := g.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("call Runtime Gateway log API: %w", err)
	}
	return response, nil
}
