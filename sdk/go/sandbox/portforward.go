package sandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// GatewayPortForward forwards Runtime gateway HTTP requests through one local
// Kubernetes Pod port-forward. It changes only the endpoint scheme and host;
// the Run-owned path, query, and HTTP headers are preserved.
type GatewayPortForward struct {
	httpClient HTTPDoer
	localURL   *url.URL
	stop       chan struct{}
	done       chan struct{}
	err        error
	errMu      sync.RWMutex
	closeOnce  sync.Once
}

// StartGatewayPortForward starts a local port-forward to one Ready Pod behind
// the shared Runtime gateway Service. The returned value implements HTTPDoer
// and can be passed as Config.HTTPClient to NewFromRESTConfig.
func StartGatewayPortForward(ctx context.Context, config *rest.Config, namespace, service string, servicePort int) (*GatewayPortForward, error) {
	if config == nil {
		return nil, errors.New("Kubernetes REST config is required")
	}
	if namespace == "" || service == "" {
		return nil, errors.New("gateway namespace and service are required")
	}
	if servicePort <= 0 || servicePort > 65535 {
		return nil, fmt.Errorf("invalid gateway service port %d", servicePort)
	}
	pods, err := corev1client.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes Pod client: %w", err)
	}
	podName, err := readyGatewayPod(ctx, pods, namespace, service)
	if err != nil {
		return nil, err
	}
	httpClient, err := rest.HTTPClientFor(config)
	if err != nil {
		return nil, fmt.Errorf("create Runtime gateway HTTP client: %w", err)
	}
	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes port-forward transport: %w", err)
	}
	apiURL, err := url.Parse(config.Host)
	if err != nil {
		return nil, fmt.Errorf("parse Kubernetes API URL: %w", err)
	}
	apiURL.Path = fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", namespace, podName)
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, apiURL)
	stop := make(chan struct{})
	ready := make(chan struct{})
	forwarder, err := portforward.NewOnAddresses(dialer, []string{"127.0.0.1"}, []string{"0:" + strconv.Itoa(servicePort)}, stop, ready, io.Discard, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("create Runtime gateway port-forward: %w", err)
	}
	forward := &GatewayPortForward{httpClient: httpClient, stop: stop, done: make(chan struct{})}
	go func() {
		defer close(forward.done)
		forward.setError(forwarder.ForwardPorts())
	}()
	select {
	case <-ctx.Done():
		forward.Close()
		return nil, ctx.Err()
	case <-ready:
	case <-forward.done:
		return nil, fmt.Errorf("start Runtime gateway port-forward: %w", forward.Error())
	}
	ports, err := forwarder.GetPorts()
	if err != nil || len(ports) != 1 {
		forward.Close()
		if err != nil {
			return nil, fmt.Errorf("get Runtime gateway local port: %w", err)
		}
		return nil, errors.New("Runtime gateway port-forward did not expose one local port")
	}
	forward.localURL = &url.URL{Scheme: "http", Host: "127.0.0.1:" + strconv.Itoa(int(ports[0].Local))}
	return forward, nil
}

// Do implements HTTPDoer. It fails after the local port-forward exits.
func (f *GatewayPortForward) Do(request *http.Request) (*http.Response, error) {
	if f == nil || f.httpClient == nil || f.localURL == nil {
		return nil, errors.New("Runtime gateway port-forward is not ready")
	}
	select {
	case <-f.done:
		if err := f.Error(); err != nil {
			return nil, fmt.Errorf("Runtime gateway port-forward: %w", err)
		}
		return nil, errors.New("Runtime gateway port-forward is closed")
	default:
	}
	copy := request.Clone(request.Context())
	endpoint := *request.URL
	endpoint.Scheme = f.localURL.Scheme
	endpoint.Host = f.localURL.Host
	copy.URL = &endpoint
	copy.Host = ""
	return f.httpClient.Do(copy)
}

// Close stops the local port-forward. It is safe to call more than once.
func (f *GatewayPortForward) Close() {
	if f == nil {
		return
	}
	f.closeOnce.Do(func() { close(f.stop) })
	<-f.done
}

// Error reports a non-nil port-forward failure after it exits.
func (f *GatewayPortForward) Error() error {
	if f == nil {
		return errors.New("Runtime gateway port-forward is nil")
	}
	f.errMu.RLock()
	defer f.errMu.RUnlock()
	return f.err
}

func (f *GatewayPortForward) setError(err error) {
	f.errMu.Lock()
	defer f.errMu.Unlock()
	f.err = err
}

func readyGatewayPod(ctx context.Context, pods corev1client.CoreV1Interface, namespace, service string) (string, error) {
	serviceObject, err := pods.Services(namespace).Get(ctx, service, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get Runtime gateway Service %q: %w", service, err)
	}
	if len(serviceObject.Spec.Selector) == 0 {
		return "", fmt.Errorf("Runtime gateway Service %q has no selector", service)
	}
	list, err := pods.Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.Set(serviceObject.Spec.Selector).String()})
	if err != nil {
		return "", fmt.Errorf("list Runtime gateway Pods: %w", err)
	}
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		if podReady(&list.Items[i]) {
			names = append(names, list.Items[i].Name)
		}
	}
	if len(names) == 0 {
		return "", fmt.Errorf("Runtime gateway Service %q has no Ready Pods", service)
	}
	sort.Strings(names)
	return names[0], nil
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
