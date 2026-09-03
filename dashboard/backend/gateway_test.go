package dashboard

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestHTTPRunLogGatewayForwardsOnlyTheRunLogRequest(t *testing.T) {
	gateway, err := NewHTTPRunLogGateway("http://gateway.example/prefix", "")
	if err != nil {
		t.Fatalf("NewHTTPRunLogGateway() error = %v", err)
	}
	gateway.client.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/prefix/v1/namespaces/team-a/runtimes/python/runs/run-uid/logs" {
			t.Fatalf("request = %s %s", request.Method, request.URL.String())
		}
		if request.URL.Query().Get("tailLines") != "42" || request.URL.Query().Get("follow") != "true" {
			t.Fatalf("query = %q", request.URL.RawQuery)
		}
		if request.Header.Get("Authorization") != "Bearer caller-token" {
			t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/x-ndjson"}}, Body: io.NopCloser(strings.NewReader(`{"stream":"stdout","message":"visible"}` + "\n"))}, nil
	})
	response, err := gateway.RunLogs(t.Context(), "caller-token", "team-a", "python", "run-uid", 42, true)
	if err != nil {
		t.Fatalf("RunLogs() error = %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "{\"stream\":\"stdout\",\"message\":\"visible\"}\n" {
		t.Fatalf("response body = %q, %v", body, err)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }
