package krt

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestWriteSnapshotRunLogsWritesStdoutAndStderr(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := writeSnapshotRunLogs(strings.NewReader(`{"items":[
		{"stream":"stdout","message":"stdout one"},
		{"stream":"audit","message":"ignore"},
		{"stream":"stderr","message":"stderr one"},
		{"stream":"stdout","message":"stdout two\n"}
	]}`), &stdout, &stderr)
	if err != nil {
		t.Fatalf("writeSnapshotRunLogs() error = %v", err)
	}
	if got := stdout.String(); got != "stdout one\nstdout two\n" {
		t.Fatalf("stdout = %q", got)
	}
	if got := stderr.String(); got != "stderr one\n" {
		t.Fatalf("stderr = %q", got)
	}
}

func TestWriteFollowRunLogsWritesEachRecord(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	input := strings.Join([]string{
		`{"stream":"stdout","message":"stdout one"}`,
		`{"stream":"stderr","message":"stderr one"}`,
		`{"stream":"audit","message":"ignore"}`,
	}, "\n")

	if err := writeFollowRunLogs(strings.NewReader(input), &stdout, &stderr); err != nil {
		t.Fatalf("writeFollowRunLogs() error = %v", err)
	}
	if got := stdout.String(); got != "stdout one\n" {
		t.Fatalf("stdout = %q", got)
	}
	if got := stderr.String(); got != "stderr one\n" {
		t.Fatalf("stderr = %q", got)
	}
}

func TestRunLogGatewayUsesGatewayRouteAndReportsError(t *testing.T) {
	baseURL, err := url.Parse("https://gateway.example.test/base")
	if err != nil {
		t.Fatal(err)
	}
	gateway := &runLogGateway{
		baseURL: baseURL,
		client: &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			if got, want := request.URL.Path, "/base/v1/namespaces/team-a/runtimes/bash/runs/uid/logs"; got != want {
				t.Fatalf("path = %q, want %q", got, want)
			}
			if got := request.URL.Query(); got.Get("tailLines") != "42" || got.Get("follow") != "true" {
				t.Fatalf("query = %q", request.URL.RawQuery)
			}
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Status:     "403 Forbidden",
				Body:       io.NopCloser(strings.NewReader(`{"error":"authorization denied"}`)),
				Header:     make(http.Header),
			}, nil
		})},
	}

	_, err = gateway.logs(context.Background(), "team-a", "bash", "uid", 42, true)
	if err == nil || !strings.Contains(err.Error(), "403 Forbidden: authorization denied") {
		t.Fatalf("logs() error = %v", err)
	}
}

func TestNewRunLogGatewayRejectsInvalidURL(t *testing.T) {
	if _, err := newRunLogGateway(nil, "not-a-url", "", false); err == nil {
		t.Fatal("newRunLogGateway() error = nil")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
