package sandbox

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

func TestSandboxCreateAndExecute(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	doer := httpDoer(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		if r.URL.Path != "/v1/namespaces/default/runtimes/bash/sessions/run-uid/operations:execute" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"command":{"exitCode":0,"stdout":"b2s="}}`))}, nil
	})
	runs := fake.NewClientBuilder().WithScheme(scheme).Build()
	client, err := New(Config{Runs: runs, HTTPClient: doer, BearerToken: "token"})
	if err != nil {
		t.Fatal(err)
	}
	sandbox, err := client.Create(t.Context(), CreateOptions{Name: "session", Namespace: "default", Runtime: "bash", Env: map[string]string{"B": "2", "A": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if sandbox.run.Spec.Mode.Session == nil || len(sandbox.run.Spec.Env) != 2 || sandbox.run.Spec.Env[0].Name != "A" {
		t.Fatalf("created Run = %#v", sandbox.run.Spec)
	}
	sandbox.run.UID = types.UID("run-uid")
	sandbox.run.Status = v1alpha1.RunStatus{Phase: v1alpha1.RunReady, Endpoint: &v1alpha1.RunEndpoint{URL: "http://gateway/v1/namespaces/default/runtimes/bash/sessions/run-uid"}}
	if _, err := sandbox.Execute(context.Background(), Command{Argv: []string{"echo", "ok"}}); err != nil {
		t.Fatal(err)
	}
}

type httpDoer func(*http.Request) (*http.Response, error)

func (do httpDoer) Do(request *http.Request) (*http.Response, error) { return do(request) }

func TestSandboxRejectsEndpointForAnotherRun(t *testing.T) {
	sandbox := &Sandbox{run: &v1alpha1.Run{ObjectMeta: metav1.ObjectMeta{UID: "run-uid"}, Status: v1alpha1.RunStatus{Phase: v1alpha1.RunReady, Endpoint: &v1alpha1.RunEndpoint{URL: "http://gateway/v1/namespaces/default/runtimes/bash/sessions/other"}}}}
	if _, err := sandbox.endpoint("files"); err == nil {
		t.Fatal("endpoint() error = nil, want UID fencing error")
	}
}
