package sandbox

import (
	"context"
	"encoding/json"
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

func TestSandboxFileMutationsUseGatewayOperationNames(t *testing.T) {
	operations := []string{}
	sandbox := readySandbox(t, httpDoer(func(request *http.Request) (*http.Response, error) {
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		for name := range payload {
			operations = append(operations, name)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	}))
	ctx := context.Background()
	if err := sandbox.CreateDirectory(ctx, "notes"); err != nil {
		t.Fatal(err)
	}
	if err := sandbox.DeleteFile(ctx, "notes/old", true); err != nil {
		t.Fatal(err)
	}
	if err := sandbox.RenameFile(ctx, "notes/a", "notes/b", true); err != nil {
		t.Fatal(err)
	}
	if strings.Join(operations, ",") != "createDirectory,deleteFile,renameFile" {
		t.Fatalf("operation names = %v", operations)
	}
}

func TestSandboxReadFilePreservesPathForRuntimeValidation(t *testing.T) {
	paths := []string{}
	sandbox := readySandbox(t, httpDoer(func(request *http.Request) (*http.Response, error) {
		paths = append(paths, request.URL.EscapedPath())
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"contents":""}`))}, nil
	}))
	if _, _, err := sandbox.ReadFile(t.Context(), "notes/file.txt", 64); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sandbox.ReadFile(t.Context(), "../outside.txt", 0); err != nil {
		t.Fatal(err)
	}
	if strings.Join(paths, ",") != "/v1/namespaces/default/runtimes/bash/sessions/run-uid/files/notes/file.txt,/v1/namespaces/default/runtimes/bash/sessions/run-uid/files/../outside.txt" {
		t.Fatalf("paths = %v", paths)
	}
}

func TestSandboxLogsFilterRunUID(t *testing.T) {
	reader := logReader(func(context.Context, string, string, string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("not-json\n{\"run_uid\":\"other\",\"stream\":\"stdout\"}\n{\"run_uid\":\"run-uid\",\"stream\":\"audit\",\"message\":\"session operation completed\",\"operation\":\"command\"}\n")), nil
	})
	sandbox := readySandbox(t, httpDoer(func(*http.Request) (*http.Response, error) { return nil, nil }))
	sandbox.client.logReader = reader
	sandbox.run.Name = "session"
	sandbox.run.Namespace = "default"
	sandbox.run.Status.AssignedPod = "runtime-pod"
	if err := sandbox.client.runs.Create(t.Context(), sandbox.run); err != nil {
		t.Fatal(err)
	}
	lines, err := sandbox.Logs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 || lines[0].Stream != "audit" || lines[0].Operation != "command" {
		t.Fatalf("logs = %#v", lines)
	}
}

type httpDoer func(*http.Request) (*http.Response, error)

func (do httpDoer) Do(request *http.Request) (*http.Response, error) { return do(request) }

type logReader func(context.Context, string, string, string) (io.ReadCloser, error)

func (read logReader) Open(ctx context.Context, namespace, pod, container string) (io.ReadCloser, error) {
	return read(ctx, namespace, pod, container)
}

func readySandbox(t *testing.T, doer HTTPDoer) *Sandbox {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	client, err := New(Config{Runs: fake.NewClientBuilder().WithScheme(scheme).Build(), HTTPClient: doer})
	if err != nil {
		t.Fatal(err)
	}
	return &Sandbox{client: client, run: &v1alpha1.Run{ObjectMeta: metav1.ObjectMeta{UID: "run-uid"}, Status: v1alpha1.RunStatus{Phase: v1alpha1.RunReady, Endpoint: &v1alpha1.RunEndpoint{URL: "http://gateway/v1/namespaces/default/runtimes/bash/sessions/run-uid"}}}}
}

func TestSandboxRejectsEndpointForAnotherRun(t *testing.T) {
	sandbox := &Sandbox{run: &v1alpha1.Run{ObjectMeta: metav1.ObjectMeta{UID: "run-uid"}, Status: v1alpha1.RunStatus{Phase: v1alpha1.RunReady, Endpoint: &v1alpha1.RunEndpoint{URL: "http://gateway/v1/namespaces/default/runtimes/bash/sessions/other"}}}}
	if _, err := sandbox.endpoint("files"); err == nil {
		t.Fatal("endpoint() error = nil, want UID fencing error")
	}
}
