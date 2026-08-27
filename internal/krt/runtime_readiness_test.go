package krt

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

func TestRuntimeListTableShowsDesiredAndReadyReplicas(t *testing.T) {
	runtimeResource := readinessTestRuntime()
	var output bytes.Buffer
	if err := writeRuntimeListTable(&output, &v1alpha1.RuntimeList{Items: []v1alpha1.Runtime{*runtimeResource}}); err != nil {
		t.Fatalf("writeRuntimeListTable() error = %v", err)
	}
	for _, want := range []string{"REPLICAS", "READY", "bash", "3", "2"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("list output %q does not contain %q", output.String(), want)
		}
	}
}

func TestRuntimeGetTableShowsDesiredAndReadyReplicas(t *testing.T) {
	var output bytes.Buffer
	if err := writeRuntimeGetTable(&output, readinessTestRuntime()); err != nil {
		t.Fatalf("writeRuntimeGetTable() error = %v", err)
	}
	lines := strings.Split(output.String(), "\n")
	if !containsRuntimeOutputFields(lines, "Replicas:", "3") {
		t.Errorf("get output %q does not contain Replicas: 3", output.String())
	}
	if !containsRuntimeOutputFields(lines, "Ready:", "2") {
		t.Errorf("get output %q does not contain Ready: 2", output.String())
	}
}

func containsRuntimeOutputFields(lines []string, want ...string) bool {
	for _, line := range lines {
		if fields := strings.Fields(line); len(fields) == len(want) && slices.Equal(fields, want) {
			return true
		}
	}
	return false
}

func TestRuntimeStructuredOutputPreservesReadyReplicas(t *testing.T) {
	var output bytes.Buffer
	if err := writeStructuredOutput(&output, outputJSON, readinessTestRuntime()); err != nil {
		t.Fatalf("writeStructuredOutput() error = %v", err)
	}
	if !strings.Contains(output.String(), `"readyReplicas": 2`) {
		t.Errorf("JSON output %q does not contain readyReplicas", output.String())
	}
}

func readinessTestRuntime() *v1alpha1.Runtime {
	return &v1alpha1.Runtime{
		ObjectMeta: metav1.ObjectMeta{Name: "bash", Namespace: "default"},
		Spec: v1alpha1.RuntimeSpec{
			Replicas: 3,
			Port:     9091,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "runtime", Image: "bash:latest"}},
			}},
		},
		Status: v1alpha1.RuntimeStatus{ReadyReplicas: 2},
	}
}
