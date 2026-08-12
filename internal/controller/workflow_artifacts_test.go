package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

func TestResolveWorkflowArtifactInputs(t *testing.T) {
	ref := workflowArtifactRef("dist.tgz")
	inputs, err := resolveWorkflowArtifactInputs([]v1alpha1.WorkflowArtifactInput{{From: "jobs.build.artifacts.dist.tgz", Path: "dist.tgz"}}, map[string]v1alpha1.JobStatus{
		"build": {Phase: v1alpha1.JobSucceeded, Artifacts: map[string]v1alpha1.ArtifactRef{"dist.tgz": ref}},
	})
	if err != nil {
		t.Fatalf("resolveWorkflowArtifactInputs: %v", err)
	}
	if len(inputs) != 1 || inputs[0].Path != "dist.tgz" || inputs[0].Ref.Name != ref.Name {
		t.Fatalf("resolved inputs = %#v", inputs)
	}

	if _, err := resolveWorkflowArtifactInputs([]v1alpha1.WorkflowArtifactInput{{From: "jobs.build.artifacts.missing", Path: "missing"}}, map[string]v1alpha1.JobStatus{
		"build": {Phase: v1alpha1.JobSucceeded, Artifacts: map[string]v1alpha1.ArtifactRef{"dist.tgz": ref}},
	}); err == nil {
		t.Fatal("missing artifact was accepted")
	}
	job, artifact, err := workflowArtifactSource("jobs.build.artifacts.dist.tgz")
	if err != nil || job != "build" || artifact != "dist.tgz" {
		t.Fatalf("workflowArtifactSource() = (%q, %q, %v), want build, dist.tgz, nil", job, artifact, err)
	}
}

func TestResolveJobArtifactRefsUsesStepOrder(t *testing.T) {
	first := workflowArtifactRef("result")
	first.Location.Filesystem.Path = "runs/first/result"
	second := workflowArtifactRef("result")
	second.Location.Filesystem.Path = "runs/second/result"
	refs := resolveJobArtifactRefs(v1alpha1.JobStatus{Steps: []v1alpha1.StepStatus{
		{Name: "first", RunName: "first-run"},
		{Name: "second", RunName: "second-run"},
	}}, map[string]*v1alpha1.Run{
		"first":  {ObjectMeta: metav1.ObjectMeta{Name: "first-run"}, Status: v1alpha1.RunStatus{Phase: v1alpha1.RunSucceeded, ArtifactRefs: []v1alpha1.ArtifactRef{first}}},
		"second": {ObjectMeta: metav1.ObjectMeta{Name: "second-run"}, Status: v1alpha1.RunStatus{Phase: v1alpha1.RunSucceeded, ArtifactRefs: []v1alpha1.ArtifactRef{second}}},
	})
	if got := refs["result"].Location.Filesystem.Path; got != second.Location.Filesystem.Path {
		t.Fatalf("result ref path = %q, want %q", got, second.Location.Filesystem.Path)
	}
}

func TestValidateWorkflowArtifactDependencies(t *testing.T) {
	jobs := map[string]v1alpha1.JobSpec{
		"build": {RunsOn: "bash", Steps: []v1alpha1.StepSpec{{Name: "build", Run: "echo build"}}},
		"deploy": {
			RunsOn: "bash", Needs: []string{"build"},
			Steps: []v1alpha1.StepSpec{{Name: "deploy", Run: "echo deploy", Artifacts: []v1alpha1.WorkflowArtifactInput{{From: "jobs.build.artifacts.dist.tgz", Path: "dist.tgz"}}}},
		},
	}
	if err := validateWorkflowArtifactDependencies(jobs, nil); err != nil {
		t.Fatalf("validate dependencies: %v", err)
	}
	jobs["deploy"] = v1alpha1.JobSpec{RunsOn: "bash", Steps: jobs["deploy"].Steps}
	if err := validateWorkflowArtifactDependencies(jobs, nil); err == nil {
		t.Fatal("artifact dependency without needs was accepted")
	}
}

func workflowArtifactRef(name string) v1alpha1.ArtifactRef {
	return v1alpha1.ArtifactRef{
		Name: name, Driver: v1alpha1.ArtifactDriverFilesystem, Type: v1alpha1.ArtifactTypeFile,
		Location:  v1alpha1.ArtifactLocation{Filesystem: &v1alpha1.FilesystemArtifactLocation{Path: "runs/source/" + name}},
		CreatedAt: metav1.Now(),
	}
}
