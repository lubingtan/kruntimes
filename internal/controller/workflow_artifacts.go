package controller

import (
	"fmt"
	"slices"
	"strings"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

func resolveWorkflowArtifactInputs(inputs []v1alpha1.WorkflowArtifactInput, statuses map[string]v1alpha1.JobStatus) ([]v1alpha1.ArtifactInput, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	resolved := make([]v1alpha1.ArtifactInput, 0, len(inputs))
	for _, input := range inputs {
		jobName, artifactName, err := workflowArtifactSource(input.From)
		if err != nil {
			return nil, err
		}
		job, found := statuses[jobName]
		if !found || job.Phase != v1alpha1.JobSucceeded {
			return nil, fmt.Errorf("artifact input %q requires succeeded job %q", input.From, jobName)
		}
		ref, found := job.Artifacts[artifactName]
		if !found {
			return nil, fmt.Errorf("artifact input %q was not produced", input.From)
		}
		resolved = append(resolved, v1alpha1.ArtifactInput{Ref: *ref.DeepCopy(), Path: input.Path})
	}
	return resolved, nil
}

func workflowArtifactSource(source string) (jobName, artifactName string, err error) {
	if !strings.HasPrefix(source, "jobs.") {
		return "", "", fmt.Errorf("invalid artifact source %q (expected jobs.<job-id>.artifacts.<name>)", source)
	}
	jobName, artifactName, found := strings.Cut(strings.TrimPrefix(source, "jobs."), ".artifacts.")
	if !found || jobName == "" || artifactName == "" {
		return "", "", fmt.Errorf("invalid artifact source %q (expected jobs.<job-id>.artifacts.<name>)", source)
	}
	return jobName, artifactName, nil
}

func validateWorkflowArtifactDependencies(jobs map[string]v1alpha1.JobSpec, actions map[string]workflowActionSnapshot) error {
	for jobName, job := range jobs {
		for _, step := range job.Steps {
			if step.Uses == "" {
				if err := validateStepArtifactDependencies(jobName, job.Needs, step.Artifacts); err != nil {
					return err
				}
				continue
			}
			action, found := actions[workflowActionSnapshotKey(jobName, step.Name)]
			if !found {
				continue
			}
			for _, actionStep := range action.Spec.Steps {
				if err := validateStepArtifactDependencies(jobName, job.Needs, actionStep.Artifacts); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateStepArtifactDependencies(jobName string, dependencies []string, inputs []v1alpha1.WorkflowArtifactInput) error {
	for _, input := range inputs {
		sourceJob, _, err := workflowArtifactSource(input.From)
		if err != nil {
			return err
		}
		if !slices.Contains(dependencies, sourceJob) {
			return fmt.Errorf("job %q artifact input %q must reference a direct needs dependency", jobName, input.From)
		}
	}
	return nil
}

func resolveJobArtifactRefs(status v1alpha1.JobStatus, childRuns map[string]*v1alpha1.Run) map[string]v1alpha1.ArtifactRef {
	refs := map[string]v1alpha1.ArtifactRef{}
	for _, step := range status.Steps {
		if step.RunName != "" {
			copyRunArtifactRefs(refs, childRunByName(childRuns, step.RunName))
		}
		for _, actionStep := range step.ActionSteps {
			copyRunArtifactRefs(refs, childRunByName(childRuns, actionStep.RunName))
		}
	}
	if len(refs) == 0 {
		return nil
	}
	return refs
}

func childRunByName(childRuns map[string]*v1alpha1.Run, name string) *v1alpha1.Run {
	for _, run := range childRuns {
		if run.Name == name {
			return run
		}
	}
	return nil
}

func copyRunArtifactRefs(destination map[string]v1alpha1.ArtifactRef, run *v1alpha1.Run) {
	if run == nil || run.Status.Phase != v1alpha1.RunSucceeded {
		return
	}
	for _, ref := range run.Status.ArtifactRefs {
		destination[ref.Name] = *ref.DeepCopy()
	}
}
