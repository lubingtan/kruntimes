package runtimed

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

// activeRun holds the execution-specific state derived once when runtimed
// claims or recovers a Run.
type activeRun struct {
	run       *v1alpha1.Run
	workDir   string
	sourceDir string
	// sourceEntrypointPrefix is the Run-local source path relative to workDir.
	// It is set only when a task shares a PersistentWorkspace workDir.
	sourceEntrypointPrefix string
	outputPath             string
	artifactDir            string
	cleanupDir             string
	deadline               time.Time
	start                  time.Time
	started                atomic.Bool
	prepared               atomic.Bool
	sessionClosed          atomic.Bool
}

func newActiveRun(run *v1alpha1.Run, start time.Time) *activeRun {
	if run == nil {
		return &activeRun{start: start}
	}

	workspaceDir := filepath.Join(workspacePath, string(run.UID))
	if run.Spec.Workspace != nil {
		workspaceDir = persistentWorkspacePath(run.Spec.Workspace.Name)
	}

	cleanupDir := workspaceDir
	sourceEntrypointPrefix := ""
	if run.Spec.Workspace != nil {
		cleanupDir = filepath.Join(workspaceDir, ".kruntimes", "runs", string(run.UID))
		if run.Spec.Mode.Task != nil {
			sourceEntrypointPrefix = filepath.Join(".kruntimes", "runs", string(run.UID))
		}
	}

	sourceDir := cleanupDir
	if run.Spec.Mode.Function != nil && run.Spec.Workspace != nil {
		sourceDir = workspaceDir
	}
	workDir := sourceDir
	if sourceEntrypointPrefix != "" {
		workDir = workspaceDir
	} else if run.Spec.Source != nil && run.Spec.Source.RepoURL != "" {
		workDir = filepath.Join(sourceDir, "repo")
	}

	ar := &activeRun{
		run:                    run,
		start:                  start,
		workDir:                workDir,
		sourceDir:              sourceDir,
		sourceEntrypointPrefix: sourceEntrypointPrefix,
		outputPath:             filepath.Join(cleanupDir, "outputs"),
		artifactDir:            filepath.Join(cleanupDir, "artifacts"),
		cleanupDir:             cleanupDir,
	}
	if run.Spec.Timeout != nil && run.Status.StartTime != nil {
		ar.deadline = run.Status.StartTime.Add(run.Spec.Timeout.Duration)
	}
	return ar
}

// cleanupRunFiles removes the temporary workspace of an unshared Run. A
// referenced PersistentWorkspace owns its entire volume lifecycle, so runtimed
// must not remove any path below its mount.
func cleanupRunFiles(ar *activeRun) error {
	if ar == nil || ar.run == nil || ar.run.Spec.Workspace != nil || ar.cleanupDir == "" {
		return nil
	}
	if err := os.RemoveAll(ar.cleanupDir); err != nil {
		return fmt.Errorf("remove Run-local workspace: %w", err)
	}
	return nil
}
