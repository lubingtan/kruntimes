package runtimed

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/kruntimes/kruntimes/internal/execpath"
)

// prepareSource materializes Run source without overwriting user files in a
// referenced task workspace. Function source stays at the workspace root so a
// runtime can resolve its handler as a module relative to the working dir.
func prepareSource(ar *activeRun) error {
	if ar == nil || ar.run == nil {
		return fmt.Errorf("Run is required")
	}
	if err := os.MkdirAll(ar.cleanupDir, 0o755); err != nil {
		return fmt.Errorf("mkdir Run-local directory %s: %w", ar.cleanupDir, err)
	}
	if ar.sourceDir != ar.cleanupDir {
		if err := os.MkdirAll(ar.sourceDir, 0o755); err != nil {
			return fmt.Errorf("mkdir source directory %s: %w", ar.sourceDir, err)
		}
	}

	run := ar.run
	if run.Spec.Source == nil {
		if err := os.MkdirAll(ar.workDir, 0o755); err != nil {
			return fmt.Errorf("mkdir working directory %s: %w", ar.workDir, err)
		}
		if _, err := execpath.ResolveEntrypoint(run.Spec.EffectiveEntrypoint(), "script"); err != nil {
			return err
		}
		return nil
	}
	if run.Spec.Source.Inline != nil {
		scriptPath := "script"
		if run.Spec.Mode.Function != nil {
			var err error
			scriptPath, err = execpath.ResolveEntrypoint(run.Spec.Source.InlinePath, "")
			if err != nil {
				return fmt.Errorf("resolve inline function source path: %w", err)
			}
		}
		scriptPath = filepath.Join(ar.sourceDir, scriptPath)
		if err := os.MkdirAll(filepath.Dir(scriptPath), 0o755); err != nil {
			return fmt.Errorf("mkdir inline source parent: %w", err)
		}
		if err := os.WriteFile(scriptPath, []byte(*run.Spec.Source.Inline), 0o644); err != nil {
			return fmt.Errorf("write inline: %w", err)
		}
		return nil
	}
	if _, err := execpath.ResolveEntrypoint(run.Spec.EffectiveEntrypoint(), "script"); err != nil {
		return err
	}
	if run.Spec.Source.RepoURL != "" {
		if _, err := prepareGitSource(ar.sourceDir, run.Spec.Source.RepoURL, run.Spec.Source.CommitSHA); err != nil {
			return err
		}
	}
	return nil
}

func runtimeExecutionInput(ar *activeRun) (string, []string, error) {
	if ar == nil || ar.run == nil {
		return "", nil, fmt.Errorf("Run is required")
	}
	run := ar.run
	if run.Spec.Source != nil && run.Spec.Source.Inline != nil {
		return ar.sourceEntrypoint("script"), nil, nil
	}
	entrypoint, err := execpath.ResolveEntrypoint(run.Spec.EffectiveEntrypoint(), "script")
	if err != nil {
		return "", nil, err
	}
	if run.Spec.Source != nil && run.Spec.Source.RepoURL != "" {
		entrypoint = ar.sourceEntrypoint(filepath.Join("repo", entrypoint))
	}
	return entrypoint, run.Spec.EffectiveArgs(), nil
}

func (ar *activeRun) sourceEntrypoint(entrypoint string) string {
	if ar.sourceEntrypointPrefix == "" {
		return entrypoint
	}
	return filepath.Join(ar.sourceEntrypointPrefix, entrypoint)
}
