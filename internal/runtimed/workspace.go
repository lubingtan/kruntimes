package runtimed

import (
	"path/filepath"
)

func persistentWorkspacePath(name string) string {
	return filepath.Join(workspacePath, "persistent", name)
}
