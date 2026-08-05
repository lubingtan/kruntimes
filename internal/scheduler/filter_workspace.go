package scheduler

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

type workspaceFilter struct {
	workspace *v1alpha1.PersistentWorkspace
	reason    filterReason
}

func newWorkspaceFilter(_ *RunReconciler, snapshot *schedulingSnapshot, _ *schedulingPreFilterState) (filterPlugin, error) {
	if snapshot.run.Spec.Workspace == nil {
		return &workspaceFilter{}, nil
	}
	if snapshot.workspace == nil {
		return &workspaceFilter{reason: filterReasonWorkspaceNotFound}, nil
	}
	workspace := snapshot.workspace
	if workspace.Spec.Runtime != snapshot.run.Spec.Runtime {
		return &workspaceFilter{reason: filterReasonWorkspaceRuntimeMismatch}, nil
	}
	if workspace.Status.Phase == v1alpha1.PersistentWorkspaceLost {
		return &workspaceFilter{reason: filterReasonWorkspaceLost}, nil
	}
	if workspace.Status.Phase != v1alpha1.PersistentWorkspaceBound || workspace.Status.BoundPod == "" || workspace.Status.BoundPodUID == "" {
		return &workspaceFilter{reason: filterReasonWorkspaceUnbound}, nil
	}
	return &workspaceFilter{workspace: workspace}, nil
}

func (f *workspaceFilter) Name() string {
	return "Workspace"
}

func (f *workspaceFilter) Filter(_ *schedulingSnapshot, pod *corev1.Pod) filterResult {
	if f.reason != "" {
		return filterResult{reason: f.reason}
	}
	if f.workspace == nil {
		return filterResult{feasible: true}
	}
	if pod.Name != f.workspace.Status.BoundPod || string(pod.UID) != f.workspace.Status.BoundPodUID {
		return filterResult{reason: filterReasonWorkspaceBoundPodMismatch}
	}
	return filterResult{feasible: true}
}
