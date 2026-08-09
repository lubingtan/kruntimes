package scheduler

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

func TestWorkspaceFilter(t *testing.T) {
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "default"},
		Spec: v1alpha1.RunSpec{
			Runtime:   "bash",
			Workspace: &v1alpha1.RunWorkspaceReference{Name: "build"},
		},
	}
	matchingPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a", UID: types.UID("pod-a")}}
	replacementPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a", UID: types.UID("replacement")}}
	otherPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runtime-b", UID: types.UID("pod-b")}}
	bound := &v1alpha1.PersistentWorkspace{
		Spec: v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"},
		Status: v1alpha1.PersistentWorkspaceStatus{
			Phase:       v1alpha1.PersistentWorkspaceBound,
			BoundPod:    "runtime-a",
			BoundPodUID: "pod-a",
		},
	}

	tests := []struct {
		name       string
		snapshot   schedulingSnapshot
		pod        *corev1.Pod
		wantReason filterReason
	}{
		{name: "missing", snapshot: schedulingSnapshot{run: run}, pod: matchingPod, wantReason: filterReasonWorkspaceNotFound},
		{name: "runtime mismatch", snapshot: schedulingSnapshot{run: run, workspace: &v1alpha1.PersistentWorkspace{Spec: v1alpha1.PersistentWorkspaceSpec{Runtime: "python"}}}, pod: matchingPod, wantReason: filterReasonWorkspaceRuntimeMismatch},
		{name: "pending", snapshot: schedulingSnapshot{run: run, workspace: &v1alpha1.PersistentWorkspace{Spec: v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"}, Status: v1alpha1.PersistentWorkspaceStatus{Phase: v1alpha1.PersistentWorkspacePending}}}, pod: matchingPod, wantReason: filterReasonWorkspaceUnbound},
		{name: "lost", snapshot: schedulingSnapshot{run: run, workspace: &v1alpha1.PersistentWorkspace{Spec: v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"}, Status: v1alpha1.PersistentWorkspaceStatus{Phase: v1alpha1.PersistentWorkspaceLost}}}, pod: matchingPod, wantReason: filterReasonWorkspaceLost},
		{name: "released", snapshot: schedulingSnapshot{run: run, workspace: &v1alpha1.PersistentWorkspace{Spec: v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"}, Status: v1alpha1.PersistentWorkspaceStatus{Phase: v1alpha1.PersistentWorkspaceReleased}}}, pod: matchingPod, wantReason: filterReasonWorkspaceReleased},
		{name: "bound other pod", snapshot: schedulingSnapshot{run: run, workspace: bound}, pod: otherPod, wantReason: filterReasonWorkspaceBoundPodMismatch},
		{name: "bound replacement pod", snapshot: schedulingSnapshot{run: run, workspace: bound}, pod: replacementPod, wantReason: filterReasonWorkspaceBoundPodMismatch},
		{name: "bound matching pod", snapshot: schedulingSnapshot{run: run, workspace: bound}, pod: matchingPod},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plugin, err := newWorkspaceFilter(nil, &test.snapshot, &schedulingPreFilterState{})
			if err != nil {
				t.Fatalf("new workspace filter: %v", err)
			}
			result := plugin.Filter(&test.snapshot, test.pod)
			if test.wantReason == "" {
				if !result.feasible {
					t.Fatalf("filter result = %#v, want feasible", result)
				}
				return
			}
			if result.feasible || result.reason != test.wantReason {
				t.Fatalf("filter result = %#v, want reason %q", result, test.wantReason)
			}
		})
	}
}

func TestWorkspaceFilterAllowsRunWithoutWorkspace(t *testing.T) {
	plugin, err := newWorkspaceFilter(nil, &schedulingSnapshot{run: &v1alpha1.Run{}}, &schedulingPreFilterState{})
	if err != nil {
		t.Fatalf("new workspace filter: %v", err)
	}
	if result := plugin.Filter(nil, &corev1.Pod{}); !result.feasible {
		t.Fatalf("filter result = %#v, want feasible", result)
	}
}

func TestPlanSchedulingCycleSelectsBoundWorkspacePod(t *testing.T) {
	now := time.Now()
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "default"},
		Spec: v1alpha1.RunSpec{
			Runtime:   "bash",
			Workspace: &v1alpha1.RunWorkspaceReference{Name: "build"},
		},
	}
	request, err := run.Spec.ResourceRequests()
	if err != nil {
		t.Fatal(err)
	}
	workspace := &v1alpha1.PersistentWorkspace{
		Spec: v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"},
		Status: v1alpha1.PersistentWorkspaceStatus{
			Phase:       v1alpha1.PersistentWorkspaceBound,
			BoundPod:    "runtime-b",
			BoundPodUID: "pod-b",
		},
	}
	podA := readyAffinityPod("runtime-a", now)
	podA.UID = "pod-a"
	podB := readyAffinityPod("runtime-b", now)
	podB.UID = "pod-b"
	reconciler := &RunReconciler{RuntimedHeartbeatStaleAfter: time.Minute}
	plan, err := reconciler.planSchedulingCycle(&schedulingSnapshot{
		run:       run,
		request:   request,
		pods:      []corev1.Pod{podA, podB},
		workspace: workspace,
		now:       now,
	})
	if err != nil {
		t.Fatalf("plan scheduling cycle: %v", err)
	}
	if plan.action != schedulingPlanBind || plan.selected == nil || plan.selected.Name != "runtime-b" {
		t.Fatalf("plan = %#v, want bind to workspace Pod runtime-b", plan)
	}
}
