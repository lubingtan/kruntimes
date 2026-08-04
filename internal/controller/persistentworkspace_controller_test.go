package controller

import (
	"context"
	"slices"
	"testing"

	"github.com/go-logr/logr/testr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

func TestPersistentWorkspaceReconcileRuntimeFound(t *testing.T) {
	ctx := context.Background()
	scheme := persistentWorkspaceTestScheme(t)
	workspace := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec: v1alpha1.PersistentWorkspaceSpec{
			Runtime: "bash",
		},
	}
	runtimeResource := &v1alpha1.Runtime{
		ObjectMeta: metav1.ObjectMeta{Name: "bash", Namespace: "default"},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PersistentWorkspace{}).
		WithObjects(workspace, runtimeResource).
		Build()
	reconciler := &PersistentWorkspaceReconciler{Client: client, Log: testr.New(t), Scheme: scheme}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "ci", Namespace: "default"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got v1alpha1.PersistentWorkspace
	if err := client.Get(ctx, types.NamespacedName{Name: "ci", Namespace: "default"}, &got); err != nil {
		t.Fatalf("get workspace: %v", err)
	}
	if got.Status.Phase != v1alpha1.PersistentWorkspacePending {
		t.Fatalf("phase = %q, want Pending", got.Status.Phase)
	}
	if got.Status.Runtime != "bash" {
		t.Fatalf("status runtime = %q, want bash", got.Status.Runtime)
	}
	assertPersistentWorkspaceCondition(t, got.Status.Conditions, persistentWorkspaceAcceptedCondition, metav1.ConditionTrue, "Accepted")
	assertPersistentWorkspaceCondition(t, got.Status.Conditions, persistentWorkspaceRuntimeCondition, metav1.ConditionTrue, "RuntimeFound")
}

func TestPersistentWorkspaceReconcileSkipsUnchangedStatus(t *testing.T) {
	ctx := context.Background()
	scheme := persistentWorkspaceTestScheme(t)
	workspace := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec:       v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"},
	}
	runtimeResource := &v1alpha1.Runtime{ObjectMeta: metav1.ObjectMeta{Name: "bash", Namespace: "default"}}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PersistentWorkspace{}).
		WithObjects(workspace, runtimeResource).
		Build()
	reconciler := &PersistentWorkspaceReconciler{Client: client, Log: testr.New(t), Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: workspace.Name, Namespace: workspace.Namespace}}

	if _, err := reconciler.Reconcile(ctx, req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	var first v1alpha1.PersistentWorkspace
	if err := client.Get(ctx, req.NamespacedName, &first); err != nil {
		t.Fatalf("get workspace after first reconcile: %v", err)
	}

	if _, err := reconciler.Reconcile(ctx, req); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	var second v1alpha1.PersistentWorkspace
	if err := client.Get(ctx, req.NamespacedName, &second); err != nil {
		t.Fatalf("get workspace after second reconcile: %v", err)
	}
	if second.ResourceVersion != first.ResourceVersion {
		t.Fatalf("resourceVersion changed from %q to %q for unchanged status", first.ResourceVersion, second.ResourceVersion)
	}
}

func TestPersistentWorkspaceReconcileRuntimeNotFound(t *testing.T) {
	ctx := context.Background()
	scheme := persistentWorkspaceTestScheme(t)
	workspace := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec: v1alpha1.PersistentWorkspaceSpec{
			Runtime: "missing",
		},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PersistentWorkspace{}).
		WithObjects(workspace).
		Build()
	reconciler := &PersistentWorkspaceReconciler{Client: client, Log: testr.New(t), Scheme: scheme}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "ci", Namespace: "default"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got v1alpha1.PersistentWorkspace
	if err := client.Get(ctx, types.NamespacedName{Name: "ci", Namespace: "default"}, &got); err != nil {
		t.Fatalf("get workspace: %v", err)
	}
	assertPersistentWorkspaceCondition(t, got.Status.Conditions, persistentWorkspaceRuntimeCondition, metav1.ConditionFalse, "RuntimeNotFound")
}

func TestPersistentWorkspaceBindsReadyRuntimePod(t *testing.T) {
	ctx := context.Background()
	scheme := persistentWorkspaceTestScheme(t)
	workspace := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec:       v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"},
	}
	runtimeResource := &v1alpha1.Runtime{ObjectMeta: metav1.ObjectMeta{Name: "bash", Namespace: "default"}}
	podB := readyPersistentWorkspacePod("runtime-b", "bash", "pod-b")
	podA := readyPersistentWorkspacePod("runtime-a", "bash", "pod-a")
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PersistentWorkspace{}).
		WithObjects(workspace, runtimeResource, podB, podA).
		Build()
	reconciler := &PersistentWorkspaceReconciler{Client: client, Log: testr.New(t), Scheme: scheme}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: workspace.Name, Namespace: workspace.Namespace}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got v1alpha1.PersistentWorkspace
	if err := client.Get(ctx, types.NamespacedName{Name: workspace.Name, Namespace: workspace.Namespace}, &got); err != nil {
		t.Fatalf("get workspace: %v", err)
	}
	if got.Status.Phase != v1alpha1.PersistentWorkspaceBound || !slices.Contains([]string{"runtime-a", "runtime-b"}, got.Status.BoundPod) || got.Status.Path != "/workspace/persistent/ci" {
		t.Fatalf("workspace status = %#v, want bound to a ready Runtime Pod", got.Status)
	}
	if wantUID := map[string]string{"runtime-a": "pod-a", "runtime-b": "pod-b"}[got.Status.BoundPod]; got.Status.BoundPodUID != wantUID {
		t.Fatalf("boundPodUID = %q, want %q for Pod %q", got.Status.BoundPodUID, wantUID, got.Status.BoundPod)
	}
	assertPersistentWorkspaceCondition(t, got.Status.Conditions, persistentWorkspaceBoundCondition, metav1.ConditionTrue, "Bound")
}

func TestSelectPersistentWorkspacePodIsStable(t *testing.T) {
	workspace := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "workspace-uid"},
	}
	pods := []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a"}}, {ObjectMeta: metav1.ObjectMeta{Name: "runtime-b"}}}
	selected := selectPersistentWorkspacePod(workspace, pods)
	for range 10 {
		if got := selectPersistentWorkspacePod(workspace, pods); got.Name != selected.Name {
			t.Fatalf("selected Pod = %q, want stable choice %q", got.Name, selected.Name)
		}
	}
}

func TestPersistentWorkspaceKeepsBindingWhenBoundPodIsUnavailable(t *testing.T) {
	ctx := context.Background()
	scheme := persistentWorkspaceTestScheme(t)
	workspace := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec:       v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"},
		Status: v1alpha1.PersistentWorkspaceStatus{
			Phase:       v1alpha1.PersistentWorkspaceBound,
			BoundPod:    "runtime-a",
			BoundPodUID: "pod-a",
			Path:        "/workspace/persistent/ci",
		},
	}
	runtimeResource := &v1alpha1.Runtime{ObjectMeta: metav1.ObjectMeta{Name: "bash", Namespace: "default"}}
	pod := readyPersistentWorkspacePod("runtime-a", "bash", "pod-a")
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.PersistentWorkspace{}).
		WithObjects(workspace, runtimeResource, pod).
		Build()
	reconciler := &PersistentWorkspaceReconciler{Client: client, Log: testr.New(t), Scheme: scheme}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: workspace.Name, Namespace: workspace.Namespace}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var got v1alpha1.PersistentWorkspace
	if err := client.Get(ctx, types.NamespacedName{Name: workspace.Name, Namespace: workspace.Namespace}, &got); err != nil {
		t.Fatalf("get workspace: %v", err)
	}
	if got.Status.Phase != v1alpha1.PersistentWorkspaceBound || got.Status.BoundPod != "runtime-a" || got.Status.BoundPodUID != "pod-a" {
		t.Fatalf("workspace status = %#v, want original binding retained", got.Status)
	}
}

func TestPersistentWorkspaceMarksLostWhenBoundPodIsGoneOrReplaced(t *testing.T) {
	for _, test := range []struct {
		name       string
		pod        *corev1.Pod
		wantReason string
	}{
		{name: "deleted", wantReason: "BoundPodDeleted"},
		{name: "replaced", pod: readyPersistentWorkspacePod("runtime-a", "bash", "replacement"), wantReason: "BoundPodReplaced"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := persistentWorkspaceTestScheme(t)
			workspace := &v1alpha1.PersistentWorkspace{
				ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
				Spec:       v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"},
				Status: v1alpha1.PersistentWorkspaceStatus{
					Phase:       v1alpha1.PersistentWorkspaceBound,
					BoundPod:    "runtime-a",
					BoundPodUID: "pod-a",
					Path:        "/workspace/persistent/ci",
				},
			}
			runtimeResource := &v1alpha1.Runtime{ObjectMeta: metav1.ObjectMeta{Name: "bash", Namespace: "default"}}
			objects := []client.Object{workspace, runtimeResource}
			if test.pod != nil {
				objects = append(objects, test.pod)
			}
			client := fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&v1alpha1.PersistentWorkspace{}).
				WithObjects(objects...).
				Build()
			reconciler := &PersistentWorkspaceReconciler{Client: client, Log: testr.New(t), Scheme: scheme}

			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: workspace.Name, Namespace: workspace.Namespace}}); err != nil {
				t.Fatalf("reconcile: %v", err)
			}

			var got v1alpha1.PersistentWorkspace
			if err := client.Get(ctx, types.NamespacedName{Name: workspace.Name, Namespace: workspace.Namespace}, &got); err != nil {
				t.Fatalf("get workspace: %v", err)
			}
			if got.Status.Phase != v1alpha1.PersistentWorkspaceLost {
				t.Fatalf("phase = %q, want Lost", got.Status.Phase)
			}
			assertPersistentWorkspaceCondition(t, got.Status.Conditions, persistentWorkspaceBoundCondition, metav1.ConditionFalse, test.wantReason)
		})
	}
}

func TestPersistentWorkspaceRuntimeWatchEnqueuesMatchingWorkspaces(t *testing.T) {
	ctx := context.Background()
	scheme := persistentWorkspaceTestScheme(t)
	matching := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "matching", Namespace: "default"},
		Spec:       v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"},
	}
	otherRuntime := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "other-runtime", Namespace: "default"},
		Spec:       v1alpha1.PersistentWorkspaceSpec{Runtime: "python"},
	}
	otherNamespace := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "other-namespace", Namespace: "other"},
		Spec:       v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(matching, otherRuntime, otherNamespace).
		Build()
	reconciler := &PersistentWorkspaceReconciler{Client: client, Log: testr.New(t), Scheme: scheme}

	requests := reconciler.workspacesForRuntime(ctx, &v1alpha1.Runtime{
		ObjectMeta: metav1.ObjectMeta{Name: "bash", Namespace: "default"},
	})
	if len(requests) != 1 {
		t.Fatalf("requests = %v, want exactly one", requests)
	}
	if got := requests[0].NamespacedName.String(); got != "default/matching" {
		t.Fatalf("request = %s, want default/matching", got)
	}
	if slices.ContainsFunc(requests, func(req ctrl.Request) bool { return req.Name == "other-runtime" || req.Name == "other-namespace" }) {
		t.Fatalf("requests include non-matching workspace: %v", requests)
	}
}

func TestPersistentWorkspacePodWatchEnqueuesRuntimeAndBoundPodWorkspaces(t *testing.T) {
	ctx := context.Background()
	scheme := persistentWorkspaceTestScheme(t)
	matchingRuntime := &v1alpha1.PersistentWorkspace{ObjectMeta: metav1.ObjectMeta{Name: "matching-runtime", Namespace: "default"}, Spec: v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"}}
	matchingBoundPod := &v1alpha1.PersistentWorkspace{ObjectMeta: metav1.ObjectMeta{Name: "matching-bound-pod", Namespace: "default"}, Spec: v1alpha1.PersistentWorkspaceSpec{Runtime: "python"}, Status: v1alpha1.PersistentWorkspaceStatus{BoundPod: "runtime-bash"}}
	other := &v1alpha1.PersistentWorkspace{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default"}, Spec: v1alpha1.PersistentWorkspaceSpec{Runtime: "python"}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(matchingRuntime, matchingBoundPod, other).Build()
	reconciler := &PersistentWorkspaceReconciler{Client: client, Log: testr.New(t), Scheme: scheme}

	requests := reconciler.workspacesForRuntimePod(ctx, readyPersistentWorkspacePod("runtime-bash", "bash", "pod-a"))
	if len(requests) != 2 {
		t.Fatalf("requests = %v, want matching runtime and bound Pod workspaces", requests)
	}
	if !slices.ContainsFunc(requests, func(req ctrl.Request) bool { return req.Name == "matching-runtime" }) || !slices.ContainsFunc(requests, func(req ctrl.Request) bool { return req.Name == "matching-bound-pod" }) {
		t.Fatalf("requests = %v, want matching workspaces", requests)
	}
}

func persistentWorkspaceTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return scheme
}

func readyPersistentWorkspacePod(name, runtimeName, uid string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(uid), Labels: map[string]string{persistentWorkspaceRuntimeLabel: runtimeName}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: v1alpha1.RuntimePodRuntimedReadyCondition, Status: corev1.ConditionTrue},
		}},
	}
}

func assertPersistentWorkspaceCondition(t *testing.T, conditions []metav1.Condition, conditionType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	condition := findCondition(conditions, conditionType)
	if condition == nil {
		t.Fatalf("condition %q not found in %#v", conditionType, conditions)
	}
	if condition.Status != status || condition.Reason != reason {
		t.Fatalf("condition %q = (%s, %s), want (%s, %s)", conditionType, condition.Status, condition.Reason, status, reason)
	}
}
