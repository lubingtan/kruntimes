package runtimed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

func TestPersistentWorkspaceCleanupRemovesDirectoryAndFinalizer(t *testing.T) {
	ctx := context.Background()
	setTestWorkspacePath(t)
	workspace := deletingPersistentWorkspace("workspace", "runtime-a", "pod-a")
	directory := persistentWorkspacePath(workspace.Name)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("create workspace directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "marker"), []byte("data"), 0o600); err != nil {
		t.Fatalf("write workspace marker: %v", err)
	}
	client := persistentWorkspaceCleanupTestClient(t, workspace, runtimePod("runtime-a", "pod-a"))
	reconciler := &PersistentWorkspaceCleanupReconciler{Client: client, PodReader: client, PodName: "runtime-a", Namespace: "default"}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: workspace.Name}}); err != nil {
		t.Fatalf("reconcile workspace cleanup: %v", err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace directory stat error = %v, want not exist", err)
	}
	var got v1alpha1.PersistentWorkspace
	err := client.Get(ctx, types.NamespacedName{Namespace: "default", Name: workspace.Name}, &got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("workspace get error = %v, want not found after finalizer removal", err)
	}
}

func TestPersistentWorkspaceCleanupWaitsForActiveLocalRun(t *testing.T) {
	ctx := context.Background()
	setTestWorkspacePath(t)
	workspace := deletingPersistentWorkspace("workspace", "runtime-a", "pod-a")
	directory := persistentWorkspacePath(workspace.Name)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("create workspace directory: %v", err)
	}
	run := &v1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "active", Namespace: "default"},
		Spec:       v1alpha1.RunSpec{Workspace: &v1alpha1.RunWorkspaceReference{Name: workspace.Name}},
		Status: v1alpha1.RunStatus{
			Phase:          v1alpha1.RunRunning,
			AssignedPod:    "runtime-a",
			AssignedPodUID: "pod-a",
		},
	}
	client := persistentWorkspaceCleanupTestClient(t, workspace, runtimePod("runtime-a", "pod-a"), run)
	reconciler := &PersistentWorkspaceCleanupReconciler{Client: client, PodReader: client, PodName: "runtime-a", Namespace: "default"}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: workspace.Name}}); err != nil {
		t.Fatalf("reconcile workspace cleanup: %v", err)
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("workspace directory removed while Run is active: %v", err)
	}
	var got v1alpha1.PersistentWorkspace
	if err := client.Get(ctx, types.NamespacedName{Namespace: "default", Name: workspace.Name}, &got); err != nil {
		t.Fatalf("get workspace: %v", err)
	}
	if !containsString(got.Finalizers, v1alpha1.PersistentWorkspaceCleanupFinalizer) {
		t.Fatalf("workspace finalizers = %v, want cleanup finalizer while Run is active", got.Finalizers)
	}
}

func TestPersistentWorkspaceCleanupFencesReplacedPod(t *testing.T) {
	ctx := context.Background()
	setTestWorkspacePath(t)
	workspace := deletingPersistentWorkspace("workspace", "runtime-a", "old-pod")
	directory := persistentWorkspacePath(workspace.Name)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("create workspace directory: %v", err)
	}
	client := persistentWorkspaceCleanupTestClient(t, workspace, runtimePod("runtime-a", "replacement-pod"))
	reconciler := &PersistentWorkspaceCleanupReconciler{Client: client, PodReader: client, PodName: "runtime-a", Namespace: "default"}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: workspace.Name}}); err != nil {
		t.Fatalf("reconcile workspace cleanup: %v", err)
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("workspace directory removed for a replaced Pod: %v", err)
	}
}

func TestPersistentWorkspaceCleanupWorkspacePredicate(t *testing.T) {
	reconciler := &PersistentWorkspaceCleanupReconciler{PodName: "runtime-a"}
	predicate := reconciler.workspacePredicate()
	matching := deletingPersistentWorkspace("matching", "runtime-a", "pod-a")
	other := deletingPersistentWorkspace("other", "runtime-b", "pod-b")

	if !predicate.Create(event.CreateEvent{Object: matching}) {
		t.Fatal("predicate rejected workspace bound to this runtimed Pod")
	}
	if predicate.Create(event.CreateEvent{Object: other}) {
		t.Fatal("predicate accepted workspace bound to another runtimed Pod")
	}
}

func deletingPersistentWorkspace(name, podName, podUID string) *v1alpha1.PersistentWorkspace {
	now := metav1.Now()
	return &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{v1alpha1.PersistentWorkspaceCleanupFinalizer},
		},
		Status: v1alpha1.PersistentWorkspaceStatus{
			Phase:       v1alpha1.PersistentWorkspaceReleased,
			BoundPod:    podName,
			BoundPodUID: podUID,
		},
	}
}

func runtimePod(name, uid string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(uid)}}
}

func persistentWorkspaceCleanupTestClient(t *testing.T, objects ...client.Object) client.WithWatch {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add kruntimes scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&v1alpha1.Run{}, persistentWorkspaceRunWorkspaceField, persistentWorkspaceRunWorkspaceIndexValues).
		WithObjects(objects...).
		Build()
}

func setTestWorkspacePath(t *testing.T) {
	t.Helper()
	previous := workspacePath
	workspacePath = t.TempDir()
	t.Cleanup(func() { workspacePath = previous })
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
