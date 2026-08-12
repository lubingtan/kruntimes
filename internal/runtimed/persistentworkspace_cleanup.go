package runtimed

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

const persistentWorkspaceRunWorkspaceField = "spec.workspace.name"

// PersistentWorkspaceCleanupReconciler removes runtime-local workspace data
// only after the PersistentWorkspace controller has requested its deletion.
// It never writes PersistentWorkspace status.
type PersistentWorkspaceCleanupReconciler struct {
	client.Client
	PodReader client.Reader
	PodName   string
	Namespace string
}

func (r *PersistentWorkspaceCleanupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var workspace v1alpha1.PersistentWorkspace
	if err := r.Get(ctx, req.NamespacedName, &workspace); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if workspace.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(&workspace, v1alpha1.PersistentWorkspaceCleanupFinalizer) {
		return ctrl.Result{}, nil
	}

	pod, err := r.boundRuntimePod(ctx, &workspace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pod == nil {
		return ctrl.Result{}, nil
	}
	active, err := r.hasActiveLocalRuns(ctx, &workspace, pod.UID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if active {
		return ctrl.Result{}, nil
	}
	if err := removePersistentWorkspaceDirectory(workspace.Name); err != nil {
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(&workspace, v1alpha1.PersistentWorkspaceCleanupFinalizer)
	if err := r.Update(ctx, &workspace); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove PersistentWorkspace cleanup finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

// boundRuntimePod returns this runtimed's Pod only when the workspace binding
// is still fenced by the same live Pod UID.
func (r *PersistentWorkspaceCleanupReconciler) boundRuntimePod(ctx context.Context, workspace *v1alpha1.PersistentWorkspace) (*corev1.Pod, error) {
	if workspace.Status.BoundPod != r.PodName || workspace.Status.BoundPodUID == "" {
		return nil, nil
	}
	reader := r.PodReader
	if reader == nil {
		reader = r.Client
	}
	var pod corev1.Pod
	key := client.ObjectKey{Namespace: r.Namespace, Name: r.PodName}
	if err := reader.Get(ctx, key, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get runtimed Pod %s: %w", key, err)
	}
	if pod.DeletionTimestamp != nil || string(pod.UID) != workspace.Status.BoundPodUID {
		return nil, nil
	}
	return &pod, nil
}

func (r *PersistentWorkspaceCleanupReconciler) hasActiveLocalRuns(ctx context.Context, workspace *v1alpha1.PersistentWorkspace, podUID types.UID) (bool, error) {
	var runs v1alpha1.RunList
	if err := r.List(ctx, &runs, client.InNamespace(workspace.Namespace), client.MatchingFields{persistentWorkspaceRunWorkspaceField: workspace.Name}); err != nil {
		return false, fmt.Errorf("list Runs for PersistentWorkspace %s/%s: %w", workspace.Namespace, workspace.Name, err)
	}
	for i := range runs.Items {
		run := &runs.Items[i]
		if run.Status.AssignedPod == r.PodName && run.Status.AssignedPodUID == string(podUID) && !runtimedRunTerminal(run) {
			return true, nil
		}
	}
	return false, nil
}

func runtimedRunTerminal(run *v1alpha1.Run) bool {
	switch run.Status.Phase {
	case v1alpha1.RunSucceeded, v1alpha1.RunFailed, v1alpha1.RunTimeout, v1alpha1.RunCancelled:
		return true
	default:
		return false
	}
}

func removePersistentWorkspaceDirectory(name string) error {
	root := filepath.Join(workspacePath, "persistent")
	directory := persistentWorkspacePath(name)
	relative, err := filepath.Rel(root, directory)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("persistent workspace path %q escapes %q", directory, root)
	}
	if err := os.RemoveAll(directory); err != nil {
		return fmt.Errorf("remove persistent workspace directory %q: %w", directory, err)
	}
	return nil
}

func (r *PersistentWorkspaceCleanupReconciler) workspacesForRun(_ context.Context, object client.Object) []reconcile.Request {
	run, ok := object.(*v1alpha1.Run)
	if !ok || run.Spec.Workspace == nil || run.Spec.Workspace.Name == "" || run.Status.AssignedPod != r.PodName {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.Workspace.Name}}}
}

// SetupWithManager registers the runtime-local cleanup controller.
func (r *PersistentWorkspaceCleanupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1alpha1.Run{}, persistentWorkspaceRunWorkspaceField, persistentWorkspaceRunWorkspaceIndexValues); err != nil {
		return fmt.Errorf("index Runs by workspace: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.PersistentWorkspace{}, builder.WithPredicates(r.workspacePredicate())).
		Watches(&v1alpha1.Run{}, handler.EnqueueRequestsFromMapFunc(r.workspacesForRun)).
		Complete(r)
}

// workspacePredicate keeps other Runtime Pods' workspaces out of this
// runtimed's reconcile queue. The Reconcile method still checks the bound Pod
// UID immediately before deleting data.
func (r *PersistentWorkspaceCleanupReconciler) workspacePredicate() predicate.Predicate {
	return predicate.NewTypedPredicateFuncs(func(object client.Object) bool {
		workspace, ok := object.(*v1alpha1.PersistentWorkspace)
		return ok && workspace.Status.BoundPod == r.PodName
	})
}

func persistentWorkspaceRunWorkspaceIndexValues(object client.Object) []string {
	run, ok := object.(*v1alpha1.Run)
	if !ok || run.Spec.Workspace == nil || run.Spec.Workspace.Name == "" {
		return nil
	}
	return []string{run.Spec.Workspace.Name}
}
