package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

// reconcileWorkspaceUsage updates the unused interval only when it changes.
// An InUse condition records the transition so repeated reconciliations do not
// indefinitely postpone a configured cleanup TTL.
func (r *PersistentWorkspaceReconciler) reconcileWorkspaceUsage(ctx context.Context, workspace *v1alpha1.PersistentWorkspace) error {
	active, err := r.workspaceHasActiveRuns(ctx, workspace)
	if err != nil {
		return err
	}
	condition := apimeta.FindStatusCondition(workspace.Status.Conditions, persistentWorkspaceInUseCondition)
	if active {
		if condition == nil || condition.Status != metav1.ConditionTrue {
			apimeta.SetStatusCondition(&workspace.Status.Conditions, workspaceInUseCondition(workspace, metav1.ConditionTrue, "ActiveRuns", "One or more Runs reference this workspace"))
		}
		return nil
	}
	if condition == nil || condition.Status != metav1.ConditionFalse {
		now := metav1.NewTime(time.Now())
		workspace.Status.LastUsedTime = &now
		apimeta.SetStatusCondition(&workspace.Status.Conditions, workspaceInUseCondition(workspace, metav1.ConditionFalse, "Unused", "No non-terminal Runs reference this workspace"))
	}
	return nil
}

func workspaceInUseCondition(workspace *v1alpha1.PersistentWorkspace, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:               persistentWorkspaceInUseCondition,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: workspace.Generation,
	}
}

func (r *PersistentWorkspaceReconciler) workspaceHasActiveRuns(ctx context.Context, workspace *v1alpha1.PersistentWorkspace) (bool, error) {
	var runs v1alpha1.RunList
	if err := r.List(ctx, &runs, client.InNamespace(workspace.Namespace), client.MatchingFields{persistentWorkspaceRunWorkspaceField: workspace.Name}); err != nil {
		return false, fmt.Errorf("list Runs for PersistentWorkspace %s/%s: %w", workspace.Namespace, workspace.Name, err)
	}
	for i := range runs.Items {
		if !runTerminal(&runs.Items[i]) {
			return true, nil
		}
	}
	return false, nil
}

func runTerminal(run *v1alpha1.Run) bool {
	switch run.Status.Phase {
	case v1alpha1.RunSucceeded, v1alpha1.RunFailed, v1alpha1.RunTimeout, v1alpha1.RunCancelled:
		return true
	default:
		return false
	}
}

func (r *PersistentWorkspaceReconciler) reconcilePersistentWorkspaceCleanup(ctx context.Context, workspace *v1alpha1.PersistentWorkspace) (ctrl.Result, error) {
	if controllerutil.AddFinalizer(workspace, v1alpha1.PersistentWorkspaceCleanupFinalizer) {
		if err := r.Update(ctx, workspace); err != nil {
			return ctrl.Result{}, fmt.Errorf("add PersistentWorkspace cleanup finalizer: %w", err)
		}
		return ctrl.Result{}, nil
	}
	if workspace.Spec.CleanupPolicy == v1alpha1.PersistentWorkspaceRetain {
		return ctrl.Result{}, nil
	}
	if workspace.Spec.TTLSecondsAfterUnused == nil || workspace.Status.LastUsedTime == nil {
		return ctrl.Result{}, nil
	}
	deadline := workspace.Status.LastUsedTime.Add(time.Duration(*workspace.Spec.TTLSecondsAfterUnused) * time.Second)
	if wait := time.Until(deadline); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}

	workspace.Status.Phase = v1alpha1.PersistentWorkspaceReleased
	apimeta.SetStatusCondition(&workspace.Status.Conditions, metav1.Condition{
		Type:               persistentWorkspaceBoundCondition,
		Status:             metav1.ConditionFalse,
		Reason:             "CleanupRequested",
		Message:            "Workspace cleanup was requested after its unused TTL elapsed",
		ObservedGeneration: workspace.Generation,
	})
	if err := r.Status().Update(ctx, workspace); err != nil {
		return ctrl.Result{}, fmt.Errorf("release PersistentWorkspace after unused TTL: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *PersistentWorkspaceReconciler) requestPersistentWorkspaceDeletion(ctx context.Context, workspace *v1alpha1.PersistentWorkspace) (ctrl.Result, error) {
	if err := r.Delete(ctx, workspace); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete released PersistentWorkspace: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *PersistentWorkspaceReconciler) reconcileDeletingWorkspace(ctx context.Context, workspace *v1alpha1.PersistentWorkspace) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(workspace, v1alpha1.PersistentWorkspaceCleanupFinalizer) {
		return ctrl.Result{}, nil
	}
	lost, err := r.boundWorkspacePodLost(ctx, workspace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if workspace.Status.Phase == v1alpha1.PersistentWorkspaceLost || lost {
		controllerutil.RemoveFinalizer(workspace, v1alpha1.PersistentWorkspaceCleanupFinalizer)
		if err := r.Update(ctx, workspace); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove PersistentWorkspace cleanup finalizer during deletion: %w", err)
		}
	}
	return ctrl.Result{}, nil
}

// boundWorkspacePodLost identifies deletion states where no runtimed can
// remove the runtime-local directory. The data is already unavailable once
// the recorded Pod no longer exists with its recorded UID.
func (r *PersistentWorkspaceReconciler) boundWorkspacePodLost(ctx context.Context, workspace *v1alpha1.PersistentWorkspace) (bool, error) {
	if workspace.Status.BoundPod == "" || workspace.Status.BoundPodUID == "" {
		return true, nil
	}
	var pod corev1.Pod
	key := client.ObjectKey{Namespace: workspace.Namespace, Name: workspace.Status.BoundPod}
	if err := r.Get(ctx, key, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("get bound Runtime Pod %s while deleting PersistentWorkspace: %w", key, err)
	}
	return pod.DeletionTimestamp != nil || string(pod.UID) != workspace.Status.BoundPodUID, nil
}
