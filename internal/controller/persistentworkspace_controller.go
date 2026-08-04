package controller

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
	"github.com/kruntimes/kruntimes/internal/runtimepod"
)

const (
	persistentWorkspaceAcceptedCondition = "Accepted"
	persistentWorkspaceRuntimeCondition  = "RuntimeReady"
	persistentWorkspaceBoundCondition    = "Bound"
	persistentWorkspacePathPrefix        = "/workspace/persistent"
	persistentWorkspaceRuntimeLabel      = "runtime"
)

// PersistentWorkspaceReconciler owns PersistentWorkspace lifecycle state.
type PersistentWorkspaceReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=kruntimes.io,resources=persistentworkspaces,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=kruntimes.io,resources=persistentworkspaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kruntimes.io,resources=runtimes,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

func (r *PersistentWorkspaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var workspace v1alpha1.PersistentWorkspace
	if err := r.Get(ctx, req.NamespacedName, &workspace); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !workspace.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	original := workspace.DeepCopy()

	if workspace.Status.Phase == "" {
		workspace.Status.Phase = v1alpha1.PersistentWorkspacePending
	}
	workspace.Status.Runtime = workspace.Spec.Runtime
	apimeta.SetStatusCondition(&workspace.Status.Conditions, metav1.Condition{
		Type:               persistentWorkspaceAcceptedCondition,
		Status:             metav1.ConditionTrue,
		Reason:             "Accepted",
		Message:            "PersistentWorkspace spec accepted",
		ObservedGeneration: workspace.Generation,
	})

	var runtimeResource v1alpha1.Runtime
	key := client.ObjectKey{Namespace: workspace.Namespace, Name: workspace.Spec.Runtime}
	if err := r.Get(ctx, key, &runtimeResource); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get runtime %s: %w", key, err)
		}
		apimeta.SetStatusCondition(&workspace.Status.Conditions, metav1.Condition{
			Type:               persistentWorkspaceRuntimeCondition,
			Status:             metav1.ConditionFalse,
			Reason:             "RuntimeNotFound",
			Message:            fmt.Sprintf("Runtime %q does not exist", workspace.Spec.Runtime),
			ObservedGeneration: workspace.Generation,
		})
		return r.updatePersistentWorkspaceStatus(ctx, original, &workspace)
	}
	apimeta.SetStatusCondition(&workspace.Status.Conditions, metav1.Condition{
		Type:               persistentWorkspaceRuntimeCondition,
		Status:             metav1.ConditionTrue,
		Reason:             "RuntimeFound",
		Message:            fmt.Sprintf("Runtime %q exists", runtimeResource.Name),
		ObservedGeneration: workspace.Generation,
	})

	if workspace.Status.Phase == v1alpha1.PersistentWorkspaceLost {
		return r.updatePersistentWorkspaceStatus(ctx, original, &workspace)
	}
	if workspace.Status.BoundPod != "" {
		if err := r.reconcileBoundWorkspace(ctx, &workspace); err != nil {
			return ctrl.Result{}, err
		}
		return r.updatePersistentWorkspaceStatus(ctx, original, &workspace)
	}

	if err := r.bindPendingWorkspace(ctx, &workspace); err != nil {
		return ctrl.Result{}, err
	}
	return r.updatePersistentWorkspaceStatus(ctx, original, &workspace)
}

func (r *PersistentWorkspaceReconciler) updatePersistentWorkspaceStatus(ctx context.Context, original, workspace *v1alpha1.PersistentWorkspace) (ctrl.Result, error) {
	if equality.Semantic.DeepEqual(original.Status, workspace.Status) {
		return ctrl.Result{}, nil
	}
	if err := r.Status().Update(ctx, workspace); err != nil {
		return ctrl.Result{}, fmt.Errorf("update persistent workspace status: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *PersistentWorkspaceReconciler) bindPendingWorkspace(ctx context.Context, workspace *v1alpha1.PersistentWorkspace) error {
	pods, err := r.readyRuntimePods(ctx, workspace.Namespace, workspace.Spec.Runtime)
	if err != nil {
		return fmt.Errorf("list ready Runtime Pods for workspace %s/%s: %w", workspace.Namespace, workspace.Name, err)
	}
	if len(pods) == 0 {
		workspace.Status.Phase = v1alpha1.PersistentWorkspacePending
		apimeta.SetStatusCondition(&workspace.Status.Conditions, metav1.Condition{
			Type:               persistentWorkspaceBoundCondition,
			Status:             metav1.ConditionFalse,
			Reason:             "NoReadyRuntimePods",
			Message:            fmt.Sprintf("Runtime %q has no ready Runtime Pods", workspace.Spec.Runtime),
			ObservedGeneration: workspace.Generation,
		})
		return nil
	}

	pod := selectPersistentWorkspacePod(workspace, pods)
	workspace.Status.Phase = v1alpha1.PersistentWorkspaceBound
	workspace.Status.BoundPod = pod.Name
	workspace.Status.BoundPodUID = string(pod.UID)
	workspace.Status.Path = persistentWorkspacePath(workspace.Name)
	apimeta.SetStatusCondition(&workspace.Status.Conditions, metav1.Condition{
		Type:               persistentWorkspaceBoundCondition,
		Status:             metav1.ConditionTrue,
		Reason:             "Bound",
		Message:            fmt.Sprintf("Bound to Runtime Pod %q", pod.Name),
		ObservedGeneration: workspace.Generation,
	})
	return nil
}

// selectPersistentWorkspacePod spreads first bindings across ready Pods while
// remaining stable when reconciliation retries with the same candidate set.
func selectPersistentWorkspacePod(workspace *v1alpha1.PersistentWorkspace, pods []corev1.Pod) corev1.Pod {
	hash := fnv.New32a()
	key := string(workspace.UID)
	if key == "" {
		key = workspace.Namespace + "/" + workspace.Name
	}
	_, _ = hash.Write([]byte(key))
	return pods[int(hash.Sum32()%uint32(len(pods)))]
}

func (r *PersistentWorkspaceReconciler) reconcileBoundWorkspace(ctx context.Context, workspace *v1alpha1.PersistentWorkspace) error {
	var pod corev1.Pod
	err := r.Get(ctx, client.ObjectKey{Namespace: workspace.Namespace, Name: workspace.Status.BoundPod}, &pod)
	if apierrors.IsNotFound(err) {
		r.markWorkspaceLost(workspace, "BoundPodDeleted", fmt.Sprintf("Bound Runtime Pod %q was deleted", workspace.Status.BoundPod))
		return nil
	}
	if err != nil {
		return fmt.Errorf("get bound Runtime Pod %s/%s: %w", workspace.Namespace, workspace.Status.BoundPod, err)
	}
	if workspace.Status.BoundPodUID == "" || string(pod.UID) != workspace.Status.BoundPodUID {
		r.markWorkspaceLost(workspace, "BoundPodReplaced", fmt.Sprintf("Bound Runtime Pod %q no longer has the recorded UID", workspace.Status.BoundPod))
		return nil
	}
	if pod.DeletionTimestamp != nil {
		r.markWorkspaceLost(workspace, "BoundPodDeleting", fmt.Sprintf("Bound Runtime Pod %q is being deleted", workspace.Status.BoundPod))
		return nil
	}

	workspace.Status.Phase = v1alpha1.PersistentWorkspaceBound
	apimeta.SetStatusCondition(&workspace.Status.Conditions, metav1.Condition{
		Type:               persistentWorkspaceBoundCondition,
		Status:             metav1.ConditionTrue,
		Reason:             "Bound",
		Message:            fmt.Sprintf("Bound to Runtime Pod %q", pod.Name),
		ObservedGeneration: workspace.Generation,
	})
	return nil
}

func (r *PersistentWorkspaceReconciler) markWorkspaceLost(workspace *v1alpha1.PersistentWorkspace, reason, message string) {
	workspace.Status.Phase = v1alpha1.PersistentWorkspaceLost
	apimeta.SetStatusCondition(&workspace.Status.Conditions, metav1.Condition{
		Type:               persistentWorkspaceBoundCondition,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: workspace.Generation,
	})
}

func (r *PersistentWorkspaceReconciler) readyRuntimePods(ctx context.Context, namespace, runtimeName string) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(namespace), client.MatchingLabels{persistentWorkspaceRuntimeLabel: runtimeName}); err != nil {
		return nil, err
	}
	pods := make([]corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pod := list.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || !podReady(&pod) || !runtimepod.IsRuntimedReady(&pod) {
			continue
		}
		pods = append(pods, pod)
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	return pods, nil
}

func podReady(pod *corev1.Pod) bool {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			return pod.Status.Conditions[i].Status == corev1.ConditionTrue
		}
	}
	return false
}

func persistentWorkspacePath(name string) string {
	return persistentWorkspacePathPrefix + "/" + name
}

func (r *PersistentWorkspaceReconciler) workspacesForRuntime(ctx context.Context, obj client.Object) []reconcile.Request {
	var list v1alpha1.PersistentWorkspaceList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		workspace := &list.Items[i]
		if workspace.Spec.Runtime != obj.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(workspace)})
	}
	return requests
}

func (r *PersistentWorkspaceReconciler) workspacesForRuntimePod(ctx context.Context, obj client.Object) []reconcile.Request {
	var list v1alpha1.PersistentWorkspaceList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	runtimeName := obj.GetLabels()[persistentWorkspaceRuntimeLabel]
	requests := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		workspace := &list.Items[i]
		if workspace.Spec.Runtime != runtimeName && workspace.Status.BoundPod != obj.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(workspace)})
	}
	return requests
}

// SetupWithManager registers the PersistentWorkspace reconciler.
func (r *PersistentWorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.PersistentWorkspace{}).
		Watches(&v1alpha1.Runtime{}, handler.EnqueueRequestsFromMapFunc(r.workspacesForRuntime)).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.workspacesForRuntimePod)).
		Complete(r)
}
