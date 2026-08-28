package runtimed

import (
	"context"
	"fmt"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

// reconcileDeletingRegistration reconciles deletion for every Run. The shared
// finalizer upgrades remote Runtime Server cleanup from best effort to a
// required, retryable deletion gate for long-lived registrations.
func (c *Controller) reconcileDeletingRegistration(ctx context.Context, run *v1alpha1.Run) (ctrl.Result, error) {
	if run == nil {
		return ctrl.Result{}, nil
	}
	ar := c.buildActiveRun(run)
	if value, exists := c.activeRuns.Load(string(run.UID)); exists {
		ar = value.(*activeRun)
		ar.run = run
	}
	if err := c.releaseRuntimeServerState(ctx, ar); err != nil {
		if controllerutil.ContainsFinalizer(run, v1alpha1.RunRegistrationCleanupFinalizer) {
			return ctrl.Result{RequeueAfter: time.Second}, err
		}
		c.Log.Error(err, "failed to release Runtime Server state for deleting legacy Run", "run", client.ObjectKeyFromObject(run))
	}
	c.releaseLocalRunState(ctx, ar)
	if controllerutil.RemoveFinalizer(run, v1alpha1.RunRegistrationCleanupFinalizer) {
		if err := c.Update(ctx, run); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove registration cleanup finalizer: %w", err)
		}
	}
	return ctrl.Result{}, nil
}
