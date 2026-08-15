package runtimed

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
	"github.com/kruntimes/kruntimes/api/v1alpha1"
	runretry "github.com/kruntimes/kruntimes/internal/retry"
	"github.com/kruntimes/kruntimes/internal/runstatus"
)

const sessionRegistrationTimeout = 10 * time.Second

func (c *Controller) startSessionRegistrationAsync(ar *activeRun) {
	go func() {
		ctx := context.Background()
		if err := c.prepareSession(ctx, ar); err != nil {
			c.applyStartSessionFailure(ctx, ar, runretry.ReasonPrepareSource, fmt.Errorf("prepare session: %w", err))
			return
		}
		if err := c.registerSession(ctx, ar); err != nil {
			c.applyStartSessionFailure(ctx, ar, runretry.ReasonSessionRegister, fmt.Errorf("register session: %w", err))
			return
		}
		uid := string(ar.run.UID)
		if value, exists := c.activeRuns.Load(uid); !exists || value != ar {
			// The Run was cancelled or deleted while local registration was in
			// flight. Do not leave a session behind after its owner released it.
			c.closeRuntimeSession(ctx, ar.run)
			return
		}
		if err := c.markSessionReady(ctx, ar); err != nil {
			c.Log.Error(err, "failed to mark Session Run ready", "run", client.ObjectKeyFromObject(ar.run))
		}
	}()
}

func (c *Controller) prepareSession(ctx context.Context, ar *activeRun) error {
	if ar == nil || ar.run == nil {
		return fmt.Errorf("active Run is required")
	}
	if ar.prepared.Load() {
		return nil
	}
	if err := prepareSource(ar); err != nil {
		return err
	}
	if err := c.stageArtifactInputs(ctx, ar); err != nil {
		return err
	}
	ar.prepared.Store(true)
	return nil
}

func (c *Controller) registerSession(ctx context.Context, ar *activeRun) error {
	if c.sessionCli == nil {
		return fmt.Errorf("SessionRuntime client is not configured")
	}
	request, err := sessionRegistrationRequest(ar.run, ar.workDir)
	if err != nil {
		return err
	}
	registrationCtx, cancel := context.WithTimeout(ctx, sessionRegistrationTimeout)
	defer cancel()
	response, err := c.sessionCli.RegisterSession(registrationCtx, request, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	if response.GetState() != pb.SessionState_SESSION_STATE_READY {
		return fmt.Errorf("Runtime Server returned session state %s", response.GetState())
	}
	return nil
}

func (c *Controller) markSessionReady(ctx context.Context, ar *activeRun) error {
	if ar == nil || ar.run == nil {
		return fmt.Errorf("active Run is required")
	}
	uid := string(ar.run.UID)
	if value, exists := c.activeRuns.Load(uid); !exists || value != ar {
		return nil
	}

	var run v1alpha1.Run
	if err := c.Get(ctx, client.ObjectKeyFromObject(ar.run), &run); err != nil {
		return client.IgnoreNotFound(err)
	}
	if run.Status.Phase != v1alpha1.RunRunning || run.Status.AssignedPod != c.PodName {
		return nil
	}
	run.Status.Phase = v1alpha1.RunReady
	run.Status.Message = "session registered"
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               runstatus.ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "SessionRegistered",
		Message:            "session is ready for gateway operations",
		LastTransitionTime: metav1.Now(),
	})
	if err := c.Status().Update(ctx, &run); err != nil {
		return err
	}
	ar.run = &run
	return nil
}

func (c *Controller) applyStartSessionFailure(ctx context.Context, ar *activeRun, reason string, startErr error) {
	if ar == nil || ar.run == nil {
		return
	}
	uid := string(ar.run.UID)
	if value, exists := c.activeRuns.Load(uid); !exists || value != ar {
		return
	}
	var run v1alpha1.Run
	if err := c.Get(ctx, client.ObjectKeyFromObject(ar.run), &run); err != nil {
		c.Log.Error(err, "failed to get Session Run after registration error", "run", client.ObjectKeyFromObject(ar.run))
		return
	}
	if run.Status.Phase != v1alpha1.RunRunning || run.Status.AssignedPod != c.PodName {
		return
	}
	ar.run = &run
	if _, err := c.applyFailure(ctx, ar, reason, startErr.Error()); err != nil {
		c.Log.Error(err, "failed to update Session Run after registration error", "run", client.ObjectKeyFromObject(ar.run))
	}
}

func (c *Controller) reconcileSessionRecovery(ctx context.Context, run *v1alpha1.Run) (ctrl.Result, error) {
	ar := c.buildActiveRun(run)
	request, err := sessionRegistrationRequest(run, ar.workDir)
	if err != nil {
		return c.applyTerminal(ctx, ar, v1alpha1.RunFailed, runretry.ReasonExecutionLost, err.Error())
	}
	if c.sessionCli == nil {
		return ctrl.Result{}, fmt.Errorf("SessionRuntime client is not configured")
	}
	recoveryCtx, cancel := context.WithTimeout(ctx, sessionRegistrationTimeout)
	defer cancel()
	response, err := c.sessionCli.GetSessionStatus(recoveryCtx, &pb.GetSessionStatusRequest{Identity: request.Identity})
	if err == nil {
		if response.GetState() == pb.SessionState_SESSION_STATE_READY {
			if !c.tryClaimActiveRun(ar) {
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			c.recordActiveRuns(run.Spec.Runtime)
			return ctrl.Result{RequeueAfter: activeRunRequeueAfter(ar)}, nil
		}
		return c.applyTerminal(ctx, ar, v1alpha1.RunFailed, runretry.ReasonExecutionLost, fmt.Sprintf("runtime session recovered in state %s", response.GetState()))
	}
	if status.Code(err) != codes.NotFound {
		return ctrl.Result{}, fmt.Errorf("get SessionRuntime status after runtimed restart: %w", err)
	}
	if run.Status.Phase == v1alpha1.RunRunning {
		if !c.tryClaimActiveRun(ar) {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		c.recordActiveRuns(run.Spec.Runtime)
		c.startSessionRegistrationAsync(ar)
		return ctrl.Result{RequeueAfter: activeRunRequeueAfter(ar)}, nil
	}
	return c.applyTerminal(ctx, ar, v1alpha1.RunFailed, runretry.ReasonExecutionLost, "runtime session not found after runtimed restart")
}

func (c *Controller) closeSessionAndApplyTerminal(ctx context.Context, ar *activeRun, phase v1alpha1.RunPhase, reason, message string) (ctrl.Result, error) {
	return c.applyTerminal(ctx, ar, phase, reason, message)
}

func (c *Controller) closeRuntimeSession(ctx context.Context, run *v1alpha1.Run) {
	if run == nil {
		return
	}
	if c.SessionOperations != nil {
		c.SessionOperations.Close(string(run.UID))
	}
	if c.sessionCli == nil {
		return
	}
	identity, err := sessionIdentityForRun(run)
	if err != nil {
		c.Log.Error(err, "failed to build SessionRuntime close request", "run", client.ObjectKeyFromObject(run))
		return
	}
	closeCtx, cancel := context.WithTimeout(ctx, executionCleanupTimeout)
	defer cancel()
	_, err = c.sessionCli.CloseSession(closeCtx, &pb.CloseSessionRequest{Identity: identity})
	if err != nil && status.Code(err) != codes.NotFound && status.Code(err) != codes.Unimplemented {
		c.Log.Error(err, "failed to close Runtime Server session", "run", client.ObjectKeyFromObject(run))
	}
}
