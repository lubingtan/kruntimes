package admission

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	webhookserver "sigs.k8s.io/controller-runtime/pkg/webhook"
	admissionwebhook "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

const defaultAuthorizationTimeout = 2 * time.Second

// RunValidationPath is the HTTPS endpoint for Run admission requests.
const RunValidationPath = "/validate-kruntimes-io-v1alpha1-run"

// SubjectAccessReviewer evaluates a Kubernetes authorization request for the
// original admission caller.
type SubjectAccessReviewer interface {
	Review(context.Context, authorizationv1.SubjectAccessReview) (authorizationv1.SubjectAccessReviewStatus, error)
}

// KubernetesSubjectAccessReviewer creates SubjectAccessReview objects through
// the Kubernetes API.
type KubernetesSubjectAccessReviewer struct {
	Client client.Client
}

func (r KubernetesSubjectAccessReviewer) Review(ctx context.Context, review authorizationv1.SubjectAccessReview) (authorizationv1.SubjectAccessReviewStatus, error) {
	if err := r.Client.Create(ctx, &review); err != nil {
		return authorizationv1.SubjectAccessReviewStatus{}, err
	}
	return review.Status, nil
}

// ServiceAccountIdentity identifies a Kubernetes ServiceAccount admission
// caller. The controller ServiceAccount is allowed to create only verified
// WorkflowRun-owned child Runs without a separate workspace use grant.
type ServiceAccountIdentity struct {
	Namespace string
	Name      string
}

func (identity ServiceAccountIdentity) matches(username string) bool {
	return identity.Namespace != "" && identity.Name != "" && username == fmt.Sprintf("system:serviceaccount:%s:%s", identity.Namespace, identity.Name)
}

// RunAdmissionValidator validates Run references that require cluster state
// before the Run is persisted. It deliberately keeps authorization out of the
// scheduler and runtimed, which do not have the original caller identity.
type RunAdmissionValidator struct {
	Reader                           client.Reader
	Reviewer                         SubjectAccessReviewer
	Decoder                          admissionwebhook.Decoder
	WorkflowControllerServiceAccount ServiceAccountIdentity
	AuthorizationTimeout             time.Duration
}

// RegisterRunAdmissionValidator installs the Run admission handler on a
// controller-runtime webhook server.
func RegisterRunAdmissionValidator(server webhookserver.Server, reader client.Reader, reviewer SubjectAccessReviewer, workflowControllerServiceAccount ServiceAccountIdentity, scheme *runtime.Scheme) {
	server.Register(RunValidationPath, &admissionwebhook.Webhook{
		Handler: &RunAdmissionValidator{
			Reader:                           reader,
			Reviewer:                         reviewer,
			Decoder:                          admissionwebhook.NewDecoder(scheme),
			WorkflowControllerServiceAccount: workflowControllerServiceAccount,
		},
	})
}

// Handle validates an admission request for a Run.
func (v *RunAdmissionValidator) Handle(ctx context.Context, request admissionwebhook.Request) admissionwebhook.Response {
	run := &v1alpha1.Run{}
	if err := v.Decoder.Decode(request, run); err != nil {
		return admissionwebhook.Errored(http.StatusBadRequest, fmt.Errorf("decode Run: %w", err))
	}
	if run.Spec.Workspace == nil {
		return admissionwebhook.Allowed("Run has no PersistentWorkspace reference")
	}

	workspace := &v1alpha1.PersistentWorkspace{}
	key := client.ObjectKey{Namespace: request.Namespace, Name: run.Spec.Workspace.Name}
	if err := v.Reader.Get(ctx, key, workspace); err != nil {
		if apierrors.IsNotFound(err) {
			return admissionwebhook.Denied(fmt.Sprintf("referenced PersistentWorkspace %q does not exist", run.Spec.Workspace.Name))
		}
		return admissionwebhook.Errored(http.StatusInternalServerError, fmt.Errorf("read referenced PersistentWorkspace: %w", err))
	}
	if workspace.Spec.Runtime != run.Spec.Runtime {
		return admissionwebhook.Denied(fmt.Sprintf("referenced PersistentWorkspace %q uses Runtime %q, not %q", workspace.Name, workspace.Spec.Runtime, run.Spec.Runtime))
	}
	if v.WorkflowControllerServiceAccount.matches(request.UserInfo.Username) {
		if err := v.validateWorkflowChildRun(ctx, request.Namespace, run, workspace); err != nil {
			return admissionwebhook.Denied(fmt.Sprintf("controller-created Run may not use PersistentWorkspace %q: %v", workspace.Name, err))
		}
		return admissionwebhook.Allowed("verified WorkflowRun-owned child Run")
	}

	authorizationCtx, cancel := context.WithTimeout(ctx, v.authorizationTimeout())
	defer cancel()
	status, err := v.Reviewer.Review(authorizationCtx, subjectAccessReviewFor(request, workspace.Name))
	if err != nil {
		return admissionwebhook.Errored(http.StatusServiceUnavailable, fmt.Errorf("authorize PersistentWorkspace use: %w", err))
	}
	if !status.Allowed {
		return admissionwebhook.Denied(fmt.Sprintf("not authorized to use PersistentWorkspace %q", workspace.Name))
	}
	return admissionwebhook.Allowed("authorized to use referenced PersistentWorkspace")
}

func (v *RunAdmissionValidator) validateWorkflowChildRun(ctx context.Context, namespace string, run *v1alpha1.Run, workspace *v1alpha1.PersistentWorkspace) error {
	owner := metav1.GetControllerOf(run)
	if owner == nil || owner.APIVersion != v1alpha1.GroupVersion.String() || owner.Kind != "WorkflowRun" || owner.Name == "" || owner.UID == "" {
		return fmt.Errorf("Run is not controlled by a WorkflowRun")
	}
	if run.Labels[v1alpha1.WorkflowRunUIDLabel] != string(owner.UID) {
		return fmt.Errorf("Run workflow UID label does not match its WorkflowRun owner")
	}
	jobName := run.Labels[v1alpha1.WorkflowJobLabel]
	if jobName == "" {
		return fmt.Errorf("Run does not identify its Workflow job")
	}

	workflowRun := &v1alpha1.WorkflowRun{}
	if err := v.Reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: owner.Name}, workflowRun); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("owning WorkflowRun %q does not exist", owner.Name)
		}
		return fmt.Errorf("read owning WorkflowRun %q: %w", owner.Name, err)
	}
	if workflowRun.UID != owner.UID {
		return fmt.Errorf("owning WorkflowRun %q has a different UID", owner.Name)
	}
	if !metav1.IsControlledBy(workspace, workflowRun) {
		return fmt.Errorf("workspace is not controlled by the owning WorkflowRun")
	}
	if workspace.Labels[v1alpha1.WorkflowRunUIDLabel] != string(workflowRun.UID) || workspace.Labels[v1alpha1.WorkflowJobLabel] != jobName {
		return fmt.Errorf("workspace does not belong to the same Workflow job")
	}
	return nil
}

func (v *RunAdmissionValidator) authorizationTimeout() time.Duration {
	if v.AuthorizationTimeout > 0 {
		return v.AuthorizationTimeout
	}
	return defaultAuthorizationTimeout
}

func subjectAccessReviewFor(request admissionwebhook.Request, workspaceName string) authorizationv1.SubjectAccessReview {
	userInfo := request.UserInfo
	extra := make(map[string]authorizationv1.ExtraValue, len(userInfo.Extra))
	for key, value := range userInfo.Extra {
		extra[key] = authorizationv1.ExtraValue(slices.Clone(value))
	}
	return authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User:   userInfo.Username,
			UID:    userInfo.UID,
			Groups: slices.Clone(userInfo.Groups),
			Extra:  extra,
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace:   request.Namespace,
				Verb:        "use",
				Group:       v1alpha1.GroupVersion.Group,
				Resource:    "persistentworkspaces",
				Subresource: "use",
				Name:        workspaceName,
			},
		},
	}
}

var _ admissionwebhook.Handler = (*RunAdmissionValidator)(nil)
