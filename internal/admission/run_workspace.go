package admission

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	webhookserver "sigs.k8s.io/controller-runtime/pkg/webhook"
	admissionwebhook "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

const defaultAuthorizationTimeout = 2 * time.Second

// RunWorkspaceValidationPath is the HTTPS endpoint for Run workspace
// authorization requests.
const RunWorkspaceValidationPath = "/validate-kruntimes-io-v1alpha1-run"

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

// RunWorkspaceValidator authorizes direct Run references to PersistentWorkspace
// objects before the Run is persisted. It deliberately keeps authorization out
// of the scheduler and runtimed, which do not have the original caller identity.
type RunWorkspaceValidator struct {
	Reader               client.Reader
	Reviewer             SubjectAccessReviewer
	Decoder              admissionwebhook.Decoder
	AuthorizationTimeout time.Duration
}

// RegisterRunWorkspaceValidator installs the Run workspace admission handler
// on a controller-runtime webhook server.
func RegisterRunWorkspaceValidator(server webhookserver.Server, reader client.Reader, reviewer SubjectAccessReviewer, scheme *runtime.Scheme) {
	server.Register(RunWorkspaceValidationPath, &admissionwebhook.Webhook{
		Handler: &RunWorkspaceValidator{
			Reader:   reader,
			Reviewer: reviewer,
			Decoder:  admissionwebhook.NewDecoder(scheme),
		},
	})
}

// Handle validates an admission request for a Run.
func (v *RunWorkspaceValidator) Handle(ctx context.Context, request admissionwebhook.Request) admissionwebhook.Response {
	run := &v1alpha1.Run{}
	if err := v.Decoder.Decode(request, run); err != nil {
		return admissionwebhook.Errored(http.StatusBadRequest, fmt.Errorf("decode Run: %w", err))
	}
	if run.Spec.Workspace == nil {
		return admissionwebhook.Allowed("Run does not reference a PersistentWorkspace")
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

func (v *RunWorkspaceValidator) authorizationTimeout() time.Duration {
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

var _ admissionwebhook.Handler = (*RunWorkspaceValidator)(nil)
