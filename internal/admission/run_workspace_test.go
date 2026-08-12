package admission

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	admissionwebhook "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

func TestRunWorkspaceValidator(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add kruntimes scheme: %v", err)
	}
	workspace := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"},
		Spec:       v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"},
	}
	mismatchedWorkspace := workspace.DeepCopy()
	mismatchedWorkspace.Spec.Runtime = "python"

	tests := []struct {
		name        string
		run         *v1alpha1.Run
		objects     []runtime.Object
		reviewer    subjectAccessReviewerFunc
		wantAllowed bool
	}{
		{
			name:        "allows run without workspace",
			run:         runWithWorkspace(nil),
			wantAllowed: true,
		},
		{
			name:        "rejects missing workspace",
			run:         runWithWorkspace(&v1alpha1.RunWorkspaceReference{Name: "missing"}),
			wantAllowed: false,
		},
		{
			name:        "rejects runtime mismatch",
			run:         runWithWorkspace(&v1alpha1.RunWorkspaceReference{Name: "build"}),
			objects:     []runtime.Object{mismatchedWorkspace},
			wantAllowed: false,
		},
		{
			name:        "allows authorized caller",
			run:         runWithWorkspace(&v1alpha1.RunWorkspaceReference{Name: "build"}),
			objects:     []runtime.Object{workspace},
			wantAllowed: true,
			reviewer: func(_ context.Context, review authorizationv1.SubjectAccessReview) (authorizationv1.SubjectAccessReviewStatus, error) {
				assertWorkspaceUseReview(t, review, "build")
				return authorizationv1.SubjectAccessReviewStatus{Allowed: true}, nil
			},
		},
		{
			name:        "rejects denied caller",
			run:         runWithWorkspace(&v1alpha1.RunWorkspaceReference{Name: "build"}),
			objects:     []runtime.Object{workspace},
			wantAllowed: false,
			reviewer: func(_ context.Context, review authorizationv1.SubjectAccessReview) (authorizationv1.SubjectAccessReviewStatus, error) {
				assertWorkspaceUseReview(t, review, "build")
				return authorizationv1.SubjectAccessReviewStatus{Allowed: false}, nil
			},
		},
		{
			name:        "fails closed when reviewer is unavailable",
			run:         runWithWorkspace(&v1alpha1.RunWorkspaceReference{Name: "build"}),
			objects:     []runtime.Object{workspace},
			wantAllowed: false,
			reviewer: func(context.Context, authorizationv1.SubjectAccessReview) (authorizationv1.SubjectAccessReviewStatus, error) {
				return authorizationv1.SubjectAccessReviewStatus{}, errors.New("authorization API unavailable")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(test.objects...).Build()
			reviewer := test.reviewer
			if reviewer == nil {
				reviewer = func(context.Context, authorizationv1.SubjectAccessReview) (authorizationv1.SubjectAccessReviewStatus, error) {
					t.Fatal("unexpected SubjectAccessReview")
					return authorizationv1.SubjectAccessReviewStatus{}, nil
				}
			}
			validator := &RunWorkspaceValidator{
				Reader:   reader,
				Reviewer: reviewer,
				Decoder:  admissionwebhook.NewDecoder(scheme),
			}
			response := validator.Handle(context.Background(), admissionRequest(t, test.run))
			if response.Allowed != test.wantAllowed {
				t.Fatalf("allowed = %t, want %t; message = %q", response.Allowed, test.wantAllowed, response.Result.Message)
			}
		})
	}
}

func TestRunWorkspaceValidatorBoundsAuthorizationTimeout(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add kruntimes scheme: %v", err)
	}
	workspace := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"},
		Spec:       v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"},
	}
	validator := &RunWorkspaceValidator{
		Reader:               fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(workspace).Build(),
		Decoder:              admissionwebhook.NewDecoder(scheme),
		AuthorizationTimeout: 100 * time.Millisecond,
		Reviewer: subjectAccessReviewerFunc(func(ctx context.Context, _ authorizationv1.SubjectAccessReview) (authorizationv1.SubjectAccessReviewStatus, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("authorization context has no deadline")
			}
			if remaining := time.Until(deadline); remaining <= 0 || remaining > time.Second {
				t.Fatalf("authorization deadline remaining = %s, want bounded timeout", remaining)
			}
			return authorizationv1.SubjectAccessReviewStatus{Allowed: true}, nil
		}),
	}
	response := validator.Handle(context.Background(), admissionRequest(t, runWithWorkspace(&v1alpha1.RunWorkspaceReference{Name: "build"})))
	if !response.Allowed {
		t.Fatalf("allowed = false, want true; message = %q", response.Result.Message)
	}
}

func TestRunWorkspaceValidatorFencesControllerWorkflowChildRuns(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add kruntimes scheme: %v", err)
	}
	workflowRun := &v1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "workflow", Namespace: "default", UID: types.UID("workflow-uid")}}
	workspace := &v1alpha1.PersistentWorkspace{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "build",
			Namespace:       "default",
			Labels:          map[string]string{v1alpha1.WorkflowRunUIDLabel: string(workflowRun.UID), v1alpha1.WorkflowJobLabel: "build"},
			OwnerReferences: []metav1.OwnerReference{workflowRunOwnerReference(workflowRun)},
		},
		Spec: v1alpha1.PersistentWorkspaceSpec{Runtime: "bash"},
	}
	validChild := runWithWorkspace(&v1alpha1.RunWorkspaceReference{Name: workspace.Name})
	validChild.Labels = map[string]string{v1alpha1.WorkflowRunUIDLabel: string(workflowRun.UID), v1alpha1.WorkflowJobLabel: "build"}
	validChild.OwnerReferences = []metav1.OwnerReference{workflowRunOwnerReference(workflowRun)}

	tests := []struct {
		name        string
		run         *v1alpha1.Run
		workspace   *v1alpha1.PersistentWorkspace
		username    string
		wantAllowed bool
		expectSAR   bool
	}{
		{
			name:        "allows verified controller child Run without SAR",
			run:         validChild,
			workspace:   workspace,
			username:    "system:serviceaccount:kruntimes:kruntimes-controller",
			wantAllowed: true,
		},
		{
			name: "rejects controller child Run for another job workspace",
			run: func() *v1alpha1.Run {
				run := validChild.DeepCopy()
				run.Labels[v1alpha1.WorkflowJobLabel] = "test"
				return run
			}(),
			workspace:   workspace,
			username:    "system:serviceaccount:kruntimes:kruntimes-controller",
			wantAllowed: false,
		},
		{
			name:        "does not let a user impersonate controller ownership",
			run:         validChild,
			workspace:   workspace,
			username:    "alice",
			wantAllowed: false,
			expectSAR:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(workflowRun, test.workspace).Build()
			sarCalled := false
			validator := &RunWorkspaceValidator{
				Reader:                           reader,
				Decoder:                          admissionwebhook.NewDecoder(scheme),
				WorkflowControllerServiceAccount: ServiceAccountIdentity{Namespace: "kruntimes", Name: "kruntimes-controller"},
				Reviewer: subjectAccessReviewerFunc(func(_ context.Context, _ authorizationv1.SubjectAccessReview) (authorizationv1.SubjectAccessReviewStatus, error) {
					sarCalled = true
					return authorizationv1.SubjectAccessReviewStatus{Allowed: false}, nil
				}),
			}
			response := validator.Handle(context.Background(), admissionRequestFor(t, test.run, test.username))
			if response.Allowed != test.wantAllowed {
				t.Fatalf("allowed = %t, want %t; message = %q", response.Allowed, test.wantAllowed, response.Result.Message)
			}
			if sarCalled != test.expectSAR {
				t.Fatalf("SubjectAccessReview called = %t, want %t", sarCalled, test.expectSAR)
			}
		})
	}
}

type subjectAccessReviewerFunc func(context.Context, authorizationv1.SubjectAccessReview) (authorizationv1.SubjectAccessReviewStatus, error)

func (f subjectAccessReviewerFunc) Review(ctx context.Context, review authorizationv1.SubjectAccessReview) (authorizationv1.SubjectAccessReviewStatus, error) {
	return f(ctx, review)
}

func runWithWorkspace(workspace *v1alpha1.RunWorkspaceReference) *v1alpha1.Run {
	return &v1alpha1.Run{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Run"},
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "default"},
		Spec: v1alpha1.RunSpec{
			Runtime:   "bash",
			Workspace: workspace,
		},
	}
}

func admissionRequest(t *testing.T, run *v1alpha1.Run) admissionwebhook.Request {
	return admissionRequestFor(t, run, "alice")
}

func admissionRequestFor(t *testing.T, run *v1alpha1.Run, username string) admissionwebhook.Request {
	t.Helper()
	raw, err := json.Marshal(run)
	if err != nil {
		t.Fatalf("marshal Run: %v", err)
	}
	return admissionwebhook.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Namespace: "default",
		UserInfo: authenticationv1.UserInfo{
			Username: username,
			UID:      "user-uid",
			Groups:   []string{"developers"},
			Extra:    map[string]authenticationv1.ExtraValue{"tenant": {"team-a"}},
		},
		Object: runtime.RawExtension{Raw: raw},
	}}
}

func workflowRunOwnerReference(workflowRun *v1alpha1.WorkflowRun) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: v1alpha1.GroupVersion.String(),
		Kind:       "WorkflowRun",
		Name:       workflowRun.Name,
		UID:        workflowRun.UID,
		Controller: &controller,
	}
}

func assertWorkspaceUseReview(t *testing.T, review authorizationv1.SubjectAccessReview, workspaceName string) {
	t.Helper()
	if review.Spec.User != "alice" || review.Spec.UID != "user-uid" {
		t.Fatalf("review subject = %q/%q, want alice/user-uid", review.Spec.User, review.Spec.UID)
	}
	if len(review.Spec.Groups) != 1 || review.Spec.Groups[0] != "developers" {
		t.Fatalf("review groups = %#v, want [developers]", review.Spec.Groups)
	}
	if got := review.Spec.Extra["tenant"]; len(got) != 1 || got[0] != "team-a" {
		t.Fatalf("review extra tenant = %#v, want [team-a]", got)
	}
	attributes := review.Spec.ResourceAttributes
	if attributes == nil {
		t.Fatal("review resource attributes are nil")
	}
	if attributes.Namespace != "default" || attributes.Verb != "use" || attributes.Group != v1alpha1.GroupVersion.Group || attributes.Resource != "persistentworkspaces" || attributes.Subresource != "use" || attributes.Name != workspaceName {
		t.Fatalf("review resource attributes = %#v", attributes)
	}
}
