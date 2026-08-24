package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
)

func TestKubernetesAuthorizerAuthenticatesAndAuthorizesRunAccess(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		review := action.(k8stesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		if review.Spec.Token != "session-token" {
			t.Fatalf("token = %q", review.Spec.Token)
		}
		review.Status = authenticationv1.TokenReviewStatus{
			Authenticated: true,
			User: authenticationv1.UserInfo{
				Username: "alice",
				UID:      "alice-uid",
				Groups:   []string{"team-a"},
			},
		}
		return true, review, nil
	})
	clientset.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		attributes := review.Spec.ResourceAttributes
		if review.Spec.User != "alice" || attributes == nil || attributes.Verb != "get" || attributes.Group != v1alpha1.GroupVersion.Group || attributes.Resource != "runs" || attributes.Namespace != "workloads" || attributes.Name != "diagnose" {
			t.Fatalf("SubjectAccessReview = %#v", review.Spec)
		}
		review.Status.Allowed = true
		return true, review, nil
	})

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer session-token")
	run := &v1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "diagnose", Namespace: "workloads"}}
	if err := (KubernetesAuthorizer{Client: clientset}).Authorize(t.Context(), request, run); err != nil {
		t.Fatalf("authorize: %v", err)
	}
}

func TestKubernetesAuthorizerRejectsMissingBearerToken(t *testing.T) {
	err := (KubernetesAuthorizer{Client: fake.NewSimpleClientset()}).Authorize(
		t.Context(),
		httptest.NewRequest(http.MethodGet, "/", nil),
		&v1alpha1.Run{},
	)
	if err == nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("error = %v, want Unauthenticated", err)
	}
}
