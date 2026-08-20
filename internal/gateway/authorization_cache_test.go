package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCachingAuthorizerCachesSuccessfulDecisionByRunIdentity(t *testing.T) {
	now := time.Now()
	base := &countingAuthorizer{}
	cache := newAuthorizationDecisionCache(2, time.Minute, func() time.Time { return now })
	authorizer := &CachingAuthorizer{authorizer: base, cache: cache}
	request := bearerRequest("session-token")
	run := cachedAuthorizationRun("run-uid-a")

	if err := authorizer.Authorize(t.Context(), request, run); err != nil {
		t.Fatalf("first Authorize: %v", err)
	}
	if err := authorizer.Authorize(t.Context(), request, run); err != nil {
		t.Fatalf("cached Authorize: %v", err)
	}
	if base.calls != 1 {
		t.Fatalf("base calls = %d, want 1", base.calls)
	}

	otherToken := bearerRequest("other-session-token")
	if err := authorizer.Authorize(t.Context(), otherToken, run); err != nil {
		t.Fatalf("Authorize for other bearer token: %v", err)
	}
	if base.calls != 2 {
		t.Fatalf("base calls = %d, want 2 after bearer token changed", base.calls)
	}

	otherIdentity := cachedAuthorizationRun("run-uid-b")
	if err := authorizer.Authorize(t.Context(), request, otherIdentity); err != nil {
		t.Fatalf("Authorize for other Run identity: %v", err)
	}
	if base.calls != 3 {
		t.Fatalf("base calls = %d, want 3 after Run UID changed", base.calls)
	}

	now = now.Add(time.Minute)
	if err := authorizer.Authorize(t.Context(), request, run); err != nil {
		t.Fatalf("Authorize after expiry: %v", err)
	}
	if base.calls != 4 {
		t.Fatalf("base calls = %d, want 4 after expiry", base.calls)
	}
}

func TestCachingAuthorizerDoesNotCacheDeniedDecision(t *testing.T) {
	base := &countingAuthorizer{err: status.Error(codes.PermissionDenied, "denied")}
	authorizer := &CachingAuthorizer{
		authorizer: base,
		cache:      newAuthorizationDecisionCache(2, time.Minute, time.Now),
	}
	request := bearerRequest("session-token")
	run := cachedAuthorizationRun("run-uid")

	for range 2 {
		if err := authorizer.Authorize(t.Context(), request, run); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("Authorize error = %v, want PermissionDenied", err)
		}
	}
	if base.calls != 2 {
		t.Fatalf("base calls = %d, want denied decisions to bypass cache", base.calls)
	}
}

func TestAuthorizationDecisionCacheBoundedCapacity(t *testing.T) {
	cache := newAuthorizationDecisionCache(2, time.Minute, time.Now)
	for _, uid := range []string{"one", "two", "three"} {
		cache.Put(authorizationCacheKey{uid: uid})
	}
	if len(cache.entries) != 2 {
		t.Fatalf("cache entries = %d, want bounded capacity 2", len(cache.entries))
	}
}

type countingAuthorizer struct {
	calls int
	err   error
}

func (a *countingAuthorizer) Authorize(context.Context, *http.Request, *v1alpha1.Run) error {
	a.calls++
	return a.err
}

func bearerRequest(token string) *http.Request {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	return request
}

func cachedAuthorizationRun(uid string) *v1alpha1.Run {
	return &v1alpha1.Run{ObjectMeta: metav1.ObjectMeta{
		Namespace: "agents",
		Name:      "diagnose",
		UID:       types.UID(uid),
	}}
}
