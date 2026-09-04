package gateway

import (
	"context"
	"crypto/sha256"
	"net/http"
	"sync"
	"time"

	"github.com/kruntimes/kruntimes/api/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultAuthorizationCacheCapacity = 1024
	defaultAuthorizationCacheTTL      = 30 * time.Second
)

// AuthorizationCacheOptions bounds cached successful bearer-token or verified
// client-certificate decisions.
// A zero or negative TTL or capacity disables caching.
type AuthorizationCacheOptions struct {
	Capacity int
	TTL      time.Duration
}

// DefaultAuthorizationCacheOptions returns the gateway's bounded cache policy.
func DefaultAuthorizationCacheOptions() AuthorizationCacheOptions {
	return AuthorizationCacheOptions{
		Capacity: defaultAuthorizationCacheCapacity,
		TTL:      defaultAuthorizationCacheTTL,
	}
}

// CachingAuthorizer caches only successful authorization decisions. It stores
// a credential digest, never the bearer token, client certificate, or
// Kubernetes user identity.
type CachingAuthorizer struct {
	authorizer Authorizer
	cache      *authorizationDecisionCache
}

// NewCachingAuthorizer wraps an Authorizer with a bounded successful-decision
// cache. It is safe for concurrent gateway requests.
func NewCachingAuthorizer(authorizer Authorizer, options AuthorizationCacheOptions) Authorizer {
	if options.Capacity <= 0 || options.TTL <= 0 {
		return authorizer
	}
	return &CachingAuthorizer{
		authorizer: authorizer,
		cache:      newAuthorizationDecisionCache(options.Capacity, options.TTL, time.Now),
	}
}

func (a *CachingAuthorizer) Authorize(ctx context.Context, request *http.Request, run *v1alpha1.Run) error {
	if a == nil || a.authorizer == nil {
		return status.Error(codes.FailedPrecondition, "Kubernetes authorization client is not configured")
	}
	key, cacheable := authorizationCacheKeyForRequest(request, run)
	if cacheable && a.cache.Get(key) {
		return nil
	}
	if err := a.authorizer.Authorize(ctx, request, run); err != nil {
		return err
	}
	if cacheable {
		a.cache.Put(key)
	}
	return nil
}

type authorizationCacheKey struct {
	credentialDigest [sha256.Size]byte
	namespace        string
	name             string
	uid              string
}

func authorizationCacheKeyForRequest(request *http.Request, run *v1alpha1.Run) (authorizationCacheKey, bool) {
	if request == nil || run == nil || run.UID == "" {
		return authorizationCacheKey{}, false
	}
	var credential []byte
	if token, ok := bearerToken(request.Header.Get("Authorization")); ok {
		credential = []byte(token)
	} else if certificate := verifiedClientCertificate(request); certificate != nil {
		credential = certificate.Raw
	} else {
		return authorizationCacheKey{}, false
	}
	return authorizationCacheKey{
		credentialDigest: sha256.Sum256(credential),
		namespace:        run.Namespace,
		name:             run.Name,
		uid:              string(run.UID),
	}, true
}

type authorizationDecisionCache struct {
	mu       sync.Mutex
	entries  map[authorizationCacheKey]time.Time
	capacity int
	ttl      time.Duration
	now      func() time.Time
}

func newAuthorizationDecisionCache(capacity int, ttl time.Duration, now func() time.Time) *authorizationDecisionCache {
	return &authorizationDecisionCache{
		entries:  make(map[authorizationCacheKey]time.Time),
		capacity: capacity,
		ttl:      ttl,
		now:      now,
	}
}

func (c *authorizationDecisionCache) Get(key authorizationCacheKey) bool {
	if c == nil || c.capacity <= 0 || c.ttl <= 0 {
		return false
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	expiresAt, exists := c.entries[key]
	if !exists {
		return false
	}
	if !now.Before(expiresAt) {
		delete(c.entries, key)
		return false
	}
	return true
}

func (c *authorizationDecisionCache) Put(key authorizationCacheKey) {
	if c == nil || c.capacity <= 0 || c.ttl <= 0 {
		return
	}
	now := c.now()
	expiresAt := now.Add(c.ttl)
	c.mu.Lock()
	defer c.mu.Unlock()
	for existingKey, existingExpiry := range c.entries {
		if !now.Before(existingExpiry) {
			delete(c.entries, existingKey)
		}
	}
	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.capacity {
		var oldestKey authorizationCacheKey
		var oldestExpiry time.Time
		for existingKey, existingExpiry := range c.entries {
			if oldestExpiry.IsZero() || existingExpiry.Before(oldestExpiry) {
				oldestKey, oldestExpiry = existingKey, existingExpiry
			}
		}
		delete(c.entries, oldestKey)
	}
	c.entries[key] = expiresAt
}
