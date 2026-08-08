package beads

import "testing"

func TestResolveStoreIdentityFollowsDeclaredCachingLayers(t *testing.T) {
	backing := NewMemStore()
	wrapped := NewCachingStoreForTest(
		NewCachingStoreForTest(backing, nil),
		nil,
	)
	if got := ResolveStoreIdentity(wrapped); got != backing {
		t.Fatalf("ResolveStoreIdentity() = %T %p, want backing %p", got, got, backing)
	}

	other := NewMemStore()
	if got := ResolveStoreIdentity(NewCachingStoreForTest(other, nil)); got != other {
		t.Fatalf("ResolveStoreIdentity(independent) = %T %p, want backing %p", got, got, other)
	}
}
