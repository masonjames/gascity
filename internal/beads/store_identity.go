package beads

import "reflect"

// StoreIdentityTargeter is implemented by transparent store wrappers that
// share the ownership identity of an inner store. The target is for identity
// comparison only: callers must keep using the original wrapper for reads and
// writes so its cache, policy, and capability behavior remains intact.
type StoreIdentityTargeter interface {
	StoreIdentityTarget() Store
}

// storeIdentityMaxResolveDepth bounds wrapper traversal so a malformed cycle
// cannot loop forever. Store stacks are only a few layers deep in practice.
const storeIdentityMaxResolveDepth = 8

// ResolveStoreIdentity follows explicitly declared transparent wrapper targets
// and returns the innermost identity carrier. It never guesses from fields or
// concrete provider types. A nil target leaves the wrapper as its own identity.
func ResolveStoreIdentity(store Store) Store {
	for range storeIdentityMaxResolveDepth {
		targeter, ok := store.(StoreIdentityTargeter)
		if !ok {
			return store
		}
		target := targeter.StoreIdentityTarget()
		if target == nil {
			return store
		}
		store = target
	}
	return store
}

// SameStoreIdentity reports whether two store values resolve to the same
// physical ownership carrier. Transparent wrappers participate only by
// implementing [StoreIdentityTargeter]; incomparable concrete values fail
// closed instead of risking an interface-comparison panic.
func SameStoreIdentity(left, right Store) bool {
	left = ResolveStoreIdentity(left)
	right = ResolveStoreIdentity(right)
	if left == nil || right == nil {
		return false
	}
	leftType := reflect.TypeOf(left)
	if leftType != reflect.TypeOf(right) || !leftType.Comparable() {
		return false
	}
	return reflect.ValueOf(left).Interface() == reflect.ValueOf(right).Interface()
}
