package beads

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
)

type guardedClaimRaceStore struct {
	Store
	onListOnce func()
	claim      func(context.Context, AssignmentClaimRequest) (Bead, bool, error)
	claimErr   error
	getCalls   int
}

func (s *guardedClaimRaceStore) List(query ListQuery) ([]Bead, error) {
	rows, err := s.Store.List(query)
	if hook := s.onListOnce; hook != nil {
		s.onListOnce = nil
		hook()
	}
	return rows, err
}

func (s *guardedClaimRaceStore) Get(id string) (Bead, error) {
	s.getCalls++
	return s.Store.Get(id)
}

func (s *guardedClaimRaceStore) ClaimAssignment(ctx context.Context, req AssignmentClaimRequest) (Bead, bool, error) {
	if s.claim != nil {
		return s.claim(ctx, req)
	}
	if s.claimErr != nil {
		return Bead{}, false, s.claimErr
	}
	claimer, ok := GuardedAssignmentClaimerFor(s.Store)
	if !ok {
		return Bead{}, false, ErrGuardedAssignmentClaimUnsupported
	}
	return claimer.ClaimAssignment(ctx, req)
}

func TestCachingStoreRejectsNonAuthoritativeGuardedClaimReadback(t *testing.T) {
	mem := NewMemStore()
	created, err := mem.Create(Bead{
		Title:    "pre-claim",
		Metadata: map[string]string{"gc.routed_to": "rig/pool"},
	})
	if err != nil {
		t.Fatal(err)
	}
	backing := &guardedClaimRaceStore{Store: mem}
	backing.claim = func(_ context.Context, _ AssignmentClaimRequest) (Bead, bool, error) {
		// A capability provider that reports success without returning the exact
		// assigned post-state violates the contract. The cache must not install it.
		return cloneBead(created), true, nil
	}
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	req := AssignmentClaimRequest{
		ID:                 created.ID,
		Actor:              "worker-1",
		ExpectedStatus:     "open",
		ExpectedMetadata:   map[string]string{"gc.routed_to": "rig/pool"},
		ForbiddenLabels:    []string{"hold:external"},
		AssignmentMetadata: map[string]string{"gc.session_id": "session-1"},
	}
	claimed, won, err := cache.ClaimAssignment(context.Background(), req)
	if err == nil || won || claimed.ID != "" {
		t.Fatalf("ClaimAssignment = (%+v, %v, %v), want zero,false,readback error", claimed, won, err)
	}
	pre := backing.getCalls
	got, err := cache.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if backing.getCalls == pre {
		t.Fatal("Get was cache-served after non-authoritative guarded-claim readback")
	}
	if got.Status != "open" || got.Assignee != "" {
		t.Fatalf("Get = %+v, want authoritative backing row", got)
	}
}

func TestCachingStoreDefiniteGuardedClaimNotFoundDoesNotMutateCache(t *testing.T) {
	mem := NewMemStore()
	backing := &guardedClaimRaceStore{
		Store:    mem,
		claimErr: fmt.Errorf("exact claim target missing: %w", ErrNotFound),
	}
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	cache.mu.RLock()
	beforeSeq := cache.mutationSeq
	beforeDirty := len(cache.dirty)
	cache.mu.RUnlock()

	claimed, won, err := cache.ClaimAssignment(context.Background(), AssignmentClaimRequest{
		ID:                 "missing-exact-id",
		Actor:              "worker-1",
		ExpectedStatus:     "open",
		ExpectedMetadata:   map[string]string{"gc.routed_to": "rig/pool"},
		ForbiddenLabels:    []string{"hold:external"},
		AssignmentMetadata: map[string]string{"gc.session_id": "session-1"},
	})
	if won || claimed.ID != "" || !errors.Is(err, ErrNotFound) {
		t.Fatalf("ClaimAssignment = (%+v, %v, %v), want zero,false,ErrNotFound", claimed, won, err)
	}
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	if cache.mutationSeq != beforeSeq || len(cache.dirty) != beforeDirty {
		t.Fatalf("definite not-found changed cache state: mutationSeq %d -> %d, dirty %d -> %d",
			beforeSeq, cache.mutationSeq, beforeDirty, len(cache.dirty))
	}
}

func TestCachingStoreGuardedClaimDoesNotOverwriteNewerEvent(t *testing.T) {
	mem := NewMemStore()
	created, err := mem.Create(Bead{
		Title:    "pre-claim",
		Metadata: map[string]string{"gc.routed_to": "rig/pool"},
	})
	if err != nil {
		t.Fatal(err)
	}
	backing := &guardedClaimRaceStore{Store: mem}
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	backing.claim = func(ctx context.Context, req AssignmentClaimRequest) (Bead, bool, error) {
		claimer, ok := GuardedAssignmentClaimerFor(mem)
		if !ok {
			t.Fatal("MemStore does not expose GuardedAssignmentClaimer")
		}
		claimed, won, err := claimer.ClaimAssignment(ctx, req)
		if err != nil || !won {
			return claimed, won, err
		}
		if err := mem.Update(req.ID, UpdateOpts{Labels: []string{"hold:mayor"}}); err != nil {
			t.Fatal(err)
		}
		newer, err := mem.Get(req.ID)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(newer)
		if err != nil {
			t.Fatal(err)
		}
		// Deterministically deliver a later backing update after the atomic claim
		// committed but before ClaimAssignment gets the returned snapshot.
		cache.ApplyEvent("bead.updated", payload)
		return claimed, true, nil
	}
	req := AssignmentClaimRequest{
		ID:                 created.ID,
		Actor:              "worker-1",
		ExpectedStatus:     "open",
		ExpectedMetadata:   map[string]string{"gc.routed_to": "rig/pool"},
		ForbiddenLabels:    []string{"hold:mayor", "hold:external"},
		AssignmentMetadata: map[string]string{"gc.session_id": "session-1"},
	}
	claimed, won, err := cache.ClaimAssignment(context.Background(), req)
	if err != nil || !won || claimed.Assignee != req.Actor {
		t.Fatalf("ClaimAssignment = (%+v, %v, %v), want committed claim", claimed, won, err)
	}
	pre := backing.getCalls
	got, err := cache.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if backing.getCalls == pre {
		t.Fatal("Get was cache-served after concurrent event; claim return must evict/dirty before trusting either snapshot")
	}
	if !slices.Contains(got.Labels, "hold:mayor") {
		t.Fatalf("cached row = %+v, newer hold event was overwritten by older claim readback", got)
	}
}

func TestCachingStoreGuardedClaimDoesNotTrustLateStaleOpenEvent(t *testing.T) {
	mem := NewMemStore()
	created, err := mem.Create(Bead{
		Title:    "pre-claim",
		Metadata: map[string]string{"gc.routed_to": "rig/pool"},
	})
	if err != nil {
		t.Fatal(err)
	}
	backing := &guardedClaimRaceStore{Store: mem}
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	backing.claim = func(ctx context.Context, req AssignmentClaimRequest) (Bead, bool, error) {
		claimer, ok := GuardedAssignmentClaimerFor(mem)
		if !ok {
			t.Fatal("MemStore does not expose GuardedAssignmentClaimer")
		}
		claimed, won, err := claimer.ClaimAssignment(ctx, req)
		if err != nil || !won {
			return claimed, won, err
		}
		stale := cloneBead(created)
		stale.Title = "late stale open projection"
		payload, err := json.Marshal(stale)
		if err != nil {
			t.Fatal(err)
		}
		// Deliver a distinguishable pre-claim projection after the real claim
		// committed but before its returned row reaches the cache.
		cache.ApplyEvent("bead.updated", payload)
		return claimed, true, nil
	}
	req := AssignmentClaimRequest{
		ID:                 created.ID,
		Actor:              "worker-1",
		ExpectedStatus:     "open",
		ExpectedMetadata:   map[string]string{"gc.routed_to": "rig/pool"},
		ForbiddenLabels:    []string{"hold:mayor", "hold:external"},
		AssignmentMetadata: map[string]string{"gc.session_id": "session-1"},
	}
	claimed, won, err := cache.ClaimAssignment(context.Background(), req)
	if err != nil || !won || claimed.Assignee != req.Actor {
		t.Fatalf("ClaimAssignment = (%+v, %v, %v), want committed claim", claimed, won, err)
	}
	pre := backing.getCalls
	got, err := cache.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if backing.getCalls == pre {
		t.Fatal("Get was cache-served from a late stale event after guarded claim")
	}
	if got.Status != "in_progress" || got.Assignee != req.Actor || got.Metadata["gc.session_id"] != "session-1" {
		t.Fatalf("Get = %+v, want authoritative claimed backing row", got)
	}
}

func TestCachingStoreAmbiguousGuardedClaimFailureDirtySurvivesConcurrentScan(t *testing.T) {
	mem := NewMemStore()
	created, err := mem.Create(Bead{
		Title:    "pre-claim",
		Metadata: map[string]string{"gc.routed_to": "rig/pool"},
	})
	if err != nil {
		t.Fatal(err)
	}
	backing := &guardedClaimRaceStore{Store: mem}
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	req := AssignmentClaimRequest{
		ID:                 created.ID,
		Actor:              "worker-1",
		ExpectedStatus:     "open",
		ExpectedMetadata:   map[string]string{"gc.routed_to": "rig/pool"},
		ForbiddenLabels:    []string{"hold:external"},
		AssignmentMetadata: map[string]string{"gc.session_id": "session-1"},
	}
	wantErr := errors.New("ambiguous guarded-claim transport failure")
	backing.onListOnce = func() {
		claimer, ok := GuardedAssignmentClaimerFor(mem)
		if !ok {
			t.Error("MemStore does not expose GuardedAssignmentClaimer")
			return
		}
		if _, won, claimErr := claimer.ClaimAssignment(context.Background(), req); claimErr != nil || !won {
			t.Errorf("out-of-band ClaimAssignment = (won %v, err %v), want success", won, claimErr)
			return
		}
		backing.claimErr = wantErr
		if _, won, claimErr := cache.ClaimAssignment(context.Background(), req); won || !errors.Is(claimErr, wantErr) {
			t.Errorf("cache ClaimAssignment = (won %v, err %v), want false, injected error", won, claimErr)
		}
		backing.claimErr = nil
	}
	if _, err := cache.List(ListQuery{Live: true, AllowScan: true}); err != nil {
		t.Fatalf("List: %v", err)
	}

	pre := backing.getCalls
	got, err := cache.Get(created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if backing.getCalls == pre {
		t.Fatal("Get was cache-served; concurrent scan erased ambiguous guarded-claim dirty mark")
	}
	if got.Status != "in_progress" || got.Assignee != "worker-1" || got.Metadata["gc.session_id"] != "session-1" {
		t.Fatalf("Get after ambiguous claim = %+v, want authoritative assigned row", got)
	}
}

var _ GuardedAssignmentClaimer = (*guardedClaimRaceStore)(nil)
