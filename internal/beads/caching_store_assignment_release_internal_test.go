package beads

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
)

type assignmentReleaseRaceStore struct {
	Store
	release  func(context.Context, AssignmentReleaseRequest) (Bead, bool, error)
	getCalls int
}

func (s *assignmentReleaseRaceStore) Get(id string) (Bead, error) {
	s.getCalls++
	return s.Store.Get(id)
}

func (s *assignmentReleaseRaceStore) ReleaseAssignment(ctx context.Context, req AssignmentReleaseRequest) (Bead, bool, error) {
	if s.release != nil {
		return s.release(ctx, req)
	}
	releaser, ok := AssignmentReleaserFor(s.Store)
	if !ok {
		return Bead{}, false, ErrAssignmentReleaseUnsupported
	}
	return releaser.ReleaseAssignment(ctx, req)
}

func seedAssignmentReleaseWork(t *testing.T, store Store) Bead {
	t.Helper()
	work, err := store.Create(Bead{
		Title:    "assigned work",
		Assignee: "worker-1",
		Metadata: map[string]string{
			"gc.routed_to":  "rig/pool",
			"gc.session_id": "session-old",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	status := "in_progress"
	if err := store.Update(work.ID, UpdateOpts{Status: &status}); err != nil {
		t.Fatal(err)
	}
	work, err = store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	return work
}

func assignmentReleaseRequestFor(work Bead) AssignmentReleaseRequest {
	revision := work.Revision
	return AssignmentReleaseRequest{
		ID:               work.ID,
		ExpectedStatus:   "in_progress",
		ExpectedAssignee: "worker-1",
		ExpectedRevision: &revision,
		ExpectedMetadata: map[string]string{
			"gc.routed_to":  "rig/pool",
			"gc.session_id": "session-old",
		},
		ForbiddenLabels: []string{"hold:mayor", "hold:external"},
		ReleaseMetadata: map[string]string{"gc.session_id": ""},
		AbsentCoLocatedMatch: &CoLocatedMatchPredicate{
			ExpectedStatus: "open",
			ClassAnyOf:     []CoLocatedClassPredicate{{ExpectedType: "coordination-row"}},
			MatchValue:     "worker-1",
			MatchID:        true,
		},
	}
}

func TestCachingStoreRejectsNonAuthoritativeAssignmentReleaseReadback(t *testing.T) {
	mem := NewMemStore()
	work := seedAssignmentReleaseWork(t, mem)
	backing := &assignmentReleaseRaceStore{Store: mem}
	backing.release = func(_ context.Context, _ AssignmentReleaseRequest) (Bead, bool, error) {
		return cloneBead(work), true, nil
	}
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}

	released, won, err := cache.ReleaseAssignment(context.Background(), assignmentReleaseRequestFor(work))
	if err == nil || won || released.ID != "" {
		t.Fatalf("ReleaseAssignment = (%+v, %v, %v), want zero,false,readback error", released, won, err)
	}
	beforeGets := backing.getCalls
	got, err := cache.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if backing.getCalls == beforeGets {
		t.Fatal("Get was cache-served after non-authoritative assignment-release readback")
	}
	if got.Status != "in_progress" || got.Assignee != "worker-1" {
		t.Fatalf("Get = %+v, want authoritative backing assignment", got)
	}
}

func TestCachingStoreRejectsAssignmentReleaseReadbackWithMetadataLoss(t *testing.T) {
	mem := NewMemStore()
	work := seedAssignmentReleaseWork(t, mem)
	backing := &assignmentReleaseRaceStore{Store: mem}
	backing.release = func(_ context.Context, _ AssignmentReleaseRequest) (Bead, bool, error) {
		forged := cloneBead(work)
		forged.Status = "open"
		forged.Assignee = ""
		forged.Revision++
		forged.Metadata = map[string]string{"gc.session_id": ""}
		return forged, true, nil
	}
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}

	released, won, err := cache.ReleaseAssignment(context.Background(), assignmentReleaseRequestFor(work))
	if err == nil || won || released.ID != "" {
		t.Fatalf("ReleaseAssignment = (%+v, %v, %v), want zero,false,metadata readback error", released, won, err)
	}
	beforeGets := backing.getCalls
	if _, err := cache.Get(work.ID); err != nil {
		t.Fatal(err)
	}
	if backing.getCalls == beforeGets {
		t.Fatal("Get was cache-served after assignment-release readback dropped expected metadata")
	}
}

func TestCachingStoreDefiniteAssignmentReleaseNotFoundDoesNotMutateCache(t *testing.T) {
	mem := NewMemStore()
	backing := &assignmentReleaseRaceStore{Store: mem}
	backing.release = func(_ context.Context, _ AssignmentReleaseRequest) (Bead, bool, error) {
		return Bead{}, false, fmt.Errorf("exact release target missing: %w", ErrNotFound)
	}
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	cache.mu.RLock()
	beforeSeq := cache.mutationSeq
	beforeDirty := len(cache.dirty)
	cache.mu.RUnlock()

	revision := int64(1)
	released, won, err := cache.ReleaseAssignment(context.Background(), AssignmentReleaseRequest{
		ID:                   "missing-exact-id",
		ExpectedStatus:       "in_progress",
		ExpectedAssignee:     "worker-1",
		ExpectedRevision:     &revision,
		ExpectedMetadata:     map[string]string{"route": "pool"},
		ForbiddenLabels:      []string{"hold:external"},
		AbsentCoLocatedMatch: &CoLocatedMatchPredicate{ExpectedStatus: "open", ClassAnyOf: []CoLocatedClassPredicate{{ExpectedType: "coordination-row"}}, MatchValue: "worker-1", MatchID: true},
	})
	if won || released.ID != "" || !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReleaseAssignment = (%+v, %v, %v), want zero,false,ErrNotFound", released, won, err)
	}
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	if cache.mutationSeq != beforeSeq || len(cache.dirty) != beforeDirty {
		t.Fatalf("definite not-found changed cache state: mutationSeq %d -> %d, dirty %d -> %d",
			beforeSeq, cache.mutationSeq, beforeDirty, len(cache.dirty))
	}
}

func TestCachingStoreAssignmentReleaseDoesNotOverwriteNewerEvent(t *testing.T) {
	mem := NewMemStore()
	work := seedAssignmentReleaseWork(t, mem)
	backing := &assignmentReleaseRaceStore{Store: mem}
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	backing.release = func(ctx context.Context, req AssignmentReleaseRequest) (Bead, bool, error) {
		releaser, ok := AssignmentReleaserFor(mem)
		if !ok {
			t.Fatal("MemStore does not expose AssignmentReleaser")
		}
		released, won, err := releaser.ReleaseAssignment(ctx, req)
		if err != nil || !won {
			return released, won, err
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
		cache.ApplyEvent("bead.updated", payload)
		return released, true, nil
	}

	released, won, err := cache.ReleaseAssignment(context.Background(), assignmentReleaseRequestFor(work))
	if err != nil || !won || released.Status != "open" || released.Assignee != "" {
		t.Fatalf("ReleaseAssignment = (%+v, %v, %v), want committed release", released, won, err)
	}
	beforeGets := backing.getCalls
	got, err := cache.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if backing.getCalls == beforeGets {
		t.Fatal("Get was cache-served after concurrent event; release return must evict/dirty before trusting either snapshot")
	}
	if !slices.Contains(got.Labels, "hold:mayor") {
		t.Fatalf("cached row = %+v, newer hold event was overwritten by older release readback", got)
	}
}

var _ AssignmentReleaser = (*assignmentReleaseRaceStore)(nil)
