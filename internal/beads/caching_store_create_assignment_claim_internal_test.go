package beads

import (
	"context"
	"encoding/json"
	"testing"
)

type createAssignmentClaimRaceStore struct {
	Store
	claim    func(context.Context, CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error)
	getCalls int
}

func (s *createAssignmentClaimRaceStore) Get(id string) (Bead, error) {
	s.getCalls++
	return s.Store.Get(id)
}

func (s *createAssignmentClaimRaceStore) CreateAssignmentClaimerHandle() (CreateAssignmentClaimer, bool) {
	return s, true
}

func (s *createAssignmentClaimRaceStore) CreateAssignmentClaim(ctx context.Context, req CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error) {
	if s.claim != nil {
		return s.claim(ctx, req)
	}
	claimer, ok := CreateAssignmentClaimerFor(s.Store)
	if !ok {
		return CreateAssignmentClaimResult{}, false, ErrCreateAssignmentClaimUnsupported
	}
	return claimer.CreateAssignmentClaim(ctx, req)
}

func TestCachingStoreCreateAssignmentClaimDoesNotOverwriteConcurrentEventsForEitherRow(t *testing.T) {
	mem := NewMemStore()
	work, err := mem.Create(Bead{Title: "work", Metadata: map[string]string{"gc.routed_to": "worker"}})
	if err != nil {
		t.Fatal(err)
	}
	backing := &createAssignmentClaimRaceStore{Store: mem}
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	backing.claim = func(ctx context.Context, req CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error) {
		claimer, ok := CreateAssignmentClaimerFor(mem)
		if !ok {
			t.Fatal("MemStore missing CreateAssignmentClaimer")
		}
		result, won, err := claimer.CreateAssignmentClaim(ctx, req)
		if err != nil || !won {
			return result, won, err
		}
		workEvent := cloneBead(result.Claimed)
		workEvent.Title = "concurrent work event"
		workPayload, err := json.Marshal(workEvent)
		if err != nil {
			t.Fatal(err)
		}
		createdEvent := cloneBead(result.Created)
		createdEvent.Title = "concurrent created event"
		createdPayload, err := json.Marshal(createdEvent)
		if err != nil {
			t.Fatal(err)
		}
		cache.ApplyEvent("bead.updated", workPayload)
		cache.ApplyEvent("bead.updated", createdPayload)
		return result, true, nil
	}

	result, won, err := cache.CreateAssignmentClaim(context.Background(), createAssignmentClaimRequest(work.ID))
	if err != nil || !won {
		t.Fatalf("CreateAssignmentClaim = (%+v, %v, %v)", result, won, err)
	}
	beforeGets := backing.getCalls
	if _, err := cache.Get(work.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Get(result.Created.ID); err != nil {
		t.Fatal(err)
	}
	if got := backing.getCalls - beforeGets; got != 2 {
		t.Fatalf("post-race cache Get backing calls = %d, want 2 (both rows evicted/dirty after seq drift)", got)
	}
}

var (
	_ Store                                 = (*createAssignmentClaimRaceStore)(nil)
	_ CreateAssignmentClaimer               = (*createAssignmentClaimRaceStore)(nil)
	_ CreateAssignmentClaimerHandleProvider = (*createAssignmentClaimRaceStore)(nil)
)
