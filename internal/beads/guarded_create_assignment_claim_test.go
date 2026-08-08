package beads

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/gastownhall/gascity/internal/fsys"
	beadslib "github.com/steveyegge/beads"
)

func createAssignmentClaimRequest(workID string) CreateAssignmentClaimRequest {
	return CreateAssignmentClaimRequest{
		Claim: AssignmentClaimRequest{
			ID:               workID,
			Actor:            "worker-1",
			ExpectedStatus:   "open",
			ExpectedMetadata: map[string]string{"gc.routed_to": "worker"},
			ForbiddenLabels:  []string{"hold:mayor", "hold:external"},
			AssignmentMetadata: map[string]string{
				"gc.session_name":           "worker-session",
				"gc.session_instance_token": "instance-1",
			},
		},
		Witness: Bead{
			Title:  "fresh witness",
			Type:   "coordination-witness",
			Labels: []string{"coordination:witness"},
			Metadata: map[string]string{
				"instance": "instance-1",
			},
		},
		CreatedWitnessIDMetadataKeys: []string{"gc.session_id"},
	}
}

func TestCreateAssignmentClaimAtomicallyCreatesWitnessAndClaimsExactWork(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) Store
	}{
		{name: "mem", open: func(*testing.T) Store { return NewMemStore() }},
		{name: "file", open: func(t *testing.T) Store {
			store, err := OpenFileStore(fsys.OSFS{}, filepath.Join(t.TempDir(), "beads.json"))
			if err != nil {
				t.Fatalf("OpenFileStore: %v", err)
			}
			return store
		}},
		{name: "sqlite", open: func(t *testing.T) Store {
			store, err := OpenSQLiteStore(t.TempDir())
			if err != nil {
				t.Fatalf("OpenSQLiteStore: %v", err)
			}
			t.Cleanup(func() { _ = store.(*SQLiteStore).CloseStore() })
			return store
		}},
		{name: "native", open: func(*testing.T) Store {
			return newNativeDoltStoreForTest(newNativeDoltMemStorage())
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			work, err := store.Create(Bead{
				Title:    "routed work",
				Metadata: map[string]string{"gc.routed_to": "worker"},
			})
			if err != nil {
				t.Fatalf("Create work: %v", err)
			}
			claimer, ok := CreateAssignmentClaimerFor(store)
			if !ok {
				t.Fatalf("%T does not expose CreateAssignmentClaimer", store)
			}

			result, won, err := claimer.CreateAssignmentClaim(context.Background(), createAssignmentClaimRequest(work.ID))
			if err != nil || !won {
				t.Fatalf("CreateAssignmentClaim = (%+v, %v, %v), want success", result, won, err)
			}
			if result.Created.ID == "" || result.Created.Title != "fresh witness" || result.Created.Type != "coordination-witness" {
				t.Fatalf("created witness = %+v, want authoritative fresh witness", result.Created)
			}
			if result.Claimed.ID != work.ID || result.Claimed.Status != "in_progress" || result.Claimed.Assignee != "worker-1" {
				t.Fatalf("claimed work = %+v, want exact in_progress ownership", result.Claimed)
			}
			if got := result.Claimed.Metadata["gc.session_id"]; got != result.Created.ID {
				t.Fatalf("dynamic witness ID metadata = %q, want %q", got, result.Created.ID)
			}
			storedCreated, err := store.Get(result.Created.ID)
			if err != nil || storedCreated.ID != result.Created.ID {
				t.Fatalf("Get(created) = (%+v, %v)", storedCreated, err)
			}
			storedClaimed, err := store.Get(work.ID)
			if err != nil || storedClaimed.Revision != result.Claimed.Revision || storedClaimed.Metadata["gc.session_id"] != result.Created.ID {
				t.Fatalf("Get(claimed) = (%+v, %v), result=%+v", storedClaimed, err, result.Claimed)
			}
		})
	}
}

func TestCreateAssignmentClaimPredicateMismatchCreatesNothing(t *testing.T) {
	for _, hold := range []string{"hold:mayor", "hold:external"} {
		t.Run(hold, func(t *testing.T) {
			store := NewMemStore()
			work, err := store.Create(Bead{
				Title:    "held routed work",
				Labels:   []string{hold},
				Metadata: map[string]string{"gc.routed_to": "worker"},
			})
			if err != nil {
				t.Fatal(err)
			}
			before, err := store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			claimer, ok := CreateAssignmentClaimerFor(store)
			if !ok {
				t.Fatal("MemStore missing create-assignment capability")
			}
			result, won, err := claimer.CreateAssignmentClaim(context.Background(), createAssignmentClaimRequest(work.ID))
			if err != nil || won || result.Created.ID != "" || result.Claimed.ID != "" {
				t.Fatalf("CreateAssignmentClaim = (%+v, %v, %v), want zero,false,nil", result, won, err)
			}
			after, err := store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Revision != before.Revision || after.Status != before.Status || after.Assignee != before.Assignee {
				t.Fatalf("predicate mismatch mutated work: before=%+v after=%+v", before, after)
			}
			rows, err := store.List(ListQuery{AllowScan: true, IncludeClosed: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("rows after mismatch = %+v, want only original work", rows)
			}
		})
	}
}

func TestCachingStoreCreateAssignmentClaimRefreshesBothAuthoritativeRows(t *testing.T) {
	backing := NewMemStore()
	work, err := backing.Create(Bead{Title: "work", Metadata: map[string]string{"gc.routed_to": "worker"}})
	if err != nil {
		t.Fatal(err)
	}
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	claimer, ok := CreateAssignmentClaimerFor(cache)
	if !ok {
		t.Fatal("CachingStore hid backing create-assignment capability")
	}
	result, won, err := claimer.CreateAssignmentClaim(context.Background(), createAssignmentClaimRequest(work.ID))
	if err != nil || !won {
		t.Fatalf("CreateAssignmentClaim = (%+v, %v, %v)", result, won, err)
	}
	if got, err := cache.Get(result.Created.ID); err != nil || got.ID != result.Created.ID {
		t.Fatalf("cached created = (%+v, %v)", got, err)
	}
	if got, err := cache.Get(work.ID); err != nil || got.Status != "in_progress" || got.Metadata["gc.session_id"] != result.Created.ID {
		t.Fatalf("cached claimed = (%+v, %v)", got, err)
	}
}

func TestCreateAssignmentClaimerForDoesNotInventCapability(t *testing.T) {
	type storeOnly struct{ Store }
	wrapped := storeOnly{Store: NewMemStore()}
	if claimer, ok := CreateAssignmentClaimerFor(wrapped); ok || claimer != nil {
		t.Fatalf("CreateAssignmentClaimerFor(store-only wrapper) = (%T, %v), want nil,false", claimer, ok)
	}
}

func TestCreateAssignmentClaimRejectsDynamicKeyExpectedMetadataCollisionWithoutMutation(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) Store
	}{
		{name: "mem", open: func(*testing.T) Store { return NewMemStore() }},
		{name: "file", open: func(t *testing.T) Store {
			store, err := OpenFileStore(fsys.OSFS{}, filepath.Join(t.TempDir(), "beads.json"))
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
		{name: "sqlite", open: func(t *testing.T) Store {
			store, err := OpenSQLiteStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.(*SQLiteStore).CloseStore() })
			return store
		}},
		{name: "native", open: func(*testing.T) Store {
			return newNativeDoltStoreForTest(newNativeDoltMemStorage())
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			work, err := store.Create(Bead{Title: "work", Metadata: map[string]string{"gc.routed_to": "worker"}})
			if err != nil {
				t.Fatal(err)
			}
			before, err := store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			req := createAssignmentClaimRequest(work.ID)
			req.CreatedWitnessIDMetadataKeys = []string{"gc.routed_to"}
			claimer, ok := CreateAssignmentClaimerFor(store)
			if !ok {
				t.Fatalf("%T missing capability", store)
			}
			result, won, err := claimer.CreateAssignmentClaim(context.Background(), req)
			if err == nil || won || result.Created.ID != "" || result.Claimed.ID != "" {
				t.Fatalf("CreateAssignmentClaim = (%+v, %v, %v), want validation error", result, won, err)
			}
			after, err := store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Revision != before.Revision || after.Status != before.Status || after.Assignee != before.Assignee || after.Metadata["gc.routed_to"] != "worker" {
				t.Fatalf("invalid dynamic key mutated work: before=%+v after=%+v", before, after)
			}
			rows, err := store.List(ListQuery{AllowScan: true, IncludeClosed: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("rows after rejected request = %+v, want only work", rows)
			}
		})
	}
}

func TestCreateAssignmentClaimDoesNotLaunderPreexistingExactPoststate(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) Store
	}{
		{name: "mem", open: func(*testing.T) Store { return NewMemStore() }},
		{name: "file", open: func(t *testing.T) Store {
			store, err := OpenFileStore(fsys.OSFS{}, filepath.Join(t.TempDir(), "beads.json"))
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
		{name: "sqlite", open: func(t *testing.T) Store {
			store, err := OpenSQLiteStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.(*SQLiteStore).CloseStore() })
			return store
		}},
		{name: "native", open: func(*testing.T) Store {
			return newNativeDoltStoreForTest(newNativeDoltMemStorage())
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			work, err := store.Create(Bead{Title: "work", Metadata: map[string]string{"gc.routed_to": "worker"}})
			if err != nil {
				t.Fatal(err)
			}
			req := createAssignmentClaimRequest(work.ID)
			witness, err := store.Create(req.Witness)
			if err != nil {
				t.Fatal(err)
			}
			effective := createAssignmentClaimEffectiveRequest(req, witness.ID)
			if err := store.Update(work.ID, assignmentClaimUpdate(effective)); err != nil {
				t.Fatal(err)
			}
			beforeWork, err := store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			beforeWitness, err := store.Get(witness.ID)
			if err != nil {
				t.Fatal(err)
			}

			claimer, ok := CreateAssignmentClaimerFor(store)
			if !ok {
				t.Fatalf("%T missing capability", store)
			}
			result, won, err := claimer.CreateAssignmentClaim(t.Context(), req)
			if err != nil || won || result.Created.ID != "" || result.Claimed.ID != "" {
				t.Fatalf("CreateAssignmentClaim over unrelated preexisting poststate = (%+v, %v, %v), want zero,false,nil", result, won, err)
			}
			afterWork, err := store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			afterWitness, err := store.Get(witness.ID)
			if err != nil {
				t.Fatal(err)
			}
			if afterWork.Revision != beforeWork.Revision || afterWitness.Revision != beforeWitness.Revision {
				t.Fatalf("preexisting loser mutated rows: work before=%+v after=%+v witness before=%+v after=%+v", beforeWork, afterWork, beforeWitness, afterWitness)
			}
		})
	}
}

type retryingCreateAssignmentNativeStorage struct {
	*nativeDoltMemStorage
	workID    string
	callbacks int
}

func (s *retryingCreateAssignmentNativeStorage) RunInTransaction(_ context.Context, _ string, fn func(beadslib.Transaction) error) error {
	s.store.mu.Lock()
	seq, rows, deps := s.store.snapshot()
	s.store.mu.Unlock()
	s.callbacks++
	if err := fn(nativeDoltTransactionForTest{storage: s}); err != nil {
		return err
	}
	// Model an internal provider retry: discard the apparently successful first
	// callback, change the exact predicate, and invoke the callback again. Only
	// the final zero-result callback may escape.
	s.store.restoreFrom(seq, rows, deps)
	if err := s.store.Update(s.workID, UpdateOpts{Labels: []string{"hold:external"}}); err != nil {
		return err
	}
	s.callbacks++
	return fn(nativeDoltTransactionForTest{storage: s})
}

func TestNativeCreateAssignmentClaimInternalCallbackRetryReturnsOnlyFinalAttempt(t *testing.T) {
	storage := &retryingCreateAssignmentNativeStorage{nativeDoltMemStorage: newNativeDoltMemStorage()}
	store := newNativeDoltStoreForTest(storage)
	work, err := store.Create(Bead{Title: "work", Metadata: map[string]string{"gc.routed_to": "worker"}})
	if err != nil {
		t.Fatal(err)
	}
	storage.workID = work.ID

	result, won, err := store.CreateAssignmentClaim(t.Context(), createAssignmentClaimRequest(work.ID))
	if err != nil || won || result.Created.ID != "" || result.Claimed.ID != "" {
		t.Fatalf("CreateAssignmentClaim = (%+v, %v, %v), want final callback mismatch", result, won, err)
	}
	if storage.callbacks != 2 {
		t.Fatalf("callbacks = %d, want 2", storage.callbacks)
	}
	current, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != "open" || current.Assignee != "" || !slices.Contains(current.Labels, "hold:external") {
		t.Fatalf("work after retried callback = %+v, want held and unclaimed", current)
	}
	rows, err := store.List(ListQuery{AllowScan: true, IncludeClosed: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows after discarded callback = %+v, want no leaked witness", rows)
	}
}

type persistedRetryCreateAssignmentNativeStorage struct {
	*nativeDoltMemStorage
	callbacks int
}

func (s *persistedRetryCreateAssignmentNativeStorage) RunInTransaction(_ context.Context, _ string, fn func(beadslib.Transaction) error) error {
	s.callbacks++
	if err := fn(nativeDoltTransactionForTest{storage: s}); err != nil {
		return err
	}
	// Model the pinned commit-before-version-stage ordering: the first
	// callback's SQL rows remain visible when the provider retries the callback.
	s.callbacks++
	return fn(nativeDoltTransactionForTest{storage: s})
}

func TestNativeCreateAssignmentClaimCommitThenCallbackRetryReusesMintedWitness(t *testing.T) {
	storage := &persistedRetryCreateAssignmentNativeStorage{nativeDoltMemStorage: newNativeDoltMemStorage()}
	store := newNativeDoltStoreForTest(storage)
	work, err := store.Create(Bead{Title: "work", Metadata: map[string]string{"gc.routed_to": "worker"}})
	if err != nil {
		t.Fatal(err)
	}

	result, won, err := store.CreateAssignmentClaim(t.Context(), createAssignmentClaimRequest(work.ID))
	if err != nil || !won {
		t.Fatalf("CreateAssignmentClaim = (%+v, %v, %v), want idempotent retry success", result, won, err)
	}
	if storage.callbacks != 2 {
		t.Fatalf("callbacks = %d, want 2", storage.callbacks)
	}
	rows, err := store.List(ListQuery{AllowScan: true, IncludeClosed: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows after commit-then-retry = %+v, want one work plus one witness", rows)
	}
	if result.Created.ID == "" || result.Claimed.Metadata["gc.session_id"] != result.Created.ID {
		t.Fatalf("retried result = %+v, want original minted witness relationship", result)
	}
}

type postCommitErrorCreateAssignmentNativeStorage struct {
	*nativeDoltMemStorage
	err error
}

func (s *postCommitErrorCreateAssignmentNativeStorage) RunInTransaction(_ context.Context, _ string, fn func(beadslib.Transaction) error) error {
	if err := runNativeDoltMemStorageTransactionForTest(s.nativeDoltMemStorage, func() error {
		return fn(nativeDoltTransactionForTest{storage: s})
	}); err != nil {
		return err
	}
	return s.err
}

func TestNativeCreateAssignmentClaimPostCommitErrorReturnsAuthoritativeCommittedRows(t *testing.T) {
	stageErr := errors.New("injected post-commit version staging failure")
	storage := &postCommitErrorCreateAssignmentNativeStorage{
		nativeDoltMemStorage: newNativeDoltMemStorage(),
		err:                  stageErr,
	}
	store := newNativeDoltStoreForTest(storage)
	work, err := store.Create(Bead{Title: "work", Metadata: map[string]string{"gc.routed_to": "worker"}})
	if err != nil {
		t.Fatal(err)
	}

	result, won, err := store.CreateAssignmentClaim(t.Context(), createAssignmentClaimRequest(work.ID))
	if err != nil || !won {
		t.Fatalf("CreateAssignmentClaim = (%+v, %v, %v), want recovered committed success", result, won, err)
	}
	if result.Created.ID == "" || result.Claimed.ID != work.ID || result.Claimed.Metadata["gc.session_id"] != result.Created.ID {
		t.Fatalf("recovered result = %+v, want exact created+claimed relationship", result)
	}
	storedCreated, err := store.Get(result.Created.ID)
	if err != nil {
		t.Fatal(err)
	}
	storedClaimed, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedCreated.Revision != result.Created.Revision || storedClaimed.Revision != result.Claimed.Revision {
		t.Fatalf("authoritative recovery mismatch: result=%+v stored=(%+v,%+v)", result, storedCreated, storedClaimed)
	}
}
