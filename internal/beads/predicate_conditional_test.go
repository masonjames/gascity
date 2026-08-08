package beads

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gastownhall/gascity/internal/fsys"
	beadslib "github.com/steveyegge/beads"
)

func TestNativeDoltStorePredicateConditionalWriterHydratesAndReturnsPoststate(t *testing.T) {
	store := newNativeDoltStoreForTest(newNativeDoltMemStorage())
	created, err := store.Create(Bead{
		Title:    "before",
		Type:     "task",
		Labels:   []string{"identity:one"},
		Metadata: map[string]string{"witness": "one"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	writer, ok := PredicateConditionalWriterFor(store)
	if !ok {
		t.Fatal("NativeDoltStore does not expose PredicateConditionalWriter")
	}
	title := "after"
	post, err := writer.UpdateIfPredicate(created.ID, func(current Bead) error {
		if current.Title != "before" || current.Status != "open" || current.Type != "task" {
			t.Fatalf("predicate current fields = %+v", current)
		}
		if !reflect.DeepEqual(current.Labels, []string{"identity:one"}) {
			t.Fatalf("predicate labels = %#v", current.Labels)
		}
		if !reflect.DeepEqual(map[string]string(current.Metadata), map[string]string{"witness": "one"}) {
			t.Fatalf("predicate metadata = %#v", current.Metadata)
		}
		return nil
	}, UpdateOpts{
		Title:    &title,
		Labels:   []string{"identity:two"},
		Metadata: map[string]string{"committed": "true"},
	})
	if err != nil {
		t.Fatalf("UpdateIfPredicate: %v", err)
	}
	if post.Title != title || !predicateTestHasLabel(post.Labels, "identity:two") || post.Metadata["committed"] != "true" {
		t.Fatalf("poststate = %+v", post)
	}
	stored, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !reflect.DeepEqual(post, stored) {
		t.Fatalf("returned poststate = %+v, stored = %+v", post, stored)
	}
}

func TestNativeDoltStorePredicateConditionalWriterRejectsCrossHandleDrift(t *testing.T) {
	tests := []struct {
		name  string
		seed  func(*testing.T, *NativeDoltStore, string) Bead
		drift func(*testing.T, *NativeDoltStore, string)
	}{
		{
			name: "label",
			drift: func(t *testing.T, store *NativeDoltStore, id string) {
				t.Helper()
				if err := store.Update(id, UpdateOpts{Labels: []string{"foreign"}}); err != nil {
					t.Fatalf("label drift: %v", err)
				}
			},
		},
		{
			name: "metadata",
			drift: func(t *testing.T, store *NativeDoltStore, id string) {
				t.Helper()
				if err := store.SetMetadata(id, "foreign", "true"); err != nil {
					t.Fatalf("metadata drift: %v", err)
				}
			},
		},
		{
			name: "status",
			drift: func(t *testing.T, store *NativeDoltStore, id string) {
				t.Helper()
				status := "in_progress"
				if err := store.Update(id, UpdateOpts{Status: &status}); err != nil {
					t.Fatalf("status drift: %v", err)
				}
			},
		},
		{
			name: "reopen",
			seed: func(t *testing.T, store *NativeDoltStore, id string) Bead {
				t.Helper()
				if err := store.Close(id); err != nil {
					t.Fatalf("Close: %v", err)
				}
				captured, err := store.Get(id)
				if err != nil {
					t.Fatalf("Get closed: %v", err)
				}
				return captured
			},
			drift: func(t *testing.T, store *NativeDoltStore, id string) {
				t.Helper()
				if err := store.Reopen(id); err != nil {
					t.Fatalf("Reopen: %v", err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			storage := &serializedNativeDoltMemStorage{nativeDoltMemStorage: newNativeDoltMemStorage()}
			first := newNativeDoltStoreForTest(storage)
			second := newNativeDoltStoreForTest(storage)
			created, err := first.Create(Bead{Title: "captured", Metadata: map[string]string{"lease": "ours"}})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			captured, err := first.Get(created.ID)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if tc.seed != nil {
				captured = tc.seed(t, first, created.ID)
			}
			tc.drift(t, second, created.ID)

			sentinel := errors.New("captured row drifted")
			writer, ok := PredicateConditionalWriterFor(first)
			if !ok {
				t.Fatal("missing predicate writer")
			}
			marker := "ours"
			_, err = writer.UpdateIfPredicate(created.ID, func(current Bead) error {
				if !reflect.DeepEqual(captured, current) {
					return sentinel
				}
				return nil
			}, UpdateOpts{Metadata: map[string]string{"marker": marker}})
			if !errors.Is(err, sentinel) {
				t.Fatalf("UpdateIfPredicate error = %v, want drift sentinel", err)
			}
			stored, getErr := first.Get(created.ID)
			if getErr != nil {
				t.Fatalf("Get after rejection: %v", getErr)
			}
			if stored.Metadata["marker"] != "" {
				t.Fatalf("rejected predicate wrote marker: %#v", stored.Metadata)
			}
		})
	}
}

func TestNativeDoltStorePredicateCallbackRetryResetsEscapingPoststate(t *testing.T) {
	var invocation atomic.Int32
	first := &beadslib.Issue{ID: "s-1", Title: "first", Status: beadslib.StatusOpen, IssueType: beadslib.TypeTask}
	second := &beadslib.Issue{ID: "s-1", Title: "second", Status: beadslib.StatusOpen, IssueType: beadslib.TypeTask}
	storage := &nativeDoltStorageSpy{}
	storage.getIssue = func(context.Context, string) (*beadslib.Issue, error) {
		if invocation.Load() == 1 {
			cloned := *first
			return &cloned, nil
		}
		cloned := *second
		return &cloned, nil
	}
	storage.runInTransaction = func(_ context.Context, _ string, fn func(beadslib.Transaction) error) error {
		invocation.Store(1)
		if err := fn(nativeDoltTransactionForTest{storage: storage}); err != nil {
			return err
		}
		invocation.Store(2)
		return fn(nativeDoltTransactionForTest{storage: storage})
	}
	store := newNativeDoltStoreForTest(storage)
	writer, ok := PredicateConditionalWriterFor(store)
	if !ok {
		t.Fatal("missing predicate writer")
	}
	sentinel := errors.New("retry snapshot rejected")
	post, err := writer.UpdateIfPredicate("s-1", func(current Bead) error {
		if current.Title == "second" {
			return sentinel
		}
		return nil
	}, UpdateOpts{Metadata: map[string]string{"marker": "true"}})
	if !errors.Is(err, sentinel) {
		t.Fatalf("UpdateIfPredicate error = %v, want retry sentinel", err)
	}
	if !reflect.DeepEqual(post, Bead{}) {
		t.Fatalf("abandoned callback poststate escaped: %+v", post)
	}
}

func TestNativeDoltStorePredicateConditionalWriterReconcilesCommitThenError(t *testing.T) {
	tests := []struct {
		name  string
		write func(PredicateConditionalWriter, string) (Bead, error)
		check func(*testing.T, Bead)
	}{
		{
			name: "update",
			write: func(writer PredicateConditionalWriter, id string) (Bead, error) {
				return writer.UpdateIfPredicate(id, func(Bead) error { return nil }, UpdateOpts{
					Metadata: map[string]string{"committed": "update"},
				})
			},
			check: func(t *testing.T, post Bead) {
				t.Helper()
				if post.Metadata["committed"] != "update" {
					t.Fatalf("update poststate = %+v", post)
				}
			},
		},
		{
			name: "close",
			write: func(writer PredicateConditionalWriter, id string) (Bead, error) {
				return writer.CloseIfPredicate(id, func(Bead) error { return nil }, UpdateOpts{
					Metadata: map[string]string{"committed": "close"},
				})
			},
			check: func(t *testing.T, post Bead) {
				t.Helper()
				if post.Status != "closed" || post.Metadata["committed"] != "close" {
					t.Fatalf("close poststate = %+v", post)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			storage := &nativePredicateCommitThenErrorStorage{
				nativeDoltMemStorage: newNativeDoltMemStorage(),
				err:                  errors.New("injected error after committed predicate transaction"),
			}
			store := newNativeDoltStoreForTest(storage)
			created, err := store.Create(Bead{Title: tc.name})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			writer, ok := PredicateConditionalWriterFor(store)
			if !ok {
				t.Fatal("missing predicate writer")
			}
			storage.armed = true
			post, err := tc.write(writer, created.ID)
			if err != nil {
				t.Fatalf("predicate write returned committed error: %v", err)
			}
			tc.check(t, post)
			stored, getErr := store.Get(created.ID)
			if getErr != nil {
				t.Fatalf("Get: %v", getErr)
			}
			if !reflect.DeepEqual(post, stored) {
				t.Fatalf("reconciled poststate = %+v, stored = %+v", post, stored)
			}
		})
	}
}

func TestNativeDoltStorePredicateConditionalWriterDoesNotLaunderLaterPoststate(t *testing.T) {
	sentinel := errors.New("injected error after committed predicate transaction")
	storage := &nativePredicateCommitThenErrorStorage{
		nativeDoltMemStorage: newNativeDoltMemStorage(),
		err:                  sentinel,
	}
	store := newNativeDoltStoreForTest(storage)
	created, err := store.Create(Bead{Title: "before"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	storage.afterCommit = func() {
		if err := storage.store.Update(created.ID, UpdateOpts{Metadata: map[string]string{"foreign": "later"}}); err != nil {
			t.Fatalf("foreign update: %v", err)
		}
	}
	storage.armed = true
	writer, ok := PredicateConditionalWriterFor(store)
	if !ok {
		t.Fatal("missing predicate writer")
	}
	post, err := writer.UpdateIfPredicate(created.ID, func(Bead) error { return nil }, UpdateOpts{
		Metadata: map[string]string{"ours": "committed"},
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("UpdateIfPredicate error = %v, want original ambiguous error", err)
	}
	if !reflect.DeepEqual(post, Bead{}) {
		t.Fatalf("unattributed later poststate escaped: %+v", post)
	}
	stored, getErr := store.Get(created.ID)
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if stored.Metadata["ours"] != "committed" || stored.Metadata["foreign"] != "later" {
		t.Fatalf("stored poststate = %+v", stored)
	}
}

func TestCachingStoreForwardsPredicateConditionalWriterAndEvicts(t *testing.T) {
	backing := newNativeDoltStoreForTest(newNativeDoltMemStorage())
	cache := NewCachingStoreForTest(backing, nil)
	created, err := cache.Create(Bead{Title: "before"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := cache.Get(created.ID); err != nil {
		t.Fatalf("prime Get: %v", err)
	}
	writer, ok := PredicateConditionalWriterFor(SessionStore{Store: cache})
	if !ok {
		t.Fatal("typed cache did not forward predicate writer capability")
	}
	title := "conditional"
	post, err := writer.UpdateIfPredicate(created.ID, func(current Bead) error {
		if current.Title != "before" {
			return errors.New("unexpected current title")
		}
		return nil
	}, UpdateOpts{Title: &title})
	if err != nil {
		t.Fatalf("UpdateIfPredicate: %v", err)
	}
	if post.Title != title {
		t.Fatalf("post title = %q", post.Title)
	}
	foreign := "foreign"
	if err := backing.Update(created.ID, UpdateOpts{Title: &foreign}); err != nil {
		t.Fatalf("foreign Update: %v", err)
	}
	got, err := cache.Get(created.ID)
	if err != nil {
		t.Fatalf("Get after foreign update: %v", err)
	}
	if got.Title != foreign {
		t.Fatalf("cache served stale conditional poststate title %q, want %q", got.Title, foreign)
	}
}

func TestPredicateConditionalWriterCapabilityDoesNotWidenRevisionCAS(t *testing.T) {
	native := newNativeDoltStoreForTest(newNativeDoltMemStorage())
	if _, ok := ConditionalWriterFor(native); ok {
		t.Fatal("NativeDoltStore falsely exposes ConditionalWriter")
	}
	if _, ok := PredicateConditionalWriterFor(native); !ok {
		t.Fatal("NativeDoltStore missing narrow predicate capability")
	}
	mem := NewMemStore()
	if _, ok := ConditionalWriterFor(mem); !ok {
		t.Fatal("MemStore lost its existing revision-CAS capability")
	}
	if _, ok := PredicateConditionalWriterFor(mem); !ok {
		t.Fatal("MemStore missing predicate capability")
	}
	if _, ok := PredicateConditionalWriterFor(SessionStore{Store: mem}); !ok {
		t.Fatal("typed class wrapper did not forward predicate capability")
	}
	if _, ok := PredicateConditionalWriterFor(predicateResolveTargetStore{Store: NewMemStore(), target: mem}); !ok {
		t.Fatal("declared policy target did not forward predicate capability")
	}
	if _, ok := PredicateConditionalWriterFor(&BdStore{}); ok {
		t.Fatal("BdStore must not advertise a transactional predicate capability")
	}
}

func TestPredicateConditionalWriterNativeStoreAdapters(t *testing.T) {
	tests := []struct {
		name     string
		newStore func(*testing.T) Store
	}{
		{
			name: "mem",
			newStore: func(*testing.T) Store {
				return NewMemStore()
			},
		},
		{
			name: "file",
			newStore: func(t *testing.T) Store {
				store, err := OpenFileStore(fsys.OSFS{}, filepath.Join(t.TempDir(), "beads.json"))
				if err != nil {
					t.Fatalf("OpenFileStore: %v", err)
				}
				return store
			},
		},
		{
			name: "sqlite",
			newStore: func(t *testing.T) Store {
				opened, err := OpenSQLiteStore(t.TempDir())
				if err != nil {
					t.Fatalf("OpenSQLiteStore: %v", err)
				}
				store := opened.(*SQLiteStore)
				t.Cleanup(func() { _ = store.CloseStore() })
				return store
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.newStore(t)
			created, err := store.Create(Bead{
				Title:    "before",
				Labels:   []string{"captured"},
				Metadata: map[string]string{"witness": "one"},
			})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			created, err = store.Get(created.ID)
			if err != nil {
				t.Fatalf("Get created: %v", err)
			}
			writer, ok := PredicateConditionalWriterFor(store)
			if !ok {
				t.Fatal("missing predicate writer")
			}
			post, err := writer.UpdateIfPredicate(created.ID, func(current Bead) error {
				if !PredicateSnapshotEqual(current, created) {
					return errors.New("unexpected current snapshot")
				}
				return nil
			}, UpdateOpts{Metadata: map[string]string{"phase": "updated"}})
			if err != nil {
				t.Fatalf("UpdateIfPredicate: %v", err)
			}
			stored, err := store.Get(created.ID)
			if err != nil {
				t.Fatalf("Get updated: %v", err)
			}
			if !PredicateSnapshotEqual(post, stored) {
				t.Fatalf("update poststate = %+v, stored = %+v", post, stored)
			}

			sentinel := errors.New("reject close")
			if _, err := writer.CloseIfPredicate(created.ID, func(Bead) error { return sentinel }, UpdateOpts{
				Metadata: map[string]string{"must_not_write": "true"},
			}); !errors.Is(err, sentinel) {
				t.Fatalf("rejected CloseIfPredicate error = %v", err)
			}
			unchanged, err := store.Get(created.ID)
			if err != nil {
				t.Fatalf("Get rejected close: %v", err)
			}
			if !PredicateSnapshotEqual(post, unchanged) {
				t.Fatalf("rejected close mutated row: before=%+v after=%+v", post, unchanged)
			}

			closed, err := writer.CloseIfPredicate(created.ID, func(current Bead) error {
				if !PredicateSnapshotEqual(current, post) {
					return errors.New("updated snapshot drifted")
				}
				return nil
			}, UpdateOpts{Metadata: map[string]string{"phase": "closed"}})
			if err != nil {
				t.Fatalf("CloseIfPredicate: %v", err)
			}
			stored, err = store.Get(created.ID)
			if err != nil {
				t.Fatalf("Get closed: %v", err)
			}
			if closed.Status != "closed" || closed.Metadata["phase"] != "closed" || !PredicateSnapshotEqual(closed, stored) {
				t.Fatalf("close poststate = %+v, stored = %+v", closed, stored)
			}
		})
	}
}

type serializedNativeDoltMemStorage struct {
	*nativeDoltMemStorage
	txMu sync.Mutex
}

type nativePredicateCommitThenErrorStorage struct {
	*nativeDoltMemStorage
	armed       bool
	err         error
	afterCommit func()
}

type predicateResolveTargetStore struct {
	Store
	target Store
}

func (s predicateResolveTargetStore) ConditionalWritesResolveTarget() Store { return s.target }

func (s *nativePredicateCommitThenErrorStorage) RunInTransaction(ctx context.Context, message string, fn func(beadslib.Transaction) error) error {
	err := s.nativeDoltMemStorage.RunInTransaction(ctx, message, fn)
	if err != nil || !s.armed {
		return err
	}
	s.armed = false
	if s.afterCommit != nil {
		s.afterCommit()
	}
	return s.err
}

func (s *serializedNativeDoltMemStorage) RunInTransaction(ctx context.Context, message string, fn func(beadslib.Transaction) error) error {
	s.txMu.Lock()
	defer s.txMu.Unlock()
	return s.nativeDoltMemStorage.RunInTransaction(ctx, message, fn)
}

func predicateTestHasLabel(labels []string, expected string) bool {
	for _, label := range labels {
		if label == expected {
			return true
		}
	}
	return false
}
