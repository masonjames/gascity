package beads

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/gastownhall/gascity/internal/fsys"
)

func TestReadyExcludeLabelsUsesAnyMatchAcrossLocalStores(t *testing.T) {
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
			if closer, ok := store.(interface{ CloseStore() error }); ok {
				t.Cleanup(func() { _ = closer.CloseStore() })
			}
			return store
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.open(t)
			want := seedReadyExcludeLabelCorpus(t, store)
			assertReadyExcludeLabelIDs(t, store.Ready, want)
		})
	}
}

func TestCachingReadyHandlesPreserveExcludeLabels(t *testing.T) {
	backing := NewMemStore()
	want := seedReadyExcludeLabelCorpus(t, backing)
	cache := NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}
	handles := HandlesFor(cache)
	t.Run("cached", func(t *testing.T) { assertReadyExcludeLabelIDs(t, handles.Cached.Ready, want) })
	t.Run("live", func(t *testing.T) { assertReadyExcludeLabelIDs(t, handles.Live.Ready, want) })
}

func seedReadyExcludeLabelCorpus(t *testing.T, store Store) []string {
	t.Helper()
	var want []string
	for _, fixture := range []Bead{
		{ID: "held-mayor", Title: "held mayor", Type: "task", Status: "open", Labels: []string{"hold:mayor"}},
		{ID: "held-external", Title: "held external", Type: "task", Status: "open", Labels: []string{"hold:external"}},
		{ID: "held-both", Title: "held both", Type: "task", Status: "open", Labels: []string{"hold:mayor", "hold:external"}},
		{ID: "unrelated", Title: "unrelated label", Type: "task", Status: "open", Labels: []string{"priority:high"}},
		{ID: "unheld", Title: "unheld", Type: "task", Status: "open"},
	} {
		created, err := store.Create(fixture)
		if err != nil {
			t.Fatalf("Create(%q): %v", fixture.ID, err)
		}
		if fixture.ID == "unrelated" || fixture.ID == "unheld" {
			want = append(want, created.ID)
		}
	}
	return want
}

func assertReadyExcludeLabelIDs(t *testing.T, ready func(...ReadyQuery) ([]Bead, error), want []string) {
	t.Helper()
	rows, err := ready(ReadyQuery{TierMode: TierBoth, ExcludeLabels: []string{"hold:mayor", "hold:external"}})
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	got := make([]string, len(rows))
	for i := range rows {
		got[i] = rows[i].ID
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Ready IDs = %v, want %v; either excluded label must suppress a row", got, want)
	}
}
