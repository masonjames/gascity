package main

import (
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
)

func TestDefaultScaleCheckDemandDeduplicatesTransparentStoreWrappers(t *testing.T) {
	const (
		template = "fixture/worker"
		sharedID = "shared-work-id"
	)

	newBacking := func() *beads.MemStore {
		return beads.NewMemStoreFrom(1, []beads.Bead{{
			ID:     sharedID,
			Title:  "routed work",
			Type:   "task",
			Status: "open",
			Metadata: map[string]string{
				beadmeta.RoutedToMetadataKey: template,
			},
		}}, nil)
	}
	wrap := func(store beads.Store) beads.Store {
		return wrapStoreWithBeadPolicies(
			beads.NewCachingStoreForTest(store, nil),
			&config.City{},
		)
	}
	run := func(t *testing.T, first, second beads.Store, want int) {
		t.Helper()
		counts, demand, partial, errs := defaultScaleCheckCountsAndDemand(nil, []defaultScaleCheckTarget{
			{template: template, storeKey: "city:fixture", store: first},
			{template: template, storeKey: "rig:fixture", store: second},
		}, newReadyDemandCache())
		if len(errs) != 0 || len(partial) != 0 {
			t.Fatalf("defaultScaleCheckCountsAndDemand errs=%v partial=%v, want complete", errs, partial)
		}
		if got := counts[template]; got != want {
			t.Fatalf("count = %d, want %d", got, want)
		}
		if got := len(demand[template].Witnesses); got != want {
			t.Fatalf("witness count = %d, want %d", got, want)
		}
	}

	t.Run("same backing counts once", func(t *testing.T) {
		backing := newBacking()
		run(t, wrap(backing), wrap(backing), 1)
	})

	t.Run("independent backings count twice", func(t *testing.T) {
		run(t, wrap(newBacking()), wrap(newBacking()), 2)
	})
}
