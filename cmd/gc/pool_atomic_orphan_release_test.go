package main

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

type injectBeforeAssignmentReleaseStore struct {
	beads.Store
	once         sync.Once
	inject       func()
	releaseCalls int
}

func (s *injectBeforeAssignmentReleaseStore) ReleaseAssignment(ctx context.Context, req beads.AssignmentReleaseRequest) (beads.Bead, bool, error) {
	s.releaseCalls++
	s.once.Do(s.inject)
	releaser, ok := beads.AssignmentReleaserFor(s.Store)
	if !ok {
		return beads.Bead{}, false, beads.ErrAssignmentReleaseUnsupported
	}
	return releaser.ReleaseAssignment(ctx, req)
}

func strictOrphanReleaseConfig() *config.City {
	return &config.City{Agents: []config.Agent{{
		Name:              "worker",
		ProjectHooks:      config.ProjectHooksForbid,
		MinActiveSessions: intPtr(0),
		MaxActiveSessions: intPtr(2),
	}}}
}

func seedStrictOrphanWork(t *testing.T, store beads.Store) beads.Bead {
	t.Helper()
	work, err := store.Create(beads.Bead{
		Title:    "strict orphan",
		Assignee: "worker-dead",
		Metadata: map[string]string{
			beadmeta.RoutedToMetadataKey:          "worker",
			beadmeta.SessionAffinityMetadataKey:   "worker-dead",
			beadmeta.ContinuationGroupMetadataKey: "group-old",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	status := "in_progress"
	if err := store.Update(work.ID, beads.UpdateOpts{Status: &status}); err != nil {
		t.Fatal(err)
	}
	work, err = store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	return work
}

func runStrictOrphanRelease(store beads.Store, work beads.Bead) []releasedPoolAssignment {
	return releaseOrphanedPoolAssignments(
		store,
		strictOrphanReleaseConfig(),
		"",
		nil,
		[]beads.Bead{work},
		[]beads.Store{store},
		nil,
		nil,
	)
}

func TestStrictOrphanReleaseIsAtomicWithLateSessionOwner(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rowType string
		labels  []string
	}{
		{name: "type and label", rowType: session.BeadType, labels: []string{session.LabelSession}},
		{name: "type only repairable row", rowType: session.BeadType},
		{name: "label only repairable row", labels: []string{session.LabelSession}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := beads.NewMemStore()
			store := &injectBeforeAssignmentReleaseStore{Store: base}
			work := seedStrictOrphanWork(t, store)
			var lateOwner beads.Bead
			store.inject = func() {
				var err error
				lateOwner, err = base.Create(beads.Bead{
					Title:  "late same-store session owner",
					Type:   tc.rowType,
					Labels: tc.labels,
					Metadata: map[string]string{
						"session_name": work.Assignee,
					},
				})
				if err != nil {
					t.Fatalf("creating late session owner: %v", err)
				}
			}

			before := work
			if released := runStrictOrphanRelease(store, work); len(released) != 0 {
				t.Fatalf("released = %+v, want none after late owner creation", released)
			}
			if store.releaseCalls != 1 {
				t.Fatalf("atomic release calls = %d, want 1", store.releaseCalls)
			}
			after, err := base.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("late session owner race mutated work:\nbefore=%+v\nafter=%+v", before, after)
			}
			afterOwner, err := base.Get(lateOwner.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(lateOwner, afterOwner) {
				t.Fatalf("late session owner race mutated witness:\nbefore=%+v\nafter=%+v", lateOwner, afterOwner)
			}
		})
	}
}

func TestStrictOrphanReleaseIsAtomicWithLastMomentCanonicalHold(t *testing.T) {
	for _, hold := range beadmeta.DispatchHoldLabels {
		t.Run(hold, func(t *testing.T) {
			base := beads.NewMemStore()
			store := &injectBeforeAssignmentReleaseStore{Store: base}
			work := seedStrictOrphanWork(t, store)
			var afterHold beads.Bead
			store.inject = func() {
				if err := base.Update(work.ID, beads.UpdateOpts{Labels: []string{hold}}); err != nil {
					t.Fatalf("adding last-moment hold: %v", err)
				}
				var err error
				afterHold, err = base.Get(work.ID)
				if err != nil {
					t.Fatal(err)
				}
			}

			if released := runStrictOrphanRelease(store, work); len(released) != 0 {
				t.Fatalf("released = %+v, want none after %s", released, hold)
			}
			after, err := base.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(afterHold, after) {
				t.Fatalf("last-moment hold refusal mutated work:\nafterHold=%+v\nafter=%+v", afterHold, after)
			}
		})
	}
}

func TestStrictOrphanReleaseRequiresAtomicCapability(t *testing.T) {
	type storeOnly struct{ beads.Store }
	base := beads.NewMemStore()
	store := &storeOnly{Store: base}
	work := seedStrictOrphanWork(t, store)
	before := work

	if released := runStrictOrphanRelease(store, work); len(released) != 0 {
		t.Fatalf("released = %+v, want none for unsupported strict store", released)
	}
	after, err := base.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("unsupported strict release mutated work:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func TestStrictOrphanReleaseSameStoreSuccessIsOneAtomicMutation(t *testing.T) {
	store := beads.NewMemStore()
	work := seedStrictOrphanWork(t, store)
	if released := runStrictOrphanRelease(store, work); len(released) != 1 || released[0].ID != work.ID {
		t.Fatalf("released = %+v, want exact work %s", released, work.ID)
	}
	after, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != "open" || after.Assignee != "" {
		t.Fatalf("released work = %+v, want open and unassigned", after)
	}
	for _, key := range beadmeta.SessionAffinityMetadataKeys {
		if after.Metadata[key] != "" {
			t.Fatalf("released work metadata[%q] = %q, want cleared", key, after.Metadata[key])
		}
	}
	if after.Revision != work.Revision+1 {
		t.Fatalf("release revision = %d, want exactly one mutation from %d", after.Revision, work.Revision)
	}
}
