package beadstest

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
)

// RunAssignmentReleaseConformance exercises the exact atomic orphan-release
// contract shared by every store that advertises AssignmentReleaser.
func RunAssignmentReleaseConformance(t *testing.T, name string, newStore func(t *testing.T) beads.Store) {
	RunAssignmentReleaseConformanceWithOptions(t, name, newStore, AssignmentReleaseOptions{})
}

// AssignmentReleaseOptions records test-fixture limitations that do not apply
// to the production provider.
type AssignmentReleaseOptions struct {
	FixtureLacksIsolationReason string
}

// RunAssignmentReleaseConformanceWithOptions is the configurable form of
// RunAssignmentReleaseConformance.
func RunAssignmentReleaseConformanceWithOptions(t *testing.T, name string, newStore func(t *testing.T) beads.Store, opts AssignmentReleaseOptions) {
	t.Helper()

	releaserFor := func(t *testing.T, store beads.Store) beads.AssignmentReleaser {
		t.Helper()
		releaser, ok := beads.AssignmentReleaserFor(store)
		if !ok {
			t.Fatalf("%s does not expose AssignmentReleaser (got %T)", name, store)
		}
		return releaser
	}
	seed := func(t *testing.T, store beads.Store) beads.Bead {
		t.Helper()
		created, err := store.Create(beads.Bead{
			Title:    "assigned work",
			Assignee: "worker-1",
			Metadata: map[string]string{
				beadmeta.RoutedToMetadataKey:             "rig/pool",
				beadmeta.SessionIDMetadataKey:            "session-old",
				beadmeta.SessionNameMetadataKey:          "worker-1",
				beadmeta.SessionInstanceTokenMetadataKey: "instance-old",
			},
		})
		if err != nil {
			t.Fatalf("Create work: %v", err)
		}
		status := "in_progress"
		if err := store.Update(created.ID, beads.UpdateOpts{Status: &status}); err != nil {
			t.Fatalf("mark work in_progress: %v", err)
		}
		current, err := store.Get(created.ID)
		if err != nil {
			t.Fatalf("Get work: %v", err)
		}
		return current
	}
	request := func(work beads.Bead) beads.AssignmentReleaseRequest {
		revision := work.Revision
		return beads.AssignmentReleaseRequest{
			ID:               work.ID,
			ExpectedStatus:   "in_progress",
			ExpectedAssignee: "worker-1",
			ExpectedRevision: &revision,
			ExpectedMetadata: map[string]string{
				beadmeta.RoutedToMetadataKey:             "rig/pool",
				beadmeta.SessionIDMetadataKey:            "session-old",
				beadmeta.SessionNameMetadataKey:          "worker-1",
				beadmeta.SessionInstanceTokenMetadataKey: "instance-old",
			},
			ForbiddenLabels: []string{"hold:mayor", "hold:external"},
			ReleaseMetadata: map[string]string{
				beadmeta.SessionIDMetadataKey:            "",
				beadmeta.SessionNameMetadataKey:          "",
				beadmeta.SessionInstanceTokenMetadataKey: "",
			},
			AbsentCoLocatedMatch: &beads.CoLocatedMatchPredicate{
				ExpectedStatus: "open",
				ClassAnyOf: []beads.CoLocatedClassPredicate{
					{ExpectedType: "session"},
					{RequiredLabels: []string{"gc:session"}},
				},
				MatchValue:            "worker-1",
				MatchID:               true,
				MetadataKeys:          []string{"session_name", "configured_named_identity", "alias"},
				DelimitedMetadataKeys: map[string]string{"alias_history": ","},
			},
		}
	}
	assertUnchanged := func(t *testing.T, store beads.Store, before beads.Bead) {
		t.Helper()
		after, err := store.Get(before.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !beadsEqualIgnoringMonotonicClock(before, after) {
			t.Fatalf("release refusal mutated work:\nbefore=%+v\nafter=%+v", before, after)
		}
	}

	t.Run("ReleasesExactAssignmentOnce", func(t *testing.T) {
		store := newStore(t)
		work := seed(t, store)
		releaser := releaserFor(t, store)
		released, ok, err := releaser.ReleaseAssignment(context.Background(), request(work))
		if err != nil || !ok {
			t.Fatalf("ReleaseAssignment = (%+v, %v, %v), want success", released, ok, err)
		}
		if released.ID != work.ID || released.Status != "open" || released.Assignee != "" {
			t.Fatalf("released = %+v, want exact work open and unassigned", released)
		}
		for key := range request(work).ReleaseMetadata {
			if released.Metadata[key] != "" {
				t.Fatalf("released.Metadata[%q] = %q, want cleared", key, released.Metadata[key])
			}
		}
		stored, err := store.Get(work.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !beadsEqualIgnoringMonotonicClock(released, stored) {
			t.Fatalf("returned release is not authoritative:\nreturned=%+v\nstored=%+v", released, stored)
		}
		beforeSecond := stored
		second, ok, err := releaser.ReleaseAssignment(context.Background(), request(work))
		if err != nil || ok || second.ID != "" {
			t.Fatalf("second ReleaseAssignment = (%+v, %v, %v), want zero,false,nil", second, ok, err)
		}
		assertUnchanged(t, store, beforeSecond)
	})

	t.Run("LateCoLocatedOwnerBlocksRelease", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			rowType  string
			labels   []string
			metadata map[string]string
		}{
			{name: "type and label exact metadata", rowType: "session", labels: []string{"gc:session"}, metadata: map[string]string{"session_name": "worker-1"}},
			{name: "type only repairable row", rowType: "session", metadata: map[string]string{"session_name": "worker-1"}},
			{name: "label only repairable row", labels: []string{"gc:session"}, metadata: map[string]string{"session_name": "worker-1"}},
			{name: "delimited metadata", rowType: "session", labels: []string{"gc:session"}, metadata: map[string]string{"alias_history": "old, worker-1,older"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				store := newStore(t)
				work := seed(t, store)
				req := request(work) // absence was observed before this row existed
				witness, err := store.Create(beads.Bead{
					Title:    "late owner witness",
					Type:     tc.rowType,
					Labels:   tc.labels,
					Metadata: tc.metadata,
				})
				if err != nil {
					t.Fatalf("Create late witness: %v", err)
				}
				before, err := store.Get(work.ID)
				if err != nil {
					t.Fatal(err)
				}
				released, ok, err := releaserFor(t, store).ReleaseAssignment(context.Background(), req)
				if err != nil || ok || released.ID != "" {
					t.Fatalf("ReleaseAssignment with late owner = (%+v, %v, %v), want zero,false,nil", released, ok, err)
				}
				assertUnchanged(t, store, before)
				afterWitness, err := store.Get(witness.ID)
				if err != nil {
					t.Fatal(err)
				}
				if !beadsEqualIgnoringMonotonicClock(witness, afterWitness) {
					t.Fatalf("release refusal mutated witness:\nbefore=%+v\nafter=%+v", witness, afterWitness)
				}
			})
		}
	})

	t.Run("LastMomentCanonicalHoldBlocksRelease", func(t *testing.T) {
		for _, hold := range []string{"hold:mayor", "hold:external"} {
			t.Run(hold, func(t *testing.T) {
				store := newStore(t)
				work := seed(t, store)
				req := request(work)
				if err := store.Update(work.ID, beads.UpdateOpts{Labels: []string{hold}}); err != nil {
					t.Fatal(err)
				}
				before, err := store.Get(work.ID)
				if err != nil {
					t.Fatal(err)
				}
				// Keep every other exact predicate current so this refusal proves the
				// forbidden-label check itself is inside the atomic operation.
				req.ExpectedRevision = &before.Revision
				released, ok, err := releaserFor(t, store).ReleaseAssignment(context.Background(), req)
				if err != nil || ok || released.ID != "" {
					t.Fatalf("ReleaseAssignment with %s = (%+v, %v, %v), want zero,false,nil", hold, released, ok, err)
				}
				assertUnchanged(t, store, before)
			})
		}
	})

	t.Run("PredicateMismatchNeverMutates", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			mutate func(t *testing.T, store beads.Store, work beads.Bead, req *beads.AssignmentReleaseRequest)
		}{
			{name: "status", mutate: func(t *testing.T, store beads.Store, work beads.Bead, req *beads.AssignmentReleaseRequest) {
				status := "open"
				if err := store.Update(work.ID, beads.UpdateOpts{Status: &status}); err != nil {
					t.Fatal(err)
				}
				current, err := store.Get(work.ID)
				if err != nil {
					t.Fatal(err)
				}
				req.ExpectedRevision = &current.Revision
			}},
			{name: "assignee", mutate: func(t *testing.T, store beads.Store, work beads.Bead, req *beads.AssignmentReleaseRequest) {
				assignee := "worker-2"
				if err := store.Update(work.ID, beads.UpdateOpts{Assignee: &assignee}); err != nil {
					t.Fatal(err)
				}
				current, err := store.Get(work.ID)
				if err != nil {
					t.Fatal(err)
				}
				req.ExpectedRevision = &current.Revision
			}},
			{name: "revision", mutate: func(_ *testing.T, _ beads.Store, _ beads.Bead, req *beads.AssignmentReleaseRequest) {
				v := *req.ExpectedRevision + 1
				req.ExpectedRevision = &v
			}},
			{name: "metadata", mutate: func(_ *testing.T, _ beads.Store, _ beads.Bead, req *beads.AssignmentReleaseRequest) {
				req.ExpectedMetadata[beadmeta.RoutedToMetadataKey] = "other/pool"
			}},
			{name: "additional current metadata", mutate: func(t *testing.T, store beads.Store, work beads.Bead, req *beads.AssignmentReleaseRequest) {
				if err := store.SetMetadata(work.ID, "concurrent", "value"); err != nil {
					t.Fatal(err)
				}
				current, err := store.Get(work.ID)
				if err != nil {
					t.Fatal(err)
				}
				// Keep the revision predicate current so only exact equality with
				// the caller's complete metadata snapshot can veto this release.
				req.ExpectedRevision = &current.Revision
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				store := newStore(t)
				work := seed(t, store)
				req := request(work)
				tc.mutate(t, store, work, &req)
				before, err := store.Get(work.ID)
				if err != nil {
					t.Fatal(err)
				}
				released, ok, err := releaserFor(t, store).ReleaseAssignment(context.Background(), req)
				if err != nil || ok || released.ID != "" {
					t.Fatalf("ReleaseAssignment mismatch = (%+v, %v, %v), want zero,false,nil", released, ok, err)
				}
				assertUnchanged(t, store, before)
			})
		}
	})

	t.Run("WrongExactIDReturnsNotFoundWithoutMutation", func(t *testing.T) {
		store := newStore(t)
		work := seed(t, store)
		req := request(work)
		req.ID += "-missing"
		released, ok, err := releaserFor(t, store).ReleaseAssignment(context.Background(), req)
		if ok || released.ID != "" || !errors.Is(err, beads.ErrNotFound) {
			t.Fatalf("wrong-ID release = (%+v, %v, %v), want zero,false,ErrNotFound", released, ok, err)
		}
		assertUnchanged(t, store, work)
	})

	t.Run("ConcurrentReleaseHasOneMutationWinner", func(t *testing.T) {
		if opts.FixtureLacksIsolationReason != "" {
			t.Skip(opts.FixtureLacksIsolationReason)
		}
		store := newStore(t)
		work := seed(t, store)
		releaser := releaserFor(t, store)
		start := make(chan struct{})
		results := make(chan bool, 2)
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, ok, err := releaser.ReleaseAssignment(context.Background(), request(work))
				results <- ok
				errs <- err
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		close(errs)
		wins := 0
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent release: %v", err)
			}
		}
		for ok := range results {
			if ok {
				wins++
			}
		}
		if wins != 1 {
			t.Fatalf("concurrent winners = %d, want 1", wins)
		}
	})
}
