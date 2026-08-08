package beadstest

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
)

// RunGuardedAssignmentClaimConformance exercises the exact, atomic assignment
// contract shared by every store that advertises GuardedAssignmentClaimer.
// newStore must return a fresh, empty store for each subtest.
func RunGuardedAssignmentClaimConformance(t *testing.T, name string, newStore func(t *testing.T) beads.Store) {
	RunGuardedAssignmentClaimConformanceWithOptions(t, name, newStore, GuardedAssignmentClaimOptions{
		TracksRevision:   true,
		TracksClaimFence: true,
	})
}

// GuardedAssignmentClaimOptions describes optional concurrency tokens a store
// projects through beads.Bead and any isolation limitation of its test fixture.
// The native Dolt adapter currently keeps both tokens internal to the upstream
// storage layer, while Mem/File/SQLite expose them directly; its in-memory
// fixture cannot prove contention, so the real-provider integration owns that
// leg instead.
type GuardedAssignmentClaimOptions struct {
	TracksRevision              bool
	TracksClaimFence            bool
	FixtureLacksIsolationReason string
}

// RunGuardedAssignmentClaimConformanceWithOptions is the configurable form of
// RunGuardedAssignmentClaimConformance.
func RunGuardedAssignmentClaimConformanceWithOptions(t *testing.T, name string, newStore func(t *testing.T) beads.Store, opts GuardedAssignmentClaimOptions) {
	t.Helper()

	claimerFor := func(t *testing.T, store beads.Store) beads.GuardedAssignmentClaimer {
		t.Helper()
		claimer, ok := beads.GuardedAssignmentClaimerFor(store)
		if !ok {
			t.Fatalf("%s does not expose GuardedAssignmentClaimer (got %T)", name, store)
		}
		return claimer
	}
	seed := func(t *testing.T, store beads.Store, labels ...string) beads.Bead {
		t.Helper()
		created, err := store.Create(beads.Bead{
			Title:  "guarded assignment",
			Labels: labels,
			Metadata: map[string]string{
				beadmeta.RoutedToMetadataKey: "rig/pool",
				"input_epoch":                "17",
			},
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		return created
	}
	request := func(id, actor string) beads.AssignmentClaimRequest {
		return beads.AssignmentClaimRequest{
			ID:               id,
			Actor:            actor,
			ExpectedStatus:   "open",
			ExpectedAssignee: "",
			ExpectedMetadata: map[string]string{
				beadmeta.RoutedToMetadataKey: "rig/pool",
				"input_epoch":                "17",
			},
			ForbiddenLabels: []string{"hold:mayor", "hold:external"},
			AssignmentMetadata: map[string]string{
				beadmeta.SessionIDMetadataKey:            "session-" + actor,
				beadmeta.SessionNameMetadataKey:          "rig-" + actor,
				beadmeta.SessionInstanceTokenMetadataKey: "instance-" + actor,
			},
		}
	}
	seedSessionWitness := func(t *testing.T, store beads.Store, actor string) beads.Bead {
		t.Helper()
		created, err := store.Create(beads.Bead{
			Title:  "guarded assignment session witness",
			Type:   "session",
			Labels: []string{"gc:session"},
			Metadata: map[string]string{
				"instance_token": "instance-" + actor,
				"session_name":   "rig-" + actor,
				"state":          "creating",
				"alias":          actor,
			},
		})
		if err != nil {
			t.Fatalf("Create session witness: %v", err)
		}
		return created
	}
	requestWithSessionWitness := func(workID, actor string, sessionBead beads.Bead) beads.AssignmentClaimRequest {
		req := request(workID, actor)
		req.CoLocatedWitness = &beads.AssignmentClaimCoLocatedWitness{
			ID:             sessionBead.ID,
			ExpectedStatus: "open",
			ExpectedType:   "session",
			RequiredLabels: []string{"gc:session"},
			ExpectedMetadata: map[string]string{
				"instance_token": "instance-" + actor,
				"session_name":   "rig-" + actor,
				"state":          "creating",
				"alias":          actor,
			},
			AbsentOrEmptyMetadata: []string{"configured_named_identity"},
		}
		return req
	}

	t.Run("ClaimsExactBeadAndWritesWitnessAtomically", func(t *testing.T) {
		store := newStore(t)
		created := seed(t, store, "ordinary")
		before, err := store.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}

		claimed, ok, err := claimerFor(t, store).ClaimAssignment(context.Background(), request(created.ID, "worker-1"))
		if err != nil || !ok {
			t.Fatalf("ClaimAssignment = (%+v, %v, %v), want success", claimed, ok, err)
		}
		if claimed.ID != created.ID || claimed.Status != "in_progress" || claimed.Assignee != "worker-1" {
			t.Fatalf("claimed = %+v, want exact bead assigned in_progress", claimed)
		}
		for key, want := range request(created.ID, "worker-1").AssignmentMetadata {
			if got := claimed.Metadata[key]; got != want {
				t.Fatalf("claimed.Metadata[%q] = %q, want %q", key, got, want)
			}
		}
		stored, err := store.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !beadsEqualIgnoringMonotonicClock(claimed, stored) {
			t.Fatalf("returned claim is not authoritative readback:\nreturned=%+v\nstored=%+v", claimed, stored)
		}
		if opts.TracksRevision && stored.Revision <= before.Revision {
			t.Fatalf("claim revision = %d, want > %d", stored.Revision, before.Revision)
		}
		if opts.TracksClaimFence && stored.ClaimFence <= before.ClaimFence {
			t.Fatalf("claim fence = %d, want > %d", stored.ClaimFence, before.ClaimFence)
		}
	})

	t.Run("RequireIdempotentNeverCreatesANewAssignment", func(t *testing.T) {
		store := newStore(t)
		created := seed(t, store, "ordinary")
		before, err := store.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		req := request(created.ID, "worker-1")
		req.RequireIdempotent = true

		claimed, ok, err := claimerFor(t, store).ClaimAssignment(context.Background(), req)
		if err != nil {
			t.Fatalf("ClaimAssignment verification: %v", err)
		}
		if ok || !reflect.DeepEqual(claimed, beads.Bead{}) {
			t.Fatalf("ClaimAssignment verification = (%+v, %v), want zero,false for open bead", claimed, ok)
		}
		after, err := store.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !beadsEqualIgnoringMonotonicClock(before, after) {
			t.Fatalf("verification mutated eligible open bead:\nbefore=%+v\nafter=%+v", before, after)
		}

		claimReq := request(created.ID, "worker-1")
		first, ok, err := claimerFor(t, store).ClaimAssignment(context.Background(), claimReq)
		if err != nil || !ok {
			t.Fatalf("initial ClaimAssignment = (%+v, %v, %v), want success", first, ok, err)
		}
		verifyReq := claimReq
		verifyReq.RequireIdempotent = true
		verified, ok, err := claimerFor(t, store).ClaimAssignment(context.Background(), verifyReq)
		if err != nil || !ok {
			t.Fatalf("idempotent verification = (%+v, %v, %v), want success", verified, ok, err)
		}
		if !beadsEqualIgnoringMonotonicClock(first, verified) {
			t.Fatalf("idempotent verification changed authoritative bead:\nfirst=%+v\nverified=%+v", first, verified)
		}
	})

	t.Run("SameOwnerSameWitnessIsIdempotent", func(t *testing.T) {
		store := newStore(t)
		created := seed(t, store)
		claimer := claimerFor(t, store)
		req := request(created.ID, "worker-1")
		first, ok, err := claimer.ClaimAssignment(context.Background(), req)
		if err != nil || !ok {
			t.Fatalf("first claim = (%+v, %v, %v)", first, ok, err)
		}
		second, ok, err := claimer.ClaimAssignment(context.Background(), req)
		if err != nil || !ok {
			t.Fatalf("same-owner claim = (%+v, %v, %v), want idempotent success", second, ok, err)
		}
		if !beadsEqualIgnoringMonotonicClock(second, first) {
			t.Fatalf("same-owner claim mutated bead:\nfirst=%+v\nsecond=%+v", first, second)
		}
	})

	t.Run("CanonicalHoldAddedAfterAssignmentRemainsIdempotent", func(t *testing.T) {
		for _, labels := range [][]string{
			{"hold:mayor"},
			{"hold:external"},
			{"hold:mayor", "hold:external"},
		} {
			store := newStore(t)
			created := seed(t, store)
			claimer := claimerFor(t, store)
			req := request(created.ID, "worker-1")
			if _, ok, err := claimer.ClaimAssignment(context.Background(), req); err != nil || !ok {
				t.Fatalf("initial claim = (%v, %v), want success", ok, err)
			}
			if err := store.Update(created.ID, beads.UpdateOpts{Labels: labels}); err != nil {
				t.Fatal(err)
			}
			before, err := store.Get(created.ID)
			if err != nil {
				t.Fatal(err)
			}
			claimed, ok, err := claimer.ClaimAssignment(context.Background(), req)
			if err != nil || !ok {
				t.Fatalf("held same-owner recovery = (%+v, %v, %v), want idempotent success", claimed, ok, err)
			}
			if !beadsEqualIgnoringMonotonicClock(claimed, before) {
				t.Fatalf("held same-owner recovery mutated bead:\nbefore=%+v\nafter=%+v", before, claimed)
			}
		}
	})

	t.Run("CoLocatedSessionWitnessMatchesAndRecoveryIsIdempotent", func(t *testing.T) {
		store := newStore(t)
		created := seed(t, store)
		sessionBead := seedSessionWitness(t, store, "worker-1")
		claimer := claimerFor(t, store)
		req := requestWithSessionWitness(created.ID, "worker-1", sessionBead)

		first, ok, err := claimer.ClaimAssignment(context.Background(), req)
		if err != nil || !ok {
			t.Fatalf("first witnessed claim = (%+v, %v, %v), want success", first, ok, err)
		}
		second, ok, err := claimer.ClaimAssignment(context.Background(), req)
		if err != nil || !ok {
			t.Fatalf("witnessed recovery claim = (%+v, %v, %v), want idempotent success", second, ok, err)
		}
		if !beadsEqualIgnoringMonotonicClock(second, first) {
			t.Fatalf("witnessed recovery mutated work bead:\nfirst=%+v\nsecond=%+v", first, second)
		}
	})

	t.Run("SessionWitnessDriftNeverMutatesWork", func(t *testing.T) {
		cases := []struct {
			name   string
			mutate func(t *testing.T, store beads.Store, sessionBead beads.Bead, req *beads.AssignmentClaimRequest)
		}{
			{name: "missing witness row", mutate: func(_ *testing.T, _ beads.Store, _ beads.Bead, req *beads.AssignmentClaimRequest) {
				req.CoLocatedWitness.ID += "-missing"
			}},
			{name: "raw status drift", mutate: func(t *testing.T, store beads.Store, sessionBead beads.Bead, _ *beads.AssignmentClaimRequest) {
				status := "blocked"
				if err := store.Update(sessionBead.ID, beads.UpdateOpts{Status: &status}); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "type drift", mutate: func(t *testing.T, store beads.Store, sessionBead beads.Bead, _ *beads.AssignmentClaimRequest) {
				issueType := "task"
				if err := store.Update(sessionBead.ID, beads.UpdateOpts{Type: &issueType}); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "required label drift", mutate: func(t *testing.T, store beads.Store, sessionBead beads.Bead, _ *beads.AssignmentClaimRequest) {
				if err := store.Update(sessionBead.ID, beads.UpdateOpts{RemoveLabels: []string{"gc:session"}}); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "instance token drift", mutate: func(t *testing.T, store beads.Store, sessionBead beads.Bead, _ *beads.AssignmentClaimRequest) {
				if err := store.SetMetadata(sessionBead.ID, "instance_token", "replacement-token"); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "session name drift", mutate: func(t *testing.T, store beads.Store, sessionBead beads.Bead, _ *beads.AssignmentClaimRequest) {
				if err := store.SetMetadata(sessionBead.ID, "session_name", "replacement-name"); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "lifecycle state drift", mutate: func(t *testing.T, store beads.Store, sessionBead beads.Bead, _ *beads.AssignmentClaimRequest) {
				if err := store.SetMetadata(sessionBead.ID, "state", "asleep"); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "alias drift", mutate: func(t *testing.T, store beads.Store, sessionBead beads.Bead, _ *beads.AssignmentClaimRequest) {
				if err := store.SetMetadata(sessionBead.ID, "alias", "replacement-alias"); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "absent-or-empty metadata drift", mutate: func(t *testing.T, store beads.Store, sessionBead beads.Bead, _ *beads.AssignmentClaimRequest) {
				if err := store.SetMetadata(sessionBead.ID, "configured_named_identity", "replacement-identity"); err != nil {
					t.Fatal(err)
				}
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				store := newStore(t)
				created := seed(t, store)
				sessionBead := seedSessionWitness(t, store, "worker-1")
				req := requestWithSessionWitness(created.ID, "worker-1", sessionBead)
				tc.mutate(t, store, sessionBead, &req)
				before, err := store.Get(created.ID)
				if err != nil {
					t.Fatal(err)
				}
				claimed, ok, err := claimerFor(t, store).ClaimAssignment(context.Background(), req)
				if err != nil || ok || claimed.ID != "" {
					t.Fatalf("ClaimAssignment with drifted session witness = (%+v, %v, %v), want zero,false,nil", claimed, ok, err)
				}
				after, err := store.Get(created.ID)
				if err != nil {
					t.Fatal(err)
				}
				if !beadsEqualIgnoringMonotonicClock(after, before) {
					t.Fatalf("session witness drift mutated work bead:\nbefore=%+v\nafter=%+v", before, after)
				}
			})
		}
	})

	t.Run("InvalidSessionWitnessNeverMutatesWork", func(t *testing.T) {
		cases := []struct {
			name   string
			mutate func(work beads.Bead, req *beads.AssignmentClaimRequest)
		}{
			{name: "partial witness", mutate: func(_ beads.Bead, req *beads.AssignmentClaimRequest) {
				req.CoLocatedWitness = &beads.AssignmentClaimCoLocatedWitness{ID: req.CoLocatedWitness.ID}
			}},
			{name: "self witness", mutate: func(work beads.Bead, req *beads.AssignmentClaimRequest) {
				req.CoLocatedWitness.ID = work.ID
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				store := newStore(t)
				created := seed(t, store)
				sessionBead := seedSessionWitness(t, store, "worker-1")
				req := requestWithSessionWitness(created.ID, "worker-1", sessionBead)
				tc.mutate(created, &req)
				before, err := store.Get(created.ID)
				if err != nil {
					t.Fatal(err)
				}
				claimed, ok, err := claimerFor(t, store).ClaimAssignment(context.Background(), req)
				if err == nil || ok || claimed.ID != "" {
					t.Fatalf("ClaimAssignment with invalid session witness = (%+v, %v, %v), want zero,false,error", claimed, ok, err)
				}
				after, err := store.Get(created.ID)
				if err != nil {
					t.Fatal(err)
				}
				if !beadsEqualIgnoringMonotonicClock(after, before) {
					t.Fatalf("invalid session witness mutated work bead:\nbefore=%+v\nafter=%+v", before, after)
				}
			})
		}
	})

	t.Run("SingleWinner", func(t *testing.T) {
		if opts.FixtureLacksIsolationReason != "" {
			t.Skip(opts.FixtureLacksIsolationReason)
		}
		store := newStore(t)
		created := seed(t, store)
		claimer := claimerFor(t, store)
		actors := []string{"worker-a", "worker-b"}
		start := make(chan struct{})
		results := make(chan struct {
			actor string
			bead  beads.Bead
			ok    bool
			err   error
		}, len(actors))
		var wg sync.WaitGroup
		for _, actor := range actors {
			actor := actor
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				bead, ok, err := claimer.ClaimAssignment(context.Background(), request(created.ID, actor))
				results <- struct {
					actor string
					bead  beads.Bead
					ok    bool
					err   error
				}{actor: actor, bead: bead, ok: ok, err: err}
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		wins := 0
		winner := ""
		for result := range results {
			if result.err != nil {
				t.Fatalf("%s claim error: %v", result.actor, result.err)
			}
			if result.ok {
				wins++
				winner = result.actor
				if result.bead.Assignee != result.actor {
					t.Fatalf("%s won but returned owner %q", result.actor, result.bead.Assignee)
				}
				for key, want := range request(created.ID, result.actor).AssignmentMetadata {
					if got := result.bead.Metadata[key]; got != want {
						t.Fatalf("%s won but returned witness %s=%q, want %q", result.actor, key, got, want)
					}
				}
			} else if result.bead.ID != "" {
				t.Fatalf("%s lost but returned non-zero bead %+v", result.actor, result.bead)
			}
		}
		if wins != 1 {
			t.Fatalf("wins = %d, want exactly one", wins)
		}
		stored, err := store.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Assignee != winner || stored.Status != "in_progress" {
			t.Fatalf("stored = %+v, want winner %q", stored, winner)
		}
		for key, want := range request(created.ID, winner).AssignmentMetadata {
			if got := stored.Metadata[key]; got != want {
				t.Fatalf("stored winner witness %s=%q, want %q", key, got, want)
			}
		}
	})

	t.Run("PredicateMismatchesNeverMutate", func(t *testing.T) {
		cases := []struct {
			name   string
			mutate func(t *testing.T, store beads.Store, created beads.Bead, req *beads.AssignmentClaimRequest)
		}{
			{name: "status drift", mutate: func(t *testing.T, store beads.Store, created beads.Bead, _ *beads.AssignmentClaimRequest) {
				status := "blocked"
				if err := store.Update(created.ID, beads.UpdateOpts{Status: &status}); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "assignee drift", mutate: func(t *testing.T, store beads.Store, created beads.Bead, _ *beads.AssignmentClaimRequest) {
				owner := "somebody-else"
				if err := store.Update(created.ID, beads.UpdateOpts{Assignee: &owner}); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "route drift", mutate: func(t *testing.T, store beads.Store, created beads.Bead, _ *beads.AssignmentClaimRequest) {
				if err := store.SetMetadata(created.ID, beadmeta.RoutedToMetadataKey, "other/pool"); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "other expected metadata drift", mutate: func(t *testing.T, store beads.Store, created beads.Bead, _ *beads.AssignmentClaimRequest) {
				if err := store.SetMetadata(created.ID, "input_epoch", "18"); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "expected metadata key absent", mutate: func(_ *testing.T, _ beads.Store, _ beads.Bead, req *beads.AssignmentClaimRequest) {
				req.ExpectedMetadata["absent"] = ""
			}},
			{name: "forbidden mayor label present", mutate: func(t *testing.T, store beads.Store, created beads.Bead, _ *beads.AssignmentClaimRequest) {
				if err := store.Update(created.ID, beads.UpdateOpts{Labels: []string{"hold:mayor"}}); err != nil {
					t.Fatal(err)
				}
			}},
			{name: "forbidden external label present", mutate: func(t *testing.T, store beads.Store, created beads.Bead, _ *beads.AssignmentClaimRequest) {
				if err := store.Update(created.ID, beads.UpdateOpts{Labels: []string{"hold:external"}}); err != nil {
					t.Fatal(err)
				}
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				store := newStore(t)
				created := seed(t, store, "unrelated")
				req := request(created.ID, "worker-1")
				tc.mutate(t, store, created, &req)
				before, err := store.Get(created.ID)
				if err != nil {
					t.Fatal(err)
				}
				claimed, ok, err := claimerFor(t, store).ClaimAssignment(context.Background(), req)
				if err != nil || ok || claimed.ID != "" {
					t.Fatalf("ClaimAssignment mismatch = (%+v, %v, %v), want zero,false,nil", claimed, ok, err)
				}
				after, err := store.Get(created.ID)
				if err != nil {
					t.Fatal(err)
				}
				if !beadsEqualIgnoringMonotonicClock(after, before) {
					t.Fatalf("predicate mismatch mutated bead:\nbefore=%+v\nafter=%+v", before, after)
				}
			})
		}
	})

	t.Run("SameOwnerDifferentWitnessFailsWithoutRewrite", func(t *testing.T) {
		store := newStore(t)
		created := seed(t, store)
		claimer := claimerFor(t, store)
		firstReq := request(created.ID, "worker-1")
		if _, ok, err := claimer.ClaimAssignment(context.Background(), firstReq); err != nil || !ok {
			t.Fatalf("first claim = ok %v err %v", ok, err)
		}
		before, err := store.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		retryReq := request(created.ID, "worker-1")
		retryReq.AssignmentMetadata[beadmeta.SessionInstanceTokenMetadataKey] = "different-instance"
		claimed, ok, err := claimer.ClaimAssignment(context.Background(), retryReq)
		if err != nil || ok || claimed.ID != "" {
			t.Fatalf("different-witness retry = (%+v, %v, %v), want zero,false,nil", claimed, ok, err)
		}
		after, err := store.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !beadsEqualIgnoringMonotonicClock(after, before) {
			t.Fatalf("different-witness retry rewrote owner evidence:\nbefore=%+v\nafter=%+v", before, after)
		}
	})

	t.Run("WrongExactIDReturnsNotFoundWithoutMutation", func(t *testing.T) {
		store := newStore(t)
		created := seed(t, store)
		before, err := store.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		claimed, ok, err := claimerFor(t, store).ClaimAssignment(context.Background(), request(created.ID+"-suffix", "worker-1"))
		if ok || claimed.ID != "" || !errors.Is(err, beads.ErrNotFound) {
			t.Fatalf("wrong-ID claim = (%+v, %v, %v), want zero,false,ErrNotFound", claimed, ok, err)
		}
		after, err := store.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !beadsEqualIgnoringMonotonicClock(after, before) {
			t.Fatalf("wrong-ID claim mutated real bead:\nbefore=%+v\nafter=%+v", before, after)
		}
	})

	t.Run("InvalidRequestNeverMutates", func(t *testing.T) {
		store := newStore(t)
		created := seed(t, store)
		before, err := store.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		req := request(created.ID, "worker-1")
		req.Actor = ""
		claimed, ok, err := claimerFor(t, store).ClaimAssignment(context.Background(), req)
		if ok || claimed.ID != "" || err == nil {
			t.Fatalf("invalid claim = (%+v, %v, %v), want zero,false,error", claimed, ok, err)
		}
		after, err := store.Get(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !beadsEqualIgnoringMonotonicClock(after, before) {
			t.Fatalf("invalid claim mutated bead:\nbefore=%+v\nafter=%+v", before, after)
		}
	})
}

func beadsEqualIgnoringMonotonicClock(left, right beads.Bead) bool {
	left.CreatedAt = left.CreatedAt.Round(0)
	left.UpdatedAt = left.UpdatedAt.Round(0)
	right.CreatedAt = right.CreatedAt.Round(0)
	right.UpdatedAt = right.UpdatedAt.Round(0)
	return reflect.DeepEqual(left, right)
}
