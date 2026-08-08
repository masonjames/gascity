package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

type hookTriggerFenceStore struct {
	beads.Store
	bead        beads.Bead
	sessionBead beads.Bead
	err         error
	getCalls    []string
}

type hookTriggerDriftStore struct {
	*beads.MemStore
	first           beads.Bead
	afterGet        func()
	afterSessionGet func()
	sessionID       string
	firstRead       bool
}

func (s *hookTriggerDriftStore) Get(id string) (beads.Bead, error) {
	if !s.firstRead {
		s.firstRead = true
		if s.afterGet != nil {
			s.afterGet()
		}
		return s.first, nil
	}
	bead, err := s.MemStore.Get(id)
	if err == nil && id == s.sessionID && s.afterSessionGet != nil {
		s.afterSessionGet()
	}
	return bead, err
}

func (s *hookTriggerDriftStore) GuardedAssignmentClaimerHandle() (beads.GuardedAssignmentClaimer, bool) {
	return s.MemStore, true
}

func (s *hookTriggerFenceStore) Get(id string) (beads.Bead, error) {
	s.getCalls = append(s.getCalls, id)
	if s.err != nil {
		return beads.Bead{}, s.err
	}
	if id == s.sessionBead.ID {
		return s.sessionBead, nil
	}
	return s.bead, nil
}

func (s *hookTriggerFenceStore) GuardedAssignmentClaimerHandle() (beads.GuardedAssignmentClaimer, bool) {
	return hookTriggerFenceClaimer{s: s}, true
}

type hookTriggerFenceClaimer struct {
	s *hookTriggerFenceStore
}

func (c hookTriggerFenceClaimer) ClaimAssignment(_ context.Context, req beads.AssignmentClaimRequest) (beads.Bead, bool, error) {
	bead := c.s.bead
	if !req.RequireIdempotent || bead.ID != req.ID || bead.Status != "in_progress" || bead.Assignee != req.Actor {
		return beads.Bead{}, false, nil
	}
	for key, expected := range req.ExpectedMetadata {
		if actual, present := bead.Metadata[key]; !present || actual != expected {
			return beads.Bead{}, false, nil
		}
	}
	for key, expected := range req.AssignmentMetadata {
		if actual, present := bead.Metadata[key]; !present || actual != expected {
			return beads.Bead{}, false, nil
		}
	}
	if witness := req.CoLocatedWitness; witness == nil || witness.ID != c.s.sessionBead.ID {
		return beads.Bead{}, false, nil
	}
	return bead, true, nil
}

func hookTriggerSessionFixture(triggerID, storeRef, assignee, route string) beads.Bead { //nolint:unparam // The explicit store witness is part of every fixture call.
	return beads.Bead{
		ID:     "session-1",
		Title:  "trigger session",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"template":                              route,
			"session_name":                          assignee,
			"instance_token":                        "instance-1",
			"state":                                 string(session.StateActive),
			"alias":                                 assignee,
			beadmeta.TriggerBeadIDMetadataKey:       triggerID,
			beadmeta.TriggerBeadStoreRefMetadataKey: storeRef,
		},
	}
}

// TestHookClaimExpectedBeadAndStoreFencePrecedesEveryMutation proves a
// triggered session never falls back to candidate federation. The hook first
// resolves exactly one canonical store reference, point-reads exactly one bead
// id, and validates ownership, route, and dispatch holds. Every refusal happens
// before the generic work-query runner and before claim, metadata, continuation,
// event, run-map, or drain-ack mutations.
func TestHookClaimExpectedBeadAndStoreFencePrecedesEveryMutation(t *testing.T) {
	const (
		beadID   = "work-expected"
		storeRef = "city:fixture-city"
		assignee = "fixture/worker-1"
		route    = "fixture/worker"
	)
	base := beads.Bead{
		ID:       beadID,
		Title:    "expected work",
		Type:     "task",
		Status:   "open",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: route},
	}
	preclaimed := base
	preclaimed.Status = "in_progress"
	preclaimed.Assignee = assignee
	preclaimed.Metadata = map[string]string{
		beadmeta.RoutedToMetadataKey:             route,
		beadmeta.SessionIDMetadataKey:            "session-1",
		beadmeta.SessionNameMetadataKey:          assignee,
		beadmeta.SessionInstanceTokenMetadataKey: "instance-1",
		beadmeta.WorkBranchMetadataKey:           "fence-test-branch",
	}
	claimOpts := hookClaimOptions{
		Assignee:           assignee,
		IdentityCandidates: []string{assignee},
		RouteTargets:       []string{route},
		Env:                []string{"GC_SESSION_ID=session-1", "GC_SESSION_NAME=" + assignee, "GC_INSTANCE_TOKEN=instance-1"},
		DrainAck:           true,
		JSON:               true,
	}

	errStoreUnavailable := errors.New("selected work store unavailable")
	cases := []struct {
		name        string
		expectation hookClaimTriggerExpectation
		bead        beads.Bead
		storeErr    error
		resolveErr  error
	}{
		{name: "missing expectation", bead: preclaimed},
		{name: "missing store", expectation: hookClaimTriggerExpectation{BeadID: beadID}, bead: preclaimed},
		{name: "missing bead", expectation: hookClaimTriggerExpectation{StoreRef: storeRef}, bead: preclaimed},
		{name: "malformed store", expectation: hookClaimTriggerExpectation{BeadID: beadID, StoreRef: "fixture"}, bead: preclaimed},
		{name: "malformed bead", expectation: hookClaimTriggerExpectation{BeadID: "work-expected\nwrong", StoreRef: storeRef}, bead: preclaimed},
		{name: "wrong store", expectation: hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef}, bead: preclaimed, resolveErr: errStoreUnavailable},
		{name: "selected store read error", expectation: hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef}, bead: preclaimed, storeErr: errStoreUnavailable},
		{name: "readback id mismatch", expectation: hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef}, bead: func() beads.Bead { b := preclaimed; b.ID = "wrong-id"; return b }()},
		{name: "open and unassigned was not controller claimed", expectation: hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef}, bead: base},
		{name: "open and held was not controller claimed", expectation: hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef}, bead: func() beads.Bead { b := base; b.Labels = []string{beadmeta.HoldMayorLabel}; return b }()},
		{name: "already claimed by another session", expectation: hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef}, bead: func() beads.Bead { b := preclaimed; b.Assignee = "fixture/worker-2"; return b }()},
		{name: "route mismatch", expectation: hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef}, bead: func() beads.Bead {
			b := preclaimed
			b.Metadata = map[string]string{
				beadmeta.RoutedToMetadataKey:             "fixture/other",
				beadmeta.SessionIDMetadataKey:            "session-1",
				beadmeta.SessionNameMetadataKey:          assignee,
				beadmeta.SessionInstanceTokenMetadataKey: "instance-1",
			}
			return b
		}()},
		{name: "session witness mismatch", expectation: hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef}, bead: func() beads.Bead {
			b := preclaimed
			b.Metadata = map[string]string{
				beadmeta.RoutedToMetadataKey:             route,
				beadmeta.SessionIDMetadataKey:            "session-other",
				beadmeta.SessionNameMetadataKey:          assignee,
				beadmeta.SessionInstanceTokenMetadataKey: "instance-1",
			}
			return b
		}()},
		{name: "incarnation witness mismatch", expectation: hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef}, bead: func() beads.Bead {
			b := preclaimed
			b.Metadata = map[string]string{
				beadmeta.RoutedToMetadataKey:             route,
				beadmeta.SessionIDMetadataKey:            "session-1",
				beadmeta.SessionNameMetadataKey:          assignee,
				beadmeta.SessionInstanceTokenMetadataKey: "instance-old",
			}
			return b
		}()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			selected := &hookTriggerFenceStore{Store: beads.NewMemStore(), bead: tc.bead, err: tc.storeErr}
			resolveCalls := 0
			var stdout, stderr bytes.Buffer
			code := claimHookExpectedTrigger(
				tc.expectation,
				hookStore{dir: "/fixture"},
				claimOpts,
				hookClaimExpectedTriggerOps{
					ResolveStore: func(ref string) (beads.Store, error) {
						resolveCalls++
						if ref != tc.expectation.StoreRef {
							t.Fatalf("resolved store ref = %q, want %q", ref, tc.expectation.StoreRef)
						}
						if tc.resolveErr != nil {
							return nil, tc.resolveErr
						}
						return selected, nil
					},
				},
				&stdout,
				&stderr,
			)
			if code == 0 {
				t.Fatalf("claimHookExpectedTrigger succeeded; stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want no drain/work result on fence refusal", stdout.String())
			}
			switch {
			case tc.expectation.BeadID == "" || tc.expectation.StoreRef == "" ||
				strings.Contains(tc.expectation.BeadID, "\n") || tc.expectation.StoreRef == "fixture":
				if resolveCalls != 0 || len(selected.getCalls) != 0 {
					t.Fatalf("invalid expectation reached store: resolve=%d get=%v", resolveCalls, selected.getCalls)
				}
			case tc.resolveErr != nil:
				if resolveCalls != 1 || len(selected.getCalls) != 0 {
					t.Fatalf("resolve failure calls: resolve=%d get=%v, want 1/0", resolveCalls, selected.getCalls)
				}
			case resolveCalls != 1 || len(selected.getCalls) != 1 || selected.getCalls[0] != tc.expectation.BeadID:
				t.Fatalf("exact read calls: resolve=%d get=%v, want one Get(%q)", resolveCalls, selected.getCalls, tc.expectation.BeadID)
			}
		})
	}

	// The same id may exist in another store. The canonical store reference is
	// the fence: the non-selected store is never read, the federated runner is
	// never called, and exact adoption is read-only after the atomic fence.
	t.Run("duplicate id in another store cannot hijack exact target", func(t *testing.T) {
		wrong := &hookTriggerFenceStore{Store: beads.NewMemStore(), bead: func() beads.Bead {
			b := preclaimed
			b.Labels = []string{beadmeta.HoldMayorLabel}
			return b
		}()}
		selected := &hookTriggerFenceStore{
			Store:       beads.NewMemStore(),
			bead:        preclaimed,
			sessionBead: hookTriggerSessionFixture(beadID, storeRef, assignee, route),
		}
		var stdout, stderr bytes.Buffer
		code := claimHookExpectedTrigger(
			hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef},
			hookStore{dir: "/fixture"},
			claimOpts,
			hookClaimExpectedTriggerOps{
				ResolveStore: func(ref string) (beads.Store, error) {
					if ref != storeRef {
						t.Fatalf("ref = %q, want %q", ref, storeRef)
					}
					return selected, nil
				},
			},
			&stdout,
			&stderr,
		)
		if code != 0 {
			t.Fatalf("code = %d, want 0; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
		}
		if got := selected.getCalls; len(got) != 2 || got[0] != beadID || got[1] != "session-1" {
			t.Fatalf("selected Get calls = %v, want [%s session-1]", got, beadID)
		}
		if len(wrong.getCalls) != 0 {
			t.Fatalf("non-selected duplicate store was read: %v", wrong.getCalls)
		}
		if !strings.Contains(stdout.String(), `"bead_id":"`+beadID+`"`) {
			t.Fatalf("stdout = %q, want exact claimed bead", stdout.String())
		}
	})

	// Canonical holds exclude only open/unassigned Tier-3 demand. Once the
	// controller atomically assigns the exact work bead to this incarnation, a
	// hold added afterward must remain transparent to the owning Tier-1/2
	// session. The exact route/store/session witnesses still fence adoption.
	for _, labels := range [][]string{
		{beadmeta.HoldMayorLabel},
		{beadmeta.HoldExternalLabel},
		{beadmeta.HoldMayorLabel, beadmeta.HoldExternalLabel},
	} {
		labels := labels
		t.Run("post-assignment holds remain transparent "+strings.Join(labels, "+"), func(t *testing.T) {
			held := preclaimed
			held.Labels = append([]string(nil), labels...)
			selected := &hookTriggerFenceStore{
				Store:       beads.NewMemStore(),
				bead:        held,
				sessionBead: hookTriggerSessionFixture(beadID, storeRef, assignee, route),
			}
			var stdout, stderr bytes.Buffer
			code := claimHookExpectedTrigger(
				hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef},
				hookStore{dir: "/fixture"},
				claimOpts,
				hookClaimExpectedTriggerOps{
					ResolveStore: func(ref string) (beads.Store, error) {
						if ref != storeRef {
							t.Fatalf("ref = %q, want %q", ref, storeRef)
						}
						return selected, nil
					},
				},
				&stdout,
				&stderr,
			)
			if code != 0 {
				t.Fatalf("code = %d, want 0; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stdout.String(), `"bead_id":"`+beadID+`"`) {
				t.Fatalf("stdout = %q, want exact held assigned bead", stdout.String())
			}
		})
	}

	t.Run("cross-store trigger has no co-located session authority", func(t *testing.T) {
		selected := &hookTriggerFenceStore{Store: beads.NewMemStore(), bead: preclaimed}
		var stdout, stderr bytes.Buffer
		code := claimHookExpectedTrigger(
			hookClaimTriggerExpectation{BeadID: beadID, StoreRef: "rig:fixture"},
			hookStore{dir: "/fixture"},
			claimOpts,
			hookClaimExpectedTriggerOps{
				ResolveStore: func(string) (beads.Store, error) { return selected, nil },
			},
			&stdout,
			&stderr,
		)
		if code != 1 {
			t.Fatalf("code = %d, want fail-closed 1; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
		}
		if stdout.Len() != 0 {
			t.Fatalf("stdout = %q, want no work result", stdout.String())
		}
		if !strings.Contains(stderr.String(), "not co-located") {
			t.Fatalf("stderr = %q, want co-located witness diagnostic", stderr.String())
		}
	})

	t.Run("authoritative session reload is already suspended", func(t *testing.T) {
		suspended := hookTriggerSessionFixture(beadID, storeRef, assignee, route)
		suspended.Metadata["state"] = string(session.StateSuspended)
		selected := &hookTriggerFenceStore{
			Store:       beads.NewMemStore(),
			bead:        preclaimed,
			sessionBead: suspended,
		}
		var stdout, stderr bytes.Buffer
		code := claimHookExpectedTrigger(
			hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef},
			hookStore{dir: "/fixture"},
			claimOpts,
			hookClaimExpectedTriggerOps{
				ResolveStore: func(string) (beads.Store, error) { return selected, nil },
			},
			&stdout,
			&stderr,
		)
		if code != 1 {
			t.Fatalf("code = %d, want fail-closed 1; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
		}
		if stdout.Len() != 0 {
			t.Fatalf("stdout = %q, want no work result", stdout.String())
		}
	})
}

func TestHookClaimPersistedTriggerRequiresCompleteExactEnvironment(t *testing.T) {
	const (
		beadID   = "work-expected"
		storeRef = "rig:fixture"
	)
	cases := []struct {
		name        string
		info        session.Info
		envBeadID   string
		envStoreRef string
		want        hookClaimTriggerExpectation
		wantErr     bool
	}{
		{name: "legacy session without persisted or environment trigger"},
		{
			name:        "persisted trigger exactly matches environment",
			info:        session.Info{TriggerBeadID: beadID, TriggerBeadStoreRef: storeRef},
			envBeadID:   beadID,
			envStoreRef: storeRef,
			want:        hookClaimTriggerExpectation{BeadID: beadID, StoreRef: storeRef},
		},
		{name: "persisted trigger missing environment", info: session.Info{TriggerBeadID: beadID, TriggerBeadStoreRef: storeRef}, wantErr: true},
		{name: "persisted trigger has partial metadata", info: session.Info{TriggerBeadID: beadID}, envBeadID: beadID, envStoreRef: storeRef, wantErr: true},
		{name: "environment trigger has no persisted authority", envBeadID: beadID, envStoreRef: storeRef, wantErr: true},
		{name: "partial environment trigger", info: session.Info{TriggerBeadID: beadID, TriggerBeadStoreRef: storeRef}, envBeadID: beadID, wantErr: true},
		{name: "bead mismatch", info: session.Info{TriggerBeadID: beadID, TriggerBeadStoreRef: storeRef}, envBeadID: "work-other", envStoreRef: storeRef, wantErr: true},
		{name: "store mismatch", info: session.Info{TriggerBeadID: beadID, TriggerBeadStoreRef: storeRef}, envBeadID: beadID, envStoreRef: "city:test-city", wantErr: true},
		{name: "malformed persisted ref", info: session.Info{TriggerBeadID: beadID, TriggerBeadStoreRef: "fixture"}, envBeadID: beadID, envStoreRef: "fixture", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hookClaimTriggerExpectationForSession(tc.info, tc.envBeadID, tc.envStoreRef)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr=%v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("expectation = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestHookClaimExpectedTriggerUsesAuthoritativeLiveRead(t *testing.T) {
	const (
		assignee = "fixture/worker-1"
		route    = "fixture/worker"
	)
	backing := beads.NewMemStore()
	seeded, err := backing.Create(beads.Bead{
		Title:    "controller-claimed work",
		Type:     "task",
		Status:   "in_progress",
		Assignee: assignee,
		Metadata: map[string]string{
			beadmeta.RoutedToMetadataKey:             route,
			beadmeta.SessionIDMetadataKey:            "session-1",
			beadmeta.SessionNameMetadataKey:          assignee,
			beadmeta.SessionInstanceTokenMetadataKey: "instance-1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cache := beads.NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	if err := backing.Update(seeded.ID, beads.UpdateOpts{Metadata: map[string]string{
		beadmeta.RoutedToMetadataKey: "fixture/other",
	}}); err != nil {
		t.Fatalf("change backing route: %v", err)
	}
	stale, err := cache.Get(seeded.ID)
	if err != nil {
		t.Fatalf("read stale cache fixture: %v", err)
	}
	if got := stale.Metadata[beadmeta.RoutedToMetadataKey]; got != route {
		t.Fatalf("fixture cache unexpectedly observed backing route %q", got)
	}

	var stdout, stderr bytes.Buffer
	code := claimHookExpectedTrigger(
		hookClaimTriggerExpectation{BeadID: seeded.ID, StoreRef: "rig:fixture"},
		hookStore{dir: "/fixture"},
		hookClaimOptions{
			Assignee:           assignee,
			IdentityCandidates: []string{assignee},
			RouteTargets:       []string{route},
			Env:                []string{"GC_SESSION_ID=session-1", "GC_SESSION_NAME=" + assignee, "GC_INSTANCE_TOKEN=instance-1"},
			DrainAck:           true,
			JSON:               true,
		},
		hookClaimExpectedTriggerOps{
			ResolveStore: func(string) (beads.Store, error) { return cache, nil },
		},
		&stdout,
		&stderr,
	)
	if code != 1 {
		t.Fatalf("code = %d, want fail-closed 1; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "does not match this session") {
		t.Fatalf("stderr = %q, want authoritative route refusal", stderr.String())
	}
}

func TestHookClaimExpectedTriggerAtomicallyRevalidatesExistingAssignmentBeforeSideEffects(t *testing.T) {
	const (
		assignee = "fixture/worker-1"
		other    = "fixture/worker-2"
		route    = "fixture/worker"
	)
	backing := beads.NewMemStore()
	seeded, err := backing.Create(beads.Bead{
		Title: "controller-claimed work",
		Type:  "task",
		Metadata: map[string]string{
			beadmeta.RoutedToMetadataKey: route,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	storeRef := "city:fixture-city"
	sessionSeed, err := backing.Create(hookTriggerSessionFixture(seeded.ID, storeRef, assignee, route))
	if err != nil {
		t.Fatal(err)
	}
	status := "in_progress"
	if err := backing.Update(seeded.ID, beads.UpdateOpts{
		Status:   &status,
		Assignee: stringPtr(assignee),
		Metadata: map[string]string{
			beadmeta.SessionIDMetadataKey:            sessionSeed.ID,
			beadmeta.SessionNameMetadataKey:          assignee,
			beadmeta.SessionInstanceTokenMetadataKey: "instance-1",
		},
	}); err != nil {
		t.Fatal(err)
	}
	initial, err := backing.Get(seeded.ID)
	if err != nil {
		t.Fatal(err)
	}
	var driftRevision int64
	selected := &hookTriggerDriftStore{
		MemStore: backing,
		first:    initial,
		afterGet: func() {
			if err := backing.Update(seeded.ID, beads.UpdateOpts{Assignee: stringPtr(other)}); err != nil {
				t.Fatalf("inject ownership drift: %v", err)
			}
			drifted, err := backing.Get(seeded.ID)
			if err != nil {
				t.Fatalf("read ownership drift: %v", err)
			}
			driftRevision = drifted.Revision
		},
	}
	var stdout, stderr bytes.Buffer
	code := claimHookExpectedTrigger(
		hookClaimTriggerExpectation{BeadID: seeded.ID, StoreRef: storeRef},
		hookStore{dir: "/fixture"},
		hookClaimOptions{
			Assignee:           assignee,
			IdentityCandidates: []string{assignee},
			RouteTargets:       []string{route},
			Env:                []string{"GC_SESSION_ID=" + sessionSeed.ID, "GC_SESSION_NAME=" + assignee, "GC_INSTANCE_TOKEN=instance-1"},
			DrainAck:           true,
			JSON:               true,
		},
		hookClaimExpectedTriggerOps{
			ResolveStore: func(string) (beads.Store, error) { return selected, nil },
		},
		&stdout,
		&stderr,
	)
	if code != 1 {
		t.Fatalf("code = %d, want fail-closed 1; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want no work result after ownership drift", stdout.String())
	}
	current, err := backing.Get(seeded.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Assignee != other {
		t.Fatalf("assignee = %q, want injected owner %q preserved", current.Assignee, other)
	}
	if current.Revision != driftRevision {
		t.Fatalf("revision = %d, want injected drift revision %d with no later mutation", current.Revision, driftRevision)
	}
}

func TestHookClaimExpectedTriggerAtomicallyRejectsSessionWitnessDriftBeforeSideEffects(t *testing.T) {
	const (
		assignee = "fixture/worker-1"
		route    = "fixture/worker"
		storeRef = "city:fixture-city"
	)
	backing := beads.NewMemStore()
	work, err := backing.Create(beads.Bead{
		Title:    "controller-claimed work",
		Type:     "task",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: route},
	})
	if err != nil {
		t.Fatal(err)
	}
	sessionSeed, err := backing.Create(hookTriggerSessionFixture(work.ID, storeRef, assignee, route))
	if err != nil {
		t.Fatal(err)
	}
	status := "in_progress"
	if err := backing.Update(work.ID, beads.UpdateOpts{
		Status:   &status,
		Assignee: stringPtr(assignee),
		Metadata: map[string]string{
			beadmeta.SessionIDMetadataKey:            sessionSeed.ID,
			beadmeta.SessionNameMetadataKey:          assignee,
			beadmeta.SessionInstanceTokenMetadataKey: "instance-1",
		},
	}); err != nil {
		t.Fatal(err)
	}
	initial, err := backing.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	selected := &hookTriggerDriftStore{
		MemStore:  backing,
		first:     initial,
		sessionID: sessionSeed.ID,
		afterSessionGet: func() {
			if err := backing.Update(sessionSeed.ID, beads.UpdateOpts{Metadata: map[string]string{
				"state": string(session.StateSuspended),
			}}); err != nil {
				t.Fatalf("inject session drift: %v", err)
			}
		},
	}
	var stdout, stderr bytes.Buffer
	code := claimHookExpectedTrigger(
		hookClaimTriggerExpectation{BeadID: work.ID, StoreRef: storeRef},
		hookStore{dir: "/fixture"},
		hookClaimOptions{
			Assignee:           assignee,
			IdentityCandidates: []string{assignee},
			RouteTargets:       []string{route},
			Env:                []string{"GC_SESSION_ID=" + sessionSeed.ID, "GC_SESSION_NAME=" + assignee, "GC_INSTANCE_TOKEN=instance-1"},
			DrainAck:           true,
			JSON:               true,
		},
		hookClaimExpectedTriggerOps{
			ResolveStore: func(string) (beads.Store, error) { return selected, nil },
		},
		&stdout,
		&stderr,
	)
	if code != 1 {
		t.Fatalf("code = %d, want fail-closed 1; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want no work result after session drift", stdout.String())
	}
	currentWork, err := backing.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if currentWork.Assignee != assignee || currentWork.Status != "in_progress" {
		t.Fatalf("work changed after session drift: %+v", currentWork)
	}
	if currentWork.Revision != initial.Revision {
		t.Fatalf("work revision = %d, want unchanged %d after session drift", currentWork.Revision, initial.Revision)
	}
}

func TestSelectHookClaimTriggerStoreUsesCanonicalScope(t *testing.T) {
	cityPath := t.TempDir()
	rigPath := filepath.Join(cityPath, "rigs", "fixture")
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "fixture", Path: rigPath}},
	}
	externalCityWorkDir := t.TempDir()
	externalRigWorkDir := t.TempDir()
	stores := []hookStore{
		{dir: externalCityWorkDir, env: []string{"STORE=city", "GC_CITY_PATH=" + cityPath, "GC_RIG=", "GC_RIG_ROOT="}},
		{dir: externalRigWorkDir, env: []string{"STORE=rig", "GC_CITY_PATH=" + cityPath, "GC_RIG=fixture", "GC_RIG_ROOT=" + rigPath}},
	}
	selected, err := selectHookClaimTriggerStore(
		stores,
		cityPath,
		"test-city",
		cfg,
		hookClaimTriggerExpectation{BeadID: "work-1", StoreRef: "rig:fixture"},
	)
	if err != nil {
		t.Fatalf("selectHookClaimTriggerStore: %v", err)
	}
	if selected.dir != externalRigWorkDir || len(selected.env) != 4 || selected.env[0] != "STORE=rig" {
		t.Fatalf("selected = %+v, want exact rig context with external real cwd", selected)
	}
	selected, err = selectHookClaimTriggerStore(
		stores,
		cityPath,
		"test-city",
		cfg,
		hookClaimTriggerExpectation{BeadID: "work-1", StoreRef: "city:test-city"},
	)
	if err != nil {
		t.Fatalf("select city trigger store: %v", err)
	}
	if selected.dir != externalCityWorkDir || len(selected.env) != 4 || selected.env[0] != "STORE=city" {
		t.Fatalf("selected city = %+v, want exact city context with external real cwd", selected)
	}
	if _, err := selectHookClaimTriggerStore(
		stores,
		cityPath,
		"test-city",
		cfg,
		hookClaimTriggerExpectation{BeadID: "work-1", StoreRef: "rig:other"},
	); err == nil {
		t.Fatal("unknown canonical store unexpectedly selected")
	}
}
