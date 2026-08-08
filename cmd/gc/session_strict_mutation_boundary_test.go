package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/beadstest"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
)

func TestReconcilerMutationBoundaryRejectsStrictDriftBeforePredicateOrSideEffect(t *testing.T) {
	for _, drift := range []struct {
		name string
		meta map[string]string
	}{
		{name: "clear", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "", beadmeta.TriggerBeadStoreRefMetadataKey: ""}},
		{name: "repoint", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "work-other"}},
		{name: "suspend", meta: map[string]string{"state": string(session.StateSuspended)}},
	} {
		t.Run(drift.name, func(t *testing.T) {
			backing := beads.NewMemStore()
			provider := runtime.NewFake()
			manager := session.NewManagerWithOptions(backing, provider)
			created, err := manager.CreateSession(context.Background(), session.CreateOptions{
				BeadOnly: true,
				Template: "strict-worker",
				Title:    "Strict Worker",
				Command:  "true",
				WorkDir:  t.TempDir(),
				Provider: "fake",
				ExtraMeta: map[string]string{
					"state":                                 string(session.StateActive),
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			captured, persisted, err := sessionFrontDoor(backing).GetPersistedResponse(created.ID)
			if err != nil {
				t.Fatalf("capture session: %v", err)
			}
			boundary := captureReconcilerMutationBoundary(captured, &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}, persisted.Revision)
			if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: drift.meta}); err != nil {
				t.Fatalf("inject drift: %v", err)
			}
			drifted, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("read drifted row: %v", err)
			}
			providerBaseline := len(provider.SnapshotCalls())
			predicateCalls := 0
			actionCalls := 0
			ran, err := boundary.run(
				backing,
				func(session.Info) (bool, error) {
					predicateCalls++
					return true, nil
				},
				func(current session.Info, front *session.Store) error {
					actionCalls++
					if err := provider.Stop(current.SessionNameMetadata); err != nil {
						return err
					}
					return front.SetMarker(current.ID, "forbidden_mutation", "true")
				},
			)
			if ran || !errors.Is(err, session.ErrConditionalMutationLost) {
				t.Fatalf("boundary after %s drift = (ran=%t, err=%v), want revision-CAS refusal", drift.name, ran, err)
			}
			if predicateCalls != 0 || actionCalls != 0 {
				t.Fatalf("boundary callbacks after %s drift = predicate %d action %d, want zero", drift.name, predicateCalls, actionCalls)
			}
			got, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("read refused row: %v", err)
			}
			if got.Revision != drifted.Revision || got.Metadata["forbidden_mutation"] != "" || got.Metadata[session.ConditionalMutationLeaseMetadataKey] != "" {
				t.Fatalf("session changed after %s drift: before=%+v after=%+v", drift.name, drifted, got)
			}
			if calls := provider.SnapshotCalls()[providerBaseline:]; len(calls) != 0 {
				t.Fatalf("provider mutations after %s drift = %+v, want none", drift.name, calls)
			}
		})
	}
}

func TestReconcilerMutationPairRejectsEitherStrictRowDriftBeforeRetirement(t *testing.T) {
	backing := beads.NewMemStore()
	provider := runtime.NewFake()
	manager := session.NewManagerWithOptions(backing, provider)
	cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
	makeSession := func(title, trigger string) session.Info {
		t.Helper()
		created, err := manager.CreateSession(context.Background(), session.CreateOptions{
			BeadOnly: true,
			Template: "strict-worker",
			Title:    title,
			Command:  "true",
			WorkDir:  t.TempDir(),
			Provider: "fake",
			ExtraMeta: map[string]string{
				"state":                                 string(session.StateActive),
				beadmeta.TriggerBeadIDMetadataKey:       trigger,
				beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
			},
		})
		if err != nil {
			t.Fatalf("CreateSession(%s): %v", title, err)
		}
		current, _, err := sessionFrontDoor(backing).GetPersistedResponse(created.ID)
		if err != nil {
			t.Fatalf("GetPersistedResponse(%s): %v", title, err)
		}
		return current
	}
	left := makeSession("left", "work-left")
	right := makeSession("right", "work-right")
	if err := backing.SetMetadata(right.ID, beadmeta.TriggerBeadIDMetadataKey, "work-repointed"); err != nil {
		t.Fatalf("repoint right: %v", err)
	}
	recorder := beadstest.NewRecordingStore(backing)
	predicateCalls := 0
	actionCalls := 0
	ran, err := runReconcilerMutationPair(
		captureReconcilerMutationBoundary(left, cfg),
		captureReconcilerMutationBoundary(right, cfg),
		recorder,
		func(session.Info, session.Info) (bool, error) {
			predicateCalls++
			return true, nil
		},
		func(currentLeft, currentRight session.Info, front *session.Store) error {
			actionCalls++
			if err := front.SetMarker(currentLeft.ID, "retired", "true"); err != nil {
				return err
			}
			return front.SetMarker(currentRight.ID, "winner", "true")
		},
	)
	if ran || !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) {
		t.Fatalf("pair boundary = (ran=%t, err=%v), want witnessed refusal", ran, err)
	}
	if predicateCalls != 0 || actionCalls != 0 {
		t.Fatalf("pair callbacks = predicate %d action %d, want zero", predicateCalls, actionCalls)
	}
	if calls := recorder.Calls(); len(calls) != 0 {
		t.Fatalf("pair mutations = %+v, want none", calls)
	}
}

func TestReconcilerMutationBoundarySeparatesWakeFromSuspendedLifecycle(t *testing.T) {
	backing := beads.NewMemStore()
	manager := session.NewManagerWithOptions(backing, runtime.NewFake())
	created, err := manager.CreateSession(context.Background(), session.CreateOptions{
		BeadOnly:     true,
		ExplicitName: "strict-worker",
		Template:     "strict-worker",
		Title:        "Strict Worker",
		Command:      "true",
		WorkDir:      t.TempDir(),
		Provider:     "fake",
		ExtraMeta: map[string]string{
			"state":                                 string(session.StateSuspended),
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	captured, persisted, err := sessionFrontDoor(backing).GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("capture session: %v", err)
	}
	boundary := captureReconcilerMutationBoundary(captured, &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}, persisted.Revision)

	wakeActions := 0
	ran, err := boundary.run(backing, nil, func(session.Info, *session.Store) error {
		wakeActions++
		return nil
	})
	if ran || !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) || wakeActions != 0 {
		t.Fatalf("suspended wake boundary = (ran=%t, err=%v, actions=%d), want witnessed refusal", ran, err, wakeActions)
	}

	lifecycleActions := 0
	ran, err = boundary.lifecycleMutation().run(backing, nil, func(session.Info, *session.Store) error {
		lifecycleActions++
		return nil
	})
	if err != nil || !ran || lifecycleActions != 1 {
		t.Fatalf("suspended lifecycle boundary = (ran=%t, err=%v, actions=%d), want one action", ran, err, lifecycleActions)
	}
}

func TestReconcilerMutationBoundaryAllowsExactDrainingLifecycleButNotWake(t *testing.T) {
	backing := beads.NewMemStore()
	manager := session.NewManagerWithOptions(backing, runtime.NewFake())
	created, err := manager.CreateSession(context.Background(), session.CreateOptions{
		BeadOnly: true,
		Template: "strict-worker",
		Title:    "Strict Worker",
		Command:  "true",
		WorkDir:  t.TempDir(),
		Provider: "fake",
		ExtraMeta: map[string]string{
			"state":                                 string(session.StateDraining),
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	captured, persisted, err := sessionFrontDoor(backing).GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("capture session: %v", err)
	}
	boundary := captureReconcilerMutationBoundary(captured, &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}, persisted.Revision)

	wakeActions := 0
	ran, err := boundary.run(backing, nil, func(session.Info, *session.Store) error {
		wakeActions++
		return nil
	})
	if ran || !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) || wakeActions != 0 {
		t.Fatalf("draining wake boundary = (ran=%t, err=%v, actions=%d), want witnessed refusal", ran, err, wakeActions)
	}

	lifecycleActions := 0
	ran, err = boundary.lifecycleMutation().run(backing, nil, func(session.Info, *session.Store) error {
		lifecycleActions++
		return nil
	})
	if err != nil || !ran || lifecycleActions != 1 {
		t.Fatalf("draining lifecycle boundary = (ran=%t, err=%v, actions=%d), want one action", ran, err, lifecycleActions)
	}
}

func TestReconcilerMutationBoundaryRejectsIncompleteCapturedTriggerBeforeCallbacks(t *testing.T) {
	for _, partial := range []session.Info{
		{ID: "session-id-only", State: session.StateActive, TriggerBeadID: "work-exact"},
		{ID: "session-ref-only", State: session.StateActive, TriggerBeadStoreRef: "city:fixture-city"},
	} {
		t.Run(partial.ID, func(t *testing.T) {
			backing := beads.NewMemStoreFrom(1, []beads.Bead{{
				ID:     partial.ID,
				Type:   session.BeadType,
				Status: "open",
				Labels: []string{session.LabelSession},
				Metadata: map[string]string{
					"state":                                 string(session.StateActive),
					beadmeta.TriggerBeadIDMetadataKey:       partial.TriggerBeadID,
					beadmeta.TriggerBeadStoreRefMetadataKey: partial.TriggerBeadStoreRef,
				},
			}}, nil)
			recorder := beadstest.NewRecordingStore(backing)
			predicateCalls := 0
			actionCalls := 0
			ran, err := captureReconcilerMutationBoundary(partial, &config.City{}).run(
				recorder,
				func(session.Info) (bool, error) {
					predicateCalls++
					return true, nil
				},
				func(session.Info, *session.Store) error {
					actionCalls++
					return nil
				},
			)
			if ran || !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) {
				t.Fatalf("partial boundary = (ran=%t, err=%v), want refusal", ran, err)
			}
			if predicateCalls != 0 || actionCalls != 0 {
				t.Fatalf("partial callbacks = predicate %d action %d, want zero", predicateCalls, actionCalls)
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("partial boundary mutations = %+v, want none", calls)
			}
		})
	}
}

func TestReconcilerMutationBoundaryRequiresCapturedZeroToRemainZero(t *testing.T) {
	makeRow := func(id string) beads.Bead {
		return beads.Bead{
			ID:     id,
			Type:   session.BeadType,
			Status: "open",
			Labels: []string{session.LabelSession},
			Metadata: map[string]string{
				"state": string(session.StateActive),
			},
		}
	}

	t.Run("single_zero_to_zero", func(t *testing.T) {
		backing := beads.NewMemStoreFrom(1, []beads.Bead{makeRow("session-zero")}, nil)
		captured, _, err := sessionFrontDoor(backing).GetPersistedResponse("session-zero")
		if err != nil {
			t.Fatalf("capture: %v", err)
		}
		actions := 0
		ran, err := captureReconcilerMutationBoundary(captured, &config.City{}).run(backing, nil, func(session.Info, *session.Store) error {
			actions++
			return nil
		})
		if err != nil || !ran || actions != 1 {
			t.Fatalf("zero->zero boundary = (ran=%t, actions=%d, err=%v), want one action", ran, actions, err)
		}
	})

	t.Run("single_zero_to_complete", func(t *testing.T) {
		backing := beads.NewMemStoreFrom(1, []beads.Bead{makeRow("session-zero")}, nil)
		captured, _, err := sessionFrontDoor(backing).GetPersistedResponse("session-zero")
		if err != nil {
			t.Fatalf("capture: %v", err)
		}
		if err := backing.Update("session-zero", beads.UpdateOpts{Metadata: map[string]string{
			beadmeta.TriggerBeadIDMetadataKey:       "work-new",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		}}); err != nil {
			t.Fatalf("bind new trigger: %v", err)
		}
		actions := 0
		ran, err := captureReconcilerMutationBoundary(captured, &config.City{}).run(backing, nil, func(session.Info, *session.Store) error {
			actions++
			return nil
		})
		if ran || !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) || actions != 0 {
			t.Fatalf("zero->complete boundary = (ran=%t, actions=%d, err=%v), want refusal", ran, actions, err)
		}
	})

	t.Run("pair_zero_truth_table", func(t *testing.T) {
		backing := beads.NewMemStoreFrom(1, []beads.Bead{makeRow("session-left"), makeRow("session-right")}, nil)
		left, _, err := sessionFrontDoor(backing).GetPersistedResponse("session-left")
		if err != nil {
			t.Fatalf("capture left: %v", err)
		}
		right, _, err := sessionFrontDoor(backing).GetPersistedResponse("session-right")
		if err != nil {
			t.Fatalf("capture right: %v", err)
		}
		leftBoundary := captureReconcilerMutationBoundary(left, &config.City{})
		rightBoundary := captureReconcilerMutationBoundary(right, &config.City{})
		actions := 0
		ran, err := runReconcilerMutationPair(leftBoundary, rightBoundary, backing, nil, func(session.Info, session.Info, *session.Store) error {
			actions++
			return nil
		})
		if err != nil || !ran || actions != 1 {
			t.Fatalf("zero pair = (ran=%t, actions=%d, err=%v), want one action", ran, actions, err)
		}
		if err := backing.Update("session-right", beads.UpdateOpts{Metadata: map[string]string{
			beadmeta.TriggerBeadIDMetadataKey:       "work-new",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		}}); err != nil {
			t.Fatalf("bind right: %v", err)
		}
		actions = 0
		ran, err = runReconcilerMutationPair(leftBoundary, rightBoundary, backing, nil, func(session.Info, session.Info, *session.Store) error {
			actions++
			return nil
		})
		if ran || !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) || actions != 0 {
			t.Fatalf("zero pair after bind = (ran=%t, actions=%d, err=%v), want refusal", ran, actions, err)
		}
	})
}

func TestCapturedSessionIdentityIgnoresBackendLabelOrder(t *testing.T) {
	captured := session.Info{ID: "session-label-order", Type: session.BeadType, Labels: []string{"gc:session", "agent:worker", "pool:slot"}}
	current := captured
	current.Labels = []string{"pool:slot", "gc:session", "agent:worker"}
	if err := validateCapturedSessionIdentity(captured, current); err != nil {
		t.Fatalf("reordered labels rejected: %v", err)
	}
	current.Labels = []string{"pool:slot", "gc:session", "agent:other"}
	if err := validateCapturedSessionIdentity(captured, current); !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) {
		t.Fatalf("changed label set error=%v, want identity mismatch", err)
	}
}

func TestAutomaticBoundaryAndStartCandidateRejectConflictingPersistedPolicy(t *testing.T) {
	store := beads.NewMemStore()
	row, err := store.Create(beads.Bead{
		Title:  "conflicting-worker",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"template":                              "inherit-worker",
			"agent_name":                            "strict-worker",
			"session_name":                          "conflicting-worker-1",
			"state":                                 string(session.StateActive),
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	info, persisted, err := sessionFrontDoor(store).GetPersistedResponse(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{Agents: []config.Agent{
		{Name: "inherit-worker", ProjectHooks: config.ProjectHooksInherit},
		{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid},
	}}
	actions := 0
	ran, err := captureReconcilerMutationBoundary(info, cfg, persisted.Revision).run(store, nil, func(session.Info, *session.Store) error {
		actions++
		return nil
	})
	if !errors.Is(err, agentutil.ErrPersistedSessionIdentityConflict) || ran || actions != 0 {
		t.Fatalf("boundary=(ran=%t actions=%d err=%v), want conflict refusal", ran, actions, err)
	}
	got, err := store.Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != row.Revision || got.Metadata[session.ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("conflict mutated row: before=%+v after=%+v", row, got)
	}

	candidate := captureStartCandidateForWake(info, TemplateParams{}, 0, cfg, "work-exact", persisted.Revision)
	if !candidate.strictLaunch || !errors.Is(candidate.policyErr, agentutil.ErrPersistedSessionIdentityConflict) {
		t.Fatalf("candidate policy=(strict=%t err=%v), want fail-closed conflict", candidate.strictLaunch, candidate.policyErr)
	}
	if err := candidate.validateAutomaticWakeCurrent(info); !errors.Is(err, agentutil.ErrPersistedSessionIdentityConflict) {
		t.Fatalf("candidate validation error=%v, want conflict", err)
	}
}

func TestSessionCircuitProgressBoundaryRejectsTriggerOrSuspensionDriftBeforeMemoryOrStoreMutation(t *testing.T) {
	for _, drift := range []struct {
		name string
		meta map[string]string
	}{
		{name: "clear", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "", beadmeta.TriggerBeadStoreRefMetadataKey: ""}},
		{name: "repoint", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "work-other"}},
		{name: "suspend", meta: map[string]string{"state": string(session.StateSuspended)}},
	} {
		t.Run(drift.name, func(t *testing.T) {
			backing := beads.NewMemStore()
			manager := session.NewManagerWithOptions(backing, runtime.NewFake())
			created, err := manager.CreateSession(context.Background(), session.CreateOptions{
				BeadOnly: true,
				Template: "strict-worker",
				Title:    "Strict Worker",
				Command:  "true",
				WorkDir:  t.TempDir(),
				Provider: "fake",
				ExtraMeta: map[string]string{
					"state":                                 string(session.StateActive),
					namedSessionMetadataKey:                 "true",
					namedSessionIdentityMetadata:            "strict-worker",
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			captured, persisted, err := sessionFrontDoor(backing).GetPersistedResponse(created.ID)
			if err != nil {
				t.Fatalf("capture session: %v", err)
			}
			if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: drift.meta}); err != nil {
				t.Fatalf("inject drift: %v", err)
			}
			recorder := beadstest.NewRecordingStore(backing)
			cb := newSessionCircuitBreaker(sessionCircuitBreakerConfig{})
			now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
			before := cb.Snapshot(now)
			changed, err := observeSessionCircuitProgressAtBoundary(
				captured,
				&config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}},
				recorder,
				cb,
				"strict-worker",
				"work-exact:open",
				now,
				captureReconcilerMutationBoundary(
					captured,
					&config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}},
					persisted.Revision,
				),
			)
			if changed || !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) {
				t.Fatalf("circuit boundary after %s = (changed=%t, err=%v), want refusal", drift.name, changed, err)
			}
			if after := cb.Snapshot(now); !reflect.DeepEqual(after, before) {
				t.Fatalf("circuit memory changed after %s: before=%+v after=%+v", drift.name, before, after)
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("circuit store mutations after %s = %+v, want none", drift.name, calls)
			}
		})
	}
}

func TestRateLimitBoundaryUsesProviderPeekWithoutReenteringSessionLock(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	backing := beads.NewMemStore()
	provider := runtime.NewFake()
	manager := session.NewManagerWithOptions(backing, provider)
	created, err := manager.CreateSession(context.Background(), session.CreateOptions{
		BeadOnly:     true,
		ExplicitName: "strict-worker",
		Template:     "strict-worker",
		Title:        "Strict Worker",
		Command:      "true",
		WorkDir:      t.TempDir(),
		Provider:     "fake",
		ExtraMeta: map[string]string{
			"state":                                 string(session.StateActive),
			"last_woke_at":                          now.Add(-10 * time.Second).Format(time.RFC3339),
			"pending_create_claim":                  "",
			"pending_create_started_at":             "",
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	current, _, err := sessionFrontDoor(backing).GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("GetPersistedResponse: %v", err)
	}
	provider.SetPeekOutput(current.SessionNameMetadata, "You've hit your limit, Pro plan\n\n/rate-limit-options")
	peek := cachedSessionPeek("", backing, provider, &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}, current, nil)
	type result struct {
		hit bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, hit, err := checkRateLimitStabilityAtBoundary(
			current,
			&config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}},
			backing,
			provider,
			false,
			newDrainTracker(),
			&clock.Fake{Time: now},
			peek,
		)
		done <- result{hit: hit, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil || !got.hit {
			t.Fatalf("rate-limit boundary = (hit=%t, err=%v), want handled", got.hit, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rate-limit boundary deadlocked by re-entering the session mutation lock")
	}
}
