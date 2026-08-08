package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
)

// storeWithoutGuard deliberately hides optional guarded-claim capabilities.
// It models bd-backed stores, whose CLI cannot atomically bind an explicit
// actor while checking route, hold, and session-witness predicates.
type storeWithoutGuard struct{ beads.Store }

// reopenBeforeGuardedClaimStore deterministically models an external actor
// reopening recovered work after the controller's live point read but before
// the backing claim transaction. Recovery must be a read-only idempotent
// verification, so the transaction may not reclaim the newly open row.
type reopenBeforeGuardedClaimStore struct {
	beads.Store
	claimCalls        int
	requireIdempotent bool
}

func (s *reopenBeforeGuardedClaimStore) GuardedAssignmentClaimerHandle() (beads.GuardedAssignmentClaimer, bool) {
	return s, true
}

func (s *reopenBeforeGuardedClaimStore) ClaimAssignment(ctx context.Context, req beads.AssignmentClaimRequest) (beads.Bead, bool, error) {
	s.claimCalls++
	s.requireIdempotent = req.RequireIdempotent
	status := "open"
	assignee := ""
	if err := s.Update(req.ID, beads.UpdateOpts{Status: &status, Assignee: &assignee}); err != nil {
		return beads.Bead{}, false, err
	}
	claimer, ok := beads.GuardedAssignmentClaimerFor(s.Store)
	if !ok {
		return beads.Bead{}, false, beads.ErrGuardedAssignmentClaimUnsupported
	}
	return claimer.ClaimAssignment(ctx, req)
}

// postClaimHoldStore models the native provider's ambiguous-commit recovery
// boundary: the exact assignment commits without a hold, then a canonical hold
// is added before the authoritative post-state is returned to the controller.
// The store-level guarded predicate authorized the mutation, so the later hold
// must remain transparent to the now-owned Tier-1/Tier-2 work.
type postClaimHoldStore struct {
	beads.Store
	holdLabel string
	claims    int
}

func (s *postClaimHoldStore) GuardedAssignmentClaimerHandle() (beads.GuardedAssignmentClaimer, bool) {
	return s, true
}

func (s *postClaimHoldStore) ConditionalWritesResolveTarget() beads.Store {
	return s.Store
}

func (s *postClaimHoldStore) ConditionalWriterHandle() (beads.ConditionalWriter, bool) {
	return beads.ConditionalWriterFor(s.Store)
}

func (s *postClaimHoldStore) ClaimAssignment(_ context.Context, req beads.AssignmentClaimRequest) (beads.Bead, bool, error) {
	s.claims++
	status := "in_progress"
	actor := req.Actor
	if err := s.Update(req.ID, beads.UpdateOpts{
		Status:   &status,
		Assignee: &actor,
		Labels:   []string{s.holdLabel},
		Metadata: req.AssignmentMetadata,
	}); err != nil {
		return beads.Bead{}, false, err
	}
	claimed, err := s.Get(req.ID)
	if err != nil {
		return beads.Bead{}, false, err
	}
	return claimed, true, nil
}

func seedGuardedClaimPoolSession(t *testing.T, store beads.Store) session.Info {
	t.Helper()
	created, err := store.Create(beads.Bead{
		Title:  "worker-1",
		Type:   sessionBeadType,
		Status: "open",
		Labels: []string{sessionBeadLabel},
		Metadata: map[string]string{
			"template":             "worker",
			"agent_name":           "worker-1",
			"alias":                "worker-1",
			"session_name":         "session-worker-1",
			"instance_token":       "instance-worker-1",
			"state":                "creating",
			"pool_slot":            "1",
			poolManagedMetadataKey: "true",
		},
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	info, err := sessionFrontDoor(store).Get(created.ID)
	if err != nil {
		t.Fatalf("load session: %v", err)
	}
	return info
}

func guardedClaimBuildParams(t *testing.T, sessionStore beads.Store, workStore beads.Store) (*agentBuildParams, *config.City, session.Info, *bytes.Buffer) {
	t.Helper()
	externalRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize isolated workdir root: %v", err)
	}
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Agents: []config.Agent{{
			Name:              "worker",
			StartCommand:      "true",
			ProjectHooks:      config.ProjectHooksForbid,
			WorkDir:           filepath.Join(externalRoot, "{{.Agent}}"),
			MinActiveSessions: intPtr(0),
			MaxActiveSessions: intPtr(2),
		}},
	}
	info := seedGuardedClaimPoolSession(t, sessionStore)
	stderr := &bytes.Buffer{}
	sp := &templateProjectHookIsolationProvider{Fake: runtime.NewFake()}
	bp := newAgentBuildParamsWithStores(
		"fixture-city",
		t.TempDir(),
		cfg,
		sp,
		time.Now().UTC(),
		beads.SessionStore{Store: sessionStore},
		beads.WorkStore{Store: workStore},
		stderr,
	)
	bp.sessionBeads = &sessionBeadSnapshot{}
	bp.sessionBeads.addInfo(info)
	bp.rigStores = map[string]beads.Store{"work": workStore}
	return bp, cfg, info, stderr
}

func createGuardedClaimWork(t *testing.T, store beads.Store, route string, labels ...string) beads.Bead {
	t.Helper()
	work, err := store.Create(beads.Bead{
		Title:  "exact routed work",
		Type:   "task",
		Status: "open",
		Labels: labels,
		Metadata: map[string]string{
			beadmeta.RoutedToMetadataKey: route,
		},
	})
	if err != nil {
		t.Fatalf("create work: %v", err)
	}
	return work
}

func TestClaimPoolRequestWorkAtomicallyRejectsSessionWitnessDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, store beads.Store, info session.Info)
	}{
		{name: "closed row", mutate: func(t *testing.T, store beads.Store, info session.Info) {
			if err := store.Close(info.ID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "instance token rotated", mutate: func(t *testing.T, store beads.Store, info session.Info) {
			if err := store.SetMetadata(info.ID, "instance_token", "rotated-token"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "lifecycle state changed", mutate: func(t *testing.T, store beads.Store, info session.Info) {
			if err := store.SetMetadata(info.ID, "state", "awake"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "alias changed", mutate: func(t *testing.T, store beads.Store, info session.Info) {
			if err := store.SetMetadata(info.ID, "alias", "other-worker"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "configured identity appeared", mutate: func(t *testing.T, store beads.Store, info session.Info) {
			if err := store.SetMetadata(info.ID, session.NamedSessionIdentityMetadata, "named-worker"); err != nil {
				t.Fatal(err)
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := beads.NewMemStore()
			bp, _, info, _ := guardedClaimBuildParams(t, store, store)
			work := createGuardedClaimWork(t, store, "worker")
			before, err := store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			tt.mutate(t, store, info)

			_, err = claimPoolRequestWork(bp, info, SessionRequest{
				Template:     "worker",
				Tier:         "new",
				WorkBeadID:   work.ID,
				WorkStoreRef: "city:fixture-city",
				WorkRouteKey: beadmeta.RoutedToMetadataKey,
				WorkRoute:    "worker",
			})
			if err == nil {
				t.Fatal("claim accepted a drifted session witness")
			}
			after, getErr := store.Get(work.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if after.Revision != before.Revision || after.Status != before.Status || after.Assignee != before.Assignee ||
				after.Metadata[beadmeta.SessionIDMetadataKey] != before.Metadata[beadmeta.SessionIDMetadataKey] {
				t.Fatalf("session witness drift mutated work: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestClaimPoolRequestWorkRejectsNonCoLocatedRigStoreBeforeMutation(t *testing.T) {
	sessionStore := beads.NewMemStore()
	workStore := beads.NewMemStore()
	bp, _, info, _ := guardedClaimBuildParams(t, sessionStore, workStore)
	work := createGuardedClaimWork(t, workStore, "worker")
	before, err := workStore.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = claimPoolRequestWork(bp, info, SessionRequest{
		Template:     "worker",
		Tier:         "new",
		WorkBeadID:   work.ID,
		WorkStoreRef: "rig:work",
		WorkRouteKey: beadmeta.RoutedToMetadataKey,
		WorkRoute:    "worker",
	})
	if err == nil || !strings.Contains(err.Error(), "co-located") {
		t.Fatalf("claim error = %v, want explicit non-co-located rig-store failure", err)
	}
	after, getErr := workStore.Get(work.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if after.Revision != before.Revision || after.Status != before.Status || after.Assignee != before.Assignee {
		t.Fatalf("cross-store rejection mutated work: before=%+v after=%+v", before, after)
	}
}

func TestForbiddenCityClaimRejectsSplitSessionAndWorkStoresBeforeAnyMutation(t *testing.T) {
	sessionStore := beads.NewMemStore()
	workStore := beads.NewMemStore()
	bp, cfg, info, stderr := guardedClaimBuildParams(t, sessionStore, workStore)
	cfg.Storage = &config.StorageConfig{Classes: config.StorageClasses{
		Work:      config.StorageWorkBinding,
		Graph:     "infra",
		Sessions:  "infra",
		Messaging: "infra",
		Orders:    "infra",
		Nudges:    "infra",
	}}

	wrongWork := createGuardedClaimWork(t, sessionStore, "worker")
	_ = createGuardedClaimWork(t, workStore, "other")
	realWork := createGuardedClaimWork(t, workStore, "worker")
	if wrongWork.ID != realWork.ID {
		t.Fatalf("fixture IDs differ: session-store duplicate = %q, real work = %q", wrongWork.ID, realWork.ID)
	}

	beforeSession, err := sessionStore.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeWrongWork, err := sessionStore.Get(wrongWork.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeRealWork, err := workStore.Get(realWork.ID)
	if err != nil {
		t.Fatal(err)
	}

	request := SessionRequest{
		Template:      "worker",
		Tier:          "new",
		SessionBeadID: info.ID,
		WorkBeadID:    realWork.ID,
		WorkStoreRef:  "city:fixture-city",
		WorkRouteKey:  beadmeta.RoutedToMetadataKey,
		WorkRoute:     "worker",
	}
	if _, err := loadPoolRequestWorkTarget(bp, request); !errors.Is(err, beads.ErrGuardedAssignmentClaimUnsupported) {
		t.Errorf("loadPoolRequestWorkTarget() error = %v, want %v", err, beads.ErrGuardedAssignmentClaimUnsupported)
	}

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{request},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Errorf("desired sessions = %d, want zero; stderr=%q", len(desired), stderr.String())
	}
	if !strings.Contains(stderr.String(), errStrictSessionWorkStoreMismatch.Error()) {
		t.Errorf("stderr = %q, want physical store-identity diagnostic", stderr.String())
	}
	for name, check := range map[string]struct {
		store  beads.Store
		id     string
		before beads.Bead
	}{
		"session":    {store: sessionStore, id: info.ID, before: beforeSession},
		"wrong work": {store: sessionStore, id: wrongWork.ID, before: beforeWrongWork},
		"real work":  {store: workStore, id: realWork.ID, before: beforeRealWork},
	} {
		after, getErr := check.store.Get(check.id)
		if getErr != nil {
			t.Errorf("load %s after rejection: %v", name, getErr)
			continue
		}
		if !reflect.DeepEqual(after, check.before) {
			t.Errorf("%s mutated across split-store rejection: before=%+v after=%+v", name, check.before, after)
		}
	}
	provider, ok := bp.sp.(*templateProjectHookIsolationProvider)
	if !ok {
		t.Fatalf("provider = %T, want templateProjectHookIsolationProvider", bp.sp)
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Errorf("runtime provider calls = %#v, want zero", calls)
	}
}

func TestRealizePoolDesiredSessionsRejectsNonCoLocatedClaimBeforeSessionReservation(t *testing.T) {
	sessionStore := beads.NewMemStore()
	workStore := beads.NewMemStore()
	externalRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize isolated workdir root: %v", err)
	}
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Agents: []config.Agent{{
			Name:              "worker",
			StartCommand:      "true",
			ProjectHooks:      config.ProjectHooksForbid,
			WorkDir:           filepath.Join(externalRoot, "{{.Agent}}"),
			MinActiveSessions: intPtr(0),
			MaxActiveSessions: intPtr(2),
		}},
	}
	stderr := &bytes.Buffer{}
	bp := newAgentBuildParams(
		"fixture-city",
		t.TempDir(),
		cfg,
		&templateProjectHookIsolationProvider{Fake: runtime.NewFake()},
		time.Now().UTC(),
		sessionStore,
		stderr,
	)
	bp.sessionBeads = &sessionBeadSnapshot{}
	bp.rigStores = map[string]beads.Store{"work": workStore}
	work := createGuardedClaimWork(t, workStore, "worker")
	before, err := workStore.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:     "worker",
			Tier:         "new",
			WorkBeadID:   work.ID,
			WorkStoreRef: "rig:work",
			WorkRouteKey: beadmeta.RoutedToMetadataKey,
			WorkRoute:    "worker",
		}},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want zero for non-co-located claim", len(desired))
	}
	sessions, err := sessionStore.List(beads.ListQuery{Type: sessionBeadType, IncludeClosed: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 0 {
		t.Fatalf("session rows = %+v, want no reservation before unsupported cross-store claim", sessions)
	}
	after, err := workStore.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != before.Revision || after.Status != before.Status || after.Assignee != before.Assignee {
		t.Fatalf("cross-store preflight mutated work: before=%+v after=%+v", before, after)
	}
}

func TestRealizePoolDesiredSessionsGuardedClaimPrecedesTriggerBindAndLaunch(t *testing.T) {
	store := beads.NewMemStore()
	bp, cfg, info, stderr := guardedClaimBuildParams(t, store, store)
	work := createGuardedClaimWork(t, store, "worker")
	desired := map[string]TemplateParams{}

	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:      "worker",
			Tier:          "new",
			SessionBeadID: info.ID,
			WorkBeadID:    work.ID,
			WorkStoreRef:  "city:fixture-city",
			WorkRouteKey:  beadmeta.RoutedToMetadataKey,
			WorkRoute:     "worker",
		}},
	}, desired, stderr)

	if len(desired) != 1 {
		t.Fatalf("desired sessions = %d, want 1 after exact guarded claim; stderr=%q", len(desired), stderr.String())
	}
	claimed, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status != "in_progress" || claimed.Assignee != "worker-1" {
		t.Fatalf("claimed work = status %q assignee %q, want in_progress/worker-1", claimed.Status, claimed.Assignee)
	}
	for key, want := range map[string]string{
		beadmeta.SessionIDMetadataKey:            info.ID,
		beadmeta.SessionNameMetadataKey:          info.SessionNameMetadata,
		beadmeta.SessionInstanceTokenMetadataKey: info.InstanceToken,
	} {
		if got := claimed.Metadata[key]; got != want {
			t.Fatalf("claimed work metadata[%q] = %q, want %q", key, got, want)
		}
	}
	bound, err := store.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := bound.Metadata[beadmeta.TriggerBeadIDMetadataKey]; got != work.ID {
		t.Fatalf("session trigger = %q, want %q after guarded claim", got, work.ID)
	}
}

func TestRealizePoolDesiredSessionsAcceptsCanonicalHoldAddedAfterAtomicClaim(t *testing.T) {
	for _, holdLabel := range beadmeta.DispatchHoldLabels {
		t.Run(holdLabel, func(t *testing.T) {
			backing := beads.NewMemStore()
			store := &postClaimHoldStore{Store: backing, holdLabel: holdLabel}
			bp, cfg, info, stderr := guardedClaimBuildParams(t, store, store)
			work := createGuardedClaimWork(t, store, "worker")
			desired := map[string]TemplateParams{}

			realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
				Template: "worker",
				Requests: []SessionRequest{{
					Template:      "worker",
					Tier:          "new",
					SessionBeadID: info.ID,
					WorkBeadID:    work.ID,
					WorkStoreRef:  "city:fixture-city",
					WorkRouteKey:  beadmeta.RoutedToMetadataKey,
					WorkRoute:     "worker",
				}},
			}, desired, stderr)

			if store.claims != 1 {
				t.Fatalf("guarded claim calls = %d, want 1; stderr=%q", store.claims, stderr.String())
			}
			if len(desired) != 1 {
				t.Fatalf("desired sessions = %d, want 1 after authorized claim; stderr=%q", len(desired), stderr.String())
			}
			if bp.suppressedSessionBeadIDs[info.ID] {
				t.Fatalf("authorized session %q was suppressed after post-claim hold", info.ID)
			}
			claimed, err := backing.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			if claimed.Status != "in_progress" || claimed.Assignee != "worker-1" ||
				!beadmeta.HasDispatchHoldLabel(claimed.Labels) {
				t.Fatalf("authoritative claimed work = %+v, want owned work with canonical hold", claimed)
			}
			bound, err := sessionFrontDoor(backing).Get(info.ID)
			if err != nil {
				t.Fatal(err)
			}
			rawSession, err := backing.Get(info.ID)
			if err != nil {
				t.Fatal(err)
			}
			if rawSession.Status != "open" || bound.TriggerBeadID != work.ID ||
				bound.TriggerBeadStoreRef != "city:fixture-city" {
				t.Fatalf("session after post-claim hold = %+v, want open exact trigger binding", bound)
			}
		})
	}
}

// TestCityDemandCarriesCanonicalStoreRefThroughClaimAndHookFence pins the
// complete city-store path. The default demand probe must not emit the legacy
// bare "city" shorthand: that value can reach the atomic controller claim but
// is intentionally rejected by the runtime hook's canonical store fence.
func TestCityDemandCarriesCanonicalStoreRefThroughClaimAndHookFence(t *testing.T) {
	cityPath := t.TempDir()
	maxActive := 1
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Agents: []config.Agent{{
			Name:              "worker",
			StartCommand:      "true",
			MaxActiveSessions: &maxActive,
		}},
	}
	store := beads.NewMemStore()
	work := createGuardedClaimWork(t, store, "worker")

	counts, demand, partial, errs := defaultScaleCheckCountsAndDemand(cfg, []defaultScaleCheckTarget{
		defaultScaleCheckTargetForAgent(cityPath, cfg, &cfg.Agents[0], store, nil),
	}, newReadyDemandCache())
	if len(errs) != 0 || len(partial) != 0 {
		t.Fatalf("demand probe errs=%v partial=%v, want complete", errs, partial)
	}
	const canonicalRef = "city:fixture-city"
	if got := demand["worker"].StoreRefs[work.ID]; got != canonicalRef {
		t.Fatalf("demand store ref = %q, want %q", got, canonicalRef)
	}
	states := ComputePoolDesiredStatesWithDemandTraced(cfg, nil, nil, counts, demand, nil)
	if len(states) != 1 || len(states[0].Requests) != 1 || states[0].Requests[0].WorkStoreRef != canonicalRef {
		t.Fatalf("pool states = %+v, want one request carrying %q", states, canonicalRef)
	}

	stderr := &bytes.Buffer{}
	bp := newAgentBuildParams("fixture-city", cityPath, cfg, runtime.NewFake(), time.Now().UTC(), store, stderr)
	bp.sessionBeads = &sessionBeadSnapshot{}
	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], states[0], desired, stderr)
	if len(desired) != 1 {
		t.Fatalf("desired sessions = %d, want 1; stderr=%q", len(desired), stderr.String())
	}
	infos := bp.sessionBeads.OpenInfos()
	if len(infos) != 1 {
		t.Fatalf("session infos = %+v, want one", infos)
	}
	info, err := sessionFrontDoor(store).Get(infos[0].ID)
	if err != nil {
		t.Fatalf("load claimed session: %v", err)
	}
	if info.TriggerBeadID != work.ID || info.TriggerBeadStoreRef != canonicalRef {
		t.Fatalf("persisted trigger = (%q, %q), want (%q, %q)", info.TriggerBeadID, info.TriggerBeadStoreRef, work.ID, canonicalRef)
	}
	expectation, err := hookClaimTriggerExpectationForSession(info, work.ID, canonicalRef)
	if err != nil {
		t.Fatalf("hook trigger expectation: %v", err)
	}
	if expectation != (hookClaimTriggerExpectation{BeadID: work.ID, StoreRef: canonicalRef}) {
		t.Fatalf("hook expectation = %+v, want exact city trigger", expectation)
	}
	selected, err := selectHookClaimTriggerStore(
		[]hookStore{{dir: cityPath, env: []string{"GC_CITY_PATH=" + cityPath}}},
		cityPath,
		"fixture-city",
		cfg,
		expectation,
	)
	if err != nil || selected.dir != cityPath {
		t.Fatalf("select canonical city trigger store = (%+v, %v), want %q", selected, err, cityPath)
	}
}

func TestRealizePoolDesiredSessionsGuardedClaimMismatchPreventsMutationAndLaunch(t *testing.T) {
	tests := []struct {
		name         string
		workRoute    string
		labels       []string
		storeRef     string
		wrapStore    bool
		requestID    func(beads.Bead) string
		requestRoute string
	}{
		{name: "route drift", workRoute: "other", storeRef: "city:fixture-city", requestRoute: "worker"},
		{name: "canonical hold", workRoute: "worker", labels: []string{beadmeta.HoldMayorLabel}, storeRef: "city:fixture-city", requestRoute: "worker"},
		{name: "wrong exact id", workRoute: "worker", storeRef: "city:fixture-city", requestRoute: "worker", requestID: func(beads.Bead) string { return "missing-work" }},
		{name: "wrong exact store", workRoute: "worker", storeRef: "rig:missing", requestRoute: "worker"},
		{name: "unsupported store", workRoute: "worker", storeRef: "city:fixture-city", requestRoute: "worker", wrapStore: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workMem := beads.NewMemStore()
			var sessionStore beads.Store = workMem
			if tt.wrapStore {
				sessionStore = storeWithoutGuard{Store: workMem}
			}
			bp, cfg, info, stderr := guardedClaimBuildParams(t, sessionStore, sessionStore)
			work := createGuardedClaimWork(t, workMem, tt.workRoute, tt.labels...)
			requestID := work.ID
			if tt.requestID != nil {
				requestID = tt.requestID(work)
			}
			beforeWork, err := workMem.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			desired := map[string]TemplateParams{}

			realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
				Template: "worker",
				Requests: []SessionRequest{{
					Template:      "worker",
					Tier:          "new",
					SessionBeadID: info.ID,
					WorkBeadID:    requestID,
					WorkStoreRef:  tt.storeRef,
					WorkRouteKey:  beadmeta.RoutedToMetadataKey,
					WorkRoute:     tt.requestRoute,
				}},
			}, desired, stderr)

			if len(desired) != 0 {
				t.Fatalf("desired sessions = %d, want 0 on guarded mismatch; stderr=%q", len(desired), stderr.String())
			}
			afterWork, err := workMem.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			if afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee ||
				afterWork.Metadata[beadmeta.SessionIDMetadataKey] != "" ||
				afterWork.Metadata[beadmeta.SessionInstanceTokenMetadataKey] != "" {
				t.Fatalf("guarded mismatch mutated work: before=%+v after=%+v", beforeWork, afterWork)
			}
			afterSession, err := sessionStore.Get(info.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got := afterSession.Metadata[beadmeta.TriggerBeadIDMetadataKey]; got != "" {
				t.Fatalf("trigger bind ran before guarded claim: got %q", got)
			}
			if !strings.Contains(stderr.String(), "exact work target preflight") {
				t.Fatalf("stderr = %q, want exact-target preflight diagnostic", stderr.String())
			}
		})
	}
}

func TestRealizePoolDesiredSessionsFreshGuardFailureCreatesNoSessionReservation(t *testing.T) {
	store := beads.NewMemStore()
	work := createGuardedClaimWork(t, store, "worker", beadmeta.HoldExternalLabel)
	cityPath := t.TempDir()
	externalRoot := t.TempDir()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Agents: []config.Agent{{
			Name:              "worker",
			StartCommand:      "true",
			ProjectHooks:      config.ProjectHooksForbid,
			WorkDir:           filepath.Join(externalRoot, "{{.AgentBase}}"),
			MinActiveSessions: intPtr(0),
			MaxActiveSessions: intPtr(2),
		}},
	}
	stderr := &bytes.Buffer{}
	sp := &templateProjectHookIsolationProvider{Fake: runtime.NewFake()}
	bp := newAgentBuildParams("fixture-city", cityPath, cfg, sp, time.Now().UTC(), store, stderr)
	bp.sessionBeads = &sessionBeadSnapshot{}
	desired := map[string]TemplateParams{}

	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:     "worker",
			Tier:         "new",
			WorkBeadID:   work.ID,
			WorkStoreRef: "city:fixture-city",
			WorkRouteKey: beadmeta.RoutedToMetadataKey,
			WorkRoute:    "worker",
		}},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want 0; stderr=%q", len(desired), stderr.String())
	}
	created, err := store.List(beads.ListQuery{Type: sessionBeadType, IncludeClosed: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(created) != 0 {
		t.Fatalf("failed exact-target preflight created session reservations: %+v", created)
	}
}

// TestRecoveredTriggerlessCreateClaimsExactDemandBeforeLaunch reproduces the
// crash window after Phase B persisted a creating session but before Phase C
// claimed its selected work. The next desired-state pass must attach the exact
// indexed demand witness to that durable in-flight session instead of treating
// the empty trigger as permission to launch anonymously.
func TestRecoveredTriggerlessCreateClaimsExactDemandBeforeLaunch(t *testing.T) {
	store := beads.NewMemStore()
	bp, cfg, info, stderr := guardedClaimBuildParams(t, store, store)
	work := createGuardedClaimWork(t, store, "worker")
	demand := map[string]scaleCheckDemand{
		"worker": {
			Witnesses: []scaleCheckDemandWitness{{
				ID: work.ID, StoreRef: "city:fixture-city",
				RouteKey: beadmeta.RoutedToMetadataKey, Route: "worker",
			}},
		},
	}

	states := ComputePoolDesiredStatesWithDemandTraced(
		cfg,
		nil,
		[]session.Info{info},
		map[string]int{"worker": 1},
		demand,
		nil,
	)
	if len(states) != 1 || len(states[0].Requests) != 1 {
		t.Fatalf("pool states = %+v, want one recovered request", states)
	}
	request := states[0].Requests[0]
	if request.SessionBeadID != info.ID || request.WorkBeadID != work.ID ||
		request.WorkStoreRef != "city:fixture-city" || request.WorkRouteKey != beadmeta.RoutedToMetadataKey ||
		request.WorkRoute != "worker" {
		t.Fatalf("recovered request = %+v, want exact session/bead/store/route witness", request)
	}

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], states[0], desired, stderr)
	if len(desired) != 1 {
		t.Fatalf("desired sessions = %d, want 1 after recovered exact claim; stderr=%q", len(desired), stderr.String())
	}
	claimed, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status != "in_progress" || claimed.Assignee != "worker-1" {
		t.Fatalf("recovered work = status %q assignee %q, want in_progress/worker-1", claimed.Status, claimed.Assignee)
	}
	bound, err := store.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if bound.Metadata[beadmeta.TriggerBeadIDMetadataKey] != work.ID ||
		bound.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey] != "city:fixture-city" {
		t.Fatalf("recovered trigger = (%q, %q), want (%q, city:fixture-city)",
			bound.Metadata[beadmeta.TriggerBeadIDMetadataKey],
			bound.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey],
			work.ID,
		)
	}
}

func TestRecoveredTriggerlessCreateIncompleteDemandWitnessFailsClosed(t *testing.T) {
	store := beads.NewMemStore()
	bp, cfg, info, stderr := guardedClaimBuildParams(t, store, store)
	work := createGuardedClaimWork(t, store, "worker")
	beforeWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeSession, err := store.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	demand := map[string]scaleCheckDemand{
		"worker": {WorkBeadIDs: []string{work.ID}},
	}
	states := ComputePoolDesiredStatesWithDemandTraced(
		cfg,
		nil,
		[]session.Info{info},
		map[string]int{"worker": 1},
		demand,
		nil,
	)
	if len(states) != 1 || len(states[0].Requests) != 1 {
		t.Fatalf("pool states = %+v, want one recovered request", states)
	}

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], states[0], desired, stderr)
	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want zero for incomplete recovered witness; stderr=%q", len(desired), stderr.String())
	}
	afterWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	afterSession, err := store.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee ||
		afterWork.Metadata[beadmeta.SessionIDMetadataKey] != beforeWork.Metadata[beadmeta.SessionIDMetadataKey] {
		t.Fatalf("incomplete witness mutated work: before=%+v after=%+v", beforeWork, afterWork)
	}
	if afterSession.Metadata[beadmeta.TriggerBeadIDMetadataKey] != beforeSession.Metadata[beadmeta.TriggerBeadIDMetadataKey] ||
		afterSession.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey] != beforeSession.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey] {
		t.Fatalf("incomplete witness mutated trigger: before=%+v after=%+v", beforeSession.Metadata, afterSession.Metadata)
	}
}

// TestRecoveredAssignedClaimRevalidatesSessionWitnessBeforeTriggerBind
// reproduces the second crash window: the exact work assignment committed, but
// the session's trigger pair did not. Recovery may bind and launch only when
// the assigned row carries the same concrete session id/name/token witness.
func TestRecoveredAssignedClaimRevalidatesSessionWitnessBeforeTriggerBind(t *testing.T) {
	for _, tc := range []struct {
		name        string
		workToken   string
		labels      []string
		wantDesired int
	}{
		{name: "exact witness resumes", workToken: "instance-worker-1", wantDesired: 1},
		{name: "mayor hold added after assignment remains owned", workToken: "instance-worker-1", labels: []string{beadmeta.HoldMayorLabel}, wantDesired: 1},
		{name: "external hold added after assignment remains owned", workToken: "instance-worker-1", labels: []string{beadmeta.HoldExternalLabel}, wantDesired: 1},
		{name: "stale token fails closed", workToken: "stale-instance-token", wantDesired: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := beads.NewMemStore()
			cityPath := t.TempDir()
			externalRoot := t.TempDir()
			cfg := &config.City{
				Workspace: config.Workspace{Name: "fixture-city"},
				Agents: []config.Agent{{
					Name:              "worker",
					StartCommand:      "true",
					ProjectHooks:      config.ProjectHooksForbid,
					WorkDir:           filepath.Join(externalRoot, "{{.AgentBase}}"),
					MinActiveSessions: intPtr(0),
					MaxActiveSessions: intPtr(2),
				}},
			}
			info := seedGuardedClaimPoolSession(t, store)
			work, err := store.Create(beads.Bead{
				Title:    "assigned before trigger bind",
				Type:     "task",
				Status:   "in_progress",
				Assignee: "worker-1",
				Labels:   tc.labels,
				Metadata: map[string]string{
					beadmeta.RoutedToMetadataKey:             "worker",
					beadmeta.SessionIDMetadataKey:            info.ID,
					beadmeta.SessionNameMetadataKey:          info.SessionNameMetadata,
					beadmeta.SessionInstanceTokenMetadataKey: tc.workToken,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Update(work.ID, beads.UpdateOpts{Status: strPtr("in_progress")}); err != nil {
				t.Fatalf("mark simulated committed claim in progress: %v", err)
			}
			beforeWork, err := store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := loadSessionBeadSnapshot(store)
			if err != nil {
				t.Fatal(err)
			}
			stderr := &bytes.Buffer{}
			sp := &templateProjectHookIsolationProvider{Fake: runtime.NewFake()}
			result := buildDesiredStateWithSessionBeads(
				"fixture-city",
				cityPath,
				time.Unix(1_700_000_000, 0).UTC(),
				cfg,
				sp,
				store,
				nil,
				snapshot,
				nil,
				stderr,
			)
			if len(result.State) != tc.wantDesired {
				t.Fatalf("desired sessions = %d, want %d; pool=%v assigned_refs=%v stderr=%q", len(result.State), tc.wantDesired, result.PoolDesiredCounts, result.AssignedWorkStoreRefs, stderr.String())
			}
			afterWork, err := store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			if afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee ||
				afterWork.Revision != beforeWork.Revision ||
				afterWork.Metadata[beadmeta.SessionIDMetadataKey] != beforeWork.Metadata[beadmeta.SessionIDMetadataKey] ||
				afterWork.Metadata[beadmeta.SessionInstanceTokenMetadataKey] != beforeWork.Metadata[beadmeta.SessionInstanceTokenMetadataKey] {
				t.Fatalf("idempotent recovery mutated assignment: before=%+v after=%+v", beforeWork, afterWork)
			}
			bound, err := store.Get(info.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantDesired == 0 {
				if bound.Metadata[beadmeta.TriggerBeadIDMetadataKey] != "" || bound.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey] != "" {
					t.Fatalf("stale witness bound trigger = (%q, %q), want empty",
						bound.Metadata[beadmeta.TriggerBeadIDMetadataKey],
						bound.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey],
					)
				}
				return
			}
			if bound.Metadata[beadmeta.TriggerBeadIDMetadataKey] != work.ID ||
				bound.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey] != "city:fixture-city" {
				t.Fatalf("recovered trigger = (%q, %q), want (%q, city:fixture-city)",
					bound.Metadata[beadmeta.TriggerBeadIDMetadataKey],
					bound.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey],
					work.ID,
				)
			}
		})
	}
}

func TestProjectHooksForbiddenRecoveredClaimRequiresCompleteRouteWitness(t *testing.T) {
	store := beads.NewMemStore()
	bp, cfg, info, _ := guardedClaimBuildParams(t, store, store)
	cfg.Agents[0].ProjectHooks = config.ProjectHooksForbid
	work := createGuardedClaimWork(t, store, "worker")
	before, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = claimPoolRequestWork(bp, info, SessionRequest{
		Template:     "worker",
		Tier:         "new",
		WorkBeadID:   work.ID,
		WorkStoreRef: "city:fixture-city",
	})
	if err == nil || !strings.Contains(err.Error(), "route witness") {
		t.Fatalf("claim error = %v, want incomplete route-witness failure", err)
	}
	after, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != before.Status || after.Assignee != before.Assignee ||
		after.Metadata[beadmeta.SessionIDMetadataKey] != before.Metadata[beadmeta.SessionIDMetadataKey] {
		t.Fatalf("incomplete forbidden-hook witness mutated work: before=%+v after=%+v", before, after)
	}
}

func TestProjectHooksForbiddenCountOnlyPoolDemandSuppressesWithoutClosingSession(t *testing.T) {
	t.Setenv("GC_SESSION", "")
	sessionStore := beads.NewMemStore()
	workStore := sessionStore
	bp, cfg, info, stderr := guardedClaimBuildParams(t, sessionStore, workStore)
	cfg.Agents[0].ProjectHooks = config.ProjectHooksForbid
	cfg.Agents[0].WorkDir = filepath.Join(t.TempDir(), "{{.Agent}}")
	bp.sp = &projectHookIsolationWorkerProvider{Fake: runtime.NewFake(), supported: true}
	desired := map[string]TemplateParams{}

	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:      "worker",
			Tier:          "new",
			SessionBeadID: info.ID,
		}},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want zero without an exact work witness", len(desired))
	}
	if !bp.suppressedSessionBeadIDs[info.ID] {
		t.Fatalf("session %q was not suppressed", info.ID)
	}
	unchanged, err := sessionStore.Get(info.ID)
	if err != nil {
		t.Fatalf("load suppressed session: %v", err)
	}
	if unchanged.Status != "open" {
		t.Fatalf("session status = %q, want open suppression-only state", unchanged.Status)
	}
	if !strings.Contains(stderr.String(), "requires an exact work bead witness") {
		t.Fatalf("stderr = %q, want exact-work-witness diagnostic", stderr.String())
	}
}

func TestProjectHooksForbiddenStaleCountOnlyRequestDoesNotCloseNewlyBoundSession(t *testing.T) {
	t.Setenv("GC_SESSION", "")
	sessionStore := beads.NewMemStore()
	workStore := sessionStore
	bp, cfg, info, stderr := guardedClaimBuildParams(t, sessionStore, workStore)
	cfg.Agents[0].ProjectHooks = config.ProjectHooksForbid
	cfg.Agents[0].WorkDir = filepath.Join(t.TempDir(), "{{.Agent}}")
	bp.sp = &projectHookIsolationWorkerProvider{Fake: runtime.NewFake(), supported: true}

	// The SessionRequest below was derived from bp.sessionBeads before this
	// exact trigger binding committed. Its empty WorkBeadID is therefore stale.
	if err := sessionStore.Update(info.ID, beads.UpdateOpts{Metadata: map[string]string{
		beadmeta.TriggerBeadIDMetadataKey:       "work-bound-after-snapshot",
		beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		"state":                                 string(session.StateActive),
	}}); err != nil {
		t.Fatalf("bind live session after snapshot: %v", err)
	}
	before, err := sessionStore.Get(info.ID)
	if err != nil {
		t.Fatalf("load drifted session: %v", err)
	}

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:      "worker",
			Tier:          "new",
			SessionBeadID: info.ID,
		}},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want zero from the stale count-only request", len(desired))
	}
	if !bp.suppressedSessionBeadIDs[info.ID] {
		t.Fatalf("session %q was not suppressed from this stale build", info.ID)
	}
	after, err := sessionStore.Get(info.ID)
	if err != nil {
		t.Fatalf("reload live session: %v", err)
	}
	if after.Revision != before.Revision || after.Status != before.Status ||
		after.Metadata[beadmeta.TriggerBeadIDMetadataKey] != before.Metadata[beadmeta.TriggerBeadIDMetadataKey] ||
		after.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey] != before.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey] ||
		after.Metadata["state"] != before.Metadata["state"] {
		t.Fatalf("stale count-only request mutated newly bound session: before=%+v after=%+v", before, after)
	}
}

func TestRecoveredAssignedClaimCannotReclaimWorkReopenedBeforeTransaction(t *testing.T) {
	backing := beads.NewMemStore()
	store := &reopenBeforeGuardedClaimStore{Store: backing}
	bp, _, info, _ := guardedClaimBuildParams(t, store, store)
	work := createGuardedClaimWork(t, store, "worker")
	status := "in_progress"
	actor := session.AssigneeIdentifier(info)
	if err := store.Update(work.ID, beads.UpdateOpts{
		Status:   &status,
		Assignee: &actor,
		Metadata: map[string]string{
			beadmeta.SessionIDMetadataKey:            info.ID,
			beadmeta.SessionNameMetadataKey:          info.SessionNameMetadata,
			beadmeta.SessionInstanceTokenMetadataKey: info.InstanceToken,
		},
	}); err != nil {
		t.Fatalf("seed recovered assignment: %v", err)
	}

	_, err := claimPoolRequestWork(bp, info, SessionRequest{
		Template:     "worker",
		Tier:         "resume",
		WorkBeadID:   work.ID,
		WorkStoreRef: "city:fixture-city",
		WorkRouteKey: beadmeta.RoutedToMetadataKey,
		WorkRoute:    "worker",
	})
	if err == nil || !strings.Contains(err.Error(), "atomic assignment predicates did not match") {
		t.Fatalf("recovered claim error = %v, want reopened-row predicate mismatch", err)
	}
	if store.claimCalls != 1 || !store.requireIdempotent {
		t.Fatalf("guarded claim calls=%d require_idempotent=%v, want one read-only recovery", store.claimCalls, store.requireIdempotent)
	}
	reopened, getErr := backing.Get(work.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if reopened.Status != "open" || reopened.Assignee != "" {
		t.Fatalf("reopened work was reclaimed: %+v", reopened)
	}
}

func TestRecoveredAssignedClaimDoesNotPublishCacheMutation(t *testing.T) {
	backing := beads.NewMemStore()
	bp, _, info, _ := guardedClaimBuildParams(t, backing, backing)
	work := createGuardedClaimWork(t, backing, "worker")
	status := "in_progress"
	actor := session.AssigneeIdentifier(info)
	if err := backing.Update(work.ID, beads.UpdateOpts{
		Status:   &status,
		Assignee: &actor,
		Metadata: map[string]string{
			beadmeta.SessionIDMetadataKey:            info.ID,
			beadmeta.SessionNameMetadataKey:          info.SessionNameMetadata,
			beadmeta.SessionInstanceTokenMetadataKey: info.InstanceToken,
		},
	}); err != nil {
		t.Fatalf("seed recovered assignment: %v", err)
	}
	var notifications []string
	cache := beads.NewCachingStoreForTest(backing, func(eventType, beadID string, _ json.RawMessage) {
		notifications = append(notifications, eventType+":"+beadID)
	})
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	bp.beadStore = cache
	before, err := backing.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := claimPoolRequestWork(bp, info, SessionRequest{
		Template:     "worker",
		Tier:         "resume",
		WorkBeadID:   work.ID,
		WorkStoreRef: "city:fixture-city",
		WorkRouteKey: beadmeta.RoutedToMetadataKey,
		WorkRoute:    "worker",
	})
	if err != nil {
		t.Fatalf("read-only recovered claim: %v", err)
	}
	if claimed.ID != work.ID || claimed.Status != "in_progress" || claimed.Assignee != actor {
		t.Fatalf("recovered claim = %+v, want exact existing assignment", claimed)
	}
	after, err := backing.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != before.Revision {
		t.Fatalf("read-only recovered claim changed backing revision %d -> %d", before.Revision, after.Revision)
	}
	if len(notifications) != 0 {
		t.Fatalf("read-only recovered claim notifications = %v, want none", notifications)
	}
}

// TestRecoveredAssignedClaimBypassesStaleCacheBeforeBinding pins the exact
// cache split used in production: ordinary Get may still expose the committed
// claim's old route while the backing store has already drifted. Recovery must
// use the live read and the backing store's idempotent guarded transaction, so
// stale cached provenance can never authorize a session trigger.
func TestRecoveredAssignedClaimBypassesStaleCacheBeforeBinding(t *testing.T) {
	backing := beads.NewMemStore()
	info := seedGuardedClaimPoolSession(t, backing)
	work := createGuardedClaimWork(t, backing, "worker")
	status := "in_progress"
	actor := "worker-1"
	if err := backing.Update(work.ID, beads.UpdateOpts{
		Status:   &status,
		Assignee: &actor,
		Metadata: map[string]string{
			beadmeta.SessionIDMetadataKey:            info.ID,
			beadmeta.SessionNameMetadataKey:          info.SessionNameMetadata,
			beadmeta.SessionInstanceTokenMetadataKey: info.InstanceToken,
		},
	}); err != nil {
		t.Fatal(err)
	}
	cache := beads.NewCachingStoreForTest(backing, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Agents: []config.Agent{{
			Name:              "worker",
			StartCommand:      "true",
			MinActiveSessions: intPtr(0),
			MaxActiveSessions: intPtr(2),
		}},
	}
	bp := newAgentBuildParams("fixture-city", t.TempDir(), cfg, runtime.NewFake(), time.Now().UTC(), cache, &bytes.Buffer{})
	bp.sessionBeads = &sessionBeadSnapshot{}
	bp.sessionBeads.addInfo(info)
	if err := backing.SetMetadata(work.ID, beadmeta.RoutedToMetadataKey, "other"); err != nil {
		t.Fatal(err)
	}
	stale, err := cache.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stale.Metadata[beadmeta.RoutedToMetadataKey] != "worker" {
		t.Fatalf("fixture cache route = %q, want stale worker", stale.Metadata[beadmeta.RoutedToMetadataKey])
	}
	before, err := backing.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = claimPoolRequestWork(bp, info, SessionRequest{
		Template:     "worker",
		Tier:         "resume",
		WorkBeadID:   work.ID,
		WorkStoreRef: "city:fixture-city",
		WorkRouteKey: beadmeta.RoutedToMetadataKey,
		WorkRoute:    "worker",
	})
	if err == nil {
		t.Fatal("recovered claim accepted stale cached route, want fail-closed backing drift")
	}
	after, err := backing.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != before.Revision || after.Status != before.Status || after.Assignee != before.Assignee ||
		after.Metadata[beadmeta.RoutedToMetadataKey] != before.Metadata[beadmeta.RoutedToMetadataKey] {
		t.Fatalf("failed stale-cache recovery mutated backing: before=%+v after=%+v", before, after)
	}
}

func runRealizePoolDesiredSessionsTriggerBindFailurePreventsLaunch(t *testing.T) {
	sessionMem := beads.NewMemStore()
	sessionStore := failUpdateStore{Store: sessionMem, err: errTestTriggerBindRejected}
	bp, cfg, info, stderr := guardedClaimBuildParams(t, sessionStore, sessionStore)
	work := createGuardedClaimWork(t, sessionMem, "worker")
	desired := map[string]TemplateParams{}

	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:      "worker",
			Tier:          "new",
			SessionBeadID: info.ID,
			WorkBeadID:    work.ID,
			WorkStoreRef:  "city:fixture-city",
			WorkRouteKey:  beadmeta.RoutedToMetadataKey,
			WorkRoute:     "worker",
		}},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want 0 after trigger-bind failure; stderr=%q", len(desired), stderr.String())
	}
	claimed, err := sessionMem.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status != "in_progress" || claimed.Assignee != "worker-1" {
		t.Fatalf("guarded claim did not commit before bind failure: %+v", claimed)
	}
	bound, err := sessionMem.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := bound.Metadata[beadmeta.TriggerBeadIDMetadataKey]; got != "" {
		t.Fatalf("failed trigger bind persisted %q, want empty", got)
	}
	if strings.Contains(stderr.String(), "continuing without trigger env") || !strings.Contains(stderr.String(), "(skipping)") {
		t.Fatalf("stderr = %q, want fail-closed skipping diagnostic", stderr.String())
	}
}

var errTestTriggerBindRejected = &triggerBindTestError{}

type triggerBindTestError struct{}

func (*triggerBindTestError) Error() string { return "trigger bind rejected" }
