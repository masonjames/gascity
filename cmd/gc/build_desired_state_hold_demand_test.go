package main

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

// labelBlindReadyStore models NativeDoltLite's deliberate Ready projection:
// readiness is authoritative, but labels are omitted. Live label-index List
// calls still return the held rows and are recorded for memoization assertions.
type labelBlindReadyStore struct {
	*beads.MemStore

	mu           sync.Mutex
	labelQueries []beads.ListQuery
	readyQueries []beads.ReadyQuery
	failLabel    string
	failErr      error
	partial      bool
	readyErr     error
	readyPartial bool
	// readyErrFilteredOnly limits a synthetic outage to the canonical-hold
	// filtered Ready query while leaving the assigned-work snapshot healthy.
	readyErrFilteredOnly bool
	dropReadyRows        bool
}

func (s *labelBlindReadyStore) Ready(query ...beads.ReadyQuery) ([]beads.Bead, error) {
	q := beads.ReadyQuery{}
	if len(query) > 0 {
		q = query[0]
		q.ExcludeLabels = append([]string(nil), q.ExcludeLabels...)
	}
	s.mu.Lock()
	s.readyQueries = append(s.readyQueries, q)
	s.mu.Unlock()
	rows, err := s.MemStore.Ready(query...)
	for i := range rows {
		rows[i].Labels = nil
	}
	if s.readyErr != nil && (!s.readyErrFilteredOnly || len(q.ExcludeLabels) > 0) {
		if s.dropReadyRows {
			rows = nil
		}
		if s.readyPartial {
			return rows, &beads.PartialResultError{Op: "filtered ready snapshot", Err: s.readyErr}
		}
		return nil, s.readyErr
	}
	return rows, err
}

func (s *labelBlindReadyStore) readyQuerySnapshot() []beads.ReadyQuery {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]beads.ReadyQuery(nil), s.readyQueries...)
	for i := range out {
		out[i].ExcludeLabels = append([]string(nil), out[i].ExcludeLabels...)
	}
	return out
}

func (s *labelBlindReadyStore) List(query beads.ListQuery) ([]beads.Bead, error) {
	rows, err := s.MemStore.List(query)
	if query.Live && query.Label != "" {
		s.mu.Lock()
		s.labelQueries = append(s.labelQueries, query)
		s.mu.Unlock()
		if query.Label == s.failLabel {
			if s.partial {
				return rows, &beads.PartialResultError{Op: "dispatch hold label index", Err: s.failErr}
			}
			return nil, s.failErr
		}
	}
	return rows, err
}

func (s *labelBlindReadyStore) labelQuerySnapshot() []beads.ListQuery {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]beads.ListQuery(nil), s.labelQueries...)
}

type transitioningCanonicalHoldStore struct {
	*labelBlindReadyStore
	workID     string
	transition bool
}

func (s *transitioningCanonicalHoldStore) List(query beads.ListQuery) ([]beads.Bead, error) {
	rows, err := s.labelBlindReadyStore.List(query)
	if err != nil || !query.Live || query.Label != beadmeta.HoldMayorLabel || s.transition {
		return rows, err
	}
	s.transition = true
	if updateErr := s.Update(s.workID, beads.UpdateOpts{
		Labels:       []string{beadmeta.HoldMayorLabel},
		RemoveLabels: []string{beadmeta.HoldExternalLabel},
	}); updateErr != nil {
		return nil, updateErr
	}
	return rows, nil
}

func TestCanonicalHoldTransitionCannotTearPoolDemandSnapshot(t *testing.T) {
	t.Parallel()

	const template = "fixture/worker"
	base := &labelBlindReadyStore{MemStore: beads.NewMemStore()}
	store := &transitioningCanonicalHoldStore{labelBlindReadyStore: base, workID: "transitioning-work"}
	created, err := store.Create(beads.Bead{
		ID:       store.workID,
		Title:    "held work changes canonical actor",
		Type:     "task",
		Status:   "open",
		Labels:   []string{beadmeta.HoldExternalLabel},
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: template},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	store.workID = created.ID

	counts, demand, partial, errs := defaultScaleCheckCountsAndDemand(nil, []defaultScaleCheckTarget{{
		template: template,
		storeKey: "rig:fixture",
		store:    store,
	}}, newReadyDemandCache())
	if len(errs) != 0 || len(partial) != 0 {
		t.Fatalf("errs=%v partial=%v, want complete coherent read", errs, partial)
	}
	if got := counts[template]; got != 0 {
		t.Fatalf("counts[%q] = %d, want zero while hold actor changes external to mayor", template, got)
	}
	if got := demand[template].Count; got != 0 {
		t.Fatalf("demand[%q].Count = %d, want zero", template, got)
	}
	if queries := store.labelQuerySnapshot(); len(queries) != 0 {
		t.Fatalf("live label-index queries = %+v, want none outside the Ready snapshot", queries)
	}
	queries := store.readyQuerySnapshot()
	if len(queries) != 1 {
		t.Fatalf("Ready queries = %+v, want exactly one", queries)
	}
	want := beads.ReadyQuery{TierMode: beads.TierBoth, ExcludeLabels: beadmeta.DispatchHoldLabels}
	if !reflect.DeepEqual(queries[0], want) {
		t.Fatalf("Ready query = %+v, want %+v", queries[0], want)
	}
}

func TestDirectControllerDemandExcludesCanonicalHolds(t *testing.T) {
	t.Parallel()

	const template = "fixture/worker"
	store := &labelBlindReadyStore{MemStore: beads.NewMemStore()}
	fixtures := []beads.Bead{
		{ID: "held-mayor", Labels: []string{beadmeta.HoldMayorLabel}},
		{ID: "held-external", Labels: []string{beadmeta.HoldExternalLabel}},
		{ID: "held-both", Labels: []string{beadmeta.HoldMayorLabel, beadmeta.HoldExternalLabel}},
		{ID: "unrelated", Labels: []string{"priority:high"}},
		{ID: "unheld"},
	}
	var wantIDs []string
	for _, fixture := range fixtures {
		fixtureName := fixture.ID
		fixture.Title = fixture.ID
		fixture.Type = "task"
		fixture.Status = "open"
		fixture.Metadata = map[string]string{beadmeta.RoutedToMetadataKey: template}
		created, err := store.Create(fixture)
		if err != nil {
			t.Fatalf("Create(%s): %v", fixture.ID, err)
		}
		if fixtureName == "unrelated" || fixtureName == "unheld" {
			wantIDs = append(wantIDs, created.ID)
		}
	}

	counts, demand, partial, errs := defaultScaleCheckCountsAndDemand(nil, []defaultScaleCheckTarget{{
		template: template,
		storeKey: "rig:fixture",
		store:    store,
	}}, newReadyDemandCache())
	if len(errs) != 0 || len(partial) != 0 {
		t.Fatalf("errs=%v partial=%v, want complete hold-index read", errs, partial)
	}
	if got := counts[template]; got != 2 {
		t.Fatalf("counts[%q] = %d, want 2 (only unrelated and unheld)", template, got)
	}
	if got, want := demand[template].WorkBeadIDs, wantIDs; !reflect.DeepEqual(got, want) {
		t.Fatalf("WorkBeadIDs = %v, want %v", got, want)
	}
}

func TestCanonicalHoldsRemainTransparentForAssignedWorkTiers(t *testing.T) {
	const (
		template    = "worker"
		sessionName = "worker-1"
	)
	minActive, maxActive := 0, 1
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:              template,
			StartCommand:      "test-cmd",
			MinActiveSessions: &minActive,
			MaxActiveSessions: &maxActive,
		}},
	}
	cases := []struct {
		name   string
		status string
		label  string
	}{
		{name: "tier 1 in progress mayor hold", status: "in_progress", label: beadmeta.HoldMayorLabel},
		{name: "tier 1 in progress external hold", status: "in_progress", label: beadmeta.HoldExternalLabel},
		{name: "tier 2 ready open mayor hold", status: "open", label: beadmeta.HoldMayorLabel},
		{name: "tier 2 ready open external hold", status: "open", label: beadmeta.HoldExternalLabel},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			store := &labelBlindReadyStore{MemStore: beads.NewMemStore()}
			sessionBead, err := store.Create(beads.Bead{
				Title:  sessionName,
				Type:   sessionBeadType,
				Status: "open",
				Labels: []string{sessionBeadLabel, "template:" + template},
				Metadata: map[string]string{
					"session_name":         sessionName,
					"agent_name":           sessionName,
					"template":             template,
					"pool_slot":            "1",
					poolManagedMetadataKey: boolMetadata(true),
					"state":                string(sessionpkg.StateActive),
					"generation":           "1",
					"instance_token":       "fixture-instance-token",
					"live_hash":            runtime.LiveFingerprint(runtime.Config{Command: "test-cmd"}),
					"pending_create_claim": "",
				},
			})
			if err != nil {
				t.Fatalf("Create(session): %v", err)
			}
			work, err := store.Create(beads.Bead{
				ID:       "assigned-work",
				Title:    "canonically held assigned work",
				Type:     "task",
				Status:   tt.status,
				Assignee: sessionBead.ID,
				Labels:   []string{tt.label},
				Metadata: map[string]string{beadmeta.RoutedToMetadataKey: template},
			})
			if err != nil {
				t.Fatalf("Create(work): %v", err)
			}

			snapshot, err := loadSessionBeadSnapshot(store)
			if err != nil {
				t.Fatalf("load session snapshot: %v", err)
			}
			result := buildDesiredStateWithSessionBeads(
				"test-city", t.TempDir(), time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC),
				cfg, runtime.NewFake(), store, nil, snapshot, nil, io.Discard,
			)
			if result.StoreQueryPartial || result.SessionQueryPartial {
				t.Fatalf("assigned hold build partial: store=%v session=%v", result.StoreQueryPartial, result.SessionQueryPartial)
			}
			if got := result.ScaleCheckCounts[template]; got != 0 {
				t.Fatalf("ScaleCheckCounts[%q] = %d, want zero fresh demand for assigned work", template, got)
			}
			if len(result.ReadyUnassignedRoutedWorkBeads) != 0 {
				t.Fatalf("fresh routed demand = %+v, want none for assigned work", result.ReadyUnassignedRoutedWorkBeads)
			}

			found := false
			for i, assigned := range result.AssignedWorkBeads {
				if assigned.ID != work.ID {
					continue
				}
				found = true
				storeRef := ""
				if i < len(result.AssignedWorkStoreRefs) {
					storeRef = result.AssignedWorkStoreRefs[i]
				}
				if !result.ReadyAssigned[storeScopedBeadKey{StoreRef: storeRef, ID: work.ID}] {
					t.Fatalf("assigned %s work %q lost its readiness verdict under %s", tt.status, work.ID, tt.label)
				}
			}
			if !found {
				t.Fatalf("AssignedWorkBeads = %+v, want held assigned work %s", result.AssignedWorkBeads, work.ID)
			}

			states := ComputePoolDesiredStates(cfg, result.AssignedWorkBeads, snapshot.OpenInfos(), result.ScaleCheckCounts)
			if len(states) != 1 || len(states[0].Requests) != 1 {
				t.Fatalf("pool desired states = %+v, want one assigned-work request", states)
			}
			request := states[0].Requests[0]
			if request.Tier != "resume" || request.SessionBeadID != sessionBead.ID || request.WorkBeadID != work.ID {
				t.Fatalf("pool request = %+v, want resume of %s for %s", request, sessionBead.ID, work.ID)
			}
		})
	}
}

func TestOpenControlDispatcherDemandFiltersCanonicalHolds(t *testing.T) {
	t.Parallel()

	const template = "core.control-dispatcher"
	maxActive := 1
	cfg := &config.City{Agents: []config.Agent{{
		Name:              config.ControlDispatcherAgentName,
		BindingName:       "core",
		StartCommand:      config.ControlDispatcherStartCommandFor("{{.Agent}}"),
		MaxActiveSessions: &maxActive,
	}}}
	tests := []struct {
		name   string
		labels []string
		want   bool
	}{
		{name: "mayor", labels: []string{beadmeta.HoldMayorLabel}},
		{name: "external", labels: []string{beadmeta.HoldExternalLabel}},
		{name: "both", labels: []string{beadmeta.HoldMayorLabel, beadmeta.HoldExternalLabel}},
		{name: "unrelated", labels: []string{"priority:high"}, want: true},
		{name: "unheld", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := &labelBlindReadyStore{MemStore: beads.NewMemStore()}
			created, err := store.Create(beads.Bead{
				ID:       tt.name,
				Type:     "task",
				Status:   "open",
				Labels:   tt.labels,
				Metadata: map[string]string{beadmeta.RoutedToMetadataKey: template},
			})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			got, partial, errs := openControlDispatcherDemand(
				cfg,
				[]beads.Bead{created},
				[]beads.Store{store},
				newReadyDemandCache(),
			)
			if len(errs) != 0 || len(partial) != 0 {
				t.Fatalf("errs=%v partial=%v, want complete", errs, partial)
			}
			if got[template] != tt.want {
				t.Fatalf("demand[%q] = %v, want %v", template, got[template], tt.want)
			}
		})
	}
}

func TestFilteredReadyFailuresNeverCacheBackfillFreshDemand(t *testing.T) {
	t.Parallel()

	const template = "fixture/worker"
	tests := []struct {
		name       string
		partialErr bool
	}{
		{name: "hard error"},
		{name: "partial rows", partialErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backing := &labelBlindReadyStore{MemStore: beads.NewMemStore()}
			_, err := backing.Create(beads.Bead{
				ID:       "unheld-work",
				Title:    "unheld work",
				Type:     "task",
				Status:   "open",
				Metadata: map[string]string{beadmeta.RoutedToMetadataKey: template},
			})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			store := beads.NewCachingStoreForTest(backing, nil)
			if err := store.Prime(context.Background()); err != nil {
				t.Fatalf("Prime: %v", err)
			}
			backing.readyErr = errors.New("filtered ready unavailable")
			backing.readyPartial = tt.partialErr
			backing.dropReadyRows = true

			counts, demand, partial, errs := defaultScaleCheckCountsAndDemand(nil, []defaultScaleCheckTarget{{
				template: template,
				storeKey: "rig:fixture",
				store:    store,
			}}, newReadyDemandCache())
			if counts[template] != 0 || demand[template].Count != 0 {
				t.Fatalf("counts=%v demand=%v, want no cache-backfilled fresh demand", counts, demand)
			}
			assertDispatchHoldPartial(t, template, partial, errs)
			if queries := backing.readyQuerySnapshot(); len(queries) != 1 {
				t.Fatalf("Ready queries = %+v, want one authoritative attempt", queries)
			}
			if queries := backing.labelQuerySnapshot(); len(queries) != 0 {
				t.Fatalf("label queries = %+v, want none", queries)
			}
		})
	}
}

func TestFilteredReadySnapshotMemoizedPerStoreAcrossDemandConsumers(t *testing.T) {
	t.Parallel()

	const workerTemplate = "fixture/worker"
	const dispatcherTemplate = "core.control-dispatcher"
	store := &labelBlindReadyStore{MemStore: beads.NewMemStore()}
	_, err := store.Create(beads.Bead{
		ID: "worker-work", Title: "worker work", Type: "task", Status: "open",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: workerTemplate},
	})
	if err != nil {
		t.Fatalf("Create worker work: %v", err)
	}
	dispatcher, err := store.Create(beads.Bead{
		ID: "dispatcher-work", Title: "dispatcher work", Type: "task", Status: "open",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: dispatcherTemplate},
	})
	if err != nil {
		t.Fatalf("Create dispatcher work: %v", err)
	}
	cache := newReadyDemandCache()
	counts, _, partial, errs := defaultScaleCheckCountsAndDemand(nil, []defaultScaleCheckTarget{{
		template: workerTemplate,
		storeKey: "rig:fixture",
		store:    store,
	}}, cache)
	if counts[workerTemplate] != 1 || len(partial) != 0 || len(errs) != 0 {
		t.Fatalf("default demand counts=%v partial=%v errs=%v", counts, partial, errs)
	}
	maxActive := 1
	cfg := &config.City{Agents: []config.Agent{{
		Name:              config.ControlDispatcherAgentName,
		BindingName:       "core",
		StartCommand:      config.ControlDispatcherStartCommandFor("{{.Agent}}"),
		MaxActiveSessions: &maxActive,
	}}}
	demand, partial, errs := openControlDispatcherDemand(cfg, []beads.Bead{dispatcher}, []beads.Store{store}, cache)
	if !demand[dispatcherTemplate] || len(partial) != 0 || len(errs) != 0 {
		t.Fatalf("dispatcher demand=%v partial=%v errs=%v", demand, partial, errs)
	}
	if queries := store.labelQuerySnapshot(); len(queries) != 0 {
		t.Fatalf("live label-index queries = %+v, want none", queries)
	}
	queries := store.readyQuerySnapshot()
	if len(queries) != 1 {
		t.Fatalf("Ready queries = %+v, want one shared filtered snapshot", queries)
	}
	want := beads.ReadyQuery{TierMode: beads.TierBoth, ExcludeLabels: beadmeta.DispatchHoldLabels}
	if !reflect.DeepEqual(queries[0], want) {
		t.Fatalf("Ready query = %+v, want %+v", queries[0], want)
	}
}

func runCanonicalHoldCompletedBarrierPreventsSequentialReplacementAfterSuspendedSessionDrain(t *testing.T) {
	const (
		template    = "worker"
		sessionName = "worker-1"
	)
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	minActive, maxActive := 0, 1
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:              template,
			StartCommand:      "test-cmd",
			MinActiveSessions: &minActive,
			MaxActiveSessions: &maxActive,
		}},
	}
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	if err := sp.Start(context.Background(), sessionName, runtime.Config{Command: "test-cmd"}); err != nil {
		t.Fatalf("Start(%s): %v", sessionName, err)
	}
	sessionBead, err := store.Create(beads.Bead{
		Title:  sessionName,
		Type:   sessionBeadType,
		Status: "open",
		Labels: []string{sessionBeadLabel, "template:" + template},
		Metadata: map[string]string{
			"session_name":         sessionName,
			"agent_name":           sessionName,
			"template":             template,
			"pool_slot":            "1",
			poolManagedMetadataKey: boolMetadata(true),
			"state":                string(sessionpkg.StateActive),
			"generation":           "1",
			"instance_token":       "fixture-instance-token",
			"live_hash":            runtime.LiveFingerprint(runtime.Config{Command: "test-cmd"}),
			"pending_create_claim": "",
		},
	})
	if err != nil {
		t.Fatalf("Create(session): %v", err)
	}
	heldWork, err := store.Create(beads.Bead{
		Title:  "held routed work",
		Type:   "task",
		Status: "open",
		Labels: []string{beadmeta.HoldMayorLabel},
		Metadata: map[string]string{
			beadmeta.RoutedToMetadataKey: template,
		},
	})
	if err != nil {
		t.Fatalf("Create(held work): %v", err)
	}

	cityPath := t.TempDir()
	clk := &clock.Fake{Time: now}
	type completedCycle struct {
		result          DesiredStateResult
		poolDesired     map[string]int
		beforeReconcile []beads.Bead
		afterReconcile  []beads.Bead
		allSessions     []beads.Bead
		woken           int
		startDelta      int
	}
	countStarts := func() int {
		count := 0
		for _, call := range sp.SnapshotCalls() {
			if call.Method == "Start" {
				count++
			}
		}
		return count
	}
	countPendingClaims := func(rows []beads.Bead) int {
		count := 0
		for _, row := range rows {
			if row.Metadata["pending_create_claim"] != "" {
				count++
			}
		}
		return count
	}
	countDesiredSlots := func(state map[string]TemplateParams) int {
		count := 0
		for _, params := range state {
			if params.TemplateName == template {
				count++
			}
		}
		return count
	}
	runCompletedCycle := func(label string, cycleCfg *config.City) completedCycle {
		t.Helper()
		snapshot, err := loadSessionBeadSnapshot(store)
		if err != nil {
			t.Fatalf("%s: load build snapshot: %v", label, err)
		}
		result := buildDesiredStateWithSessionBeads(
			"test-city", cityPath, clk.Now().UTC(), cycleCfg, sp, store, nil, snapshot, nil, io.Discard,
		)
		if result.snapshotQueryPartial() {
			t.Fatalf("%s: desired-state build is partial: store=%v session=%v", label, result.StoreQueryPartial, result.SessionQueryPartial)
		}

		cfgNames := configuredSessionNames(cycleCfg, "test-city", store)
		syncSessionBeads(cityPath, store, result.State, sp, cfgNames, cycleCfg, clk, io.Discard, true)
		beforeReconcile, err := loadSessionBeads(store)
		if err != nil {
			t.Fatalf("%s: reload after session sync: %v", label, err)
		}
		poolDesired := PoolDesiredCounts(ComputePoolDesiredStates(
			cycleCfg, result.AssignedWorkBeads, sessionInfosFromBeads(beforeReconcile), result.ScaleCheckCounts,
		))
		if poolDesired == nil {
			poolDesired = make(map[string]int)
		}
		mergeNamedSessionDemand(poolDesired, result.NamedSessionDemand, cycleCfg)

		startsBefore := countStarts()
		startTracker := &asyncStartTracker{}
		woken := reconcileSessionBeadsAtPath(
			context.Background(), cityPath, beforeReconcile, result.State, cfgNames,
			cycleCfg, sp, store, nil, result.AssignedWorkBeads, nil,
			nil, newDrainTracker(), poolDesired, result.snapshotQueryPartial(), nil, "test-city",
			nil, clk, events.Discard, 0, 0, io.Discard, io.Discard,
			withAsyncStartExecution(), withAsyncStartTracker(startTracker),
		)
		if !startTracker.wait(-1) {
			t.Fatalf("%s: asynchronous start barrier did not complete", label)
		}
		afterReconcile, err := loadSessionBeads(store)
		if err != nil {
			t.Fatalf("%s: reload after reconciliation: %v", label, err)
		}
		allSessions, err := store.List(beads.ListQuery{Type: sessionBeadType, IncludeClosed: true})
		if err != nil {
			t.Fatalf("%s: list all session rows: %v", label, err)
		}
		return completedCycle{
			result:          result,
			poolDesired:     poolDesired,
			beforeReconcile: beforeReconcile,
			afterReconcile:  afterReconcile,
			allSessions:     allSessions,
			woken:           woken,
			startDelta:      countStarts() - startsBefore,
		}
	}

	// Complete the real build -> session-sync -> reload -> reconcile path while
	// the concrete session is still active. This is the hold-observing barrier:
	// the template is runnable, but the canonical hold contributes no fresh
	// count, pool request, work-set entry, wake, or replacement.
	initial := runCompletedCycle("initial hold barrier", cfg)
	if got := initial.result.ScaleCheckCounts[template]; got != 0 {
		t.Fatalf("initial hold barrier ScaleCheckCounts[%q] = %d, want 0", template, got)
	}
	if got := initial.poolDesired[template]; got != 0 {
		t.Fatalf("initial hold barrier poolDesired[%q] = %d, want 0", template, got)
	}
	if len(initial.result.WorkSet) != 0 || len(initial.result.ReadyUnassignedRoutedWorkBeads) != 0 {
		t.Fatalf("initial hold barrier work_set=%v routed_demand=%+v, want both empty", initial.result.WorkSet, initial.result.ReadyUnassignedRoutedWorkBeads)
	}
	if got := countDesiredSlots(initial.result.State); got != 0 {
		t.Fatalf("initial hold barrier desired %q slots = %d, want 0", template, got)
	}
	if cfg.Agents[0].Suspended {
		t.Fatal("initial hold barrier accidentally suspended the template")
	}
	if initial.woken != 0 || initial.startDelta != 0 {
		t.Fatalf("initial hold barrier woken=%d start_delta=%d, want both zero", initial.woken, initial.startDelta)
	}
	if got := len(initial.beforeReconcile); got != 1 || initial.beforeReconcile[0].ID != sessionBead.ID {
		t.Fatalf("initial hold barrier pre-reconcile sessions = %+v, want only existing %s", initial.beforeReconcile, sessionBead.ID)
	}
	if got := len(initial.afterReconcile); got != 1 || initial.afterReconcile[0].ID != sessionBead.ID {
		t.Fatalf("initial hold barrier post-reconcile sessions = %+v, want only existing %s", initial.afterReconcile, sessionBead.ID)
	}
	if got := countPendingClaims(initial.allSessions); got != 0 {
		t.Fatalf("initial hold barrier pending-create claims = %d, want 0", got)
	}
	if err := store.Update(sessionBead.ID, beads.UpdateOpts{Metadata: map[string]string{
		"state":        string(sessionpkg.StateSuspended),
		"sleep_intent": string(sessionpkg.SleepReasonUserHold),
		"sleep_reason": string(sessionpkg.SleepReasonUserHold),
		"held_until":   now.Add(100 * time.Hour).Format(time.RFC3339),
	}}); err != nil {
		t.Fatalf("suspend only concrete session: %v", err)
	}
	sessionBead, err = store.Get(sessionBead.ID)
	if err != nil {
		t.Fatalf("reload suspended session: %v", err)
	}

	// A per-session suspend is an instance lifecycle operation. Drive that real
	// suspended instance through drain and drain-ack; it must not be confused
	// with config.Agent.Suspended, which suppresses the whole template.
	dt := newDrainTracker()
	dops := newFakeDrainOps()
	desiredState := map[string]TemplateParams{sessionName: {
		Command:      "test-cmd",
		SessionName:  sessionName,
		TemplateName: template,
	}}
	idle := newFakeIdleTracker()
	idle.idle[sessionName] = true
	reconcileSessionBeads(
		context.Background(), []beads.Bead{sessionBead}, desiredState, map[string]bool{template: true},
		cfg, sp, store, dops, nil, nil, dt, map[string]int{}, false, nil, "",
		idle, clk, events.Discard, 0, 0, io.Discard, io.Discard,
	)
	if drain := dt.get(sessionBead.ID); drain == nil || drain.reason != string(sessionpkg.SleepReasonUserHold) {
		t.Fatalf("suspended session drain = %#v, want user-hold", drain)
	}
	if err := dops.setDrainAck(sessionName); err != nil {
		t.Fatalf("setDrainAck(%s): %v", sessionName, err)
	}
	sessionBead, err = store.Get(sessionBead.ID)
	if err != nil {
		t.Fatalf("Get(session before drain-ack): %v", err)
	}
	stopTracker := &asyncStartTracker{}
	reconcileSessionBeads(
		context.Background(), []beads.Bead{sessionBead}, desiredState, map[string]bool{template: true},
		cfg, sp, store, dops, nil, nil, dt, map[string]int{}, false, nil, "",
		nil, clk, events.Discard, 0, 0, io.Discard, io.Discard,
		withAsyncDrainAckStopTracker(stopTracker),
	)
	if !stopTracker.wait(-1) {
		t.Fatal("drain-ack stop barrier did not complete")
	}
	if sp.IsRunning(sessionName) {
		t.Fatalf("runtime %q still running after completed drain-ack stop", sessionName)
	}

	pendingSnapshot, err := loadSessionBeadSnapshot(store)
	if err != nil {
		t.Fatalf("load stop-pending snapshot: %v", err)
	}
	finalizeTracker := &asyncStartTracker{}
	if got := finalizeDrainAckStopPendingSessions(
		"", cfg, sp, beads.SessionStore{Store: store}, nil, pendingSnapshot.OpenInfos(),
		dops, dt, finalizeTracker, clk, events.Discard, io.Discard,
	); got != 1 {
		t.Fatalf("finalized stop-pending sessions = %d, want 1", got)
	}
	if !finalizeTracker.wait(-1) {
		t.Fatal("final reconciliation left asynchronous stop work in flight")
	}
	terminal, err := store.Get(sessionBead.ID)
	if err != nil {
		t.Fatalf("Get(terminal session): %v", err)
	}
	if terminal.Status != "closed" || terminal.Metadata["state"] != string(sessionpkg.StateDrained) {
		t.Fatalf("terminal session status=%q state=%q, want closed/drained", terminal.Status, terminal.Metadata["state"])
	}

	assertZeroReplacementCycle := func(label string, cycleCfg *config.City) {
		t.Helper()
		cycle := runCompletedCycle(label, cycleCfg)
		if got := cycle.result.ScaleCheckCounts[template]; got != 0 {
			t.Fatalf("%s: ScaleCheckCounts[%q] = %d, want 0", label, template, got)
		}
		if got := cycle.poolDesired[template]; got != 0 {
			t.Fatalf("%s: poolDesired[%q] = %d, want 0", label, template, got)
		}
		if got := countDesiredSlots(cycle.result.State); got != 0 {
			t.Fatalf("%s: desired %q slots = %d, want 0", label, template, got)
		}
		if len(cycle.result.WorkSet) != 0 || len(cycle.result.ReadyUnassignedRoutedWorkBeads) != 0 {
			t.Fatalf("%s: work_set=%v routed_demand=%+v, want both empty", label, cycle.result.WorkSet, cycle.result.ReadyUnassignedRoutedWorkBeads)
		}
		if cycle.woken != 0 || cycle.startDelta != 0 {
			t.Fatalf("%s: woken=%d start_delta=%d, want both zero", label, cycle.woken, cycle.startDelta)
		}
		if len(cycle.beforeReconcile) != 0 || len(cycle.afterReconcile) != 0 {
			t.Fatalf("%s: open sessions before/after reconcile = %+v / %+v, want none", label, cycle.beforeReconcile, cycle.afterReconcile)
		}
		if got := countPendingClaims(cycle.allSessions); got != 0 {
			t.Fatalf("%s: pending-create claims = %d, want 0", label, got)
		}
		if got := len(cycle.allSessions); got != 1 || cycle.allSessions[0].ID != sessionBead.ID {
			t.Fatalf("%s: all session rows = %+v, want only terminal %s", label, cycle.allSessions, sessionBead.ID)
		}
	}

	// Two completed async-barrier cycles pin the sequential-replacement incident:
	// a held bead remains visible in the store, but cannot mint a first or later
	// replacement after the suspended concrete session has fully drained.
	assertZeroReplacementCycle("held cycle 1", cfg)
	assertZeroReplacementCycle("held cycle 2", cfg)

	// Removing the work hold creates real routed demand. Template suspension is
	// the stronger configuration boundary and must suppress that demand before
	// the positive session-only-suspension control is allowed to start anything.
	if err := store.Update(heldWork.ID, beads.UpdateOpts{RemoveLabels: []string{beadmeta.HoldMayorLabel}}); err != nil {
		t.Fatalf("remove canonical hold: %v", err)
	}
	suspendedCfg := *cfg
	suspendedCfg.Agents = append([]config.Agent(nil), cfg.Agents...)
	suspendedCfg.Agents[0].Suspended = true
	assertZeroReplacementCycle("template suspension control", &suspendedCfg)

	// The next eligible completed pass must create exactly one replacement. Its
	// pending-create claim is visible after session sync, and the async completion
	// barrier must leave that same bead active with the claim cleared.
	positive := runCompletedCycle("session-only suspension control", cfg)
	if got := positive.result.ScaleCheckCounts[template]; got != 1 {
		t.Fatalf("session-only suspension control: ScaleCheckCounts[%q] = %d, want 1", template, got)
	}
	if got := positive.poolDesired[template]; got != 1 {
		t.Fatalf("session-only suspension control: poolDesired[%q] = %d, want 1", template, got)
	}
	if got := countDesiredSlots(positive.result.State); got != 1 {
		t.Fatalf("session-only suspension control: desired %q slots = %d, want 1", template, got)
	}
	if positive.woken != 1 || positive.startDelta != 1 {
		t.Fatalf("session-only suspension control: woken=%d start_delta=%d, want exactly one replacement", positive.woken, positive.startDelta)
	}
	if got := len(positive.beforeReconcile); got != 1 {
		t.Fatalf("session-only suspension control: pre-reconcile sessions = %+v, want one", positive.beforeReconcile)
	}
	replacement := positive.beforeReconcile[0]
	if replacement.ID == sessionBead.ID {
		t.Fatalf("session-only suspension control reused drained session %s", sessionBead.ID)
	}
	if got := replacement.Metadata["state"]; got != string(sessionpkg.StateStartPending) {
		t.Fatalf("session-only suspension control: replacement state before reconcile = %q, want start-pending", got)
	}
	if got := replacement.Metadata["pending_create_claim"]; got != "true" {
		t.Fatalf("session-only suspension control: pending_create_claim before reconcile = %q, want true", got)
	}
	if got := len(positive.afterReconcile); got != 1 || positive.afterReconcile[0].ID != replacement.ID {
		t.Fatalf("session-only suspension control: post-reconcile sessions = %+v, want replacement %s", positive.afterReconcile, replacement.ID)
	}
	started := positive.afterReconcile[0]
	if got := started.Metadata["state"]; got != string(sessionpkg.StateActive) {
		t.Fatalf("session-only suspension control: replacement state after barrier = %q, want active", got)
	}
	if got := started.Metadata["pending_create_claim"]; got != "" {
		t.Fatalf("session-only suspension control: pending_create_claim after barrier = %q, want cleared", got)
	}
	if !sp.IsRunning(started.Metadata["session_name"]) {
		t.Fatalf("session-only suspension control: replacement runtime %q is not running", started.Metadata["session_name"])
	}
	if got := len(positive.allSessions); got != 2 {
		t.Fatalf("session-only suspension control: all session rows = %+v, want one drained plus one replacement", positive.allSessions)
	}
}

func assertDispatchHoldPartial(t *testing.T, template string, partial map[string]bool, errs []error) {
	t.Helper()
	if !partial[template] {
		t.Fatalf("partial templates = %v, want %q", partial, template)
	}
	if len(errs) != 1 {
		t.Fatalf("errs = %v, want one filtered Ready error", errs)
	}
}
