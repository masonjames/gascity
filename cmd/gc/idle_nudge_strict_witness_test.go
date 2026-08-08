package main

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/beadstest"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
)

type strictBackstopExactProvider struct {
	*projectHookIsolationWorkerProvider
}

func (p *strictBackstopExactProvider) requireExact(name string, expected runtime.Incarnation) error {
	checks := map[string]string{
		"GC_SESSION_ID":     expected.SessionID,
		"GC_INSTANCE_TOKEN": expected.InstanceToken,
		"GC_RUNTIME_EPOCH":  expected.Epoch,
	}
	for key, want := range checks {
		got, err := p.GetMeta(name, key)
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("%w: %s=%q want %q", runtime.ErrIncarnationMismatch, key, got, want)
		}
	}
	return nil
}

func (p *strictBackstopExactProvider) RunLiveExact(name string, expected runtime.Incarnation, _ runtime.Config) error {
	return p.requireExact(name, expected)
}

func (p *strictBackstopExactProvider) NudgeExact(name string, expected runtime.Incarnation, content []runtime.ContentBlock) error {
	if err := p.requireExact(name, expected); err != nil {
		return err
	}
	return p.Nudge(name, content)
}

func (p *strictBackstopExactProvider) StopExact(name string, expected runtime.Incarnation) error {
	if err := p.requireExact(name, expected); err != nil {
		return err
	}
	return p.Stop(name)
}

func (p *strictBackstopExactProvider) SetMetaExact(name string, expected runtime.Incarnation, key, value string) error {
	if err := p.requireExact(name, expected); err != nil {
		return err
	}
	return p.SetMeta(name, key, value)
}

func (p *strictBackstopExactProvider) RemoveMetaExact(name string, expected runtime.Incarnation, key string) error {
	if err := p.requireExact(name, expected); err != nil {
		return err
	}
	return p.RemoveMeta(name, key)
}

var (
	_ runtime.ExactIncarnationProvider         = (*strictBackstopExactProvider)(nil)
	_ runtime.ExactIncarnationMetadataProvider = (*strictBackstopExactProvider)(nil)
	_ runtime.Provider                         = (*strictBackstopExactProvider)(nil)
)

type strictBackstopWitnessDriftStore struct {
	*beads.MemStore
	sessionID string
	drift     map[string]string
	readErr   error

	once sync.Once
	mu   sync.Mutex

	getCalled           bool
	drifted             bool
	revisionBeforeDrift int64
	revisionAfterDrift  int64
	driftErr            error
}

func (s *strictBackstopWitnessDriftStore) Get(id string) (beads.Bead, error) {
	if id == s.sessionID {
		s.once.Do(func() {
			before, err := s.MemStore.Get(id)
			s.mu.Lock()
			s.getCalled = true
			if err != nil {
				s.driftErr = err
				s.mu.Unlock()
				return
			}
			s.revisionBeforeDrift = before.Revision
			if s.readErr != nil {
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()

			if err := s.SetMetadataBatch(id, s.drift); err != nil {
				s.mu.Lock()
				s.driftErr = err
				s.mu.Unlock()
				return
			}
			after, err := s.MemStore.Get(id)
			s.mu.Lock()
			defer s.mu.Unlock()
			if err != nil {
				s.driftErr = err
				return
			}
			s.drifted = true
			s.revisionAfterDrift = after.Revision
		})

		s.mu.Lock()
		readErr := s.readErr
		driftErr := s.driftErr
		s.mu.Unlock()
		if driftErr != nil {
			return beads.Bead{}, driftErr
		}
		if readErr != nil {
			return beads.Bead{}, readErr
		}
	}
	return s.MemStore.Get(id)
}

func (s *strictBackstopWitnessDriftStore) snapshot() (called, drifted bool, before, after int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getCalled, s.drifted, s.revisionBeforeDrift, s.revisionAfterDrift, s.driftErr
}

func strictIdleClaimFixture(t *testing.T) (*config.City, beads.Bead, *strictBackstopExactProvider, string) {
	t.Helper()
	cityPath := t.TempDir()
	workRoot := t.TempDir()
	maxSessions := 1
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Providers: map[string]config.ProviderSpec{"codex": {Command: "codex", PathCheck: "true"}},
		Agents: []config.Agent{{
			Name:              "agent-a",
			Provider:          "codex",
			WorkDir:           filepath.Join(workRoot, "{{.AgentBase}}"),
			ProjectHooks:      config.ProjectHooksForbid,
			MaxActiveSessions: &maxSessions,
			Nudge:             "claim now",
		}},
	}
	sessionBead := idleClaimPoolSession()
	sessionBead.Metadata["state"] = "active"
	sessionBead.Metadata["provider"] = "codex"
	sessionBead.Metadata["command"] = "codex"
	sessionBead.Metadata["work_dir"] = filepath.Join(workRoot, "agent-a")
	sessionBead.Metadata["agent_name"] = "agent-a"
	sessionBead.Metadata["generation"] = "1"
	sessionBead.Metadata["instance_token"] = "instance-a"
	sessionBead.Metadata[testTriggerBeadStoreRefKey] = "city:fixture-city"
	fake := runningIdleClaimFake(t, "session-a")
	for key, value := range map[string]string{
		"GC_SESSION_ID":     sessionBead.ID,
		"GC_INSTANCE_TOKEN": "instance-a",
		"GC_RUNTIME_EPOCH":  "1",
	} {
		if err := fake.SetMeta("session-a", key, value); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	provider := &strictBackstopExactProvider{projectHookIsolationWorkerProvider: &projectHookIsolationWorkerProvider{Fake: fake, supported: true}}
	return cfg, sessionBead, provider, cityPath
}

func seedStrictBackstopMarker(sessionBead *beads.Bead, observedAt time.Time) {
	sessionBead.Metadata[idleClaimNudgeTriggerKey] = "work-a"
	sessionBead.Metadata[idleClaimNudgeTriggerStoreRefKey] = "city:fixture-city"
	sessionBead.Metadata[idleClaimNudgeCountKey] = "0"
	sessionBead.Metadata[idleClaimNudgeAtKey] = observedAt.UTC().Format(time.RFC3339)
}

func assertStrictBackstopMarkerUnchanged(t *testing.T, got beads.Bead, observedAt time.Time) {
	t.Helper()
	if value := got.Metadata[idleClaimNudgeTriggerKey]; value != "work-a" {
		t.Fatalf("marker trigger = %q, want work-a", value)
	}
	if value := got.Metadata[idleClaimNudgeTriggerStoreRefKey]; value != "city:fixture-city" {
		t.Fatalf("marker trigger store = %q, want city:fixture-city", value)
	}
	if value := got.Metadata[idleClaimNudgeCountKey]; value != "0" {
		t.Fatalf("marker count = %q, want unchanged 0", value)
	}
	if value := got.Metadata[idleClaimNudgeAtKey]; value != observedAt.UTC().Format(time.RFC3339) {
		t.Fatalf("marker time = %q, want unchanged %q", value, observedAt.UTC().Format(time.RFC3339))
	}
}

func TestStrictIdleClaimBackstopRefusesPreReserveWitnessDrift(t *testing.T) {
	tests := []struct {
		name  string
		drift map[string]string
		err   error
	}{
		{
			name: "trigger cleared",
			drift: map[string]string{
				testTriggerBeadIDKey:       "",
				testTriggerBeadStoreRefKey: "",
			},
		},
		{
			name: "trigger repointed",
			drift: map[string]string{
				testTriggerBeadIDKey:       "work-b",
				testTriggerBeadStoreRefKey: "city:fixture-city",
			},
		},
		{
			name:  "session suspended",
			drift: map[string]string{"state": "suspended"},
		},
		{
			name: "same id different store",
			drift: map[string]string{
				testTriggerBeadIDKey:       "work-a",
				testTriggerBeadStoreRefKey: "rig:other",
			},
		},
		{
			name: "authoritative read failure",
			err:  errors.New("injected authoritative read failure"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, sessionBead, provider, cityPath := strictIdleClaimFixture(t)
			observedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			seedStrictBackstopMarker(&sessionBead, observedAt)
			backing := beads.NewMemStoreFrom(0, []beads.Bead{sessionBead}, nil)
			initial, err := backing.Get(sessionBead.ID)
			if err != nil {
				t.Fatalf("initial Get: %v", err)
			}
			store := &strictBackstopWitnessDriftStore{
				MemStore:  backing,
				sessionID: sessionBead.ID,
				drift:     tt.drift,
				readErr:   tt.err,
			}
			var out bytes.Buffer

			nudgeStalledPoolClaims(
				provider,
				cfg,
				store,
				[]beads.Bead{sessionBead},
				[]beads.Bead{{ID: "work-a", Status: "open"}},
				[]string{"city:fixture-city"},
				observedAt.Add(idleClaimNudgeGrace+time.Second),
				&out,
				cityPath,
			)

			called, drifted, beforeDrift, afterDrift, driftErr := store.snapshot()
			if driftErr != nil {
				t.Fatalf("injecting witness drift: %v", driftErr)
			}
			if !called {
				t.Fatal("strict backstop did not take an authoritative session read before reserve")
			}
			wantRevision := initial.Revision
			if tt.err == nil {
				if !drifted {
					t.Fatal("strict backstop did not reach injected witness drift")
				}
				if beforeDrift != initial.Revision {
					t.Fatalf("revision before drift = %d, want initial %d (marker mutated before authorization)", beforeDrift, initial.Revision)
				}
				if afterDrift != initial.Revision+1 {
					t.Fatalf("revision after drift = %d, want %d", afterDrift, initial.Revision+1)
				}
				wantRevision = afterDrift
			}
			final, err := backing.Get(sessionBead.ID)
			if err != nil {
				t.Fatalf("final Get: %v", err)
			}
			if final.Revision != wantRevision {
				t.Fatalf("final revision = %d, want %d (strict refusal must be mutation-free)", final.Revision, wantRevision)
			}
			assertStrictBackstopMarkerUnchanged(t, final, observedAt)
			if got := provider.CountCalls("Nudge", "session-a"); got != 0 {
				t.Fatalf("provider Nudge calls = %d, want 0 after strict witness refusal", got)
			}
		})
	}
}

func TestStrictIdleClaimBackstopAuthorizesObservationAndClearBeforeMutation(t *testing.T) {
	tests := []struct {
		name string
		work []beads.Bead
		seed func(*beads.Bead, time.Time)
	}{
		{
			name: "observe",
			work: []beads.Bead{{ID: "work-a", Status: "open"}},
			seed: func(sessionBead *beads.Bead, observedAt time.Time) {
				sessionBead.Metadata[idleClaimNudgeTriggerKey] = "old-work"
				sessionBead.Metadata[idleClaimNudgeTriggerStoreRefKey] = "city:fixture-city"
				sessionBead.Metadata[idleClaimNudgeCountKey] = "2"
				sessionBead.Metadata[idleClaimNudgeAtKey] = observedAt.UTC().Format(time.RFC3339)
			},
		},
		{
			name: "clear",
			work: nil,
			seed: seedStrictBackstopMarker,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, sessionBead, provider, cityPath := strictIdleClaimFixture(t)
			observedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			tt.seed(&sessionBead, observedAt)
			backing := beads.NewMemStoreFrom(0, []beads.Bead{sessionBead}, nil)
			initial, err := backing.Get(sessionBead.ID)
			if err != nil {
				t.Fatalf("initial Get: %v", err)
			}
			store := &strictBackstopWitnessDriftStore{
				MemStore:  backing,
				sessionID: sessionBead.ID,
				readErr:   errors.New("injected authoritative read failure"),
			}
			var out bytes.Buffer

			nudgeStalledPoolClaims(
				provider,
				cfg,
				store,
				[]beads.Bead{sessionBead},
				tt.work,
				[]string{"city:fixture-city"},
				observedAt.Add(idleClaimNudgeGrace+time.Second),
				&out,
				cityPath,
			)

			called, _, _, _, driftErr := store.snapshot()
			if driftErr != nil {
				t.Fatalf("authoritative read fixture: %v", driftErr)
			}
			if !called {
				t.Fatalf("strict backstop did not authorize %s through an authoritative read", tt.name)
			}
			final, err := backing.Get(sessionBead.ID)
			if err != nil {
				t.Fatalf("final Get: %v", err)
			}
			if final.Revision != initial.Revision {
				t.Fatalf("final revision = %d, want unchanged %d after %s authorization failure", final.Revision, initial.Revision, tt.name)
			}
			if got := provider.CountCalls("Nudge", "session-a"); got != 0 {
				t.Fatalf("provider Nudge calls = %d, want 0", got)
			}
		})
	}
}

func TestIdleClaimBackstopMarkerIdentityIncludesTriggerStoreRef(t *testing.T) {
	provider := runningIdleClaimFake(t, "session-a")
	cfg := idleClaimTestCfg()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sessionBead := idleClaimPoolSession()
	sessionBead.Metadata[testTriggerBeadStoreRefKey] = "rig:new"
	sessionBead.Metadata[idleClaimNudgeTriggerKey] = "work-a"
	sessionBead.Metadata[idleClaimNudgeTriggerStoreRefKey] = "city:old"
	sessionBead.Metadata[idleClaimNudgeCountKey] = "2"
	sessionBead.Metadata[idleClaimNudgeAtKey] = base.Format(time.RFC3339)
	store := beads.NewMemStoreFrom(0, []beads.Bead{sessionBead}, nil)
	var out bytes.Buffer

	nudgeStalledPoolClaims(
		provider,
		cfg,
		store,
		[]beads.Bead{sessionBead},
		[]beads.Bead{{ID: "work-a", Status: "open"}},
		[]string{"rig:new"},
		base.Add(time.Hour),
		&out,
	)

	if got := provider.CountCalls("Nudge", "session-a"); got != 0 {
		t.Fatalf("provider Nudge calls = %d, want 0 while a new store-scoped grace window is observed", got)
	}
	final, err := store.Get(sessionBead.ID)
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	if got := final.Metadata[idleClaimNudgeTriggerKey]; got != "work-a" {
		t.Fatalf("marker trigger = %q, want work-a", got)
	}
	if got := final.Metadata[idleClaimNudgeTriggerStoreRefKey]; got != "rig:new" {
		t.Fatalf("marker store ref = %q, want rig:new", got)
	}
	if got := final.Metadata[idleClaimNudgeCountKey]; got != "0" {
		t.Fatalf("marker count = %q, want fresh observation count 0", got)
	}
	if got := final.Metadata[idleClaimNudgeAtKey]; got != base.Add(time.Hour).Format(time.RFC3339) {
		t.Fatalf("marker at = %q, want new-store observation %q", got, base.Add(time.Hour).Format(time.RFC3339))
	}
}

func TestStrictIdleClaimBackstopAuthorizedReserveRemainsWriteAhead(t *testing.T) {
	cfg, sessionBead, provider, cityPath := strictIdleClaimFixture(t)
	observedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedStrictBackstopMarker(&sessionBead, observedAt)
	store := beads.NewMemStoreFrom(0, []beads.Bead{sessionBead}, nil)
	var out bytes.Buffer

	nudgeStalledPoolClaims(
		provider,
		cfg,
		store,
		[]beads.Bead{sessionBead},
		[]beads.Bead{{ID: "work-a", Status: "open"}},
		[]string{"city:fixture-city"},
		observedAt.Add(idleClaimNudgeGrace+time.Second),
		&out,
		cityPath,
	)

	final, err := store.Get(sessionBead.ID)
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	if got := final.Metadata[idleClaimNudgeCountKey]; got != "1" {
		t.Fatalf("marker count = %q, want write-ahead reservation 1", got)
	}
	if got := provider.CountCalls("Nudge", "session-a"); got != 1 {
		t.Fatalf("provider Nudge calls = %d, want 1 after authorized reservation", got)
	}
}

func TestStrictDrainAckClearUsesExactIncarnationAndChainsRevision(t *testing.T) {
	cfg, sessionBead, provider, _ := strictIdleClaimFixture(t)
	for key, value := range map[string]string{
		"GC_DRAIN_ACK":                  "1",
		reconcilerDrainAckSourceKey:     reconcilerDrainAckSourceValue,
		reconcilerDrainAckReasonKey:     "suspended",
		reconcilerDrainAckGenerationKey: "1",
	} {
		if err := provider.SetMeta("session-a", key, value); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	store := beads.NewMemStoreFrom(0, []beads.Bead{sessionBead}, nil)
	captured := mustGetTestBead(t, store, sessionBead.ID)
	boundary := captureReconcilerMutationBoundary(
		session.InfoFromPersistedBead(captured), cfg, captured.Revision,
	).lifecycleMutation()

	if !clearReconcilerDrainAckMetadataAtBoundary(
		session.InfoFromPersistedBead(captured), cfg, store, provider, boundary,
	) {
		t.Fatal("strict exact-incarnation drain-ack clear refused")
	}
	for _, key := range []string{"GC_DRAIN_ACK", reconcilerDrainAckSourceKey, reconcilerDrainAckReasonKey, reconcilerDrainAckGenerationKey} {
		if got, err := provider.GetMeta("session-a", key); err != nil || got != "" {
			t.Fatalf("%s = %q, %v; want cleared", key, got, err)
		}
	}
	final := mustGetTestBead(t, store, sessionBead.ID)
	if final.Revision <= captured.Revision {
		t.Fatalf("revision = %d, want greater than captured %d after chained exact metadata effects", final.Revision, captured.Revision)
	}
}

func TestStrictPhaseTwoDrainFailsClosedWithoutLeasedDestructiveBoundary(t *testing.T) {
	cfg, sessionBead, provider, _ := strictIdleClaimFixture(t)
	store := beads.NewMemStoreFrom(0, []beads.Bead{sessionBead}, nil)
	captured := mustGetTestBead(t, store, sessionBead.ID)
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	dt := newDrainTracker()
	dt.set(sessionBead.ID, &drainState{
		startedAt:  now.Add(-time.Minute),
		deadline:   now.Add(-time.Second),
		reason:     "suspended",
		generation: 1,
	})
	providerCalls := len(provider.SnapshotCalls())

	advanceSessionDrainsWithSessionsTraced(
		dt,
		provider,
		store,
		func(id string) (session.Info, bool) {
			row, err := store.Get(id)
			if err != nil {
				return session.Info{}, false
			}
			return session.InfoFromPersistedBead(row), true
		},
		map[string]wakeEvaluation{},
		cfg,
		&clock.Fake{Time: now},
		nil,
		func(info session.Info) reconcilerMutationBoundary {
			return captureReconcilerMutationBoundary(info, cfg, captured.Revision)
		},
	)

	if dt.get(sessionBead.ID) == nil {
		t.Fatal("strict drain was consumed despite missing leased destructive worker boundary")
	}
	final := mustGetTestBead(t, store, sessionBead.ID)
	if final.Revision != captured.Revision {
		t.Fatalf("revision = %d, want unchanged %d", final.Revision, captured.Revision)
	}
	if got := len(provider.SnapshotCalls()); got != providerCalls {
		t.Fatalf("provider calls advanced from %d to %d; strict unsupported drain must have zero effects", providerCalls, got)
	}
}

func TestStrictContinuationBackstopRefusesPreReserveWitnessDrift(t *testing.T) {
	cfg, sessionBead, provider, cityPath := strictIdleClaimFixture(t)
	sessionBead.Metadata["generation"] = "1"
	sessionBead.Metadata["alias"] = "current-alias"
	candidate := validContinuationCandidate("step-a", "session-a")
	now := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	backing := beads.NewMemStoreFrom(0, []beads.Bead{sessionBead}, nil)
	seedContinuationMarker(t, backing, sessionBead, candidate, 0, now.Add(-idleClaimNudgeGrace-time.Second))
	sessionBead = mustGetTestBead(t, backing, sessionBead.ID)
	initialRevision := sessionBead.Revision
	store := &strictBackstopWitnessDriftStore{
		MemStore:  backing,
		sessionID: sessionBead.ID,
		drift:     map[string]string{"state": "suspended"},
	}
	var out bytes.Buffer

	nudgeStalledPoolContinuations(
		provider,
		cfg,
		store,
		[]beads.Bead{sessionBead},
		[]ContinuationClaimCandidate{candidate},
		false,
		now,
		&out,
		cityPath,
	)

	called, drifted, beforeDrift, afterDrift, driftErr := store.snapshot()
	if driftErr != nil {
		t.Fatalf("injecting continuation witness drift: %v", driftErr)
	}
	if !called || !drifted {
		t.Fatalf("continuation authorization = {called:%v drifted:%v}, want both true", called, drifted)
	}
	if beforeDrift != initialRevision || afterDrift != initialRevision+1 {
		t.Fatalf("continuation revisions = {%d %d}, want {%d %d}", beforeDrift, afterDrift, initialRevision, initialRevision+1)
	}
	final := mustGetTestBead(t, backing, sessionBead.ID)
	if final.Revision != afterDrift {
		t.Fatalf("final revision = %d, want witness-only revision %d", final.Revision, afterDrift)
	}
	if got := final.Metadata[continuationClaimNudgeCountKey]; got != "0" {
		t.Fatalf("continuation marker count = %q, want unchanged 0", got)
	}
	if got := provider.CountCalls("Nudge", "session-a"); got != 0 {
		t.Fatalf("provider Nudge calls = %d, want 0", got)
	}
}

func TestStrictBackstopMissingSessionRowIsZeroWrite(t *testing.T) {
	cfg, sessionBead, provider, cityPath := strictIdleClaimFixture(t)
	observedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedStrictBackstopMarker(&sessionBead, observedAt)
	store := beadstest.NewRecordingStore(beads.NewMemStore())
	var out bytes.Buffer

	nudgeStalledPoolClaims(
		provider,
		cfg,
		store,
		[]beads.Bead{sessionBead},
		[]beads.Bead{{ID: "work-a", Status: "open"}},
		[]string{"city:fixture-city"},
		observedAt.Add(idleClaimNudgeGrace+time.Second),
		&out,
		cityPath,
	)

	if calls := store.Calls(); len(calls) != 0 {
		t.Fatalf("missing-row mutation calls = %#v, want none", calls)
	}
	if got := provider.CountCalls("Nudge", "session-a"); got != 0 {
		t.Fatalf("provider Nudge calls = %d, want 0", got)
	}
}

func TestStrictIdleClaimBackstopRejectsSplitSessionAndWorkStoresBeforeEveryMarkerMutation(t *testing.T) {
	tests := []struct {
		name string
		work []beads.Bead
		seed func(*beads.Bead, time.Time)
	}{
		{
			name: "reserve",
			work: []beads.Bead{{ID: "work-a", Status: "open"}},
			seed: seedStrictBackstopMarker,
		},
		{
			name: "observe",
			work: []beads.Bead{{ID: "work-a", Status: "open"}},
			seed: func(sessionBead *beads.Bead, observedAt time.Time) {
				sessionBead.Metadata[idleClaimNudgeTriggerKey] = "old-work"
				sessionBead.Metadata[idleClaimNudgeTriggerStoreRefKey] = "city:fixture-city"
				sessionBead.Metadata[idleClaimNudgeCountKey] = "2"
				sessionBead.Metadata[idleClaimNudgeAtKey] = observedAt.UTC().Format(time.RFC3339)
			},
		},
		{
			name: "clear",
			work: nil,
			seed: seedStrictBackstopMarker,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, sessionBead, provider, cityPath := strictIdleClaimFixture(t)
			cfg.Storage = &config.StorageConfig{Classes: config.StorageClasses{
				Work:      config.StorageWorkBinding,
				Graph:     "infra",
				Sessions:  "infra",
				Messaging: "infra",
				Orders:    "infra",
				Nudges:    "infra",
			}}
			observedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			tt.seed(&sessionBead, observedAt)
			sessionStore := beads.NewMemStoreFrom(0, []beads.Bead{sessionBead}, nil)
			// The physical work store deliberately carries the same session ID and
			// trigger pair. ID equality must never substitute for physical store
			// ownership at the strict automatic-mutation boundary.
			workStore := beads.NewMemStoreFrom(0, []beads.Bead{sessionBead}, nil)
			initialSession := mustGetTestBead(t, sessionStore, sessionBead.ID)
			initialWorkTwin := mustGetTestBead(t, workStore, sessionBead.ID)
			var out bytes.Buffer

			nudgeStalledPoolClaimsWithCanonicalWorkStore(
				provider,
				cfg,
				sessionStore,
				workStore,
				[]beads.Bead{sessionBead},
				tt.work,
				[]string{"city:fixture-city"},
				observedAt.Add(idleClaimNudgeGrace+time.Second),
				&out,
				cityPath,
			)

			finalSession := mustGetTestBead(t, sessionStore, sessionBead.ID)
			if finalSession.Revision != initialSession.Revision {
				t.Fatalf("session revision = %d, want unchanged %d before split-store %s refusal", finalSession.Revision, initialSession.Revision, tt.name)
			}
			finalWorkTwin := mustGetTestBead(t, workStore, sessionBead.ID)
			if finalWorkTwin.Revision != initialWorkTwin.Revision {
				t.Fatalf("work-store twin revision = %d, want unchanged %d", finalWorkTwin.Revision, initialWorkTwin.Revision)
			}
			if got := provider.CountCalls("Nudge", "session-a"); got != 0 {
				t.Fatalf("provider Nudge calls = %d, want 0", got)
			}
		})
	}
}

var _ beads.Store = (*strictBackstopWitnessDriftStore)(nil)
