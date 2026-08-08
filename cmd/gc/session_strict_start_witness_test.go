package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/beadstest"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/worker"
)

type conditionalRecordingStore struct {
	*beadstest.RecordingStore
}

type gatedStrictExactStartProvider struct {
	*strictBackstopExactProvider
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *gatedStrictExactStartProvider) ObserveExact(name string, expected runtime.Incarnation, _ []string) (runtime.IncarnationObservation, error) {
	if !p.IsRunning(name) {
		return runtime.IncarnationObservation{}, runtime.ErrSessionNotFound
	}
	if err := p.requireExact(name, expected); err != nil {
		return runtime.IncarnationObservation{}, err
	}
	return runtime.IncarnationObservation{Running: true, Alive: true}, nil
}

func (p *gatedStrictExactStartProvider) StartExact(ctx context.Context, name string, desired runtime.Incarnation, cfg runtime.Config) error {
	p.once.Do(func() { close(p.entered) })
	select {
	case <-p.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := p.Start(ctx, name, cfg); err != nil {
		return err
	}
	for key, value := range map[string]string{
		"GC_SESSION_ID":                  desired.SessionID,
		"GC_INSTANCE_TOKEN":              desired.InstanceToken,
		"GC_RUNTIME_EPOCH":               desired.Epoch,
		session.RuntimeOperationTokenEnv: desired.OperationToken,
	} {
		if err := p.SetMeta(name, key, value); err != nil {
			return err
		}
	}
	return nil
}

func TestExecutePlannedStartsRunsStrictCandidateSynchronouslyWhenAsyncEnabled(t *testing.T) {
	store := beads.NewMemStore()
	workDir := t.TempDir()
	if _, err := store.Create(beads.Bead{ID: "work-exact", Title: "strict demand", Type: "task"}); err != nil {
		t.Fatalf("Create(work): %v", err)
	}
	provider := &gatedStrictExactStartProvider{
		strictBackstopExactProvider: &strictBackstopExactProvider{
			projectHookIsolationWorkerProvider: &projectHookIsolationWorkerProvider{Fake: runtime.NewFake(), supported: true},
		},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	t.Cleanup(func() {
		select {
		case <-provider.release:
		default:
			close(provider.release)
		}
	})
	manager := session.NewManagerWithOptions(store, provider)
	created, err := manager.CreateSession(context.Background(), session.CreateOptions{
		BeadOnly: true,
		Template: "strict-worker",
		Title:    "Strict Worker",
		Command:  "codex",
		WorkDir:  workDir,
		Provider: "codex",
		ExtraMeta: map[string]string{
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:test-city",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	captured, persisted, err := sessionFrontDoor(store).GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("GetPersistedResponse: %v", err)
	}
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Providers: map[string]config.ProviderSpec{
			"codex": {Command: "codex", PathCheck: "true"},
		},
		Agents: []config.Agent{{
			Name:              "strict-worker",
			Provider:          "codex",
			Session:           config.SessionTransportTmux,
			WorkDir:           workDir,
			ProjectHooks:      config.ProjectHooksForbid,
			MaxActiveSessions: intPtr(1),
		}},
	}
	tp := TemplateParams{
		Command:      "codex",
		SessionName:  captured.SessionNameMetadata,
		TemplateName: "strict-worker",
		ResolvedProvider: &config.ResolvedProvider{
			Name:    "codex",
			Command: "codex",
		},
	}
	candidate := captureStartCandidateForWake(captured, tp, 0, cfg, "work-exact", persisted.Revision)
	if !candidate.strictLaunch || candidate.policyErr != nil || candidate.name() == "" || candidate.logicalTemplate(cfg) == "" {
		t.Fatalf("captured strict candidate = (strict=%t policyErr=%v name=%q template=%q info=%+v)", candidate.strictLaunch, candidate.policyErr, candidate.name(), candidate.logicalTemplate(cfg), captured)
	}
	resolvedRuntime, err := resolvedWorkerRuntimeWithConfig("", cfg, captured, "")
	if err != nil {
		t.Fatalf("resolve strict runtime: %v", err)
	}
	if resolvedRuntime == nil || resolvedRuntime.Hints.ProviderName == "" || resolvedRuntime.WorkDir == "" || !resolvedRuntime.Hints.ProjectHooksForbidden {
		t.Fatalf("resolved strict runtime = %+v", resolvedRuntime)
	}
	desired := map[string]TemplateParams{tp.SessionName: tp}
	if waves, ok := candidateWaveOrder([]startCandidate{candidate}, cfg, desired, provider, "test-city", store, &clock.Fake{}); !ok || len(waves) != 1 || waves[0] != 0 {
		t.Fatalf("candidate waves = %v, ok=%t", waves, ok)
	}
	if !allDependenciesAliveForTemplateWithClock(candidate.logicalTemplate(cfg), cfg, desired, provider, "test-city", store, &clock.Fake{}) {
		t.Fatal("dependency gate rejected strict candidate")
	}
	var stderr bytes.Buffer
	recorder := &capturingRecorder{}
	done := make(chan int, 1)
	go func() {
		done <- executePlannedStartsTraced(
			context.Background(), []startCandidate{candidate}, cfg, desired, provider, store,
			"test-city", "", &clock.Fake{Time: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)},
			recorder, time.Minute, io.Discard, &stderr, nil,
			withAsyncStartExecution(),
			withCanonicalCityWorkStore(beads.WorkStore{Store: store}),
		)
	}()

	select {
	case <-provider.entered:
	case got := <-done:
		t.Fatalf("strict reconcile returned before exact Start: woken=%d stderr=%q", got, stderr.String())
	case <-time.After(2 * time.Second):
		t.Fatalf("strict exact Start was not reached; stderr=%q", stderr.String())
	}
	select {
	case got := <-done:
		t.Fatalf("strict async-enabled reconcile returned before Start completed: woken=%d stderr=%q", got, stderr.String())
	case <-time.After(100 * time.Millisecond):
	}
	close(provider.release)
	select {
	case got := <-done:
		if got != 1 {
			t.Fatalf("woken = %d, want 1; stderr=%q", got, stderr.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("strict reconcile did not finish after Start release; stderr=%q", stderr.String())
	}
}

func (s *conditionalRecordingStore) ConditionalWriterHandle() (beads.ConditionalWriter, bool) {
	if s == nil || s.RecordingStore == nil {
		return nil, false
	}
	return beads.ConditionalWriterFor(s.Store)
}

func (s *conditionalRecordingStore) ConditionalWritesResolveTarget() beads.Store {
	if s == nil || s.RecordingStore == nil {
		return nil
	}
	return s.Store
}

func TestPrepareStrictStartCandidateRejectsCapturedTriggerDriftBeforeEveryMutation(t *testing.T) {
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
			recorder := &conditionalRecordingStore{RecordingStore: beadstest.NewRecordingStore(backing)}
			provider := runtime.NewFake()
			manager := session.NewManagerWithOptions(recorder, provider)
			created, err := manager.CreateSession(context.Background(), session.CreateOptions{
				BeadOnly: true,
				Template: "strict-worker",
				Title:    "Strict Worker",
				Command:  "codex",
				WorkDir:  t.TempDir(),
				Provider: "codex",
				ExtraMeta: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			_, capturedPersisted, err := sessionFrontDoor(backing).GetPersistedResponse(created.ID)
			if err != nil {
				t.Fatalf("capture decision revision: %v", err)
			}
			if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: drift.meta}); err != nil {
				t.Fatalf("inject drift: %v", err)
			}
			recorder.Reset()
			circuitCalls := 0
			cfg := &config.City{Agents: []config.Agent{{
				Name:         "strict-worker",
				ProjectHooks: config.ProjectHooksForbid,
			}}}
			candidate := captureStartCandidateForWake(
				created,
				TemplateParams{TemplateName: "strict-worker"},
				0,
				cfg,
				"work-exact",
				capturedPersisted.Revision,
			)
			if !candidate.strictLaunch {
				t.Fatal("production candidate capture did not derive strict launch policy")
			}
			if candidate.expectedWitness != (session.LiveBoundaryWitness{TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"}) {
				t.Fatalf("captured witness = %+v, want exact trigger pair", candidate.expectedWitness)
			}
			_, err = prepareStartCandidateForCity(
				candidate,
				"/cities/fixture-city",
				"fixture-city",
				cfg,
				provider,
				recorder,
				beads.WorkStore{Store: recorder},
				&clock.Fake{},
				io.Discard,
				nil,
				func(session.Info) error {
					circuitCalls++
					return recorder.SetMetadata(created.ID, "circuit_restart_count", "1")
				},
			)
			if !errors.Is(err, session.ErrConditionalMutationLost) {
				t.Fatalf("prepare error = %v, want ErrConditionalMutationLost", err)
			}
			if circuitCalls != 0 {
				t.Fatalf("circuit callback calls = %d, want 0", circuitCalls)
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("session writes after drift = %+v, want none", calls)
			}
			if calls := provider.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("provider observe/stop/start calls after drift = %+v, want none", calls)
			}

			handled, fold := cycleAliveSessionForFreshReassign(
				created,
				candidate.tp,
				provider,
				recorder,
				cfg,
				nil,
				created.SessionNameMetadata,
				"work-next",
				time.Now(),
				io.Discard,
				io.Discard,
				nil,
			)
			if !handled || fold != nil {
				t.Fatalf("fresh reassign after drift = (handled=%t, fold=%+v), want handled refusal", handled, fold)
			}
			if committed := commitAsyncStartResultWithContext(
				context.Background(),
				startResult{
					prepared: preparedStart{candidate: candidate},
					outcome:  TraceOutcomeSuccess,
					started:  time.Now(),
					finished: time.Now(),
				},
				provider,
				recorder,
				&clock.Fake{},
				nil,
				0,
				io.Discard,
				io.Discard,
				nil,
			); committed {
				t.Fatal("async start result committed after strict witness drift")
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("fresh-cycle/async mutations after drift = %+v, want none", calls)
			}
			if calls := provider.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("fresh-cycle/async provider calls after drift = %+v, want none", calls)
			}
		})
	}
}

type failNthGetStore struct {
	beads.Store
	failAt int
	gets   int
}

func (s *failNthGetStore) Get(id string) (beads.Bead, error) {
	s.gets++
	if s.gets == s.failAt {
		return beads.Bead{}, errors.New("injected session read failure")
	}
	return s.Store.Get(id)
}

func (s *failNthGetStore) ConditionalWriterHandle() (beads.ConditionalWriter, bool) {
	return beads.ConditionalWriterFor(s.Store)
}

func (s *failNthGetStore) ConditionalWritesResolveTarget() beads.Store {
	return s.Store
}

func TestPrepareStrictStartCandidateCommitsUnderCapturedRevisionLease(t *testing.T) {
	store := beads.NewMemStore()
	provider := runtime.NewFake()
	manager := session.NewManagerWithOptions(store, provider)
	created, err := manager.CreateSession(context.Background(), session.CreateOptions{
		BeadOnly: true,
		Template: "strict-worker",
		Title:    "Strict Worker",
		Command:  "true",
		WorkDir:  t.TempDir(),
		Provider: "fake",
		ExtraMeta: map[string]string{
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	_, persisted, err := sessionFrontDoor(store).GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("capture decision: %v", err)
	}
	cfg := &config.City{Agents: []config.Agent{{
		Name:         "strict-worker",
		ProjectHooks: config.ProjectHooksForbid,
	}}}
	candidate := captureStartCandidateForWake(
		created,
		TemplateParams{TemplateName: "strict-worker"},
		0,
		cfg,
		"work-exact",
		persisted.Revision,
	)
	prepared, err := prepareStartCandidateForCity(
		candidate,
		"/cities/fixture-city",
		"fixture-city",
		cfg,
		provider,
		store,
		beads.WorkStore{Store: store},
		&clock.Fake{Time: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)},
		io.Discard,
		nil,
	)
	if err != nil {
		t.Fatalf("prepareStartCandidateForCity: %v", err)
	}
	if prepared == nil {
		t.Fatal("prepared start is nil")
	}
	final, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get final: %v", err)
	}
	if final.Revision <= persisted.Revision {
		t.Fatalf("final revision=%d, want > captured %d", final.Revision, persisted.Revision)
	}
	if final.Metadata[session.ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("conditional lease remains after preparation: %q", final.Metadata[session.ConditionalMutationLeaseMetadataKey])
	}
	if !prepared.candidate.revisionCaptured || prepared.candidate.capturedRevision != final.Revision {
		t.Fatalf("prepared decision revision=(%d,%t), want final %d", prepared.candidate.capturedRevision, prepared.candidate.revisionCaptured, final.Revision)
	}
}

func TestStrictAsyncSecondReadFailureLeavesLeaseAndProviderUntouched(t *testing.T) {
	backing := beads.NewMemStore()
	provider := &strictBackstopExactProvider{projectHookIsolationWorkerProvider: &projectHookIsolationWorkerProvider{
		Fake:      runtime.NewFake(),
		supported: true,
	}}
	manager := session.NewManagerWithOptions(backing, provider)
	workRoot := t.TempDir()
	workDir := filepath.Join(workRoot, "strict-worker")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("create strict work dir: %v", err)
	}
	created, err := manager.CreateSession(context.Background(), session.CreateOptions{
		BeadOnly:  true,
		Template:  "strict-worker",
		Title:     "Strict Worker",
		Command:   "codex",
		WorkDir:   workDir,
		Provider:  "codex",
		Transport: config.SessionTransportTmux,
		ExtraMeta: map[string]string{
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
			"last_woke_at":                          "2026-08-08T12:00:00Z",
			"pending_create_claim":                  "true",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := provider.Start(context.Background(), created.SessionNameMetadata, runtime.Config{Command: "codex"}); err != nil {
		t.Fatalf("seed exact runtime: %v", err)
	}
	for key, value := range map[string]string{
		"GC_SESSION_ID":     created.ID,
		"GC_INSTANCE_TOKEN": created.InstanceToken,
		"GC_RUNTIME_EPOCH":  created.Generation,
	} {
		if err := provider.SetMeta(created.SessionNameMetadata, key, value); err != nil {
			t.Fatalf("seed exact runtime %s: %v", key, err)
		}
	}
	before, err := backing.Get(created.ID)
	if err != nil {
		t.Fatalf("Get before: %v", err)
	}
	maxActive := 1
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Providers: map[string]config.ProviderSpec{"codex": {Command: "codex", PathCheck: "true"}},
		Agents: []config.Agent{{
			Name:              "strict-worker",
			Provider:          "codex",
			WorkDir:           filepath.Join(workRoot, "{{.AgentBase}}"),
			ProjectHooks:      config.ProjectHooksForbid,
			MaxActiveSessions: &maxActive,
		}},
	}
	candidate := captureStartCandidateForWake(created, TemplateParams{TemplateName: "strict-worker"}, 0, cfg, "work-exact", before.Revision)
	faulty := &failNthGetStore{Store: backing, failAt: 2}
	recorder := &conditionalRecordingStore{RecordingStore: beadstest.NewRecordingStore(faulty)}
	providerBaseline := len(provider.SnapshotCalls())
	var stderr bytes.Buffer
	if committed := commitAsyncStartResultWithContext(
		context.Background(),
		startResult{
			prepared: preparedStart{candidate: candidate},
			outcome:  TraceOutcomeSuccess,
			started:  time.Now(),
			finished: time.Now(),
		},
		provider,
		recorder,
		&clock.Fake{},
		nil,
		0,
		io.Discard,
		&stderr,
		nil,
		cfg,
	); committed {
		t.Fatal("async start result committed after second authoritative read failed")
	}
	if faulty.gets != 2 {
		t.Fatalf("session reads = %d, want entry validation plus failing refresh; stderr=%q", faulty.gets, stderr.String())
	}
	if calls := recorder.Calls(); len(calls) != 0 {
		t.Fatalf("session mutations after refresh failure = %+v, want none", calls)
	}
	after, err := backing.Get(created.ID)
	if err != nil {
		t.Fatalf("Get after: %v", err)
	}
	if after.Revision != before.Revision || after.Metadata["last_woke_at"] != before.Metadata["last_woke_at"] || after.Metadata["pending_create_claim"] != before.Metadata["pending_create_claim"] {
		t.Fatalf("lease changed after refresh failure: before revision=%d last_woke=%q claim=%q; after revision=%d last_woke=%q claim=%q",
			before.Revision, before.Metadata["last_woke_at"], before.Metadata["pending_create_claim"],
			after.Revision, after.Metadata["last_woke_at"], after.Metadata["pending_create_claim"])
	}
	if calls := provider.SnapshotCalls()[providerBaseline:]; len(calls) != 0 {
		t.Fatalf("provider calls after refresh failure = %+v, want none", calls)
	}
}

func TestRecoverRunningPendingCreateStrictRejectsCapturedTriggerOrSuspensionDrift(t *testing.T) {
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
			workDir := t.TempDir()
			manager := session.NewManagerWithOptions(backing, provider)
			created, err := manager.CreateSession(context.Background(), session.CreateOptions{
				BeadOnly: true,
				Template: "strict-worker",
				Title:    "Strict Worker",
				Command:  "true",
				WorkDir:  workDir,
				Provider: "fake",
				ExtraMeta: map[string]string{
					"state":                                 string(session.StateActive),
					"pending_create_claim":                  "true",
					"started_config_hash":                   "stale-core-hash",
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			captured, persisted, err := sessionFrontDoor(backing).GetPersistedResponse(created.ID)
			if err != nil {
				t.Fatalf("capture pending-create row: %v", err)
			}
			if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: drift.meta}); err != nil {
				t.Fatalf("inject drift after snapshot: %v", err)
			}
			before, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("Get before recovery: %v", err)
			}
			recorder := &conditionalRecordingStore{RecordingStore: beadstest.NewRecordingStore(backing)}
			providerBaseline := len(provider.SnapshotCalls())
			cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
			tp := TemplateParams{
				Command:      "true",
				SessionName:  captured.SessionNameMetadata,
				TemplateName: "strict-worker",
				WorkDir:      workDir,
				Hints: agent.StartupHints{
					ProviderName:          "fake",
					ProjectHooksForbidden: true,
				},
			}
			ok, fold := recoverRunningPendingCreate(
				captured, tp, cfg, recorder,
				&clock.Fake{Time: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)}, nil,
				captureReconcilerMutationBoundary(captured, cfg, persisted.Revision),
			)
			if ok || fold != nil {
				t.Fatalf("strict pending-create recovery after %s drift = (ok=%t, fold=%+v), want refused with no fold", drift.name, ok, fold)
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("strict pending-create recovery writes after %s drift = %+v, want none", drift.name, calls)
			}
			after, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("Get after recovery: %v", err)
			}
			if after.Revision != before.Revision || after.Metadata["pending_create_claim"] != before.Metadata["pending_create_claim"] || after.Metadata["instance_token"] != before.Metadata["instance_token"] || after.Metadata["started_config_hash"] != before.Metadata["started_config_hash"] {
				t.Fatalf("strict pending-create recovery mutated %s drifted row: before=%+v after=%+v", drift.name, before.Metadata, after.Metadata)
			}
			if calls := provider.SnapshotCalls()[providerBaseline:]; len(calls) != 0 {
				t.Fatalf("strict pending-create recovery provider calls after %s drift = %+v, want none", drift.name, calls)
			}
		})
	}
}

func TestRecoverRunningPendingCreateStrictExactWitnessProceeds(t *testing.T) {
	backing := beads.NewMemStore()
	provider := runtime.NewFake()
	workDir := t.TempDir()
	manager := session.NewManagerWithOptions(backing, provider)
	created, err := manager.CreateSession(context.Background(), session.CreateOptions{
		BeadOnly: true,
		Template: "strict-worker",
		Title:    "Strict Worker",
		Command:  "true",
		WorkDir:  workDir,
		Provider: "fake",
		ExtraMeta: map[string]string{
			"state":                                 string(session.StateActive),
			"pending_create_claim":                  "true",
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	captured, persisted, err := sessionFrontDoor(backing).GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("capture pending-create row: %v", err)
	}
	recorder := &conditionalRecordingStore{RecordingStore: beadstest.NewRecordingStore(backing)}
	providerBaseline := len(provider.SnapshotCalls())
	ok, fold := recoverRunningPendingCreate(
		captured,
		TemplateParams{
			Command:      "true",
			SessionName:  captured.SessionNameMetadata,
			TemplateName: "strict-worker",
			WorkDir:      workDir,
			Hints: agent.StartupHints{
				ProviderName:          "fake",
				ProjectHooksForbidden: true,
			},
		},
		&config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}},
		recorder,
		&clock.Fake{Time: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)}, nil,
		captureReconcilerMutationBoundary(
			captured,
			&config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}},
			persisted.Revision,
		),
	)
	if !ok || fold["pending_create_claim"] != "" {
		t.Fatalf("strict exact pending-create recovery = (ok=%t, fold=%+v), want committed claim clear", ok, fold)
	}
	current, err := backing.Get(created.ID)
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if current.Metadata["pending_create_claim"] != "" || current.Metadata["creation_complete_at"] == "" {
		t.Fatalf("strict exact pending-create row not committed: %+v", current.Metadata)
	}
	if calls := provider.SnapshotCalls()[providerBaseline:]; len(calls) != 0 {
		t.Fatalf("strict exact pending-create recovery provider calls = %+v, want none", calls)
	}
}

func TestExecutePlannedStrictPrepareErrorDoesNotClearLeaseAfterTriggerDrift(t *testing.T) {
	backing := beads.NewMemStore()
	recorder := &conditionalRecordingStore{RecordingStore: beadstest.NewRecordingStore(backing)}
	provider := runtime.NewFake()
	workDir := t.TempDir()
	otherWorkDir := t.TempDir()
	manager := session.NewManagerWithOptions(recorder, provider)
	created, err := manager.CreateSession(context.Background(), session.CreateOptions{
		BeadOnly: true,
		Template: "strict-worker",
		Title:    "Strict Worker",
		Command:  "codex",
		WorkDir:  workDir,
		Provider: "codex",
		ExtraMeta: map[string]string{
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
			"last_woke_at":                          "2026-08-08T12:00:00Z",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	_, persisted, err := sessionFrontDoor(backing).GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("capture start decision: %v", err)
	}
	tp := TemplateParams{
		Command:      "codex",
		SessionName:  created.SessionNameMetadata,
		TemplateName: "strict-worker",
		WorkDir:      workDir,
		Hints: agent.StartupHints{
			ProviderName:          "codex",
			ProjectHooksForbidden: true,
		},
	}
	cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
	candidate := captureStartCandidateForWake(created, tp, 0, cfg, "work-exact", persisted.Revision)
	var afterDrift beads.Bead
	woken := executePlannedStarts(
		context.Background(),
		[]startCandidate{candidate},
		cfg,
		map[string]TemplateParams{created.SessionNameMetadata: tp},
		provider,
		recorder,
		"fixture-city",
		&clock.Fake{Time: time.Date(2026, 8, 8, 12, 1, 0, 0, time.UTC)},
		nil,
		5*time.Second,
		io.Discard,
		io.Discard,
		withTaskWorkDirResolver(func(startCandidate, *config.City) string {
			if updateErr := backing.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{
				beadmeta.TriggerBeadIDMetadataKey: "work-repointed",
			}}); updateErr != nil {
				t.Fatalf("inject trigger drift: %v", updateErr)
			}
			afterDrift, err = backing.Get(created.ID)
			if err != nil {
				t.Fatalf("Get after trigger drift: %v", err)
			}
			recorder.Reset()
			return otherWorkDir
		}),
	)
	if woken != 0 {
		t.Fatalf("woken = %d, want 0 after strict preparation error", woken)
	}
	if afterDrift.ID == "" {
		t.Fatal("work-dir resolver did not execute production preparation path")
	}
	if calls := recorder.Calls(); len(calls) != 0 {
		t.Fatalf("session mutations after strict prepare drift/error = %+v, want none", calls)
	}
	current, err := backing.Get(created.ID)
	if err != nil {
		t.Fatalf("Get final session: %v", err)
	}
	if current.Metadata["last_woke_at"] != afterDrift.Metadata["last_woke_at"] ||
		current.Metadata[beadmeta.TriggerBeadIDMetadataKey] != afterDrift.Metadata[beadmeta.TriggerBeadIDMetadataKey] {
		t.Fatalf("strict prepare error mutated authority after drift: before=%+v after=%+v", afterDrift.Metadata, current.Metadata)
	}
	if current.Metadata[session.ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("strict prepare error left its own conditional lease behind: %q", current.Metadata[session.ConditionalMutationLeaseMetadataKey])
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls after strict preparation error = %+v, want none", calls)
	}
}

func TestCommitSyncStrictStartRejectsPostProviderTriggerOrSuspensionDrift(t *testing.T) {
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
				Command:  "codex",
				WorkDir:  t.TempDir(),
				Provider: "codex",
				ExtraMeta: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
			candidate := captureStartCandidateForWake(created, TemplateParams{TemplateName: "strict-worker"}, 0, cfg, "work-exact")
			if err := provider.SetMeta(created.SessionNameMetadata, "GC_DRAIN_ACK", "1"); err != nil {
				t.Fatalf("SetMeta GC_DRAIN_ACK: %v", err)
			}
			if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: drift.meta}); err != nil {
				t.Fatalf("inject post-provider drift: %v", err)
			}
			afterDrift, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("Get after drift: %v", err)
			}
			recorder := beadstest.NewRecordingStore(backing)
			providerBaseline := len(provider.SnapshotCalls())
			committed := commitSyncStartResultTraced(
				startResult{
					prepared: preparedStart{candidate: candidate},
					outcome:  TraceOutcomeSuccess,
					started:  time.Now(),
					finished: time.Now(),
				},
				provider,
				sessionFrontDoor(recorder),
				&clock.Fake{},
				nil,
				0,
				io.Discard,
				io.Discard,
				nil,
			)
			if committed {
				t.Fatal("synchronous strict start committed after post-provider drift")
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("session mutations after post-provider drift = %+v, want none", calls)
			}
			current, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("Get final session: %v", err)
			}
			if current.Revision != afterDrift.Revision {
				t.Fatalf("session revision after rejected sync commit = %d, want %d", current.Revision, afterDrift.Revision)
			}
			if calls := provider.SnapshotCalls()[providerBaseline:]; len(calls) != 0 {
				t.Fatalf("provider drain-ack cleanup after post-provider drift = %+v, want none", calls)
			}
		})
	}
}

func TestStrictPostProviderBoundaryRejectsRunLiveAndRelaunchPostActionsAfterDrift(t *testing.T) {
	for _, boundary := range []string{"run_live_success", "relaunch_success", "relaunch_prepare_error"} {
		for _, drift := range []struct {
			name string
			meta map[string]string
		}{
			{name: "clear", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "", beadmeta.TriggerBeadStoreRefMetadataKey: ""}},
			{name: "repoint", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "work-other"}},
			{name: "suspend", meta: map[string]string{"state": string(session.StateSuspended)}},
		} {
			t.Run(boundary+"/"+drift.name, func(t *testing.T) {
				backing := beads.NewMemStore()
				provider := runtime.NewFake()
				manager := session.NewManagerWithOptions(backing, provider)
				created, err := manager.CreateSession(context.Background(), session.CreateOptions{
					BeadOnly: true,
					Template: "strict-worker",
					Title:    "Strict Worker",
					Command:  "codex",
					WorkDir:  t.TempDir(),
					Provider: "codex",
					ExtraMeta: map[string]string{
						beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
						beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
					},
				})
				if err != nil {
					t.Fatalf("CreateSession: %v", err)
				}
				tp := TemplateParams{TemplateName: "strict-worker"}
				cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
				switch boundary {
				case "run_live_success":
					if err := provider.RunLive(created.SessionNameMetadata, runtime.Config{}); err != nil {
						t.Fatalf("simulate RunLive provider return: %v", err)
					}
				case "relaunch_success":
					if err := provider.Start(context.Background(), created.SessionNameMetadata, runtime.Config{Command: "codex"}); err != nil {
						t.Fatalf("seed relaunch runtime: %v", err)
					}
					if err := provider.Relaunch(context.Background(), created.SessionNameMetadata, runtime.Config{}); err != nil {
						t.Fatalf("simulate Relaunch provider return: %v", err)
					}
				case "relaunch_prepare_error":
					// Preparation returned after the initial witnessed boundary but
					// before provider Relaunch, leaving only abort cleanup to fence.
				}
				if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: drift.meta}); err != nil {
					t.Fatalf("inject post-boundary drift: %v", err)
				}
				afterDrift, err := backing.Get(created.ID)
				if err != nil {
					t.Fatalf("Get after drift: %v", err)
				}
				recorder := beadstest.NewRecordingStore(backing)
				actionCalls := 0
				err = withStrictReconcilerPostProviderBoundary(created, tp, cfg, sessionFrontDoor(recorder), func(session.Info) error {
					actionCalls++
					return recorder.SetMetadata(created.ID, "forbidden_post_action", boundary)
				})
				if !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) {
					t.Fatalf("post-boundary error = %v, want ErrLiveBoundaryWitnessMismatch", err)
				}
				if actionCalls != 0 {
					t.Fatalf("post-boundary action calls = %d, want 0", actionCalls)
				}
				if calls := recorder.Calls(); len(calls) != 0 {
					t.Fatalf("post-boundary mutations = %+v, want none", calls)
				}
				current, err := backing.Get(created.ID)
				if err != nil {
					t.Fatalf("Get final session: %v", err)
				}
				if current.Revision != afterDrift.Revision {
					t.Fatalf("post-boundary revision = %d, want %d", current.Revision, afterDrift.Revision)
				}
			})
		}
	}
}

func TestPostProviderBoundaryPreservesInheritAction(t *testing.T) {
	backing := beads.NewMemStore()
	manager := session.NewManagerWithOptions(backing, runtime.NewFake())
	created, err := manager.CreateSession(context.Background(), session.CreateOptions{
		BeadOnly: true,
		Template: "ordinary-worker",
		Title:    "Ordinary Worker",
		Command:  "codex",
		WorkDir:  t.TempDir(),
		Provider: "codex",
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	actionCalls := 0
	err = withStrictReconcilerPostProviderBoundary(
		created,
		TemplateParams{TemplateName: "ordinary-worker"},
		&config.City{Agents: []config.Agent{{Name: "ordinary-worker", ProjectHooks: config.ProjectHooksInherit}}},
		sessionFrontDoor(backing),
		func(session.Info) error {
			actionCalls++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("inherit post-boundary action: %v", err)
	}
	if actionCalls != 1 {
		t.Fatalf("inherit action calls = %d, want 1", actionCalls)
	}
	if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{
		beadmeta.TriggerBeadIDMetadataKey:       "work-new",
		beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
	}}); err != nil {
		t.Fatalf("bind trigger after capture: %v", err)
	}
	actionCalls = 0
	err = withStrictReconcilerPostProviderBoundary(
		created,
		TemplateParams{TemplateName: "ordinary-worker"},
		&config.City{Agents: []config.Agent{{Name: "ordinary-worker", ProjectHooks: config.ProjectHooksInherit}}},
		sessionFrontDoor(backing),
		func(session.Info) error {
			actionCalls++
			return nil
		},
	)
	if !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) || actionCalls != 0 {
		t.Fatalf("inherit zero->complete post-boundary = (actions=%d, err=%v), want refusal", actionCalls, err)
	}
}

func TestAutomaticStartCommitsRejectInheritedZeroWitnessAcquiringOwner(t *testing.T) {
	for _, mode := range []string{"sync", "async"} {
		t.Run(mode, func(t *testing.T) {
			backing := beads.NewMemStore()
			provider := runtime.NewFake()
			manager := session.NewManagerWithOptions(backing, provider)
			created, err := manager.CreateSession(context.Background(), session.CreateOptions{
				BeadOnly: true,
				Template: "ordinary-worker",
				Title:    "Ordinary Worker",
				Command:  "true",
				WorkDir:  t.TempDir(),
				Provider: "fake",
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			candidate := captureStartCandidateForWake(
				created,
				TemplateParams{TemplateName: "ordinary-worker"},
				0,
				&config.City{Agents: []config.Agent{{Name: "ordinary-worker", ProjectHooks: config.ProjectHooksInherit}}},
				"work-new",
			)
			if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{
				beadmeta.TriggerBeadIDMetadataKey:       "work-new",
				beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
			}}); err != nil {
				t.Fatalf("bind trigger after capture: %v", err)
			}
			afterBind, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("Get after bind: %v", err)
			}
			recorder := beadstest.NewRecordingStore(backing)
			result := startResult{
				prepared: preparedStart{candidate: candidate},
				outcome:  TraceOutcomeSuccess,
				started:  time.Now(),
				finished: time.Now(),
			}
			var committed bool
			if mode == "sync" {
				committed = commitSyncStartResultTraced(result, provider, sessionFrontDoor(recorder), &clock.Fake{}, nil, 0, io.Discard, io.Discard, nil)
			} else {
				committed = commitAsyncStartResultWithContext(context.Background(), result, provider, recorder, &clock.Fake{}, nil, 0, io.Discard, io.Discard, nil)
			}
			if committed {
				t.Fatal("automatic start result adopted an owner attached after zero-witness capture")
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("automatic %s commit mutations = %+v, want none", mode, calls)
			}
			current, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("Get final: %v", err)
			}
			if current.Revision != afterBind.Revision {
				t.Fatalf("automatic %s commit revision = %d, want %d", mode, current.Revision, afterBind.Revision)
			}
		})
	}
}

func TestCaptureStartCandidateForWakePreservesInheritBehaviorAndTriggerWitness(t *testing.T) {
	info := session.Info{ID: "session-inherit", Template: "ordinary-worker", TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"}
	candidate := captureStartCandidateForWake(
		info,
		TemplateParams{TemplateName: "ordinary-worker"},
		3,
		&config.City{Agents: []config.Agent{{Name: "ordinary-worker", ProjectHooks: config.ProjectHooksInherit}}},
		"work-exact",
	)
	if candidate.strictLaunch {
		t.Fatal("inherit candidate unexpectedly enabled strict launch fencing")
	}
	if candidate.expectedWitness != (session.LiveBoundaryWitness{TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"}) || candidate.currentProcessingBeadID != "work-exact" {
		t.Fatalf("inherit candidate witness/deferred mutation fields = %+v, want captured pair and deferred current-work mutation", candidate)
	}
	if candidate.info.ID != info.ID || candidate.order != 3 {
		t.Fatalf("inherit candidate base fields changed: %+v", candidate)
	}
}

func TestAliveAutomaticTransitionEnforcesZeroWitnessTruthTable(t *testing.T) {
	backing := beads.NewMemStore()
	provider := runtime.NewFake()
	manager := session.NewManagerWithOptions(backing, provider)
	created, err := manager.CreateSession(context.Background(), session.CreateOptions{
		BeadOnly: true,
		Template: "ordinary-worker",
		Title:    "Ordinary Worker",
		Command:  "true",
		WorkDir:  t.TempDir(),
		Provider: "fake",
		ExtraMeta: map[string]string{
			"state": string(session.StateActive),
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	candidate := captureStartCandidateForWake(
		created,
		TemplateParams{TemplateName: "ordinary-worker"},
		0,
		&config.City{Agents: []config.Agent{{Name: "ordinary-worker", ProjectHooks: config.ProjectHooksInherit}}},
		"work-one",
	)
	tick := newReconcileTick([]session.Info{created})
	err = reconcileStrictAliveWakeTransition(
		candidate,
		AwakeDecision{ShouldWake: true, AssignedWorkBeadID: "work-one"},
		"ordinary-worker",
		provider,
		backing,
		&config.City{Agents: []config.Agent{{Name: "ordinary-worker", ProjectHooks: config.ProjectHooksInherit}}},
		nil,
		sessionFrontDoor(backing),
		newDrainTracker(),
		&clock.Fake{},
		tick,
		io.Discard,
		io.Discard,
		nil,
	)
	if err != nil {
		t.Fatalf("zero->zero alive transition: %v", err)
	}
	afterZero, err := backing.Get(created.ID)
	if err != nil {
		t.Fatalf("Get after zero transition: %v", err)
	}
	if got := afterZero.Metadata[session.CurrentBeadIDKey]; got != "work-one" {
		t.Fatalf("zero->zero current work = %q, want work-one", got)
	}

	staleCandidate := captureStartCandidateForWake(
		session.InfoFromPersistedBead(afterZero),
		TemplateParams{TemplateName: "ordinary-worker"},
		0,
		&config.City{Agents: []config.Agent{{Name: "ordinary-worker", ProjectHooks: config.ProjectHooksInherit}}},
		"work-two",
	)
	if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{
		beadmeta.TriggerBeadIDMetadataKey:       "work-two",
		beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
	}}); err != nil {
		t.Fatalf("bind trigger after zero capture: %v", err)
	}
	afterBind, err := backing.Get(created.ID)
	if err != nil {
		t.Fatalf("Get after bind: %v", err)
	}
	err = reconcileStrictAliveWakeTransition(
		staleCandidate,
		AwakeDecision{ShouldWake: true, AssignedWorkBeadID: "work-two"},
		"ordinary-worker",
		provider,
		backing,
		&config.City{Agents: []config.Agent{{Name: "ordinary-worker", ProjectHooks: config.ProjectHooksInherit}}},
		nil,
		sessionFrontDoor(backing),
		newDrainTracker(),
		&clock.Fake{},
		newReconcileTick([]session.Info{session.InfoFromPersistedBead(afterZero)}),
		io.Discard,
		io.Discard,
		nil,
	)
	if !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) {
		t.Fatalf("zero->complete alive transition error = %v, want mismatch", err)
	}
	final, err := backing.Get(created.ID)
	if err != nil {
		t.Fatalf("Get final: %v", err)
	}
	if final.Revision != afterBind.Revision || final.Metadata[session.CurrentBeadIDKey] != "work-one" {
		t.Fatalf("zero->complete alive transition mutated row: before=%+v after=%+v", afterBind, final)
	}
}

func TestPrepareInheritStartCandidateRejectsIncompleteCapturedTriggerBeforeMutation(t *testing.T) {
	for _, partial := range []struct {
		name string
		id   string
		ref  string
	}{
		{name: "id_only", id: "work-exact"},
		{name: "store_only", ref: "city:fixture-city"},
	} {
		t.Run(partial.name, func(t *testing.T) {
			backing := beads.NewMemStore()
			recorder := beadstest.NewRecordingStore(backing)
			provider := runtime.NewFake()
			manager := session.NewManagerWithOptions(recorder, provider)
			created, err := manager.CreateSession(context.Background(), session.CreateOptions{
				BeadOnly: true,
				Template: "ordinary-worker",
				Title:    "Ordinary Worker",
				Command:  "true",
				WorkDir:  t.TempDir(),
				Provider: "fake",
				ExtraMeta: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       partial.id,
					beadmeta.TriggerBeadStoreRefMetadataKey: partial.ref,
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			cfg := &config.City{Agents: []config.Agent{{Name: "ordinary-worker", ProjectHooks: config.ProjectHooksInherit}}}
			candidate := captureStartCandidateForWake(created, TemplateParams{TemplateName: "ordinary-worker"}, 0, cfg, "")
			if candidate.expectedWitness.TriggerBeadID != partial.id || candidate.expectedWitness.TriggerBeadStoreRef != partial.ref {
				t.Fatalf("captured partial witness = %+v, want (%q,%q)", candidate.expectedWitness, partial.id, partial.ref)
			}
			recorder.Reset()
			_, err = prepareStartCandidateForCity(
				candidate,
				"/cities/fixture-city",
				"fixture-city",
				cfg,
				provider,
				recorder,
				beads.WorkStore{Store: recorder},
				&clock.Fake{},
				io.Discard,
				nil,
			)
			if !errors.Is(err, worker.ErrLaunchUnauthorized) {
				t.Fatalf("prepare error = %v, want ErrLaunchUnauthorized", err)
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("partial witness session mutations = %+v, want none", calls)
			}
			if calls := provider.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("partial witness provider calls = %+v, want none", calls)
			}
		})
	}
}

func TestPrepareInheritStartCandidateRevalidatesWakeEligibilityBeforePreWake(t *testing.T) {
	for _, state := range []session.State{session.StateSuspended, session.StateDraining} {
		t.Run(string(state), func(t *testing.T) {
			backing := beads.NewMemStore()
			recorder := beadstest.NewRecordingStore(backing)
			provider := runtime.NewFake()
			manager := session.NewManagerWithOptions(recorder, provider)
			created, err := manager.CreateSession(context.Background(), session.CreateOptions{
				BeadOnly: true,
				Template: "ordinary-worker",
				Title:    "Ordinary Worker",
				Command:  "true",
				WorkDir:  t.TempDir(),
				Provider: "fake",
				ExtraMeta: map[string]string{
					"state": string(session.StateActive),
				},
			})
			if err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			cfg := &config.City{Agents: []config.Agent{{Name: "ordinary-worker", ProjectHooks: config.ProjectHooksInherit}}}
			candidate := captureStartCandidateForWake(created, TemplateParams{TemplateName: "ordinary-worker"}, 0, cfg, "")
			if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{"state": string(state)}}); err != nil {
				t.Fatalf("inject state drift: %v", err)
			}
			afterDrift, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("Get after drift: %v", err)
			}
			recorder.Reset()
			providerBaseline := len(provider.SnapshotCalls())
			_, err = prepareStartCandidateForCity(
				candidate,
				"/cities/fixture-city",
				"fixture-city",
				cfg,
				provider,
				recorder,
				beads.WorkStore{Store: recorder},
				&clock.Fake{},
				io.Discard,
				nil,
			)
			if !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) {
				t.Fatalf("prepare error = %v, want wake-eligibility refusal", err)
			}
			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("state-drift session mutations = %+v, want none", calls)
			}
			if calls := provider.SnapshotCalls()[providerBaseline:]; len(calls) != 0 {
				t.Fatalf("state-drift provider calls = %+v, want none", calls)
			}
			final, err := backing.Get(created.ID)
			if err != nil {
				t.Fatalf("Get final: %v", err)
			}
			if final.Revision != afterDrift.Revision || final.Metadata["last_woke_at"] != afterDrift.Metadata["last_woke_at"] {
				t.Fatalf("state-drift row changed: before=%+v after=%+v", afterDrift, final)
			}
		})
	}
}

func TestReconcileAliveStrictWakeRejectsTriggerOrSuspensionDriftBeforeCurrentWorkAndDrainCancel(t *testing.T) {
	for _, wakeMode := range []string{"resume", "fresh"} {
		for _, drift := range []struct {
			name string
			meta map[string]string
		}{
			{name: "clear", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "", beadmeta.TriggerBeadStoreRefMetadataKey: ""}},
			{name: "repoint", meta: map[string]string{beadmeta.TriggerBeadIDMetadataKey: "work-other"}},
			{name: "suspend", meta: map[string]string{"state": string(session.StateSuspended)}},
		} {
			t.Run(wakeMode+"/"+drift.name, func(t *testing.T) {
				env := newRestartRequestTestEnv()
				env.cfg = &config.City{
					Workspace: config.Workspace{Name: "test-city"},
					Agents: []config.Agent{{
						Name:              "strict-worker",
						StartCommand:      "true",
						ProjectHooks:      config.ProjectHooksForbid,
						MaxActiveSessions: restartRequestTestIntPtr(1),
					}},
					NamedSessions: []config.NamedSession{{Template: "strict-worker", Mode: "on_demand"}},
				}
				sessionName := config.NamedSessionRuntimeName(env.cfg.Workspace.Name, env.cfg.Workspace, "strict-worker")
				env.desiredState[sessionName] = TemplateParams{
					Command:      "true",
					SessionName:  sessionName,
					TemplateName: "strict-worker",
					ResolvedProvider: &config.ResolvedProvider{
						SessionIDFlag: "--session-id",
					},
				}
				sessionBead := env.createSessionBead(sessionName)
				env.setSessionMetadata(&sessionBead, map[string]string{
					namedSessionMetadataKey:                 "true",
					namedSessionIdentityMetadata:            "strict-worker",
					namedSessionModeMetadata:                "on_demand",
					"template":                              "strict-worker",
					"state":                                 string(session.StateActive),
					"wake_mode":                             wakeMode,
					session.CurrentBeadIDKey:                "work-old",
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:test-city",
					"session_key":                           "conversation-A",
				})
				if err := env.sp.Start(context.Background(), sessionName, runtime.Config{Command: "true"}); err != nil {
					t.Fatalf("start provider: %v", err)
				}
				env.dt.set(sessionBead.ID, &drainState{reason: "idle", generation: 1, ackSet: true})
				backing := env.store
				captured, _, err := sessionFrontDoor(backing).GetPersistedResponse(sessionBead.ID)
				if err != nil {
					t.Fatalf("capture persisted session: %v", err)
				}
				candidate := captureStartCandidateForWake(
					captured,
					env.desiredState[sessionName],
					0,
					env.cfg,
					"work-next",
				)
				if !candidate.strictLaunch {
					t.Fatal("production candidate capture did not enable strict launch fencing")
				}
				if err := backing.Update(sessionBead.ID, beads.UpdateOpts{Metadata: drift.meta}); err != nil {
					t.Fatalf("inject drift after candidate capture: %v", err)
				}
				afterDrift, err := backing.Get(sessionBead.ID)
				if err != nil {
					t.Fatalf("Get after drift: %v", err)
				}
				recorder := beadstest.NewRecordingStore(backing)
				providerBaseline := len(env.sp.SnapshotCalls())
				tick := newReconcileTick([]session.Info{captured})
				err = reconcileStrictAliveWakeTransition(
					candidate,
					AwakeDecision{ShouldWake: true, AssignedWorkBeadID: "work-next", RequiresFreshCycle: wakeMode == "fresh"},
					sessionName,
					env.sp,
					recorder,
					env.cfg,
					nil,
					sessionFrontDoor(recorder),
					env.dt,
					env.clk,
					tick,
					io.Discard,
					io.Discard,
					nil,
				)
				if !errors.Is(err, session.ErrAutomaticRuntimeExactEffectUnsupported) {
					t.Fatalf("alive transition error = %v, want ErrAutomaticRuntimeExactEffectUnsupported", err)
				}
				if calls := recorder.Calls(); len(calls) != 0 {
					t.Fatalf("session/work mutations after alive drift = %+v, want none", calls)
				}
				current, err := backing.Get(sessionBead.ID)
				if err != nil {
					t.Fatalf("Get final session: %v", err)
				}
				if current.Revision != afterDrift.Revision || current.Metadata[session.CurrentBeadIDKey] != "work-old" {
					t.Fatalf("alive transition mutated drifted row: revision %d -> %d, current work=%q", afterDrift.Revision, current.Revision, current.Metadata[session.CurrentBeadIDKey])
				}
				if env.dt.get(sessionBead.ID) == nil {
					t.Fatal("strict witness refusal canceled the pending drain")
				}
				calls := env.sp.SnapshotCalls()
				for _, call := range calls[providerBaseline:] {
					if call.Method == "RemoveMeta" || call.Method == "Stop" {
						t.Fatalf("provider mutation after alive drift = %+v", call)
					}
				}
			})
		}
	}
}

func TestReconcileAliveStrictWakeFailsClosedWithoutLeasedDestructiveBoundary(t *testing.T) {
	env := newRestartRequestTestEnv()
	env.cfg = &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:              "strict-worker",
			StartCommand:      "true",
			ProjectHooks:      config.ProjectHooksForbid,
			MaxActiveSessions: restartRequestTestIntPtr(1),
		}},
		NamedSessions: []config.NamedSession{{Template: "strict-worker", Mode: "on_demand"}},
	}
	sessionName := config.NamedSessionRuntimeName(env.cfg.Workspace.Name, env.cfg.Workspace, "strict-worker")
	env.desiredState[sessionName] = TemplateParams{Command: "true", SessionName: sessionName, TemplateName: "strict-worker"}
	sessionBead := env.createSessionBead(sessionName)
	env.setSessionMetadata(&sessionBead, map[string]string{
		namedSessionMetadataKey:                 "true",
		namedSessionIdentityMetadata:            "strict-worker",
		namedSessionModeMetadata:                "on_demand",
		"template":                              "strict-worker",
		"state":                                 string(session.StateActive),
		"pin_awake":                             "true",
		session.CurrentBeadIDKey:                "work-old",
		beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
		beadmeta.TriggerBeadStoreRefMetadataKey: "city:test-city",
	})
	if err := env.sp.Start(context.Background(), sessionName, runtime.Config{Command: "true"}); err != nil {
		t.Fatalf("start provider: %v", err)
	}
	if err := env.sp.SetMeta(sessionName, "GC_SESSION_ID", sessionBead.ID); err != nil {
		t.Fatalf("SetMeta GC_SESSION_ID: %v", err)
	}
	env.dt.set(sessionBead.ID, &drainState{reason: "idle", generation: 1, ackSet: true})
	workBead := beads.Bead{ID: "work-next", Title: "next work", Type: "task", Status: "in_progress", Assignee: "strict-worker"}

	providerBaseline := len(env.sp.SnapshotCalls())
	reconcileSessionBeadsWithAssignedWork(env, []beads.Bead{sessionBead}, []beads.Bead{workBead})

	current, err := env.store.Get(sessionBead.ID)
	if err != nil {
		t.Fatalf("Get final session: %v", err)
	}
	if got := current.Metadata[session.CurrentBeadIDKey]; got != "work-old" {
		t.Fatalf("%s = %q, want unchanged work-old after fail-closed strict alive transition; stderr=%q", session.CurrentBeadIDKey, got, env.stderr.String())
	}
	if env.dt.get(sessionBead.ID) == nil {
		t.Fatal("fail-closed strict alive transition canceled the pending drain")
	}
	for _, call := range env.sp.SnapshotCalls()[providerBaseline:] {
		if call.Method == "RemoveMeta" || call.Method == "Stop" {
			t.Fatalf("fail-closed strict alive transition mutated provider: %+v", call)
		}
	}
}

func TestStrictNamedReconcilerRejectsSplitSessionAndWorkStoresBeforeAnyMutation(t *testing.T) {
	for _, state := range []string{"asleep", "alive"} {
		t.Run(state, func(t *testing.T) {
			sessionBacking := beads.NewMemStore()
			workBacking := beads.NewMemStore()
			duplicateWork, err := workBacking.Create(beads.Bead{
				Title:  "same-id work copy",
				Type:   "task",
				Status: "in_progress",
			})
			if err != nil {
				t.Fatalf("Create work duplicate: %v", err)
			}

			env := newRestartRequestTestEnv()
			env.store = sessionBacking
			env.cfg = &config.City{
				Workspace: config.Workspace{Name: "test-city"},
				Agents: []config.Agent{{
					Name:              "strict-worker",
					StartCommand:      "true",
					ProjectHooks:      config.ProjectHooksForbid,
					MaxActiveSessions: restartRequestTestIntPtr(1),
				}},
				NamedSessions: []config.NamedSession{{Template: "strict-worker", Mode: "on_demand"}},
			}
			env.cfg.Daemon.SessionCircuitBreaker = true
			sessionName := config.NamedSessionRuntimeName(env.cfg.Workspace.Name, env.cfg.Workspace, "strict-worker")
			templateParams := TemplateParams{
				Command:      "true",
				SessionName:  sessionName,
				TemplateName: "strict-worker",
				WorkDir:      t.TempDir(),
				Hints: agent.StartupHints{
					ProviderName:          "fake",
					ProjectHooksForbidden: true,
				},
			}
			if state == "alive" {
				env.desiredState[sessionName] = templateParams
			}
			sessionBead := env.createSessionBead(sessionName)
			if sessionBead.ID != duplicateWork.ID {
				t.Fatalf("fixture IDs differ: session=%q work=%q, want same ID in distinct stores", sessionBead.ID, duplicateWork.ID)
			}
			persistedState := string(session.StateAsleep)
			if state == "alive" {
				persistedState = string(session.StateActive)
			}
			env.setSessionMetadata(&sessionBead, map[string]string{
				namedSessionMetadataKey:                 "true",
				namedSessionIdentityMetadata:            "strict-worker",
				namedSessionModeMetadata:                "on_demand",
				"template":                              "strict-worker",
				"agent_name":                            "strict-worker",
				"state":                                 persistedState,
				"pin_awake":                             "true",
				session.CurrentBeadIDKey:                "work-old",
				beadmeta.TriggerBeadIDMetadataKey:       duplicateWork.ID,
				beadmeta.TriggerBeadStoreRefMetadataKey: "city:test-city",
				sessionCircuitStateMetadata:             circuitClosed.String(),
			})
			if state == "asleep" && !preserveConfiguredNamedSessionBeadInfo(session.InfoFromPersistedBead(sessionBead), env.cfg, "test-city") {
				t.Fatal("asleep fixture does not exercise configured-named preservation")
			}
			if state == "alive" {
				if err := env.sp.Start(context.Background(), sessionName, runtime.Config{Command: "true"}); err != nil {
					t.Fatalf("seed live provider session: %v", err)
				}
				if err := env.sp.SetMeta(sessionName, "GC_SESSION_ID", sessionBead.ID); err != nil {
					t.Fatalf("seed provider session ID: %v", err)
				}
			}

			drainBefore := &drainState{reason: "idle", generation: 1, ackSet: true}
			env.dt.set(sessionBead.ID, drainBefore)
			breaker := newSessionCircuitBreaker(sessionCircuitBreakerConfig{})
			restoreBreaker := setSessionCircuitBreakerForTest(breaker)
			defer restoreBreaker()

			before, err := sessionBacking.Get(sessionBead.ID)
			if err != nil {
				t.Fatalf("Get before reconcile: %v", err)
			}
			recorder := beadstest.NewRecordingStore(sessionBacking)
			workRecorder := beadstest.NewRecordingStore(workBacking)
			providerBaseline := len(env.sp.SnapshotCalls())
			_ = reconcileSessionBeads(
				context.Background(),
				[]beads.Bead{sessionBead},
				env.desiredState,
				map[string]bool{sessionName: true},
				env.cfg,
				env.sp,
				recorder,
				nil,
				nil,
				nil,
				env.dt,
				map[string]int{"strict-worker": 1},
				false,
				nil,
				"test-city",
				nil,
				env.clk,
				env.rec,
				0,
				0,
				io.Discard,
				io.Discard,
				append(env.startOptions, withCanonicalCityWorkStore(beads.WorkStore{Store: workRecorder}))...,
			)

			if calls := recorder.Calls(); len(calls) != 0 {
				t.Fatalf("split-store strict %s session mutations = %+v, want none", state, calls)
			}
			after, err := sessionBacking.Get(sessionBead.ID)
			if err != nil {
				t.Fatalf("Get after reconcile: %v", err)
			}
			if after.Revision != before.Revision {
				t.Fatalf("split-store strict %s session revision = %d, want unchanged %d", state, after.Revision, before.Revision)
			}
			if calls := workRecorder.Calls(); len(calls) != 0 {
				t.Fatalf("split-store strict %s work mutations = %+v, want none", state, calls)
			}
			if calls := env.sp.SnapshotCalls()[providerBaseline:]; len(calls) != 0 {
				t.Fatalf("split-store strict %s provider calls = %+v, want none", state, calls)
			}
			if got := env.dt.get(sessionBead.ID); got != drainBefore {
				t.Fatalf("split-store strict %s drain state = %+v, want original marker %+v", state, got, drainBefore)
			}
			if got := breaker.Snapshot(env.clk.Now()); len(got) != 0 {
				t.Fatalf("split-store strict %s circuit state = %+v, want untouched", state, got)
			}
		})
	}
}
