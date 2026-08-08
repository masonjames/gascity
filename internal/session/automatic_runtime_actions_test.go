package session

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
)

const automaticRuntimeResumeCommandForTest = "codex resume session-123"

type automaticRuntimeProvider struct {
	*runtime.Fake
	store                *beads.MemStore
	driftStart           bool
	startErr             error
	absentMeta           bool
	driftLive            bool
	driftNudge           bool
	driftRelaunch        bool
	driftPeek            bool
	replaceAtEffect      bool
	replaceAtObserve     bool
	replaceAtStop        bool
	postEffectObserveErr error
	unsupportedExact     string
	markerSeen           bool
	sessionID            string
	routeLeaseValue      string
	detectedTransport    string
	detectCalls          int
	routed               int
	unrouted             int
}

func (p *automaticRuntimeProvider) Start(ctx context.Context, name string, cfg runtime.Config) error {
	if err := p.Fake.Start(ctx, name, cfg); err != nil {
		return err
	}
	for _, key := range []string{"GC_SESSION_ID", "GC_INSTANCE_TOKEN", "GC_RUNTIME_EPOCH", RuntimeOperationTokenEnv} {
		if err := p.SetMeta(name, key, cfg.Env[key]); err != nil {
			return err
		}
	}
	if p.driftStart {
		current, err := p.store.Get(cfg.Env["GC_SESSION_ID"])
		if err != nil {
			return err
		}
		if err := p.store.UpdateIfMatch(current.ID, current.Revision, beads.UpdateOpts{
			Metadata: map[string]string{"external_after_start": "won"},
		}); err != nil {
			return err
		}
	}
	return p.startErr
}

func (p *automaticRuntimeProvider) StartExact(ctx context.Context, name string, desired runtime.Incarnation, cfg runtime.Config) error {
	if p.unsupportedExact == "start" {
		return runtime.ErrExactIncarnationUnsupported
	}
	if p.IsRunning(name) {
		return runtime.ErrIncarnationMismatch
	}
	if cfg.Env["GC_SESSION_ID"] != desired.SessionID ||
		cfg.Env["GC_INSTANCE_TOKEN"] != desired.InstanceToken ||
		cfg.Env["GC_RUNTIME_EPOCH"] != desired.Epoch ||
		cfg.Env[RuntimeOperationTokenEnv] != desired.OperationToken {
		return runtime.ErrIncarnationMismatch
	}
	return p.Start(ctx, name, cfg)
}

func (p *automaticRuntimeProvider) GetMeta(name, key string) (string, error) {
	if p.absentMeta && !p.IsRunning(name) {
		return "", fmt.Errorf("%w: %s", runtime.ErrSessionNotFound, name)
	}
	return p.Fake.GetMeta(name, key)
}

func (p *automaticRuntimeProvider) RouteACP(string) {
	if current, err := p.store.Get(p.sessionID); err == nil {
		p.routeLeaseValue = current.Metadata[ConditionalMutationLeaseMetadataKey]
	}
	p.routed++
}

func (p *automaticRuntimeProvider) Unroute(string) {
	p.unrouted++
}

func (p *automaticRuntimeProvider) DetectTransport(string) string {
	p.detectCalls++
	return p.detectedTransport
}

func (p *automaticRuntimeProvider) RunLive(name string, cfg runtime.Config) error {
	if err := p.Fake.RunLive(name, cfg); err != nil {
		return err
	}
	if p.driftLive {
		return p.injectProviderBoundaryDrift(cfg.Env["GC_SESSION_ID"], "external_after_run_live")
	}
	return nil
}

func (p *automaticRuntimeProvider) Nudge(name string, content []runtime.ContentBlock) error {
	if p.store != nil {
		if liveID, _ := p.Fake.GetMeta(name, "GC_SESSION_ID"); liveID != "" {
			if current, err := p.store.Get(liveID); err == nil {
				p.markerSeen = current.Metadata["backstop_marker"] == "reserved"
			}
		}
	}
	if err := p.Fake.Nudge(name, content); err != nil {
		return err
	}
	if p.driftNudge {
		liveID, _ := p.Fake.GetMeta(name, "GC_SESSION_ID")
		return p.injectProviderBoundaryDrift(liveID, "external_after_nudge")
	}
	return nil
}

func (p *automaticRuntimeProvider) Stop(name string) error {
	if err := p.Fake.Stop(name); err != nil {
		return err
	}
	return nil
}

func (p *automaticRuntimeProvider) Peek(name string, lines int) (string, error) {
	output, err := p.Fake.Peek(name, lines)
	if err != nil {
		return "", err
	}
	if p.driftPeek {
		liveID, _ := p.Fake.GetMeta(name, "GC_SESSION_ID")
		if err := p.injectProviderBoundaryDrift(liveID, "external_after_peek"); err != nil {
			return "", err
		}
	}
	return output, nil
}

func (p *automaticRuntimeProvider) RunLiveExact(name string, expected runtime.Incarnation, cfg runtime.Config) error {
	if p.unsupportedExact == "run-live" {
		return runtime.ErrExactIncarnationUnsupported
	}
	if err := p.requireExactIncarnation(name, expected); err != nil {
		return err
	}
	return p.RunLive(name, cfg)
}

func (p *automaticRuntimeProvider) NudgeExact(name string, expected runtime.Incarnation, content []runtime.ContentBlock) error {
	if p.unsupportedExact == "nudge" {
		return runtime.ErrExactIncarnationUnsupported
	}
	if err := p.requireExactIncarnation(name, expected); err != nil {
		return err
	}
	return p.Nudge(name, content)
}

func (p *automaticRuntimeProvider) StopExact(name string, expected runtime.Incarnation) error {
	if p.unsupportedExact == "stop" {
		return runtime.ErrExactIncarnationUnsupported
	}
	if p.replaceAtStop {
		p.replaceAtStop = false
		_ = p.SetMeta(name, "GC_INSTANCE_TOKEN", "replacement-token")
	}
	if err := p.requireExactIncarnation(name, expected); err != nil {
		return err
	}
	return p.Stop(name)
}

func (p *automaticRuntimeProvider) RelaunchExact(ctx context.Context, name string, expected runtime.Incarnation, cfg runtime.Config) error {
	if p.unsupportedExact == "relaunch" {
		return runtime.ErrExactIncarnationUnsupported
	}
	if err := p.requireExactIncarnation(name, expected); err != nil {
		return err
	}
	if err := p.Relaunch(ctx, name, cfg); err != nil {
		return err
	}
	for _, key := range []string{"GC_SESSION_ID", "GC_INSTANCE_TOKEN", "GC_RUNTIME_EPOCH", RuntimeOperationTokenEnv} {
		if err := p.SetMeta(name, key, cfg.Env[key]); err != nil {
			return err
		}
	}
	if p.driftRelaunch {
		return p.injectProviderBoundaryDrift(cfg.Env["GC_SESSION_ID"], "external_after_relaunch")
	}
	return nil
}

func (p *automaticRuntimeProvider) SetMetaExact(name string, expected runtime.Incarnation, key, value string) error {
	if p.unsupportedExact == "metadata" {
		return runtime.ErrExactIncarnationUnsupported
	}
	if err := p.requireExactIncarnation(name, expected); err != nil {
		return err
	}
	return p.SetMeta(name, key, value)
}

func (p *automaticRuntimeProvider) RemoveMetaExact(name string, expected runtime.Incarnation, key string) error {
	if err := p.requireExactIncarnation(name, expected); err != nil {
		return err
	}
	return p.RemoveMeta(name, key)
}

func (p *automaticRuntimeProvider) ObserveExact(name string, expected runtime.Incarnation, processNames []string) (runtime.IncarnationObservation, error) {
	if p.unsupportedExact == "observe" {
		return runtime.IncarnationObservation{}, runtime.ErrExactIncarnationUnsupported
	}
	if !p.IsRunning(name) {
		return runtime.IncarnationObservation{}, runtime.ErrSessionNotFound
	}
	if p.replaceAtObserve {
		p.replaceAtObserve = false
		_ = p.SetMeta(name, "GC_INSTANCE_TOKEN", "replacement-token")
	}
	if err := p.requireExactIncarnation(name, expected); err != nil {
		return runtime.IncarnationObservation{}, err
	}
	if operationToken, _ := p.Fake.GetMeta(name, RuntimeOperationTokenEnv); operationToken != "" && p.postEffectObserveErr != nil {
		err := p.postEffectObserveErr
		p.postEffectObserveErr = nil
		return runtime.IncarnationObservation{}, err
	}
	liveness := runtime.ObserveLiveness(p.Fake, name, processNames)
	return runtime.IncarnationObservation{Running: liveness.Running, Alive: liveness.Alive}, nil
}

func (p *automaticRuntimeProvider) PeekExact(name string, expected runtime.Incarnation, lines int) (string, error) {
	if p.unsupportedExact == "peek" {
		return "", runtime.ErrExactIncarnationUnsupported
	}
	if err := p.requireExactIncarnation(name, expected); err != nil {
		return "", err
	}
	return p.Peek(name, lines)
}

func (p *automaticRuntimeProvider) requireExactIncarnation(name string, expected runtime.Incarnation) error {
	if p.replaceAtEffect {
		p.replaceAtEffect = false
		_ = p.SetMeta(name, "GC_INSTANCE_TOKEN", "replacement-token")
	}
	for key, want := range map[string]string{
		"GC_SESSION_ID":     expected.SessionID,
		"GC_INSTANCE_TOKEN": expected.InstanceToken,
		"GC_RUNTIME_EPOCH":  expected.Epoch,
	} {
		got, err := p.Fake.GetMeta(name, key)
		if err != nil || got != want {
			return fmt.Errorf("%w: %s=%q want %q", runtime.ErrIncarnationMismatch, key, got, want)
		}
	}
	if expected.OperationToken != "" {
		got, err := p.Fake.GetMeta(name, RuntimeOperationTokenEnv)
		if err != nil || got != expected.OperationToken {
			return fmt.Errorf("%w: operation token=%q want %q", runtime.ErrIncarnationMismatch, got, expected.OperationToken)
		}
	}
	return nil
}

func (p *automaticRuntimeProvider) injectProviderBoundaryDrift(id, key string) error {
	current, err := p.store.Get(id)
	if err != nil {
		return err
	}
	return p.store.UpdateIfMatch(id, current.Revision, beads.UpdateOpts{Metadata: map[string]string{key: "won"}})
}

func TestAutomaticStartPreparedUsesExactRevisionLeaseAndRuntimeOperationToken(t *testing.T) {
	store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
	decision := automaticRuntimeDecisionForTest(info, persisted)

	result, err := manager.StartPreparedRuntimeOnlyForReconciler(
		context.Background(),
		info.ID,
		automaticRuntimeResumeCommandForTest,
		runtime.Config{Command: "codex", WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true},
		decision,
	)
	if err != nil {
		t.Fatalf("StartPreparedRuntimeOnlyForReconciler: %v", err)
	}
	if !result.Attempted {
		t.Fatal("Attempted = false, want true")
	}
	if provider.CountCalls("Start", info.SessionName) != 1 {
		t.Fatalf("Start calls = %d, want 1", provider.CountCalls("Start", info.SessionName))
	}
	cfg := provider.LastStartConfig(info.SessionName)
	if cfg == nil || cfg.Env[RuntimeOperationTokenEnv] == "" {
		t.Fatalf("start config operation token = %#v, want nonempty", cfg)
	}
	if cfg.Env["GC_SESSION_ID"] != info.ID || cfg.Env["GC_INSTANCE_TOKEN"] != info.InstanceToken || cfg.Env["GC_RUNTIME_EPOCH"] != info.Generation {
		t.Fatalf("start runtime identity env = %#v, want id/token/epoch from captured row", cfg.Env)
	}
	current, getErr := store.Get(info.ID)
	if getErr != nil {
		t.Fatalf("Get final session: %v", getErr)
	}
	if current.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("final lease = %q, want cleared", current.Metadata[ConditionalMutationLeaseMetadataKey])
	}
	if result.Commit.Info.ID != info.ID || result.Commit.Persisted.Revision != current.Revision {
		t.Fatalf("commit = %#v, want final session revision %d", result.Commit, current.Revision)
	}
}

func TestAutomaticStartPreparedTreatsGoneMetadataAsAbsentAndPublishesACPRouteAfterCommit(t *testing.T) {
	store, manager, provider, info, _ := newAutomaticRuntimeFixture(t)
	provider.absentMeta = true
	if err := store.Update(info.ID, beads.UpdateOpts{Metadata: map[string]string{"transport": "acp"}}); err != nil {
		t.Fatalf("seed ACP transport: %v", err)
	}
	info, persisted, err := manager.PersistedStore().GetPersistedResponse(info.ID)
	if err != nil {
		t.Fatalf("reload ACP session: %v", err)
	}
	decision := automaticRuntimeDecisionForTest(info, persisted)

	_, err = manager.StartPreparedRuntimeOnlyForReconciler(
		context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, decision,
	)
	if err != nil {
		t.Fatalf("StartPreparedRuntimeOnlyForReconciler: %v", err)
	}
	if provider.CountCalls("Start", info.SessionName) != 1 {
		t.Fatalf("Start calls = %d, want 1", provider.CountCalls("Start", info.SessionName))
	}
	if provider.routed != 1 || provider.unrouted != 0 {
		t.Fatalf("ACP routes = route:%d unroute:%d, want 1/0", provider.routed, provider.unrouted)
	}
	if provider.routeLeaseValue != "" {
		t.Fatalf("ACP route published while mutation lease was still held: %q", provider.routeLeaseValue)
	}
}

func TestAutomaticRuntimeTransportNeverAdoptsNameDetectedBackend(t *testing.T) {
	_, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
	provider.detectedTransport = "acp"

	_, err := manager.StartPreparedRuntimeOnlyForReconciler(
		context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, automaticRuntimeDecisionForTest(info, persisted),
	)
	if err != nil {
		t.Fatalf("StartPreparedRuntimeOnlyForReconciler: %v", err)
	}
	if provider.detectCalls != 0 {
		t.Fatalf("DetectTransport calls = %d, want none inside automatic authority decision", provider.detectCalls)
	}
	if provider.routed != 0 || provider.unrouted != 1 {
		t.Fatalf("published routes = route:%d unroute:%d, want captured default backend", provider.routed, provider.unrouted)
	}
}

func TestAutomaticStartPreparedSkipsUnattestedProcessOrphanCleanup(t *testing.T) {
	_, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
	provider.OrphanedRuntimes[info.ID] = runtime.LiveRuntime{
		SessionID:    info.ID,
		Epoch:        99,
		ProviderName: "replacement-runtime",
	}

	_, err := manager.StartPreparedRuntimeOnlyForReconciler(
		context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, automaticRuntimeDecisionForTest(info, persisted),
	)
	if err != nil {
		t.Fatalf("StartPreparedRuntimeOnlyForReconciler: %v", err)
	}
	if provider.CountCalls("FindRuntimesBySessionID", info.ID) != 0 || provider.CountCalls("TerminateRuntime", info.ID) != 0 {
		t.Fatalf("process-table calls = %+v, strict automatic start must not scan or terminate an unattested same-ID runtime", provider.SnapshotCalls())
	}
}

func TestAutomaticStartPreparedHealthyExistingRequiresAtomicExactObservation(t *testing.T) {
	store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
	seedAutomaticRuntime(t, provider, info)
	provider.replaceAtObserve = true

	_, err := manager.StartPreparedRuntimeOnlyForReconciler(
		context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, automaticRuntimeDecisionForTest(info, persisted),
	)
	if !errors.Is(err, runtime.ErrIncarnationMismatch) {
		t.Fatalf("StartPreparedRuntimeOnlyForReconciler error = %v, want exact-observation incarnation mismatch", err)
	}
	if provider.CountCalls("Start", info.SessionName) != 1 || provider.CountCalls("Stop", info.SessionName) != 0 {
		t.Fatalf("provider effects = %+v, want seed Start only", provider.SnapshotCalls())
	}
	current, getErr := store.Get(info.ID)
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if current.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("exact observation refusal left held lease %q", current.Metadata[ConditionalMutationLeaseMetadataKey])
	}
}

func TestAutomaticStartPreparedRejectsPreAcquireDriftAndAbandonedLease(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *beads.MemStore, Info)
		want   error
	}{
		{
			name: "revision drift",
			mutate: func(t *testing.T, store *beads.MemStore, info Info) {
				t.Helper()
				if err := store.Update(info.ID, beads.UpdateOpts{Metadata: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey: "work-repointed",
				}}); err != nil {
					t.Fatalf("inject trigger drift: %v", err)
				}
			},
			want: ErrConditionalMutationLost,
		},
		{
			name: "abandoned lease",
			mutate: func(t *testing.T, store *beads.MemStore, info Info) {
				t.Helper()
				if err := store.Update(info.ID, beads.UpdateOpts{Metadata: map[string]string{
					ConditionalMutationLeaseMetadataKey: `{"version":1,"nonce":"abandoned","action":"start","predicates":[]}`,
				}}); err != nil {
					t.Fatalf("seed abandoned lease: %v", err)
				}
			},
			want: ErrAutomaticRuntimeRecoveryBlocked,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
			decision := automaticRuntimeDecisionForTest(info, persisted)
			tc.mutate(t, store, info)
			if tc.name == "abandoned lease" {
				currentInfo, currentPersisted, err := manager.PersistedStore().GetPersistedResponse(info.ID)
				if err != nil {
					t.Fatalf("reload abandoned lease: %v", err)
				}
				decision = automaticRuntimeDecisionForTest(currentInfo, currentPersisted)
			}
			_, err := manager.StartPreparedRuntimeOnlyForReconciler(
				context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, decision,
			)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if provider.CountCalls("Start", info.SessionName) != 0 || provider.CountCalls("Stop", info.SessionName) != 0 {
				t.Fatalf("provider effects after refusal = %+v, want none", provider.SnapshotCalls())
			}
		})
	}
}

func TestAutomaticStartPreparedFailsClosedWithoutSessionOrProviderCapability(t *testing.T) {
	t.Run("runtime config policy mismatch", func(t *testing.T) {
		_, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
		decision := automaticRuntimeDecisionForTest(info, persisted)

		_, err := manager.StartPreparedRuntimeOnlyForReconciler(
			context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir}, decision,
		)
		if !errors.Is(err, ErrAutomaticRuntimeDecisionInvalid) {
			t.Fatalf("error = %v, want runtime policy mismatch", err)
		}
		if provider.CountCalls("Start", info.SessionName) != 0 {
			t.Fatalf("provider calls = %+v, want zero", provider.SnapshotCalls())
		}
	})

	t.Run("conditional session writer unsupported", func(t *testing.T) {
		store, _, provider, info, persisted := newAutomaticRuntimeFixture(t)
		manager := NewManagerWithOptions(conditionalMutationStoreOnly{Store: store}, provider)
		decision := automaticRuntimeDecisionForTest(info, persisted)

		_, err := manager.StartPreparedRuntimeOnlyForReconciler(
			context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, decision,
		)
		if !errors.Is(err, ErrConditionalMutationUnsupported) {
			t.Fatalf("error = %v, want conditional mutation unsupported", err)
		}
		if provider.CountCalls("Start", info.SessionName) != 0 {
			t.Fatalf("provider calls = %+v, want zero", provider.SnapshotCalls())
		}
	})

	t.Run("exact incarnation provider unsupported", func(t *testing.T) {
		store, _, _, info, persisted := newAutomaticRuntimeFixture(t)
		provider := runtime.NewFake()
		manager := NewManagerWithOptions(store, provider)
		decision := automaticRuntimeDecisionForTest(info, persisted)

		_, err := manager.StartPreparedRuntimeOnlyForReconciler(
			context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, decision,
		)
		if !errors.Is(err, ErrAutomaticRuntimeExactEffectUnsupported) {
			t.Fatalf("error = %v, want exact provider unsupported", err)
		}
		if provider.CountCalls("Start", info.SessionName) != 0 {
			t.Fatalf("provider calls = %+v, want zero", provider.SnapshotCalls())
		}
	})
}

func TestAutomaticStartPreparedCommitDriftContainsOnlyStampedRuntime(t *testing.T) {
	store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
	provider.driftStart = true
	decision := automaticRuntimeDecisionForTest(info, persisted)

	_, err := manager.StartPreparedRuntimeOnlyForReconciler(
		context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProcessNames: []string{"codex"}, ProviderName: "codex", ProjectHooksForbidden: true}, decision,
	)
	if !errors.Is(err, ErrConditionalMutationLost) {
		t.Fatalf("error = %v, want conditional mutation lost", err)
	}
	if provider.CountCalls("Start", info.SessionName) != 1 || provider.CountCalls("Stop", info.SessionName) != 1 {
		t.Fatalf("provider effects = %+v, want one Start and exact containment Stop", provider.SnapshotCalls())
	}
	if provider.IsRunning(info.SessionName) {
		t.Fatal("runtime remains after lost-commit containment")
	}
	current, getErr := store.Get(info.ID)
	if getErr != nil {
		t.Fatalf("Get final session: %v", getErr)
	}
	if current.Metadata["external_after_start"] != "won" {
		t.Fatalf("session metadata = %#v, want external winner preserved", current.Metadata)
	}
}

func TestAutomaticStartPreparedErrorContainsAttestedRuntimeOrPreservesLease(t *testing.T) {
	t.Run("effect happened", func(t *testing.T) {
		store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
		provider.startErr = errors.New("ambiguous start response")
		decision := automaticRuntimeDecisionForTest(info, persisted)
		_, err := manager.StartPreparedRuntimeOnlyForReconciler(
			context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, decision,
		)
		if err == nil || provider.CountCalls("Stop", info.SessionName) != 1 {
			t.Fatalf("error=%v provider=%+v, want error and exact Stop", err, provider.SnapshotCalls())
		}
		current, getErr := store.Get(info.ID)
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if current.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("contained start error left lease %q", current.Metadata[ConditionalMutationLeaseMetadataKey])
		}
	})

	t.Run("effect cannot be proven quiescent", func(t *testing.T) {
		store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
		provider.StartErrors[info.SessionName] = errors.New("ambiguous remote start")
		decision := automaticRuntimeDecisionForTest(info, persisted)
		_, err := manager.StartPreparedRuntimeOnlyForReconciler(
			context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, decision,
		)
		if !errors.Is(err, ErrAutomaticRuntimeEffectUncompensated) {
			t.Fatalf("error = %v, want uncompensated ambiguity", err)
		}
		current, getErr := store.Get(info.ID)
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if current.Metadata[ConditionalMutationLeaseMetadataKey] == "" {
			t.Fatal("ambiguous start error cleared the persisted safety hold")
		}
	})
}

func TestAutomaticCreatedRuntimeObservationFailureContainsBeforeLeaseRelease(t *testing.T) {
	for _, tc := range []struct {
		name   string
		invoke func(*Manager, Info, PersistedResponse) error
		seed   bool
	}{
		{
			name: "start",
			invoke: func(manager *Manager, info Info, persisted PersistedResponse) error {
				_, err := manager.StartPreparedRuntimeOnlyForReconciler(
					context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, automaticRuntimeDecisionForTest(info, persisted),
				)
				return err
			},
		},
		{
			name: "relaunch",
			seed: true,
			invoke: func(manager *Manager, info Info, persisted PersistedResponse) error {
				_, err := manager.RelaunchForReconciler(
					context.Background(), info.ID, runtime.Config{Command: "codex", WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, automaticRuntimeDecisionForTest(info, persisted),
				)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
			if tc.seed {
				seedAutomaticRuntime(t, provider, info)
			}
			observeErr := errors.New("post-effect exact observation failed")
			provider.postEffectObserveErr = observeErr

			err := tc.invoke(manager, info, persisted)
			if !errors.Is(err, observeErr) {
				t.Fatalf("error = %v, want post-effect observation error", err)
			}
			if provider.CountCalls("Stop", info.SessionName) != 1 || provider.IsRunning(info.SessionName) {
				t.Fatalf("provider calls = %+v running=%v, want exact op-token containment", provider.SnapshotCalls(), provider.IsRunning(info.SessionName))
			}
			current, getErr := store.Get(info.ID)
			if getErr != nil {
				t.Fatalf("Get: %v", getErr)
			}
			if current.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
				t.Fatalf("contained post-observe failure left lease %q", current.Metadata[ConditionalMutationLeaseMetadataKey])
			}
		})
	}
}

func TestAutomaticStartPreparedPreservesLeaseOnAmbiguousRecycleStop(t *testing.T) {
	store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
	seedAutomaticRuntime(t, provider, info)
	provider.Zombies[info.SessionName] = true
	provider.StopErrors[info.SessionName] = errors.New("ambiguous exact stop response")
	decision := automaticRuntimeDecisionForTest(info, persisted)

	_, err := manager.StartPreparedRuntimeOnlyForReconciler(
		context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProcessNames: []string{"codex"}, ProviderName: "codex", ProjectHooksForbidden: true}, decision,
	)
	if !errors.Is(err, ErrAutomaticRuntimeEffectUncompensated) {
		t.Fatalf("error = %v, want uncompensated recycle stop", err)
	}
	if provider.CountCalls("Start", info.SessionName) != 1 || provider.CountCalls("Stop", info.SessionName) != 1 {
		t.Fatalf("provider calls = %+v, want seed Start plus one exact Stop and no replacement Start", provider.SnapshotCalls())
	}
	current, getErr := store.Get(info.ID)
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if current.Metadata[ConditionalMutationLeaseMetadataKey] == "" {
		t.Fatal("ambiguous recycle Stop cleared the persisted safety hold")
	}
}

func TestAutomaticRuntimeDecisionTreatsLabelsAsASet(t *testing.T) {
	_, _, _, info, persisted := newAutomaticRuntimeFixture(t)
	info.Labels = []string{"agent:strict-worker", LabelSession}
	decision := automaticRuntimeDecisionForTest(info, persisted)
	current := info
	current.Labels = []string{LabelSession, "agent:strict-worker"}
	if err := validateAutomaticRuntimeSnapshot(current, decision, false); err != nil {
		t.Fatalf("label reorder rejected at unchanged identity: %v", err)
	}
}

func TestAutomaticRuntimeDecisionRejectsFullPersistedProjectionDrift(t *testing.T) {
	_, _, _, info, persisted := newAutomaticRuntimeFixture(t)
	decision := automaticRuntimeDecisionForTest(info, persisted)
	for _, tc := range []struct {
		name   string
		mutate func(*Info)
	}{
		{name: "agent name", mutate: func(current *Info) { current.AgentName = "other-agent" }},
		{name: "pool slot", mutate: func(current *Info) { current.PoolSlot = "2" }},
		{name: "resume command", mutate: func(current *Info) { current.ResumeCommand = "other --resume" }},
		{name: "continuation epoch", mutate: func(current *Info) { current.ContinuationEpoch = "2" }},
		{name: "provider kind", mutate: func(current *Info) { current.ProviderKind = "other-provider" }},
		{name: "label membership", mutate: func(current *Info) { current.Labels = append(current.Labels, "agent:other") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := info
			current.Labels = append([]string(nil), info.Labels...)
			tc.mutate(&current)
			if err := validateAutomaticRuntimeSnapshot(current, decision, false); !errors.Is(err, ErrAutomaticRuntimeDecisionInvalid) {
				t.Fatalf("validateAutomaticRuntimeSnapshot error = %v, want invalid exact projection", err)
			}
		})
	}
}

func TestAutomaticRunLiveLostCommitPreservesWithoutStoppingExistingRuntime(t *testing.T) {
	store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
	seedAutomaticRuntime(t, provider, info)
	provider.driftLive = true
	decision := automaticRuntimeDecisionForTest(info, persisted)
	cfg := runtime.Config{Command: "codex", WorkDir: info.WorkDir, Env: map[string]string{"GC_SESSION_ID": info.ID}, ProviderName: "codex", ProjectHooksForbidden: true}

	_, err := manager.RunLiveForReconciler(context.Background(), info.ID, cfg, decision)
	if !errors.Is(err, ErrConditionalMutationLost) {
		t.Fatalf("RunLiveForReconciler error = %v, want lease lost", err)
	}
	if provider.CountCalls("RunLive", info.SessionName) != 1 || provider.CountCalls("Stop", info.SessionName) != 0 {
		t.Fatalf("provider effects = %+v, want RunLive and no post-drift Stop", provider.SnapshotCalls())
	}
	current, getErr := store.Get(info.ID)
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if current.Metadata["external_after_run_live"] != "won" {
		t.Fatalf("metadata = %#v, want external winner", current.Metadata)
	}
	if current.Metadata[ConditionalMutationLeaseMetadataKey] == "" {
		t.Fatal("post-RunLive authority drift cleared safety hold")
	}
}

func TestAutomaticRelaunchStampsOperationTokenAndContainsOnLostCommit(t *testing.T) {
	for _, drift := range []bool{false, true} {
		t.Run(fmt.Sprintf("drift=%v", drift), func(t *testing.T) {
			store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
			seedAutomaticRuntime(t, provider, info)
			provider.driftRelaunch = drift
			decision := automaticRuntimeDecisionForTest(info, persisted)

			commit, err := manager.RelaunchForReconciler(
				context.Background(), info.ID, runtime.Config{Command: "codex", WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, decision,
			)
			if provider.CountCalls("Relaunch", info.SessionName) != 1 {
				t.Fatalf("provider calls = %+v, want one exact Relaunch", provider.SnapshotCalls())
			}
			current, getErr := store.Get(info.ID)
			if getErr != nil {
				t.Fatalf("Get: %v", getErr)
			}
			if drift {
				if !errors.Is(err, ErrConditionalMutationLost) {
					t.Fatalf("RelaunchForReconciler error = %v, want lease lost", err)
				}
				if provider.CountCalls("Stop", info.SessionName) != 1 || provider.IsRunning(info.SessionName) {
					t.Fatalf("provider calls = %+v running=%v, want exact replacement containment", provider.SnapshotCalls(), provider.IsRunning(info.SessionName))
				}
				if current.Metadata["external_after_relaunch"] != "won" {
					t.Fatalf("metadata = %#v, want external winner", current.Metadata)
				}
				return
			}
			if err != nil {
				t.Fatalf("RelaunchForReconciler: %v", err)
			}
			if commit.Info.ID != info.ID || commit.Persisted.Revision != current.Revision {
				t.Fatalf("commit = %#v, want final session revision %d", commit, current.Revision)
			}
			operationToken, metaErr := provider.GetMeta(info.SessionName, RuntimeOperationTokenEnv)
			if metaErr != nil || operationToken == "" {
				t.Fatalf("relaunch operation token = %q, %v; want nonempty", operationToken, metaErr)
			}
		})
	}
}

func TestAutomaticNudgeCommitsMarkerBeforeDeliveryAndPreservesOnPostEffectDrift(t *testing.T) {
	for _, drift := range []bool{false, true} {
		t.Run(fmt.Sprintf("drift=%v", drift), func(t *testing.T) {
			store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
			seedAutomaticRuntime(t, provider, info)
			provider.driftNudge = drift
			decision := automaticRuntimeDecisionForTest(info, persisted)
			delivered, commit, err := manager.NudgeForReconciler(
				context.Background(), info.ID, "check work", decision, map[string]string{"backstop_marker": "reserved"},
			)
			if !provider.markerSeen {
				t.Fatal("provider Nudge ran before write-ahead marker was persisted")
			}
			if !delivered {
				t.Fatal("Delivered = false, want true after provider success")
			}
			current, getErr := store.Get(info.ID)
			if getErr != nil {
				t.Fatalf("Get: %v", getErr)
			}
			if current.Metadata["backstop_marker"] != "reserved" {
				t.Fatalf("marker = %q, want reserved", current.Metadata["backstop_marker"])
			}
			switch {
			case drift:
				if !errors.Is(err, ErrAutomaticRuntimeEffectUncompensated) {
					t.Fatalf("NudgeForReconciler error = %v, want uncompensated drift", err)
				}
				if current.Metadata[ConditionalMutationLeaseMetadataKey] == "" {
					t.Fatal("post-Nudge drift cleared safety hold")
				}
			case err != nil:
				t.Fatalf("NudgeForReconciler: %v", err)
			case commit.Info.ID != info.ID || commit.Persisted.Revision != current.Revision:
				t.Fatalf("commit = %#v, want final session revision %d", commit, current.Revision)
			}
		})
	}
}

func TestAutomaticRunLiveRejectsRuntimeReplacementAtExactEffectBoundary(t *testing.T) {
	store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
	seedAutomaticRuntime(t, provider, info)
	provider.replaceAtEffect = true
	decision := automaticRuntimeDecisionForTest(info, persisted)
	_, err := manager.RunLiveForReconciler(context.Background(), info.ID, runtime.Config{Command: "codex", WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, decision)
	if !errors.Is(err, runtime.ErrIncarnationMismatch) {
		t.Fatalf("RunLiveForReconciler error = %v, want atomic provider incarnation mismatch", err)
	}
	if provider.CountCalls("RunLive", info.SessionName) != 0 {
		t.Fatalf("provider calls = %+v, live effect reached replacement", provider.SnapshotCalls())
	}
	if !provider.IsRunning(info.SessionName) {
		t.Fatal("replacement runtime was mutated or stopped")
	}
	current, getErr := store.Get(info.ID)
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if current.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("atomic incarnation mismatch left held lease %q despite guaranteed zero effect", current.Metadata[ConditionalMutationLeaseMetadataKey])
	}
}

func TestAutomaticExactNoEffectMismatchReleasesLease(t *testing.T) {
	t.Run("nudge", func(t *testing.T) {
		store, manager, provider, info, _ := newAutomaticRuntimeFixture(t)
		if err := store.Update(info.ID, beads.UpdateOpts{Metadata: map[string]string{"backstop_marker": "available"}}); err != nil {
			t.Fatalf("seed original marker: %v", err)
		}
		info, persisted, err := manager.PersistedStore().GetPersistedResponse(info.ID)
		if err != nil {
			t.Fatalf("reload marker snapshot: %v", err)
		}
		seedAutomaticRuntime(t, provider, info)
		provider.replaceAtEffect = true

		_, _, err = manager.NudgeForReconciler(
			context.Background(), info.ID, "check work", automaticRuntimeDecisionForTest(info, persisted), map[string]string{"backstop_marker": "reserved"},
		)
		if !errors.Is(err, runtime.ErrIncarnationMismatch) {
			t.Fatalf("NudgeForReconciler error = %v, want atomic mismatch", err)
		}
		if provider.CountCalls("Nudge", info.SessionName) != 0 {
			t.Fatalf("provider calls = %+v, nudge reached replacement", provider.SnapshotCalls())
		}
		current, getErr := store.Get(info.ID)
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if current.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("atomic Nudge mismatch left held lease %q", current.Metadata[ConditionalMutationLeaseMetadataKey])
		}
		if current.Metadata["backstop_marker"] != "available" {
			t.Fatalf("atomic Nudge mismatch left marker %q, want original value", current.Metadata["backstop_marker"])
		}
	})

	t.Run("recycle stop", func(t *testing.T) {
		store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
		seedAutomaticRuntime(t, provider, info)
		provider.Zombies[info.SessionName] = true
		provider.replaceAtStop = true

		_, err := manager.StartPreparedRuntimeOnlyForReconciler(
			context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProcessNames: []string{"codex"}, ProviderName: "codex", ProjectHooksForbidden: true}, automaticRuntimeDecisionForTest(info, persisted),
		)
		if !errors.Is(err, runtime.ErrIncarnationMismatch) {
			t.Fatalf("StartPreparedRuntimeOnlyForReconciler error = %v, want atomic Stop mismatch", err)
		}
		if provider.CountCalls("Stop", info.SessionName) != 0 {
			t.Fatalf("provider calls = %+v, Stop reached replacement", provider.SnapshotCalls())
		}
		current, getErr := store.Get(info.ID)
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if current.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("atomic Stop mismatch left held lease %q", current.Metadata[ConditionalMutationLeaseMetadataKey])
		}
	})
}

func TestAutomaticExactCapabilityRefusalReleasesLeaseWithoutProviderEffect(t *testing.T) {
	for _, tc := range []struct {
		name       string
		operation  string
		effectCall string
		invoke     func(*Manager, Info, AutomaticRuntimeDecision) error
	}{
		{
			name:       "run live",
			operation:  "run-live",
			effectCall: "RunLive",
			invoke: func(manager *Manager, info Info, decision AutomaticRuntimeDecision) error {
				_, err := manager.RunLiveForReconciler(context.Background(), info.ID, runtime.Config{Command: "codex", WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, decision)
				return err
			},
		},
		{
			name:       "nudge",
			operation:  "nudge",
			effectCall: "Nudge",
			invoke: func(manager *Manager, info Info, decision AutomaticRuntimeDecision) error {
				_, _, err := manager.NudgeForReconciler(context.Background(), info.ID, "check work", decision, map[string]string{"backstop_marker": "reserved"})
				return err
			},
		},
		{
			name:       "recycle stop",
			operation:  "stop",
			effectCall: "Stop",
			invoke: func(manager *Manager, info Info, decision AutomaticRuntimeDecision) error {
				_, err := manager.StartPreparedRuntimeOnlyForReconciler(context.Background(), info.ID, automaticRuntimeResumeCommandForTest, runtime.Config{WorkDir: info.WorkDir, ProcessNames: []string{"codex"}, ProviderName: "codex", ProjectHooksForbidden: true}, decision)
				return err
			},
		},
		{
			name:       "runtime metadata",
			operation:  "metadata",
			effectCall: "SetMeta",
			invoke: func(manager *Manager, info Info, decision AutomaticRuntimeDecision) error {
				_, err := manager.SetRuntimeMetadataForReconciler(context.Background(), info.ID, "GC_DRAIN", "1", decision)
				return err
			},
		},
		{
			name:       "relaunch",
			operation:  "relaunch",
			effectCall: "Relaunch",
			invoke: func(manager *Manager, info Info, decision AutomaticRuntimeDecision) error {
				_, err := manager.RelaunchForReconciler(context.Background(), info.ID, runtime.Config{Command: "codex", WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, decision)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
			seedAutomaticRuntime(t, provider, info)
			if tc.operation == "stop" {
				provider.Zombies[info.SessionName] = true
			}
			provider.unsupportedExact = tc.operation
			before := provider.CountCalls(tc.effectCall, info.SessionName)

			err := tc.invoke(manager, info, automaticRuntimeDecisionForTest(info, persisted))
			if !errors.Is(err, runtime.ErrExactIncarnationUnsupported) || !errors.Is(err, ErrAutomaticRuntimeExactEffectUnsupported) {
				t.Fatalf("error = %v, want exact-incarnation unsupported refusal", err)
			}
			if got := provider.CountCalls(tc.effectCall, info.SessionName); got != before {
				t.Fatalf("%s calls advanced from %d to %d after unsupported refusal", tc.effectCall, before, got)
			}
			current, getErr := store.Get(info.ID)
			if getErr != nil {
				t.Fatalf("Get: %v", getErr)
			}
			if current.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
				t.Fatalf("unsupported refusal left held lease %q", current.Metadata[ConditionalMutationLeaseMetadataKey])
			}
			if tc.operation == "nudge" && current.Metadata["backstop_marker"] != "" {
				t.Fatalf("unsupported Nudge left write-ahead marker %q after zero provider effect", current.Metadata["backstop_marker"])
			}
		})
	}
}

func TestAutomaticPeekDiscardsOutputAfterPersistedDecisionDrift(t *testing.T) {
	store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
	seedAutomaticRuntime(t, provider, info)
	provider.SetPeekOutput(info.SessionName, "sensitive stale output")
	provider.driftPeek = true
	decision := automaticRuntimeDecisionForTest(info, persisted)

	output, err := manager.PeekForReconciler(info.ID, 20, decision)
	if output != "" || !errors.Is(err, ErrConditionalMutationLost) {
		t.Fatalf("PeekForReconciler = (%q, %v), want discarded output and lost decision", output, err)
	}
	current, getErr := store.Get(info.ID)
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if current.Metadata["external_after_peek"] != "won" {
		t.Fatalf("metadata = %#v, want external winner", current.Metadata)
	}
}

func TestAutomaticExactReadsRejectRuntimeReplacementAtReadBoundary(t *testing.T) {
	t.Run("observe", func(t *testing.T) {
		store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
		seedAutomaticRuntime(t, provider, info)
		provider.replaceAtObserve = true

		_, observation, err := manager.ObserveRuntimeForReconciler(info.ID, nil, automaticRuntimeDecisionForTest(info, persisted), false)
		if !errors.Is(err, runtime.ErrIncarnationMismatch) || observation != (RuntimeObservation{}) {
			t.Fatalf("ObserveRuntimeForReconciler = (%#v, %v), want empty observation and exact mismatch", observation, err)
		}
		current, getErr := store.Get(info.ID)
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if current.Revision != persisted.Revision {
			t.Fatalf("session revision = %d, want unchanged %d after refused pure read", current.Revision, persisted.Revision)
		}
	})

	t.Run("peek", func(t *testing.T) {
		store, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
		seedAutomaticRuntime(t, provider, info)
		provider.SetPeekOutput(info.SessionName, "replacement output")
		provider.replaceAtEffect = true

		output, err := manager.PeekForReconciler(info.ID, 20, automaticRuntimeDecisionForTest(info, persisted))
		if output != "" || !errors.Is(err, runtime.ErrIncarnationMismatch) {
			t.Fatalf("PeekForReconciler = (%q, %v), want empty output and exact mismatch", output, err)
		}
		if provider.CountCalls("Peek", info.SessionName) != 0 {
			t.Fatalf("provider calls = %+v, replacement output was read", provider.SnapshotCalls())
		}
		current, getErr := store.Get(info.ID)
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if current.Revision != persisted.Revision {
			t.Fatalf("session revision = %d, want unchanged %d after refused pure read", current.Revision, persisted.Revision)
		}
	})
}

func TestAutomaticRuntimeMetadataRequiresExactIncarnationAtEffectBoundary(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprintf("replace=%v", replace), func(t *testing.T) {
			_, manager, provider, info, persisted := newAutomaticRuntimeFixture(t)
			seedAutomaticRuntime(t, provider, info)
			provider.replaceAtEffect = replace
			decision := automaticRuntimeDecisionForTest(info, persisted)

			commit, err := manager.SetRuntimeMetadataForReconciler(
				context.Background(), info.ID, "GC_DRAIN", "1", decision,
			)
			if replace {
				if !errors.Is(err, runtime.ErrIncarnationMismatch) {
					t.Fatalf("SetRuntimeMetadataForReconciler error = %v, want incarnation mismatch", err)
				}
				if value, _ := provider.GetMeta(info.SessionName, "GC_DRAIN"); value != "" {
					t.Fatalf("replacement GC_DRAIN = %q, want untouched", value)
				}
				return
			}
			if err != nil {
				t.Fatalf("SetRuntimeMetadataForReconciler: %v", err)
			}
			if commit.Info.ID != info.ID || commit.Persisted.Revision <= persisted.Revision {
				t.Fatalf("commit = %#v, want advanced exact revision", commit)
			}
			if value, _ := provider.GetMeta(info.SessionName, "GC_DRAIN"); value != "1" {
				t.Fatalf("GC_DRAIN = %q, want 1", value)
			}
		})
	}
}

func seedAutomaticRuntime(t *testing.T, provider *automaticRuntimeProvider, info Info) {
	t.Helper()
	if err := provider.Fake.Start(context.Background(), info.SessionName, runtime.Config{Command: "codex"}); err != nil {
		t.Fatalf("seed runtime: %v", err)
	}
	for key, value := range map[string]string{
		"GC_SESSION_ID":     info.ID,
		"GC_INSTANCE_TOKEN": info.InstanceToken,
		"GC_RUNTIME_EPOCH":  info.Generation,
	} {
		if err := provider.SetMeta(info.SessionName, key, value); err != nil {
			t.Fatalf("seed runtime metadata %s: %v", key, err)
		}
	}
}

func newAutomaticRuntimeFixture(t *testing.T) (*beads.MemStore, *Manager, *automaticRuntimeProvider, Info, PersistedResponse) {
	t.Helper()
	workDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize WorkDir: %v", err)
	}
	store := beads.NewMemStore()
	provider := &automaticRuntimeProvider{Fake: runtime.NewFake(), store: store}
	manager := NewManagerWithOptions(store, provider)
	created, err := manager.CreateSession(context.Background(), CreateOptions{
		BeadOnly: true,
		Template: "strict-worker",
		Title:    "Strict Worker",
		Command:  "codex",
		WorkDir:  workDir,
		Provider: "codex",
		ExtraMeta: map[string]string{
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
			"agent_name":                            "strict-worker",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	provider.sessionID = created.ID
	info, persisted, err := manager.PersistedStore().GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("GetPersistedResponse: %v", err)
	}
	return store, manager, provider, info, persisted
}

func automaticRuntimeDecisionForTest(info Info, persisted PersistedResponse) AutomaticRuntimeDecision {
	return AutomaticRuntimeDecision{
		Captured:              info,
		ExpectedRevision:      persisted.Revision,
		RevisionCaptured:      true,
		Witness:               LiveBoundaryWitness{TriggerBeadID: info.TriggerBeadID, TriggerBeadStoreRef: info.TriggerBeadStoreRef},
		ProjectHooksForbidden: true,
		ValidatePolicy: func(Info, PersistedResponse, bool) error {
			return nil
		},
	}
}
