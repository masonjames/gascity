package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
)

func TestStatusProviderPreservesExactIncarnationCapabilities(t *testing.T) {
	base := &statusExactRecordingProvider{Provider: runtime.NewFake()}
	wrapped := newBoundedStatusProvider(base)
	expected := runtime.Incarnation{SessionID: "session-1", InstanceToken: "instance-1", Epoch: "7"}

	exact, ok := wrapped.(runtime.ExactIncarnationProvider)
	if !ok {
		t.Fatalf("%T does not expose ExactIncarnationProvider", wrapped)
	}
	metadata, ok := wrapped.(runtime.ExactIncarnationMetadataProvider)
	if !ok {
		t.Fatalf("%T does not expose ExactIncarnationMetadataProvider", wrapped)
	}
	relaunch, ok := wrapped.(runtime.ExactIncarnationRelaunchProvider)
	if !ok {
		t.Fatalf("%T does not expose ExactIncarnationRelaunchProvider", wrapped)
	}
	observer, ok := wrapped.(runtime.ExactIncarnationObserver)
	if !ok {
		t.Fatalf("%T does not expose ExactIncarnationObserver", wrapped)
	}
	peeker, ok := wrapped.(runtime.ExactIncarnationPeekProvider)
	if !ok {
		t.Fatalf("%T does not expose ExactIncarnationPeekProvider", wrapped)
	}
	router, ok := wrapped.(interface {
		RouteACP(string)
		Unroute(string)
	})
	if !ok {
		t.Fatalf("%T does not preserve the paired ACP route capability", wrapped)
	}

	if err := exact.NudgeExact("agent-a", expected, runtime.TextContent("continue")); err != nil {
		t.Fatalf("NudgeExact: %v", err)
	}
	if err := exact.StopExact("agent-a", expected); err != nil {
		t.Fatalf("StopExact: %v", err)
	}
	if err := metadata.SetMetaExact("agent-a", expected, "GC_DRAIN", "1"); err != nil {
		t.Fatalf("SetMetaExact: %v", err)
	}
	if err := relaunch.RelaunchExact(context.Background(), "agent-a", expected, runtime.Config{}); err != nil {
		t.Fatalf("RelaunchExact: %v", err)
	}
	start, err := runtime.ResolveExactIncarnationStart(wrapped, "agent-a", "", runtime.Config{})
	if err != nil {
		t.Fatalf("ResolveExactIncarnationStart: %v", err)
	}
	if err := start(context.Background(), "agent-a", expected, runtime.Config{}); err != nil {
		t.Fatalf("StartExact: %v", err)
	}
	if _, err := observer.ObserveExact("agent-a", expected, nil); err != nil {
		t.Fatalf("ObserveExact: %v", err)
	}
	if _, err := peeker.PeekExact("agent-a", expected, 20); err != nil {
		t.Fatalf("PeekExact: %v", err)
	}
	router.RouteACP("agent-a")
	router.Unroute("agent-a")
	if base.nudges != 1 || base.stops != 1 || base.metadataSets != 1 || base.relaunches != 1 || base.starts != 1 || base.observes != 1 || base.peeks != 1 || base.routes != 1 || base.unroutes != 1 {
		t.Fatalf("forwarded effects = nudges:%d stops:%d metadata:%d relaunch:%d starts:%d observes:%d peeks:%d routes:%d unroutes:%d, want all 1", base.nudges, base.stops, base.metadataSets, base.relaunches, base.starts, base.observes, base.peeks, base.routes, base.unroutes)
	}
}

func TestStatusProviderWrappedUnsupportedBackendRefusesAutomaticStartBeforeEffect(t *testing.T) {
	workDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize WorkDir: %v", err)
	}
	store := beads.NewMemStore()
	base := runtime.NewFake()
	manager := session.NewManagerWithOptions(store, newBoundedStatusProvider(base))
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
			"agent_name":                            "strict-worker",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	info, persisted, err := manager.PersistedStore().GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("GetPersistedResponse: %v", err)
	}
	decision := session.AutomaticRuntimeDecision{
		Captured:              info,
		ExpectedRevision:      persisted.Revision,
		RevisionCaptured:      true,
		Witness:               session.LiveBoundaryWitness{TriggerBeadID: info.TriggerBeadID, TriggerBeadStoreRef: info.TriggerBeadStoreRef},
		ProjectHooksForbidden: true,
		ValidatePolicy: func(session.Info, session.PersistedResponse, bool) error {
			return nil
		},
	}

	_, err = manager.StartPreparedRuntimeOnlyForReconciler(
		context.Background(), info.ID, "codex resume session-123", runtime.Config{WorkDir: info.WorkDir, ProviderName: "codex", ProjectHooksForbidden: true}, decision,
	)
	if !errors.Is(err, runtime.ErrExactIncarnationUnsupported) || !errors.Is(err, session.ErrAutomaticRuntimeExactEffectUnsupported) {
		t.Fatalf("StartPreparedRuntimeOnlyForReconciler error = %v, want exact-incarnation unsupported", err)
	}
	if base.CountCalls("Start", info.SessionName) != 0 {
		t.Fatalf("base provider calls = %+v, want zero Start", base.SnapshotCalls())
	}
	current, getErr := store.Get(info.ID)
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if current.Metadata[session.ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("unsupported automatic start left held lease %q", current.Metadata[session.ConditionalMutationLeaseMetadataKey])
	}
}

func TestStatusProviderExactStartResolverRequiresContainmentAndObserver(t *testing.T) {
	base := &statusStartObserveOnlyProvider{Provider: runtime.NewFake()}
	wrapped := newBoundedStatusProvider(base)

	start, err := runtime.ResolveExactIncarnationStart(wrapped, "agent-a", "", runtime.Config{})
	if !errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
		t.Fatalf("ResolveExactIncarnationStart error = %v, want exact-incarnation unsupported", err)
	}
	if start != nil {
		t.Fatal("ResolveExactIncarnationStart returned a start handle without exact containment")
	}
	if base.starts != 0 {
		t.Fatalf("StartExact calls = %d, want 0", base.starts)
	}
}

func TestStatusProviderExactRelaunchRequiresContainmentAndObserver(t *testing.T) {
	t.Run("relaunch only", func(t *testing.T) {
		base := &statusRelaunchOnlyProvider{Provider: runtime.NewFake()}
		wrapped := newBoundedStatusProvider(base)
		relauncher := wrapped.(runtime.ExactIncarnationRelaunchProvider)

		err := relauncher.RelaunchExact(context.Background(), "agent-a", runtime.Incarnation{}, runtime.Config{})
		if !errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
			t.Fatalf("RelaunchExact error = %v, want exact-incarnation unsupported", err)
		}
		if base.relaunches != 0 {
			t.Fatalf("RelaunchExact calls = %d, want 0", base.relaunches)
		}
	})

	t.Run("relaunch and observe without containment", func(t *testing.T) {
		base := &statusRelaunchObserveOnlyProvider{statusRelaunchOnlyProvider: statusRelaunchOnlyProvider{Provider: runtime.NewFake()}}
		wrapped := newBoundedStatusProvider(base)
		relauncher := wrapped.(runtime.ExactIncarnationRelaunchProvider)

		err := relauncher.RelaunchExact(context.Background(), "agent-a", runtime.Incarnation{}, runtime.Config{})
		if !errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
			t.Fatalf("RelaunchExact error = %v, want exact-incarnation unsupported", err)
		}
		if base.relaunches != 0 {
			t.Fatalf("RelaunchExact calls = %d, want 0", base.relaunches)
		}
	})
}

type statusExactRecordingProvider struct {
	runtime.Provider
	nudges       int
	stops        int
	metadataSets int
	relaunches   int
	starts       int
	observes     int
	peeks        int
	routes       int
	unroutes     int
}

type statusStartObserveOnlyProvider struct {
	runtime.Provider
	starts int
}

type statusRelaunchOnlyProvider struct {
	runtime.Provider
	relaunches int
}

func (p *statusRelaunchOnlyProvider) RelaunchExact(context.Context, string, runtime.Incarnation, runtime.Config) error { //nolint:unparam // Interface test double.
	p.relaunches++
	return nil
}

type statusRelaunchObserveOnlyProvider struct {
	statusRelaunchOnlyProvider
}

func (*statusRelaunchObserveOnlyProvider) ObserveExact(string, runtime.Incarnation, []string) (runtime.IncarnationObservation, error) { //nolint:unparam // Interface test double.
	return runtime.IncarnationObservation{Running: true, Alive: true}, nil
}

func (p *statusStartObserveOnlyProvider) StartExact(context.Context, string, runtime.Incarnation, runtime.Config) error { //nolint:unparam // Interface test double.
	p.starts++
	return nil
}

func (*statusStartObserveOnlyProvider) ObserveExact(string, runtime.Incarnation, []string) (runtime.IncarnationObservation, error) {
	return runtime.IncarnationObservation{}, runtime.ErrSessionNotFound
}

func (p *statusExactRecordingProvider) RunLiveExact(string, runtime.Incarnation, runtime.Config) error {
	return nil
}

func (p *statusExactRecordingProvider) NudgeExact(string, runtime.Incarnation, []runtime.ContentBlock) error { //nolint:unparam // Interface test double.
	p.nudges++
	return nil
}

func (p *statusExactRecordingProvider) StopExact(string, runtime.Incarnation) error { //nolint:unparam // Interface test double.
	p.stops++
	return nil
}

func (p *statusExactRecordingProvider) SetMetaExact(string, runtime.Incarnation, string, string) error { //nolint:unparam // Interface test double.
	p.metadataSets++
	return nil
}

func (p *statusExactRecordingProvider) RemoveMetaExact(string, runtime.Incarnation, string) error {
	return nil
}

func (p *statusExactRecordingProvider) RelaunchExact(context.Context, string, runtime.Incarnation, runtime.Config) error { //nolint:unparam // Interface test double.
	p.relaunches++
	return nil
}

func (p *statusExactRecordingProvider) StartExact(context.Context, string, runtime.Incarnation, runtime.Config) error { //nolint:unparam // Interface test double.
	p.starts++
	return nil
}

func (p *statusExactRecordingProvider) ObserveExact(string, runtime.Incarnation, []string) (runtime.IncarnationObservation, error) { //nolint:unparam // Interface test double.
	p.observes++
	return runtime.IncarnationObservation{Running: true, Alive: true}, nil
}

func (p *statusExactRecordingProvider) PeekExact(string, runtime.Incarnation, int) (string, error) { //nolint:unparam // Interface test double.
	p.peeks++
	return "output", nil
}

func (p *statusExactRecordingProvider) RouteACP(string) {
	p.routes++
}

func (p *statusExactRecordingProvider) Unroute(string) {
	p.unroutes++
}
