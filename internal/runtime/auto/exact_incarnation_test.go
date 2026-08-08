package auto

import (
	"context"
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/runtime"
)

func TestExactIncarnationEffectsReachOnlyTheRoutedBackend(t *testing.T) {
	local := newExactRecordingProvider()
	remote := newExactRecordingProvider()
	provider := New(local, remote)
	expected := runtime.Incarnation{SessionID: "session-1", InstanceToken: "instance-1", Epoch: "7"}
	remoteExpected := expected
	remoteExpected.Transport = "acp"

	if err := provider.NudgeExact("local-session", expected, runtime.TextContent("local")); err != nil {
		t.Fatalf("local NudgeExact: %v", err)
	}
	provider.RouteACP("remote-session")
	provider.Unroute("remote-session")
	if err := provider.StopExact("remote-session", remoteExpected); err != nil {
		t.Fatalf("transport-bound remote StopExact after mutable unroute: %v", err)
	}
	if err := provider.SetMetaExact("remote-session", remoteExpected, "GC_DRAIN", "1"); err != nil {
		t.Fatalf("remote SetMetaExact: %v", err)
	}
	start, err := runtime.ResolveExactIncarnationStart(provider, "fresh-remote", "acp", runtime.Config{})
	if err != nil {
		t.Fatalf("ResolveExactIncarnationStart: %v", err)
	}
	if err := start(context.Background(), "fresh-remote", remoteExpected, runtime.Config{}); err != nil {
		t.Fatalf("remote StartExact: %v", err)
	}
	provider.mu.RLock()
	publishedBeforeCommit := provider.routes["fresh-remote"]
	provider.mu.RUnlock()
	if publishedBeforeCommit {
		t.Fatal("exact start resolver published mutable route before the caller committed authority")
	}
	if _, err := provider.ObserveExact("fresh-remote", remoteExpected, nil); err != nil {
		t.Fatalf("remote ObserveExact: %v", err)
	}
	if _, err := provider.PeekExact("fresh-remote", remoteExpected, 20); err != nil {
		t.Fatalf("remote PeekExact: %v", err)
	}

	if local.nudges != 1 || local.stops != 0 {
		t.Fatalf("local effects = nudges:%d stops:%d, want 1/0", local.nudges, local.stops)
	}
	if remote.nudges != 0 || remote.stops != 1 || remote.metadataSets != 1 || remote.starts != 1 || remote.observes != 1 || remote.peeks != 1 {
		t.Fatalf("remote effects = nudges:%d stops:%d metadata:%d starts:%d observes:%d peeks:%d, want 0/1/1/1/1/1", remote.nudges, remote.stops, remote.metadataSets, remote.starts, remote.observes, remote.peeks)
	}
}

func TestExactStartResolverRefusesUnsupportedSelectedBackend(t *testing.T) {
	provider := New(runtime.NewFake(), newExactRecordingProvider())
	if _, err := runtime.ResolveExactIncarnationStart(provider, "local-session", "", runtime.Config{}); !errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
		t.Fatalf("ResolveExactIncarnationStart error = %v, want unsupported", err)
	}
}

func TestExactStartResolverRequiresSelectedBackendContainment(t *testing.T) {
	provider := New(newIncompleteExactStartProvider(), newExactRecordingProvider())
	if _, err := runtime.ResolveExactIncarnationStart(provider, "local-session", "", runtime.Config{}); !errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
		t.Fatalf("ResolveExactIncarnationStart error = %v, want unsupported without exact Stop containment", err)
	}
}

func TestExactRunLiveReportsPreEffectIsolationRefusalAsUnsupported(t *testing.T) {
	provider := New(newExactRecordingProvider(), newExactRecordingProvider())
	err := provider.RunLiveExact(
		"local-session",
		runtime.Incarnation{SessionID: "session-1", InstanceToken: "instance-1", Epoch: "7"},
		runtime.Config{ProjectHooksForbidden: true},
	)
	if !errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
		t.Fatalf("RunLiveExact error = %v, want pre-effect exact-incarnation unsupported", err)
	}
}

type exactRecordingProvider struct {
	runtime.Provider
	nudges       int
	stops        int
	metadataSets int
	starts       int
	observes     int
	peeks        int
}

func newExactRecordingProvider() *exactRecordingProvider {
	return &exactRecordingProvider{Provider: runtime.NewFake()}
}

func (p *exactRecordingProvider) RunLiveExact(string, runtime.Incarnation, runtime.Config) error {
	return nil
}

func (p *exactRecordingProvider) NudgeExact(string, runtime.Incarnation, []runtime.ContentBlock) error {
	p.nudges++
	return nil
}

func (p *exactRecordingProvider) StopExact(string, runtime.Incarnation) error {
	p.stops++
	return nil
}

func (p *exactRecordingProvider) SetMetaExact(string, runtime.Incarnation, string, string) error {
	p.metadataSets++
	return nil
}

func (p *exactRecordingProvider) RemoveMetaExact(string, runtime.Incarnation, string) error {
	return nil
}

func (p *exactRecordingProvider) StartExact(context.Context, string, runtime.Incarnation, runtime.Config) error {
	p.starts++
	return nil
}

func (p *exactRecordingProvider) ObserveExact(string, runtime.Incarnation, []string) (runtime.IncarnationObservation, error) {
	p.observes++
	return runtime.IncarnationObservation{Running: true, Alive: true}, nil
}

func (p *exactRecordingProvider) PeekExact(string, runtime.Incarnation, int) (string, error) {
	p.peeks++
	return "output", nil
}

type incompleteExactStartProvider struct {
	runtime.Provider
}

func newIncompleteExactStartProvider() *incompleteExactStartProvider {
	return &incompleteExactStartProvider{Provider: runtime.NewFake()}
}

func (p *incompleteExactStartProvider) StartExact(context.Context, string, runtime.Incarnation, runtime.Config) error {
	return nil
}

func (p *incompleteExactStartProvider) ObserveExact(string, runtime.Incarnation, []string) (runtime.IncarnationObservation, error) {
	return runtime.IncarnationObservation{Running: true, Alive: true}, nil
}
