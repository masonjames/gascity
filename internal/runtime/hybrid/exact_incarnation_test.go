package hybrid

import (
	"context"
	"testing"

	"github.com/gastownhall/gascity/internal/runtime"
)

func TestHybridDoesNotExposeExactIncarnationCapabilities(t *testing.T) {
	provider := New(newHybridExactRecordingProvider(), newHybridExactRecordingProvider(), func(string) bool { return false })
	for _, check := range []struct {
		name string
		ok   bool
	}{
		{"effects", implementsExactIncarnationProvider(provider)},
		{"metadata", implementsExactIncarnationMetadataProvider(provider)},
		{"start", implementsExactIncarnationStartResolver(provider)},
		{"observe", implementsExactIncarnationObserver(provider)},
		{"peek", implementsExactIncarnationPeekProvider(provider)},
	} {
		if check.ok {
			t.Fatalf("hybrid Provider exposes %s exact-incarnation capability through a mutable routing predicate", check.name)
		}
	}
}

func implementsExactIncarnationProvider(provider runtime.Provider) bool {
	_, ok := provider.(runtime.ExactIncarnationProvider)
	return ok
}

func implementsExactIncarnationMetadataProvider(provider runtime.Provider) bool {
	_, ok := provider.(runtime.ExactIncarnationMetadataProvider)
	return ok
}

func implementsExactIncarnationStartResolver(provider runtime.Provider) bool {
	_, ok := provider.(runtime.ExactIncarnationStartResolver)
	return ok
}

func implementsExactIncarnationObserver(provider runtime.Provider) bool {
	_, ok := provider.(runtime.ExactIncarnationObserver)
	return ok
}

func implementsExactIncarnationPeekProvider(provider runtime.Provider) bool {
	_, ok := provider.(runtime.ExactIncarnationPeekProvider)
	return ok
}

type hybridExactRecordingProvider struct {
	runtime.Provider
}

func newHybridExactRecordingProvider() *hybridExactRecordingProvider {
	return &hybridExactRecordingProvider{Provider: runtime.NewFake()}
}

func (p *hybridExactRecordingProvider) RunLiveExact(string, runtime.Incarnation, runtime.Config) error {
	return nil
}

func (p *hybridExactRecordingProvider) NudgeExact(string, runtime.Incarnation, []runtime.ContentBlock) error {
	return nil
}

func (p *hybridExactRecordingProvider) StopExact(string, runtime.Incarnation) error {
	return nil
}

func (p *hybridExactRecordingProvider) SetMetaExact(string, runtime.Incarnation, string, string) error {
	return nil
}

func (p *hybridExactRecordingProvider) RemoveMetaExact(string, runtime.Incarnation, string) error {
	return nil
}

func (p *hybridExactRecordingProvider) StartExact(context.Context, string, runtime.Incarnation, runtime.Config) error {
	return nil
}

func (p *hybridExactRecordingProvider) ObserveExact(string, runtime.Incarnation, []string) (runtime.IncarnationObservation, error) {
	return runtime.IncarnationObservation{Running: true, Alive: true}, nil
}

func (p *hybridExactRecordingProvider) PeekExact(string, runtime.Incarnation, int) (string, error) {
	return "output", nil
}
