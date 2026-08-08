package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gastownhall/gascity/internal/runtime"
)

var (
	statusProviderCallTimeout    = 50 * time.Millisecond
	statusProviderTimeoutWarning = func() {
		fmt.Fprintln(os.Stderr, "gc status: runtime status probe timed out; using partial status")
	}
)

type statusProvider struct {
	base     runtime.Provider
	warnOnce sync.Once
	partial  atomic.Bool
}

var (
	_ runtime.RelaunchProvider                 = (*statusProvider)(nil)
	_ runtime.ExactIncarnationProvider         = (*statusProvider)(nil)
	_ runtime.ExactIncarnationMetadataProvider = (*statusProvider)(nil)
	_ runtime.ExactIncarnationRelaunchProvider = (*statusProvider)(nil)
	_ runtime.ExactIncarnationStartResolver    = (*statusProvider)(nil)
	_ runtime.ExactIncarnationObserver         = (*statusProvider)(nil)
	_ runtime.ExactIncarnationPeekProvider     = (*statusProvider)(nil)
)

func statusProviderPartial(sp any) bool {
	p, ok := sp.(*statusProvider)
	return ok && p.partial.Load()
}

func markStatusProviderPartial(sp any) {
	if p, ok := sp.(*statusProvider); ok {
		p.partial.Store(true)
	}
}

func (p *statusProvider) StatusPartial() bool {
	return p.partial.Load()
}

func newBoundedStatusProvider(base runtime.Provider) runtime.Provider {
	if sp, ok := base.(*statusProvider); ok {
		return sp
	}
	return &statusProvider{base: base}
}

func boundedStatusCall[T any](p *statusProvider, fallback T, fn func() T) T {
	if statusProviderCallTimeout <= 0 {
		return fn()
	}
	resultCh := make(chan T, 1)
	go func() {
		resultCh <- fn()
	}()
	select {
	case result := <-resultCh:
		return result
	case <-time.After(statusProviderCallTimeout):
		p.partial.Store(true)
		p.warnOnce.Do(statusProviderTimeoutWarning)
		return fallback
	}
}

func (p *statusProvider) Start(ctx context.Context, name string, cfg runtime.Config) error {
	return p.base.Start(ctx, name, cfg)
}

func (p *statusProvider) Stop(name string) error {
	return p.base.Stop(name)
}

func (p *statusProvider) Interrupt(name string) error {
	return p.base.Interrupt(name)
}

func (p *statusProvider) IsRunning(name string) bool {
	return boundedStatusCall(p, false, func() bool {
		return p.base.IsRunning(name)
	})
}

func (p *statusProvider) IsAttached(name string) bool {
	return boundedStatusCall(p, false, func() bool {
		return p.base.IsAttached(name)
	})
}

func (p *statusProvider) Attach(name string) error {
	return p.base.Attach(name)
}

func (p *statusProvider) ProcessAlive(name string, processNames []string) bool {
	return boundedStatusCall(p, false, func() bool {
		return p.base.ProcessAlive(name, processNames)
	})
}

func (p *statusProvider) ObserveLiveness(name string, processNames []string) runtime.Liveness {
	return boundedStatusCall(p, runtime.Liveness{}, func() runtime.Liveness {
		return runtime.ObserveLiveness(p.base, name, processNames)
	})
}

func (p *statusProvider) Nudge(name string, content []runtime.ContentBlock) error {
	return p.base.Nudge(name, content)
}

func (p *statusProvider) SetMeta(name, key, value string) error {
	return p.base.SetMeta(name, key, value)
}

func (p *statusProvider) GetMeta(name, key string) (string, error) {
	result := boundedStatusCall(p, struct {
		value string
		err   error
	}{}, func() struct {
		value string
		err   error
	} {
		value, err := p.base.GetMeta(name, key)
		return struct {
			value string
			err   error
		}{value: value, err: err}
	})
	return result.value, result.err
}

func (p *statusProvider) RemoveMeta(name, key string) error {
	return p.base.RemoveMeta(name, key)
}

func (p *statusProvider) Peek(name string, lines int) (string, error) {
	result := boundedStatusCall(p, struct {
		value string
		err   error
	}{}, func() struct {
		value string
		err   error
	} {
		value, err := p.base.Peek(name, lines)
		return struct {
			value string
			err   error
		}{value: value, err: err}
	})
	return result.value, result.err
}

func (p *statusProvider) ListRunning(prefix string) ([]string, error) {
	result := boundedStatusCall(p, struct {
		value []string
		err   error
	}{}, func() struct {
		value []string
		err   error
	} {
		value, err := p.base.ListRunning(prefix)
		return struct {
			value []string
			err   error
		}{value: value, err: err}
	})
	return result.value, result.err
}

func (p *statusProvider) RouteACP(name string) {
	if router, ok := p.base.(interface{ RouteACP(string) }); ok {
		router.RouteACP(name)
	}
}

// Unroute preserves the paired ACP route capability through the status
// wrapper. Exact starts publish a route only after their session commit.
func (p *statusProvider) Unroute(name string) {
	if router, ok := p.base.(interface{ Unroute(string) }); ok {
		router.Unroute(name)
	}
}

func (p *statusProvider) GetLastActivity(name string) (time.Time, error) {
	result := boundedStatusCall(p, struct {
		value time.Time
		err   error
	}{}, func() struct {
		value time.Time
		err   error
	} {
		value, err := p.base.GetLastActivity(name)
		return struct {
			value time.Time
			err   error
		}{value: value, err: err}
	})
	return result.value, result.err
}

func (p *statusProvider) ClearScrollback(name string) error {
	return p.base.ClearScrollback(name)
}

func (p *statusProvider) CopyTo(name, src, relDst string) error {
	return p.base.CopyTo(name, src, relDst)
}

func (p *statusProvider) SendKeys(name string, keys ...string) error {
	return p.base.SendKeys(name, keys...)
}

func (p *statusProvider) RunLive(name string, cfg runtime.Config) error {
	return p.base.RunLive(name, cfg)
}

// RunLiveExact preserves the routed provider's exact-incarnation live effect
// through the status-only wrapper. Mutations are never timeout-bounded.
func (p *statusProvider) RunLiveExact(name string, expected runtime.Incarnation, cfg runtime.Config) error {
	exact, ok := p.base.(runtime.ExactIncarnationProvider)
	if !ok {
		return fmt.Errorf("%w: status provider base does not support exact-incarnation live operations", runtime.ErrExactIncarnationUnsupported)
	}
	return exact.RunLiveExact(name, expected, cfg)
}

// NudgeExact preserves exact-incarnation delivery through the status wrapper.
func (p *statusProvider) NudgeExact(name string, expected runtime.Incarnation, content []runtime.ContentBlock) error {
	exact, ok := p.base.(runtime.ExactIncarnationProvider)
	if !ok {
		return fmt.Errorf("%w: status provider base does not support exact-incarnation nudges", runtime.ErrExactIncarnationUnsupported)
	}
	return exact.NudgeExact(name, expected, content)
}

// StopExact preserves exact-incarnation teardown through the status wrapper.
func (p *statusProvider) StopExact(name string, expected runtime.Incarnation) error {
	exact, ok := p.base.(runtime.ExactIncarnationProvider)
	if !ok {
		return fmt.Errorf("%w: status provider base does not support exact-incarnation stops", runtime.ErrExactIncarnationUnsupported)
	}
	return exact.StopExact(name, expected)
}

// ResolveExactIncarnationStart binds a fresh exact start through the wrapped
// provider without invoking its ordinary name-only Start path.
func (p *statusProvider) ResolveExactIncarnationStart(name, transport string, cfg runtime.Config) (runtime.ExactIncarnationStart, error) {
	// A successful automatic start must be observable and containable through
	// the same concrete base. statusProvider itself implements the forwarding
	// interfaces structurally, so resolving StartExact without these base checks
	// would advertise a start handle whose post-effect attestation or rollback
	// can only fail after the runtime has already been created.
	if _, ok := p.base.(runtime.ExactIncarnationProvider); !ok {
		return nil, fmt.Errorf("%w: status provider base cannot contain an exact start", runtime.ErrExactIncarnationUnsupported)
	}
	if _, ok := p.base.(runtime.ExactIncarnationObserver); !ok {
		return nil, fmt.Errorf("%w: status provider base cannot observe an exact start", runtime.ErrExactIncarnationUnsupported)
	}
	return runtime.ResolveExactIncarnationStart(p.base, name, transport, cfg)
}

// ObserveExact preserves exact-incarnation observation through the status wrapper.
func (p *statusProvider) ObserveExact(name string, expected runtime.Incarnation, processNames []string) (runtime.IncarnationObservation, error) {
	observer, ok := p.base.(runtime.ExactIncarnationObserver)
	if !ok {
		return runtime.IncarnationObservation{}, fmt.Errorf("%w: status provider base does not support exact-incarnation observation", runtime.ErrExactIncarnationUnsupported)
	}
	return observer.ObserveExact(name, expected, processNames)
}

// PeekExact preserves exact-incarnation output reads through the status wrapper.
func (p *statusProvider) PeekExact(name string, expected runtime.Incarnation, lines int) (string, error) {
	peeker, ok := p.base.(runtime.ExactIncarnationPeekProvider)
	if !ok {
		return "", fmt.Errorf("%w: status provider base does not support exact-incarnation peek", runtime.ErrExactIncarnationUnsupported)
	}
	return peeker.PeekExact(name, expected, lines)
}

// SetMetaExact preserves exact-incarnation metadata through the status wrapper.
func (p *statusProvider) SetMetaExact(name string, expected runtime.Incarnation, key, value string) error {
	exact, ok := p.base.(runtime.ExactIncarnationMetadataProvider)
	if !ok {
		return fmt.Errorf("%w: status provider base does not support exact-incarnation metadata", runtime.ErrExactIncarnationUnsupported)
	}
	return exact.SetMetaExact(name, expected, key, value)
}

// RemoveMetaExact preserves exact-incarnation metadata removal through the status wrapper.
func (p *statusProvider) RemoveMetaExact(name string, expected runtime.Incarnation, key string) error {
	exact, ok := p.base.(runtime.ExactIncarnationMetadataProvider)
	if !ok {
		return fmt.Errorf("%w: status provider base does not support exact-incarnation metadata", runtime.ErrExactIncarnationUnsupported)
	}
	return exact.RemoveMetaExact(name, expected, key)
}

// Relaunch forwards a warm-box agent relaunch to the wrapped provider when it
// supports one, so the reconciler's RelaunchProvider type-assert is not masked
// by the status wrapper. Not bounded — it is a mutation, not a status probe.
func (p *statusProvider) Relaunch(ctx context.Context, name string, cfg runtime.Config) error {
	if rp, ok := p.base.(runtime.RelaunchProvider); ok {
		return rp.Relaunch(ctx, name, cfg)
	}
	return runtime.ErrRelaunchUnsupported
}

// RelaunchExact preserves exact-incarnation warm relaunch through the status wrapper.
func (p *statusProvider) RelaunchExact(ctx context.Context, name string, expected runtime.Incarnation, cfg runtime.Config) error {
	// A relaunch may replace the existing runtime before Manager performs its
	// post-effect observation and final session commit. Refuse before dispatch
	// unless the concrete base can both attest the replacement and contain the
	// operation-token-stamped incarnation if either later step fails.
	if _, ok := p.base.(runtime.ExactIncarnationProvider); !ok {
		return fmt.Errorf("%w: status provider base cannot contain an exact relaunch", runtime.ErrExactIncarnationUnsupported)
	}
	if _, ok := p.base.(runtime.ExactIncarnationObserver); !ok {
		return fmt.Errorf("%w: status provider base cannot observe an exact relaunch", runtime.ErrExactIncarnationUnsupported)
	}
	exact, ok := p.base.(runtime.ExactIncarnationRelaunchProvider)
	if !ok {
		return fmt.Errorf("%w: status provider base does not support exact-incarnation relaunch", runtime.ErrExactIncarnationUnsupported)
	}
	return exact.RelaunchExact(ctx, name, expected, cfg)
}

func (p *statusProvider) Capabilities() runtime.ProviderCapabilities {
	return p.base.Capabilities()
}

func (p *statusProvider) Pending(name string) (*runtime.PendingInteraction, error) {
	ip, ok := p.base.(runtime.InteractionProvider)
	if !ok {
		return nil, nil
	}
	result := boundedStatusCall(p, struct {
		value *runtime.PendingInteraction
		err   error
	}{}, func() struct {
		value *runtime.PendingInteraction
		err   error
	} {
		value, err := ip.Pending(name)
		return struct {
			value *runtime.PendingInteraction
			err   error
		}{value: value, err: err}
	})
	return result.value, result.err
}

func (p *statusProvider) Respond(name string, response runtime.InteractionResponse) error {
	ip, ok := p.base.(runtime.InteractionProvider)
	if !ok {
		return runtime.ErrInteractionUnsupported
	}
	return ip.Respond(name, response)
}
