// Package auto provides a composite [runtime.Provider] that routes
// sessions to a default backend (typically tmux) or ACP based on
// per-session registration. Sessions are registered as ACP via
// [Provider.RouteACP] before [Provider.Start] is called. Unregistered
// sessions route to the default backend.
package auto

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gastownhall/gascity/internal/runtime"
)

// Provider routes session operations to a default or ACP backend
// based on per-session registration.
type Provider struct {
	defaultSP runtime.Provider
	acpSP     runtime.Provider

	mu     sync.RWMutex
	routes map[string]bool // true = ACP
}

var (
	_ runtime.Provider                               = (*Provider)(nil)
	_ runtime.DeadRuntimeSessionChecker              = (*Provider)(nil)
	_ runtime.InteractionProvider                    = (*Provider)(nil)
	_ runtime.InterruptBoundaryWaitProvider          = (*Provider)(nil)
	_ runtime.InterruptedTurnResetProvider           = (*Provider)(nil)
	_ runtime.ProjectHookIsolationCapabilityProvider = (*Provider)(nil)
	_ runtime.TransportCapabilityProvider            = (*Provider)(nil)
	_ runtime.RelaunchProvider                       = (*Provider)(nil)
	_ runtime.LivenessObserver                       = (*Provider)(nil)
	_ runtime.ExactIncarnationProvider               = (*Provider)(nil)
	_ runtime.ExactIncarnationMetadataProvider       = (*Provider)(nil)
	_ runtime.ExactIncarnationStartResolver          = (*Provider)(nil)
	_ runtime.ExactIncarnationObserver               = (*Provider)(nil)
	_ runtime.ExactIncarnationPeekProvider           = (*Provider)(nil)
)

// New creates a composite provider. defaultSP handles sessions not
// registered as ACP. acpSP handles sessions registered via RouteACP.
func New(defaultSP, acpSP runtime.Provider) *Provider {
	return &Provider{
		defaultSP: defaultSP,
		acpSP:     acpSP,
		routes:    make(map[string]bool),
	}
}

// RouteACP registers a session name to use the ACP backend.
// Must be called before Start for that session.
func (p *Provider) RouteACP(name string) {
	p.mu.Lock()
	p.routes[name] = true
	p.mu.Unlock()
}

// Unroute removes a session's routing entry. Called on Stop to avoid
// leaking entries for destroyed sessions.
func (p *Provider) Unroute(name string) {
	p.mu.Lock()
	delete(p.routes, name)
	p.mu.Unlock()
}

func (p *Provider) route(name string) runtime.Provider {
	p.mu.RLock()
	isACP := p.routes[name]
	p.mu.RUnlock()
	if isACP {
		return p.acpSP
	}
	return p.defaultSP
}

func (p *Provider) exactTarget(expected runtime.Incarnation) runtime.Provider {
	if expected.Transport == "acp" {
		return p.acpSP
	}
	return p.defaultSP
}

// launchTarget returns the backend that will perform a launch mutation and
// enforces any filesystem-isolation capability carried by the launch config.
// Resolve this from the actual name route, not only the configured transport:
// recovered routes can differ from the current template selection.
func (p *Provider) launchTarget(name string, cfg runtime.Config) (runtime.Provider, error) {
	p.mu.RLock()
	isACP := p.routes[name]
	p.mu.RUnlock()
	target := p.defaultSP
	transport := ""
	label := "default"
	if isACP {
		target = p.acpSP
		transport = "acp"
		label = "acp"
	}
	if cfg.ProjectHooksForbidden {
		provider, ok := target.(runtime.ProjectHookIsolationCapabilityProvider)
		if !ok || !provider.SupportsProjectHookIsolation(transport) {
			return nil, fmt.Errorf("session %q: routed %s provider cannot attest project hook isolation", name, label)
		}
	}
	return target, nil
}

// SupportsTransport reports whether this provider can route the requested
// session transport.
func (p *Provider) SupportsTransport(transport string) bool {
	if transport != "acp" {
		return true
	}
	if provider, ok := p.acpSP.(runtime.TransportCapabilityProvider); ok {
		return provider.SupportsTransport(transport)
	}
	return false
}

// SupportsProjectHookIsolation forwards the attestation query only to the
// backend a launch operation uses for the selected transport. ACP selections
// route to acpSP; every other supported transport routes to defaultSP.
func (p *Provider) SupportsProjectHookIsolation(transport string) bool {
	var target runtime.Provider
	if transport == "acp" {
		target = p.acpSP
	} else {
		target = p.defaultSP
	}
	provider, ok := target.(runtime.ProjectHookIsolationCapabilityProvider)
	return ok && provider.SupportsProjectHookIsolation(transport)
}

// DetectTransport reports the backend currently hosting the named session.
// It returns "acp" for ACP-backed sessions and "" for default or unknown.
func (p *Provider) DetectTransport(name string) string {
	if p.defaultSP.IsRunning(name) {
		return ""
	}
	if p.acpSP.IsRunning(name) {
		return "acp"
	}
	return ""
}

// Start delegates to the routed backend.
func (p *Provider) Start(ctx context.Context, name string, cfg runtime.Config) error {
	target, err := p.launchTarget(name, cfg)
	if err != nil {
		return err
	}
	return target.Start(ctx, name, cfg)
}

// ResolveExactIncarnationStart binds a fresh exact start to the backend named
// by the intended transport before RouteACP or any provider effect occurs.
func (p *Provider) ResolveExactIncarnationStart(name, transport string, cfg runtime.Config) (runtime.ExactIncarnationStart, error) {
	target := p.defaultSP
	label := "default"
	if transport == "acp" {
		target = p.acpSP
		label = "acp"
	}
	if cfg.ProjectHooksForbidden {
		isolation, ok := target.(runtime.ProjectHookIsolationCapabilityProvider)
		if !ok || !isolation.SupportsProjectHookIsolation(transport) {
			return nil, fmt.Errorf("session %q: routed %s provider cannot attest project hook isolation", name, label)
		}
	}
	if _, ok := target.(runtime.ExactIncarnationProvider); !ok {
		return nil, fmt.Errorf("%w: session %q routed %s provider cannot contain an exact fresh start", runtime.ErrExactIncarnationUnsupported, name, label)
	}
	if _, ok := target.(runtime.ExactIncarnationObserver); !ok {
		return nil, fmt.Errorf("%w: session %q routed %s provider cannot attest an exact fresh start", runtime.ErrExactIncarnationUnsupported, name, label)
	}
	start, err := runtime.ResolveExactIncarnationStart(target, name, transport, cfg)
	if err != nil {
		return nil, err
	}
	return start, nil
}

// Stop delegates to the routed backend and cleans up the route entry
// only on success. If the routed backend fails, tries the other backend
// to handle stale/missing route entries (e.g., after controller restart).
func (p *Provider) Stop(name string) error {
	primary := p.route(name)
	primaryLabel := "default"
	otherLabel := "acp"
	primaryRunning := primary.IsRunning(name)
	p.mu.RLock()
	primaryExplicitRoute := p.routes[name]
	p.mu.RUnlock()
	err := primary.Stop(name)
	if err == nil && primaryRunning {
		p.Unroute(name)
		return nil
	}
	// Fall through to the other backend in case the route is stale.
	var other runtime.Provider
	p.mu.RLock()
	if p.routes[name] {
		primaryLabel = "acp"
		otherLabel = "default"
		other = p.defaultSP
	} else {
		other = p.acpSP
	}
	p.mu.RUnlock()
	otherRunning := other.IsRunning(name)
	if err == nil {
		if primaryExplicitRoute {
			if otherRunning {
				return fmt.Errorf("%s backend: stop succeeded without liveness confirmation while %s backend still reports the session running", primaryLabel, otherLabel)
			}
			p.Unroute(name)
			return nil
		}
		err = fmt.Errorf("%w: %q", runtime.ErrSessionNotFound, name)
	}
	otherErr := other.Stop(name)
	if otherErr == nil {
		if !otherRunning {
			otherErr = fmt.Errorf("%w: %q", runtime.ErrSessionNotFound, name)
		} else if (primaryRunning || primaryExplicitRoute) && !runtime.IsSessionGone(err) {
			return fmt.Errorf("%s backend: %w", primaryLabel, err)
		}
	}
	mergedErr := runtime.MergeBackendStopErrors(
		runtime.BackendError{Label: primaryLabel, Err: err},
		runtime.BackendError{Label: otherLabel, Err: otherErr},
	)
	if mergedErr == nil {
		p.Unroute(name)
		return nil
	}
	return mergedErr
}

// Interrupt delegates to the routed backend.
func (p *Provider) Interrupt(name string) error {
	return p.route(name).Interrupt(name)
}

// IsRunning checks the routed backend first. If it reports not running,
// falls through to the other backend to handle route table inconsistencies.
func (p *Provider) IsRunning(name string) bool {
	if p.route(name).IsRunning(name) {
		return true
	}
	// Fall through: check the other backend in case routing is stale.
	p.mu.RLock()
	isACP := p.routes[name]
	p.mu.RUnlock()
	if isACP {
		return p.defaultSP.IsRunning(name)
	}
	return p.acpSP.IsRunning(name)
}

// IsDeadRuntimeSession checks both backends for a positive dead-artifact
// report because ListRunning is also merged across both backends.
func (p *Provider) IsDeadRuntimeSession(name string) (bool, error) {
	primary := p.route(name)
	if dead, err := providerDeadRuntimeSession(primary, name); dead || err != nil {
		return dead, err
	}
	p.mu.RLock()
	isACP := p.routes[name]
	p.mu.RUnlock()
	if isACP {
		return providerDeadRuntimeSession(p.defaultSP, name)
	}
	return providerDeadRuntimeSession(p.acpSP, name)
}

func providerDeadRuntimeSession(sp runtime.Provider, name string) (bool, error) {
	checker, ok := sp.(runtime.DeadRuntimeSessionChecker)
	if !ok {
		return false, nil
	}
	return checker.IsDeadRuntimeSession(name)
}

// IsAttached delegates to the routed backend.
func (p *Provider) IsAttached(name string) bool {
	return p.route(name).IsAttached(name)
}

// Attach delegates to the routed backend. ACP sessions return an error.
func (p *Provider) Attach(name string) error {
	p.mu.RLock()
	isACP := p.routes[name]
	p.mu.RUnlock()
	if isACP {
		return fmt.Errorf("agent %q uses ACP transport (no terminal to attach to)", name)
	}
	return p.defaultSP.Attach(name)
}

// ProcessAlive delegates to the routed backend.
func (p *Provider) ProcessAlive(name string, processNames []string) bool {
	return p.route(name).ProcessAlive(name, processNames)
}

// ObserveLiveness delegates to the routed backend through runtime.ObserveLiveness
// so the backend's native LivenessObserver fast-path is preserved — e.g. herdr's
// agent-status liveness. Without this, wrapping a LivenessObserver backend in an
// auto router would silently collapse it to the generic IsRunning+ProcessAlive
// fold (the fragile process-table walk), reintroducing the singleton
// restart-loop for any city that also routes some sessions to ACP.
func (p *Provider) ObserveLiveness(name string, processNames []string) runtime.Liveness {
	primary := runtime.ObserveLiveness(p.route(name), name, processNames)
	if primary.Running {
		return primary
	}
	// Fall through: check the other backend in case routing is stale
	// (e.g. after a controller restart clears the in-memory route table),
	// matching IsRunning's recovery so a live ACP singleton on a
	// herdr-default city is not misread as dead.
	p.mu.RLock()
	isACP := p.routes[name]
	p.mu.RUnlock()
	other := p.acpSP
	if isACP {
		other = p.defaultSP
	}
	return runtime.ObserveLiveness(other, name, processNames)
}

// Nudge delegates to the routed backend.
func (p *Provider) Nudge(name string, content []runtime.ContentBlock) error {
	return p.route(name).Nudge(name, content)
}

// WaitForIdle delegates to the routed backend when it supports explicit
// idle-boundary waiting.
func (p *Provider) WaitForIdle(ctx context.Context, name string, timeout time.Duration) error {
	if wp, ok := p.route(name).(runtime.IdleWaitProvider); ok {
		return wp.WaitForIdle(ctx, name, timeout)
	}
	return runtime.ErrInteractionUnsupported
}

// NudgeNow delegates to the routed backend when it supports immediate
// injection without an internal wait-idle step.
func (p *Provider) NudgeNow(name string, content []runtime.ContentBlock) error {
	if np, ok := p.route(name).(runtime.ImmediateNudgeProvider); ok {
		return np.NudgeNow(name, content)
	}
	return p.route(name).Nudge(name, content)
}

// ResetInterruptedTurn delegates to the routed backend when it supports
// provider-native interrupted-turn discard semantics.
func (p *Provider) ResetInterruptedTurn(ctx context.Context, name string) error {
	if rp, ok := p.route(name).(runtime.InterruptedTurnResetProvider); ok {
		return rp.ResetInterruptedTurn(ctx, name)
	}
	return runtime.ErrInteractionUnsupported
}

// Relaunch forwards a warm-box agent relaunch to the routed backend when it
// supports one, so the reconciler's RelaunchProvider type-assert is not masked
// by the auto router.
func (p *Provider) Relaunch(ctx context.Context, name string, cfg runtime.Config) error {
	target, err := p.launchTarget(name, cfg)
	if err != nil {
		return err
	}
	if rp, ok := target.(runtime.RelaunchProvider); ok {
		return rp.Relaunch(ctx, name, cfg)
	}
	return runtime.ErrRelaunchUnsupported
}

// WaitForInterruptBoundary delegates to the routed backend when it can confirm
// a provider-native interrupt boundary before the next turn is injected.
func (p *Provider) WaitForInterruptBoundary(ctx context.Context, name string, since time.Time, timeout time.Duration) error {
	if wp, ok := p.route(name).(runtime.InterruptBoundaryWaitProvider); ok {
		return wp.WaitForInterruptBoundary(ctx, name, since, timeout)
	}
	return runtime.ErrInteractionUnsupported
}

// Pending delegates to the routed backend when it supports structured
// interactions.
func (p *Provider) Pending(name string) (*runtime.PendingInteraction, error) {
	if ip, ok := p.route(name).(runtime.InteractionProvider); ok {
		return ip.Pending(name)
	}
	return nil, runtime.ErrInteractionUnsupported
}

// Respond delegates to the routed backend when it supports structured
// interactions.
func (p *Provider) Respond(name string, response runtime.InteractionResponse) error {
	if ip, ok := p.route(name).(runtime.InteractionProvider); ok {
		return ip.Respond(name, response)
	}
	return runtime.ErrInteractionUnsupported
}

// SetMeta delegates to the routed backend.
func (p *Provider) SetMeta(name, key, value string) error {
	return p.route(name).SetMeta(name, key, value)
}

// GetMeta delegates to the routed backend.
func (p *Provider) GetMeta(name, key string) (string, error) {
	return p.route(name).GetMeta(name, key)
}

// RemoveMeta delegates to the routed backend.
func (p *Provider) RemoveMeta(name, key string) error {
	return p.route(name).RemoveMeta(name, key)
}

// Peek delegates to the routed backend.
func (p *Provider) Peek(name string, lines int) (string, error) {
	return p.route(name).Peek(name, lines)
}

// ListRunning queries both backends and returns best-effort results plus a
// partial-list error when one backend fails.
func (p *Provider) ListRunning(prefix string) ([]string, error) {
	defaultList, dErr := p.defaultSP.ListRunning(prefix)
	acpList, aErr := p.acpSP.ListRunning(prefix)
	return runtime.MergeBackendListResults(
		runtime.BackendListResult{Label: "default", Names: defaultList, Err: dErr},
		runtime.BackendListResult{Label: "acp", Names: acpList, Err: aErr},
	)
}

// GetLastActivity delegates to the routed backend.
func (p *Provider) GetLastActivity(name string) (time.Time, error) {
	return p.route(name).GetLastActivity(name)
}

// ClearScrollback delegates to the routed backend.
func (p *Provider) ClearScrollback(name string) error {
	return p.route(name).ClearScrollback(name)
}

// CopyTo delegates to the routed backend.
func (p *Provider) CopyTo(name, src, relDst string) error {
	return p.route(name).CopyTo(name, src, relDst)
}

// SendKeys delegates to the routed backend.
func (p *Provider) SendKeys(name string, keys ...string) error {
	return p.route(name).SendKeys(name, keys...)
}

// RunLive delegates to the routed backend.
func (p *Provider) RunLive(name string, cfg runtime.Config) error {
	target, err := p.launchTarget(name, cfg)
	if err != nil {
		return err
	}
	return target.RunLive(name, cfg)
}

// RunLiveExact forwards an exact-incarnation live operation only to the
// backend selected by the captured transport, independent of mutable routes.
func (p *Provider) RunLiveExact(name string, expected runtime.Incarnation, cfg runtime.Config) error {
	target := p.exactTarget(expected)
	if cfg.ProjectHooksForbidden {
		isolation, ok := target.(runtime.ProjectHookIsolationCapabilityProvider)
		if !ok || !isolation.SupportsProjectHookIsolation(expected.Transport) {
			return fmt.Errorf("%w: session %q exact provider cannot attest project hook isolation", runtime.ErrExactIncarnationUnsupported, name)
		}
	}
	exact, ok := target.(runtime.ExactIncarnationProvider)
	if !ok {
		return fmt.Errorf("%w: session %q routed provider does not support exact-incarnation live operations", runtime.ErrExactIncarnationUnsupported, name)
	}
	return exact.RunLiveExact(name, expected, cfg)
}

// NudgeExact forwards exact-incarnation delivery only to the routed backend.
func (p *Provider) NudgeExact(name string, expected runtime.Incarnation, content []runtime.ContentBlock) error {
	exact, ok := p.exactTarget(expected).(runtime.ExactIncarnationProvider)
	if !ok {
		return fmt.Errorf("%w: session %q routed provider does not support exact-incarnation nudges", runtime.ErrExactIncarnationUnsupported, name)
	}
	return exact.NudgeExact(name, expected, content)
}

// StopExact forwards exact-incarnation teardown only to the routed backend.
// It does not use Stop's cross-backend fallback or clear a name route that may
// already belong to a replacement incarnation.
func (p *Provider) StopExact(name string, expected runtime.Incarnation) error {
	exact, ok := p.exactTarget(expected).(runtime.ExactIncarnationProvider)
	if !ok {
		return fmt.Errorf("%w: session %q routed provider does not support exact-incarnation stops", runtime.ErrExactIncarnationUnsupported, name)
	}
	return exact.StopExact(name, expected)
}

// ObserveExact forwards exact-incarnation observation only to the routed backend.
func (p *Provider) ObserveExact(name string, expected runtime.Incarnation, processNames []string) (runtime.IncarnationObservation, error) {
	observer, ok := p.exactTarget(expected).(runtime.ExactIncarnationObserver)
	if !ok {
		return runtime.IncarnationObservation{}, fmt.Errorf("%w: session %q routed provider does not support exact-incarnation observation", runtime.ErrExactIncarnationUnsupported, name)
	}
	return observer.ObserveExact(name, expected, processNames)
}

// PeekExact forwards exact-incarnation output reads only to the routed backend.
func (p *Provider) PeekExact(name string, expected runtime.Incarnation, lines int) (string, error) {
	peeker, ok := p.exactTarget(expected).(runtime.ExactIncarnationPeekProvider)
	if !ok {
		return "", fmt.Errorf("%w: session %q routed provider does not support exact-incarnation peek", runtime.ErrExactIncarnationUnsupported, name)
	}
	return peeker.PeekExact(name, expected, lines)
}

// SetMetaExact forwards exact-incarnation metadata only to the routed backend.
func (p *Provider) SetMetaExact(name string, expected runtime.Incarnation, key, value string) error {
	exact, ok := p.exactTarget(expected).(runtime.ExactIncarnationMetadataProvider)
	if !ok {
		return fmt.Errorf("%w: session %q routed provider does not support exact-incarnation metadata", runtime.ErrExactIncarnationUnsupported, name)
	}
	return exact.SetMetaExact(name, expected, key, value)
}

// RemoveMetaExact forwards exact-incarnation metadata removal only to the routed backend.
func (p *Provider) RemoveMetaExact(name string, expected runtime.Incarnation, key string) error {
	exact, ok := p.exactTarget(expected).(runtime.ExactIncarnationMetadataProvider)
	if !ok {
		return fmt.Errorf("%w: session %q routed provider does not support exact-incarnation metadata", runtime.ErrExactIncarnationUnsupported, name)
	}
	return exact.RemoveMetaExact(name, expected, key)
}

// Capabilities returns the intersection of both backends' capabilities.
// A capability is reported only if both default and ACP support it.
func (p *Provider) Capabilities() runtime.ProviderCapabilities {
	dc := p.defaultSP.Capabilities()
	ac := p.acpSP.Capabilities()
	return runtime.ProviderCapabilities{
		CanReportAttachment: dc.CanReportAttachment && ac.CanReportAttachment,
		CanReportActivity:   dc.CanReportActivity && ac.CanReportActivity,
	}
}

// SleepCapability reports idle sleep capability for the routed backend.
func (p *Provider) SleepCapability(name string) runtime.SessionSleepCapability {
	if scp, ok := p.route(name).(runtime.SleepCapabilityProvider); ok {
		return scp.SleepCapability(name)
	}
	return runtime.SessionSleepCapabilityDisabled
}
