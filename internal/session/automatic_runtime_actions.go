package session

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
)

// RuntimeOperationTokenEnv binds a newly created or relaunched runtime to the
// exact conditional-mutation operation that authorized its provider effect.
const RuntimeOperationTokenEnv = "GC_RUNTIME_OPERATION_TOKEN"

var (
	// ErrAutomaticRuntimeDecisionInvalid reports an incomplete automatic action
	// token. Automatic actions never infer a missing revision or incarnation.
	ErrAutomaticRuntimeDecisionInvalid = errors.New("automatic runtime decision is invalid")
	// ErrAutomaticRuntimeIncarnationMismatch reports that provider metadata does
	// not attest the exact persisted runtime selected by the decision snapshot.
	ErrAutomaticRuntimeIncarnationMismatch = errors.New("automatic runtime incarnation mismatch")
	// ErrAutomaticRuntimeEffectUncompensated reports a successful provider effect
	// that could not be contained after its final session commit was refused.
	ErrAutomaticRuntimeEffectUncompensated = errors.New("automatic runtime effect could not be compensated")
	// ErrAutomaticRuntimeExactEffectUnsupported reports a provider whose
	// name-only effect API cannot atomically fence runtime replacement.
	ErrAutomaticRuntimeExactEffectUnsupported = errors.New("automatic exact-incarnation provider effect unsupported")
	// ErrAutomaticRuntimeRecoveryBlocked reports a persisted operation lease left
	// by a dead or disconnected owner. Name-only providers cannot prove a remote
	// request is quiescent, so automatic takeover is deliberately forbidden.
	ErrAutomaticRuntimeRecoveryBlocked = errors.New("automatic runtime recovery blocked by held mutation lease")
)

// AutomaticRuntimePolicyValidator re-resolves current configuration policy
// from the freshly leased persisted row. Worker supplies the conservative
// configured-agent resolver; session treats the result as an opaque predicate.
type AutomaticRuntimePolicyValidator func(Info, PersistedResponse, bool) error

// AutomaticRuntimeDecision is the immutable, same-read token for one
// reconciler provider effect. ExpectedRevision zero is valid only when
// RevisionCaptured is true.
type AutomaticRuntimeDecision struct {
	Captured              Info
	ExpectedRevision      int64
	RevisionCaptured      bool
	Witness               LiveBoundaryWitness
	ProjectHooksForbidden bool
	ValidatePolicy        AutomaticRuntimePolicyValidator
}

// AutomaticRuntimeCommit is the exact persisted session snapshot produced by
// the final lease commit for one reconciler provider action. Callers chain this
// revision into any later automatic mutation instead of reusing the decision's
// pre-effect revision.
type AutomaticRuntimeCommit struct {
	Info      Info
	Persisted PersistedResponse
}

// StartPreparedRuntimeOnlyForReconciler starts an already-persisted automatic
// session while one exact revision lease covers validation, provider effect,
// and final commit. A lost final commit contains only the runtime stamped with
// this operation's nonce.
func (m *Manager) StartPreparedRuntimeOnlyForReconciler(
	ctx context.Context,
	id string,
	resumeCommand string,
	hints runtime.Config,
	decision AutomaticRuntimeDecision,
) (PreparedRuntimeStartResult, error) {
	var result PreparedRuntimeStartResult
	err := m.withAutomaticRuntimeDecision(id, "start-prepared", decision, false, func(lease *ConditionalMutationLease, info Info, _ PersistedResponse) error {
		if err := validateAutomaticRuntimeConfigPolicy(hints, decision); err != nil {
			return err
		}
		info = automaticRuntimeInfoWithCapturedTransport(info)
		sessionName := strings.TrimSpace(info.SessionName)
		exactProvider, err := m.automaticExactIncarnationProvider()
		if err != nil {
			return err
		}
		exactObserver, err := m.automaticExactIncarnationObserver()
		if err != nil {
			return err
		}
		observation, observeErr := exactObserver.ObserveExact(sessionName, automaticRuntimeIncarnation(info, ""), hints.ProcessNames)
		runtimeExists := true
		if observeErr != nil {
			switch {
			case runtime.IsSessionGone(observeErr):
				runtimeExists = false
			case errors.Is(observeErr, runtime.ErrExactIncarnationUnsupported):
				return automaticExactEffectUnsupported(observeErr)
			default:
				return observeErr
			}
		}
		if runtimeExists && observation.Running && observation.Alive {
			if err := lease.Commit(beads.UpdateOpts{}); err != nil {
				return err
			}
			result.Commit = automaticRuntimeCommitFromLease(lease)
			m.publishAutomaticRuntimeRoute(info, sessionName)
			return nil
		}

		command := strings.TrimSpace(resumeCommand)
		if command == "" {
			return fmt.Errorf("%w: empty prepared resume command", ErrAutomaticRuntimeDecisionInvalid)
		}
		cfg, err := automaticRuntimeConfig(info, command, hints, lease.Nonce())
		if err != nil {
			return err
		}
		transport := info.Transport
		exactStart, err := runtime.ResolveExactIncarnationStart(m.sp, sessionName, transport, cfg)
		if err != nil {
			if errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
				return automaticExactEffectUnsupported(err)
			}
			return err
		}
		finalized, err := runtime.FinalizeProjectHookIsolatedConfig(cfg)
		if err != nil {
			return err
		}
		if err := runtime.MaterializeProjectHookIsolatedConfigRoots(finalized); err != nil {
			return err
		}

		if runtimeExists {
			recycleBegin := time.Now()
			if err := exactProvider.StopExact(sessionName, automaticRuntimeIncarnation(info, "")); err != nil && !runtime.IsSessionGone(err) {
				if errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
					return automaticExactEffectUnsupported(err)
				}
				if errors.Is(err, runtime.ErrIncarnationMismatch) {
					return err
				}
				return preserveAutomaticRuntimeLease(
					lease,
					ErrAutomaticRuntimeEffectUncompensated,
					fmt.Errorf("recycling exact runtime %q: %w", sessionName, err),
				)
			}
			result.RecycleDuration = time.Since(recycleBegin)
			result.Recycled = true
		}
		result.Attempted = true
		desired := automaticRuntimeIncarnation(info, lease.Nonce())
		if err := exactStart(ctx, sessionName, desired, finalized); err != nil {
			if errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
				return automaticExactEffectUnsupported(err)
			}
			if errors.Is(err, runtime.ErrIncarnationMismatch) {
				return err
			}
			startErr := fmt.Errorf("starting exact prepared runtime %q: %w", sessionName, err)
			_, attestErr := exactObserver.ObserveExact(sessionName, desired, hints.ProcessNames)
			if attestErr == nil {
				containErr := m.containAutomaticRuntime(info, lease.Nonce())
				if containErr == nil {
					return startErr
				}
				return preserveAutomaticRuntimeLease(lease, startErr, containErr)
			}
			return preserveAutomaticRuntimeLease(lease, startErr, ErrAutomaticRuntimeEffectUncompensated, attestErr)
		}
		if _, err := exactObserver.ObserveExact(sessionName, desired, hints.ProcessNames); err != nil {
			if containErr := m.containAutomaticRuntime(info, lease.Nonce()); containErr != nil {
				return preserveAutomaticRuntimeLease(lease, ErrAutomaticRuntimeEffectUncompensated, err, containErr)
			}
			return err
		}
		if err := lease.Commit(beads.UpdateOpts{}); err != nil {
			if containErr := m.containAutomaticRuntime(info, lease.Nonce()); containErr != nil {
				return preserveAutomaticRuntimeLease(lease, err, containErr)
			}
			return err
		}
		result.Commit = automaticRuntimeCommitFromLease(lease)
		m.publishAutomaticRuntimeRoute(info, sessionName)
		return nil
	})
	return result, normalizeAutomaticRuntimeLeaseError(err)
}

// RunLiveForReconciler applies one automatic live configuration only to the
// exact persisted runtime incarnation selected by decision. If the provider
// succeeds but the final session commit loses, the exact incarnation is
// stopped; a runtime that can no longer be attested is never guessed at.
func (m *Manager) RunLiveForReconciler(
	ctx context.Context,
	id string,
	cfg runtime.Config,
	decision AutomaticRuntimeDecision,
) (AutomaticRuntimeCommit, error) {
	var commit AutomaticRuntimeCommit
	if err := ctx.Err(); err != nil {
		return AutomaticRuntimeCommit{}, err
	}
	err := m.withAutomaticRuntimeDecision(id, "run-live", decision, false, func(lease *ConditionalMutationLease, info Info, _ PersistedResponse) error {
		if err := validateAutomaticRuntimeConfigPolicy(cfg, decision); err != nil {
			return err
		}
		info = automaticRuntimeInfoWithCapturedTransport(info)
		exactProvider, err := m.automaticExactIncarnationProvider()
		if err != nil {
			return err
		}
		cfg, err = automaticExistingRuntimeConfig(info, cfg)
		if err != nil {
			return err
		}
		finalized, err := runtime.FinalizeProjectHookIsolatedConfig(cfg)
		if err != nil {
			return err
		}
		if err := runtime.MaterializeProjectHookIsolatedConfigRoots(finalized); err != nil {
			return err
		}
		if err := exactProvider.RunLiveExact(strings.TrimSpace(info.SessionName), automaticRuntimeIncarnation(info, ""), finalized); err != nil {
			if errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
				return automaticExactEffectUnsupported(err)
			}
			if errors.Is(err, runtime.ErrIncarnationMismatch) {
				return err
			}
			if runtime.IsSessionGone(err) {
				return err
			}
			return preserveAutomaticRuntimeLease(lease, ErrAutomaticRuntimeEffectUncompensated, fmt.Errorf("running live config: %w", err))
		}
		if err := lease.Commit(beads.UpdateOpts{}); err != nil {
			// RunLive mutates the preexisting incarnation and has no operation
			// token naming a runtime created by this lease. Once row authority
			// drifts, stopping that incarnation would be a second unauthorized
			// effect; preserve the lease for explicit recovery instead.
			return preserveAutomaticRuntimeLease(lease, err, ErrAutomaticRuntimeEffectUncompensated)
		}
		commit = automaticRuntimeCommitFromLease(lease)
		return nil
	})
	return commit, normalizeAutomaticRuntimeLeaseError(err)
}

// RelaunchForReconciler warm-relaunches only the exact provider incarnation
// selected by decision. The replacement runtime is stamped with the lease
// nonce so a lost final commit can contain that runtime and no other.
func (m *Manager) RelaunchForReconciler(
	ctx context.Context,
	id string,
	cfg runtime.Config,
	decision AutomaticRuntimeDecision,
) (AutomaticRuntimeCommit, error) {
	var commit AutomaticRuntimeCommit
	if err := ctx.Err(); err != nil {
		return AutomaticRuntimeCommit{}, err
	}
	err := m.withAutomaticRuntimeDecision(id, "relaunch", decision, false, func(lease *ConditionalMutationLease, info Info, _ PersistedResponse) error {
		if err := validateAutomaticRuntimeConfigPolicy(cfg, decision); err != nil {
			return err
		}
		info = automaticRuntimeInfoWithCapturedTransport(info)
		if _, err := m.automaticExactIncarnationProvider(); err != nil {
			return err
		}
		exactObserver, err := m.automaticExactIncarnationObserver()
		if err != nil {
			return err
		}
		relauncher, ok := m.sp.(runtime.ExactIncarnationRelaunchProvider)
		if !ok {
			return ErrAutomaticRuntimeExactEffectUnsupported
		}
		cfg, err = automaticExistingRuntimeConfig(info, cfg)
		if err != nil {
			return err
		}
		if cfg.Env == nil {
			cfg.Env = make(map[string]string)
		}
		cfg.Env[RuntimeOperationTokenEnv] = lease.Nonce()
		finalized, err := runtime.FinalizeProjectHookIsolatedConfig(cfg)
		if err != nil {
			return err
		}
		if err := runtime.MaterializeProjectHookIsolatedConfigRoots(finalized); err != nil {
			return err
		}
		name := strings.TrimSpace(info.SessionName)
		desired := automaticRuntimeIncarnation(info, lease.Nonce())
		if err := relauncher.RelaunchExact(ctx, name, automaticRuntimeIncarnation(info, ""), finalized); err != nil {
			if errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
				return automaticExactEffectUnsupported(err)
			}
			if errors.Is(err, runtime.ErrIncarnationMismatch) {
				return err
			}
			if runtime.IsSessionGone(err) {
				return err
			}
			relaunchErr := fmt.Errorf("relaunching exact runtime %q: %w", name, err)
			_, attestErr := exactObserver.ObserveExact(name, desired, cfg.ProcessNames)
			if attestErr == nil {
				containErr := m.containAutomaticRuntime(info, lease.Nonce())
				if containErr == nil {
					return relaunchErr
				}
				return preserveAutomaticRuntimeLease(lease, relaunchErr, containErr)
			}
			return preserveAutomaticRuntimeLease(lease, relaunchErr, ErrAutomaticRuntimeEffectUncompensated, attestErr)
		}
		if _, err := exactObserver.ObserveExact(name, desired, cfg.ProcessNames); err != nil {
			if containErr := m.containAutomaticRuntime(info, lease.Nonce()); containErr != nil {
				return preserveAutomaticRuntimeLease(lease, ErrAutomaticRuntimeEffectUncompensated, err, containErr)
			}
			return err
		}
		if err := lease.Commit(beads.UpdateOpts{}); err != nil {
			if containErr := m.containAutomaticRuntime(info, lease.Nonce()); containErr != nil {
				return preserveAutomaticRuntimeLease(lease, err, containErr)
			}
			return err
		}
		commit = automaticRuntimeCommitFromLease(lease)
		return nil
	})
	return commit, normalizeAutomaticRuntimeLeaseError(err)
}

// NudgeForReconciler persists markerPatch and delivers one live-only nudge
// under the same automatic decision lease. The marker is write-ahead: provider
// delivery never occurs before its successful exact-revision patch.
func (m *Manager) NudgeForReconciler(
	ctx context.Context,
	id string,
	message string,
	decision AutomaticRuntimeDecision,
	markerPatch map[string]string,
) (bool, AutomaticRuntimeCommit, error) {
	var commit AutomaticRuntimeCommit
	if err := ctx.Err(); err != nil {
		return false, AutomaticRuntimeCommit{}, err
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return false, AutomaticRuntimeCommit{}, fmt.Errorf("%w: empty automatic nudge", ErrAutomaticRuntimeDecisionInvalid)
	}
	delivered := false
	err := m.withAutomaticRuntimeDecision(id, "nudge", decision, false, func(lease *ConditionalMutationLease, info Info, persisted PersistedResponse) error {
		info = automaticRuntimeInfoWithCapturedTransport(info)
		exactProvider, err := m.automaticExactIncarnationProvider()
		if err != nil {
			return err
		}
		markerRollback := automaticRuntimeMetadataValues(persisted.Metadata, markerPatch)
		if len(markerPatch) > 0 {
			if err := lease.Patch(beads.UpdateOpts{Metadata: cloneAutomaticRuntimeMetadata(markerPatch)}); err != nil {
				return err
			}
		}
		if err := exactProvider.NudgeExact(strings.TrimSpace(info.SessionName), automaticRuntimeIncarnation(info, ""), runtime.TextContent(message)); err != nil {
			if errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
				return rollbackAutomaticRuntimeNudgeMarker(lease, markerRollback, automaticExactEffectUnsupported(err))
			}
			if errors.Is(err, runtime.ErrIncarnationMismatch) {
				return rollbackAutomaticRuntimeNudgeMarker(lease, markerRollback, err)
			}
			if runtime.IsSessionGone(err) {
				return rollbackAutomaticRuntimeNudgeMarker(lease, markerRollback, err)
			}
			return preserveAutomaticRuntimeLease(lease, ErrAutomaticRuntimeEffectUncompensated, fmt.Errorf("nudging exact runtime: %w", err))
		}
		delivered = true
		if err := lease.Commit(beads.UpdateOpts{}); err != nil {
			return preserveAutomaticRuntimeLease(lease, err, ErrAutomaticRuntimeEffectUncompensated)
		}
		commit = automaticRuntimeCommitFromLease(lease)
		return nil
	})
	return delivered, commit, normalizeAutomaticRuntimeLeaseError(err)
}

// SetRuntimeMetadataForReconciler mutates one provider metadata key only on
// the exact runtime incarnation selected by decision.
func (m *Manager) SetRuntimeMetadataForReconciler(
	ctx context.Context,
	id string,
	key string,
	value string,
	decision AutomaticRuntimeDecision,
) (AutomaticRuntimeCommit, error) {
	return m.mutateRuntimeMetadataForReconciler(ctx, id, key, value, false, decision)
}

// RemoveRuntimeMetadataForReconciler removes one provider metadata key only
// from the exact runtime incarnation selected by decision.
func (m *Manager) RemoveRuntimeMetadataForReconciler(
	ctx context.Context,
	id string,
	key string,
	decision AutomaticRuntimeDecision,
) (AutomaticRuntimeCommit, error) {
	return m.mutateRuntimeMetadataForReconciler(ctx, id, key, "", true, decision)
}

func (m *Manager) mutateRuntimeMetadataForReconciler(
	ctx context.Context,
	id string,
	key string,
	value string,
	remove bool,
	decision AutomaticRuntimeDecision,
) (AutomaticRuntimeCommit, error) {
	var commit AutomaticRuntimeCommit
	if err := ctx.Err(); err != nil {
		return AutomaticRuntimeCommit{}, err
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return AutomaticRuntimeCommit{}, fmt.Errorf("%w: empty provider metadata key", ErrAutomaticRuntimeDecisionInvalid)
	}
	action := "set-runtime-metadata"
	if remove {
		action = "remove-runtime-metadata"
	}
	err := m.withAutomaticRuntimeDecision(id, action, decision, true, func(lease *ConditionalMutationLease, info Info, _ PersistedResponse) error {
		info = automaticRuntimeInfoWithCapturedTransport(info)
		provider, ok := m.sp.(runtime.ExactIncarnationMetadataProvider)
		if !ok {
			return ErrAutomaticRuntimeExactEffectUnsupported
		}
		name := strings.TrimSpace(info.SessionName)
		expected := automaticRuntimeIncarnation(info, "")
		var effectErr error
		if remove {
			effectErr = provider.RemoveMetaExact(name, expected, key)
		} else {
			effectErr = provider.SetMetaExact(name, expected, key, value)
		}
		if effectErr != nil {
			if errors.Is(effectErr, runtime.ErrExactIncarnationUnsupported) {
				return automaticExactEffectUnsupported(effectErr)
			}
			if errors.Is(effectErr, runtime.ErrIncarnationMismatch) {
				return effectErr
			}
			if runtime.IsSessionGone(effectErr) {
				return effectErr
			}
			return preserveAutomaticRuntimeLease(lease, ErrAutomaticRuntimeEffectUncompensated, fmt.Errorf("mutating exact runtime metadata %q: %w", key, effectErr))
		}
		if err := lease.Commit(beads.UpdateOpts{}); err != nil {
			return preserveAutomaticRuntimeLease(lease, ErrAutomaticRuntimeEffectUncompensated, err)
		}
		commit = automaticRuntimeCommitFromLease(lease)
		return nil
	})
	return commit, normalizeAutomaticRuntimeLeaseError(err)
}

// ObserveRuntimeForReconciler performs a pure provider read bracketed by exact
// persisted revision and policy checks. It intentionally does not acquire a
// persisted lease, avoiding revision churn on every controller tick.
func (m *Manager) ObserveRuntimeForReconciler(
	id string,
	processNames []string,
	decision AutomaticRuntimeDecision,
	lifecycle bool,
) (Info, RuntimeObservation, error) {
	info, err := m.loadAutomaticRuntimeDecision(id, decision, lifecycle)
	if err != nil {
		return Info{}, RuntimeObservation{}, normalizeAutomaticRuntimeLeaseError(err)
	}
	info = automaticRuntimeInfoWithCapturedTransport(info)
	exactObserver, err := m.automaticExactIncarnationObserver()
	if err != nil {
		return Info{}, RuntimeObservation{}, err
	}
	exactObservation, err := exactObserver.ObserveExact(strings.TrimSpace(info.SessionName), automaticRuntimeIncarnation(info, ""), processNames)
	observation := RuntimeObservation{SessionName: info.SessionName}
	if err != nil {
		if !runtime.IsSessionGone(err) {
			if errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
				err = automaticExactEffectUnsupported(err)
			}
			return Info{}, RuntimeObservation{}, err
		}
	} else {
		observation.Running = exactObservation.Running
		observation.Alive = exactObservation.Alive
		observation.Attached = exactObservation.Attached
		observation.LastActive = exactObservation.LastActivity
	}
	if _, err := m.loadAutomaticRuntimeDecision(id, decision, lifecycle); err != nil {
		return Info{}, RuntimeObservation{}, normalizeAutomaticRuntimeLeaseError(err)
	}
	return info, observation, nil
}

// PeekForReconciler performs a pure exact-incarnation provider read and
// discards its output if the original persisted decision changed meanwhile.
func (m *Manager) PeekForReconciler(id string, lines int, decision AutomaticRuntimeDecision) (string, error) {
	info, err := m.loadAutomaticRuntimeDecision(id, decision, false)
	if err != nil {
		return "", normalizeAutomaticRuntimeLeaseError(err)
	}
	info = automaticRuntimeInfoWithCapturedTransport(info)
	peeker, ok := m.sp.(runtime.ExactIncarnationPeekProvider)
	if !ok {
		return "", automaticExactEffectUnsupported(runtime.ErrExactIncarnationUnsupported)
	}
	output, err := peeker.PeekExact(strings.TrimSpace(info.SessionName), automaticRuntimeIncarnation(info, ""), lines)
	if err != nil {
		if errors.Is(err, runtime.ErrExactIncarnationUnsupported) {
			return "", automaticExactEffectUnsupported(err)
		}
		return "", err
	}
	if _, err := m.loadAutomaticRuntimeDecision(id, decision, false); err != nil {
		return "", normalizeAutomaticRuntimeLeaseError(err)
	}
	return output, nil
}

func (m *Manager) loadAutomaticRuntimeDecision(id string, decision AutomaticRuntimeDecision, lifecycle bool) (Info, error) {
	if err := validateAutomaticRuntimeDecision(id, decision, lifecycle); err != nil {
		return Info{}, err
	}
	info, persisted, err := m.PersistedStore().GetPersistedResponse(id)
	if err != nil {
		return Info{}, err
	}
	if persisted.Revision != decision.ExpectedRevision {
		return Info{}, conditionalMutationRevisionLost(id, decision.ExpectedRevision, persisted.Revision)
	}
	if err := RequireNoConditionalMutationLease(persisted); err != nil {
		return Info{}, err
	}
	if err := validateAutomaticRuntimeSnapshot(info, decision, lifecycle); err != nil {
		return Info{}, err
	}
	if err := decision.ValidatePolicy(info, persisted, decision.ProjectHooksForbidden); err != nil {
		return Info{}, err
	}
	return info, nil
}

func (m *Manager) withAutomaticRuntimeDecision(
	id string,
	action string,
	decision AutomaticRuntimeDecision,
	lifecycle bool,
	fn func(*ConditionalMutationLease, Info, PersistedResponse) error,
) error {
	if m == nil || m.store == nil || m.sp == nil {
		return fmt.Errorf("%w: missing manager store or provider", ErrAutomaticRuntimeDecisionInvalid)
	}
	if fn == nil {
		return fmt.Errorf("%w: nil provider-effect callback", ErrAutomaticRuntimeDecisionInvalid)
	}
	if err := validateAutomaticRuntimeDecision(id, decision, lifecycle); err != nil {
		return err
	}
	front := NewStore(beads.SessionStore{Store: m.store})
	return front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        id,
		ExpectedRevision: decision.ExpectedRevision,
		Action:           action,
		Predicates: []ConditionalMutationPredicate{
			{
				Identity: "automatic-original-authority-identity-state",
				Validate: func(current Info, _ PersistedResponse) error {
					return validateAutomaticRuntimeSnapshot(current, decision, lifecycle)
				},
			},
			{
				Identity: "automatic-current-config-policy",
				Validate: func(current Info, persisted PersistedResponse) error {
					return decision.ValidatePolicy(current, persisted, decision.ProjectHooksForbidden)
				},
			},
		},
	}, func(lease *ConditionalMutationLease) error {
		info, persisted := lease.Current()
		return fn(lease, info, persisted)
	})
}

func validateAutomaticRuntimeDecision(id string, decision AutomaticRuntimeDecision, lifecycle bool) error {
	id = strings.TrimSpace(id)
	if id == "" || strings.TrimSpace(decision.Captured.ID) != id {
		return fmt.Errorf("%w: captured session id %q does not match %q", ErrAutomaticRuntimeDecisionInvalid, decision.Captured.ID, id)
	}
	if !decision.RevisionCaptured {
		return fmt.Errorf("%w: session %q revision presence was not captured", ErrAutomaticRuntimeDecisionInvalid, id)
	}
	if decision.ValidatePolicy == nil {
		return fmt.Errorf("%w: session %q has no current-config policy validator", ErrAutomaticRuntimeDecisionInvalid, id)
	}
	if strings.TrimSpace(decision.Captured.InstanceToken) == "" {
		return fmt.Errorf("%w: session %q has no captured instance token", ErrAutomaticRuntimeDecisionInvalid, id)
	}
	if _, err := automaticRuntimeEpoch(decision.Captured); err != nil {
		return err
	}
	return validateAutomaticRuntimeSnapshot(decision.Captured, decision, lifecycle)
}

func validateAutomaticRuntimeSnapshot(current Info, decision AutomaticRuntimeDecision, lifecycle bool) error {
	var err error
	if lifecycle {
		err = ValidateReconcilerLifecycleInfo(current, decision.Witness)
	} else {
		err = ValidateReconcilerWakeInfo(current, decision.Witness)
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(automaticRuntimeComparableInfo(current), automaticRuntimeComparableInfo(decision.Captured)) {
		return fmt.Errorf("%w: session %q authority, identity, state, or incarnation changed", ErrAutomaticRuntimeDecisionInvalid, current.ID)
	}
	return nil
}

func (m *Manager) containAutomaticRuntime(info Info, operationToken string) error {
	exactProvider, providerErr := m.automaticExactIncarnationProvider()
	if providerErr != nil {
		return errors.Join(ErrAutomaticRuntimeEffectUncompensated, providerErr)
	}
	if err := exactProvider.StopExact(strings.TrimSpace(info.SessionName), automaticRuntimeIncarnation(info, operationToken)); err != nil && !runtime.IsSessionGone(err) {
		return errors.Join(ErrAutomaticRuntimeEffectUncompensated, fmt.Errorf("stopping exact stamped runtime: %w", err))
	}
	return nil
}

func (m *Manager) automaticExactIncarnationProvider() (runtime.ExactIncarnationProvider, error) {
	provider, ok := m.sp.(runtime.ExactIncarnationProvider)
	if !ok {
		return nil, automaticExactEffectUnsupported(runtime.ErrExactIncarnationUnsupported)
	}
	return provider, nil
}

func (m *Manager) automaticExactIncarnationObserver() (runtime.ExactIncarnationObserver, error) {
	observer, ok := m.sp.(runtime.ExactIncarnationObserver)
	if !ok {
		return nil, automaticExactEffectUnsupported(runtime.ErrExactIncarnationUnsupported)
	}
	return observer, nil
}

func (m *Manager) publishAutomaticRuntimeRoute(info Info, sessionName string) {
	router, ok := m.sp.(acpRouteRegistrar)
	if !ok {
		return
	}
	if strings.TrimSpace(info.Transport) == "acp" {
		router.RouteACP(sessionName)
		return
	}
	router.Unroute(sessionName)
}

func automaticRuntimeInfoWithCapturedTransport(info Info) Info {
	// Strict automatic actions select only from the same persisted snapshot as
	// their authority decision. In particular, do not call DetectTransport:
	// name-only liveness can observe a replacement backend and silently adopt
	// it after the session decision was captured. Empty is the persisted
	// provider's default backend; persisted MCP evidence selects ACP.
	info.Transport = normalizeTransport(info.Provider, info.TransportMetadata)
	if info.Transport == "" && (strings.TrimSpace(info.MCPIdentity) != "" || strings.TrimSpace(info.MCPServersSnapshot) != "") {
		info.Transport = "acp"
	}
	return info
}

func automaticExactEffectUnsupported(err error) error {
	return errors.Join(ErrAutomaticRuntimeExactEffectUnsupported, err)
}

func automaticRuntimeIncarnation(info Info, operationToken string) runtime.Incarnation {
	epoch, _ := automaticRuntimeEpoch(info)
	return runtime.Incarnation{
		SessionID:      strings.TrimSpace(info.ID),
		InstanceToken:  strings.TrimSpace(info.InstanceToken),
		Epoch:          epoch,
		OperationToken: strings.TrimSpace(operationToken),
		Transport:      strings.TrimSpace(info.Transport),
	}
}

func automaticRuntimeCommitFromLease(lease *ConditionalMutationLease) AutomaticRuntimeCommit {
	info, persisted := lease.Current()
	return AutomaticRuntimeCommit{Info: info, Persisted: persisted}
}

func validateAutomaticRuntimeConfigPolicy(cfg runtime.Config, decision AutomaticRuntimeDecision) error {
	if cfg.ProjectHooksForbidden != decision.ProjectHooksForbidden {
		return fmt.Errorf(
			"%w: runtime config project-hooks strict=%v differs from captured strict=%v",
			ErrAutomaticRuntimeDecisionInvalid,
			cfg.ProjectHooksForbidden,
			decision.ProjectHooksForbidden,
		)
	}
	return nil
}

func automaticRuntimeConfig(info Info, command string, hints runtime.Config, operationToken string) (runtime.Config, error) {
	generation, err := strconv.Atoi(strings.TrimSpace(info.Generation))
	if err != nil || generation <= 0 {
		return runtime.Config{}, fmt.Errorf("%w: session %q generation %q is not a positive integer", ErrAutomaticRuntimeDecisionInvalid, info.ID, info.Generation)
	}
	continuation := DefaultContinuationEpoch
	if raw := strings.TrimSpace(info.ContinuationEpoch); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed <= 0 {
			return runtime.Config{}, fmt.Errorf("%w: session %q continuation epoch %q is not a positive integer", ErrAutomaticRuntimeDecisionInvalid, info.ID, info.ContinuationEpoch)
		}
		continuation = parsed
	}
	cfg := hints
	cfg.Command = command
	if strings.TrimSpace(cfg.WorkDir) == "" {
		cfg.WorkDir = info.WorkDir
	}
	cfg.Env = mergeEnv(cfg.Env, RuntimeEnvWithSessionContext(info, generation, continuation, strings.TrimSpace(info.InstanceToken)))
	if cfg.Env == nil {
		cfg.Env = make(map[string]string)
	}
	cfg.Env[RuntimeOperationTokenEnv] = strings.TrimSpace(operationToken)
	return runtime.SyncWorkDirEnv(cfg), nil
}

func automaticExistingRuntimeConfig(info Info, cfg runtime.Config) (runtime.Config, error) {
	generation, err := strconv.Atoi(strings.TrimSpace(info.Generation))
	if err != nil || generation <= 0 {
		return runtime.Config{}, fmt.Errorf("%w: session %q generation %q is not a positive integer", ErrAutomaticRuntimeDecisionInvalid, info.ID, info.Generation)
	}
	continuation := DefaultContinuationEpoch
	if raw := strings.TrimSpace(info.ContinuationEpoch); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed <= 0 {
			return runtime.Config{}, fmt.Errorf("%w: session %q continuation epoch %q is not a positive integer", ErrAutomaticRuntimeDecisionInvalid, info.ID, info.ContinuationEpoch)
		}
		continuation = parsed
	}
	cfg.Env = mergeEnv(cfg.Env, RuntimeEnvWithSessionContext(info, generation, continuation, strings.TrimSpace(info.InstanceToken)))
	return runtime.SyncWorkDirEnv(cfg), nil
}

func automaticRuntimeEpoch(info Info) (string, error) {
	raw := strings.TrimSpace(info.Generation)
	generation, err := strconv.Atoi(raw)
	if err != nil || generation <= 0 {
		return "", fmt.Errorf("%w: session %q generation %q is not a positive integer", ErrAutomaticRuntimeDecisionInvalid, info.ID, info.Generation)
	}
	return strconv.Itoa(generation), nil
}

func normalizeAutomaticRuntimeLeaseError(err error) error {
	if errors.Is(err, ErrConditionalMutationHeld) {
		return errors.Join(ErrAutomaticRuntimeRecoveryBlocked, err)
	}
	return err
}

func automaticRuntimeComparableInfo(info Info) Info {
	labelSet := make(map[string]struct{}, len(info.Labels))
	for _, label := range info.Labels {
		labelSet[label] = struct{}{}
	}
	info.Labels = make([]string, 0, len(labelSet))
	for label := range labelSet {
		info.Labels = append(info.Labels, label)
	}
	sort.Strings(info.Labels)
	return info
}

func preserveAutomaticRuntimeLease(lease *ConditionalMutationLease, errs ...error) error {
	if lease == nil {
		return errors.Join(errs...)
	}
	return errors.Join(append(errs, lease.Preserve())...)
}

func cloneAutomaticRuntimeMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func automaticRuntimeMetadataValues(current, keys map[string]string) map[string]string {
	if len(keys) == 0 {
		return nil
	}
	values := make(map[string]string, len(keys))
	for key := range keys {
		values[key] = current[key]
	}
	return values
}

func rollbackAutomaticRuntimeNudgeMarker(lease *ConditionalMutationLease, markerRollback map[string]string, effectErr error) error {
	if len(markerRollback) == 0 {
		return effectErr
	}
	if err := lease.Patch(beads.UpdateOpts{Metadata: cloneAutomaticRuntimeMetadata(markerRollback)}); err != nil {
		return errors.Join(effectErr, fmt.Errorf("restoring automatic nudge marker after refused provider effect: %w", err))
	}
	return effectErr
}
