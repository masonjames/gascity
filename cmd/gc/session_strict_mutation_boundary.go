package main

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

var errReconcilerMutationPredicateFalse = errors.New("reconciler mutation predicate false")

// reconcilerMutationBoundary captures the ownership witness seen by an
// automatic controller decision. A configured strict session always enforces
// the boundary; routed inherit sessions with a complete persisted trigger pair
// also retain that pair so an automatic mutation cannot silently adopt a new
// owner between decision and side effect.
type reconcilerMutationBoundary struct {
	captured         session.Info
	capturedRevision int64
	revisionCaptured bool
	expected         session.LiveBoundaryWitness
	strict           bool
	enforce          bool
	policyErr        error
	// allowSuspended distinguishes automatic wake/live actions from automatic
	// lifecycle containment. A suspended concrete session must remain
	// non-wakeable, while stop/drain/retire actions must still be able to
	// converge that exact suspended incarnation.
	allowSuspended bool
	revisionSink   func(int64)
}

func (b reconcilerMutationBoundary) withRevisionSink(sink func(int64)) reconcilerMutationBoundary {
	b.revisionSink = sink
	return b
}

func selectedReconcilerMutationBoundary(
	info session.Info,
	cfg *config.City,
	boundaries ...reconcilerMutationBoundary,
) reconcilerMutationBoundary {
	if len(boundaries) > 0 {
		return boundaries[0]
	}
	return captureReconcilerMutationBoundary(info, cfg)
}

func captureReconcilerMutationBoundary(info session.Info, cfg *config.City, revisions ...int64) reconcilerMutationBoundary {
	boundary := reconcilerMutationBoundary{captured: info, enforce: strings.TrimSpace(info.ID) != ""}
	if len(revisions) > 0 {
		boundary.capturedRevision = revisions[0]
		boundary.revisionCaptured = true
	}
	triggerID := strings.TrimSpace(info.TriggerBeadID)
	triggerStoreRef := strings.TrimSpace(info.TriggerBeadStoreRef)
	if triggerID != "" || triggerStoreRef != "" {
		boundary.expected = session.LiveBoundaryWitness{
			TriggerBeadID:       triggerID,
			TriggerBeadStoreRef: triggerStoreRef,
		}
		boundary.enforce = true
	}
	resolution, err := agentutil.ResolvePersistedSessionAgent(cfg, info)
	if err != nil {
		boundary.policyErr = err
		boundary.enforce = true
		return boundary
	}
	if resolution.Strict {
		boundary.strict = true
		boundary.enforce = true
	}
	return boundary
}

func captureReconcilerMutationBoundaryForTarget(
	store beads.Store,
	cfg *config.City,
	target string,
) (reconcilerMutationBoundary, error) {
	id, err := session.ResolveSessionID(store, strings.TrimSpace(target))
	if err != nil {
		return reconcilerMutationBoundary{}, err
	}
	info, persisted, err := sessionFrontDoor(store).GetPersistedResponse(id)
	if err != nil {
		return reconcilerMutationBoundary{}, err
	}
	return captureReconcilerMutationBoundary(info, cfg, persisted.Revision), nil
}

func (b reconcilerMutationBoundary) validateCurrent(current session.Info) error {
	if b.policyErr != nil {
		return b.policyErr
	}
	expectedID := strings.TrimSpace(b.expected.TriggerBeadID)
	expectedStoreRef := strings.TrimSpace(b.expected.TriggerBeadStoreRef)
	if (expectedID == "") != (expectedStoreRef == "") {
		return fmt.Errorf("%w: automatic mutation requires a complete trigger pair", session.ErrLiveBoundaryWitnessMismatch)
	}
	if b.strict && expectedID == "" {
		return fmt.Errorf("%w: strict automatic mutation requires a trigger pair", session.ErrLiveBoundaryWitnessMismatch)
	}
	if b.allowSuspended {
		return session.ValidateReconcilerLifecycleInfo(current, b.expected)
	}
	return session.ValidateReconcilerWakeInfo(current, b.expected)
}

// lifecycleMutation selects the eligibility used by automatic stop, drain,
// close, and retirement actions. Those actions must retain the captured exact
// trigger witness but may intentionally operate on a suspended session. Each
// caller still re-evaluates its action-specific state predicate under the lock.
func (b reconcilerMutationBoundary) lifecycleMutation() reconcilerMutationBoundary {
	b.allowSuspended = true
	return b
}

// legacyAutomaticRuntimeEffectError refuses a configured strict or ambiguous
// automatic action before it reaches a name-only provider mutation. Those
// effects are safe only through an AutomaticRuntimeDecision lease plus exact
// incarnation API; callers that still depend on the legacy worker/provider
// Stop, Kill, Close, or metadata surface must remain inert for strict rows.
func (b reconcilerMutationBoundary) legacyAutomaticRuntimeEffectError() error {
	if b.policyErr != nil {
		return b.policyErr
	}
	if b.strict {
		return session.ErrAutomaticRuntimeExactEffectUnsupported
	}
	return nil
}

func (b reconcilerMutationBoundary) automaticRuntimeDecision() (session.AutomaticRuntimeDecision, error) {
	if b.policyErr != nil {
		return session.AutomaticRuntimeDecision{}, b.policyErr
	}
	if !b.enforce || strings.TrimSpace(b.captured.ID) == "" || !b.revisionCaptured {
		return session.AutomaticRuntimeDecision{}, fmt.Errorf(
			"%w: automatic provider effect requires a bead-backed captured decision revision",
			session.ErrAutomaticRuntimeDecisionInvalid,
		)
	}
	if err := b.validateCurrent(b.captured); err != nil {
		return session.AutomaticRuntimeDecision{}, err
	}
	return session.AutomaticRuntimeDecision{
		Captured:              b.captured,
		ExpectedRevision:      b.capturedRevision,
		RevisionCaptured:      true,
		Witness:               b.expected,
		ProjectHooksForbidden: b.strict,
	}, nil
}

func (b reconcilerMutationBoundary) afterAutomaticRuntimeCommit(
	commit session.AutomaticRuntimeCommit,
) (reconcilerMutationBoundary, error) {
	if strings.TrimSpace(commit.Info.ID) == "" || commit.Info.ID != b.captured.ID {
		return reconcilerMutationBoundary{}, fmt.Errorf(
			"%w: automatic provider commit session %q does not match captured %q",
			session.ErrAutomaticRuntimeDecisionInvalid,
			commit.Info.ID,
			b.captured.ID,
		)
	}
	if err := b.validateCurrent(commit.Info); err != nil {
		return reconcilerMutationBoundary{}, err
	}
	if err := validateCapturedSessionIdentity(b.captured, commit.Info); err != nil {
		return reconcilerMutationBoundary{}, err
	}
	b.captured = commit.Info
	b.capturedRevision = commit.Persisted.Revision
	b.revisionCaptured = true
	if b.revisionSink != nil {
		b.revisionSink(commit.Persisted.Revision)
	}
	return b, nil
}

// run revalidates an automatic mutation immediately before its predicate and
// side effects. For enforced boundaries the predicate, provider action, and
// session/work commit all execute under the same per-session mutation lock.
// The bool reports that the predicate remained true and action ran.
func (b reconcilerMutationBoundary) run(
	store beads.Store,
	predicate func(session.Info) (bool, error),
	action func(session.Info, *session.Store) error,
) (bool, error) {
	return b.runPersisted(
		store,
		func(current session.Info, _ session.PersistedResponse) (bool, error) {
			if predicate == nil {
				return true, nil
			}
			return predicate(current)
		},
		func(current session.Info, _ session.PersistedResponse, front *session.Store) error {
			if action == nil {
				return nil
			}
			return action(current, front)
		},
	)
}

func (b reconcilerMutationBoundary) runPersisted(
	store beads.Store,
	predicate func(session.Info, session.PersistedResponse) (bool, error),
	action func(session.Info, session.PersistedResponse, *session.Store) error,
) (bool, error) {
	if !b.enforce {
		if predicate != nil {
			allowed, err := predicate(b.captured, session.PersistedResponse{})
			if err != nil || !allowed {
				return false, err
			}
		}
		if action != nil {
			if err := action(b.captured, session.PersistedResponse{}, sessionFrontDoor(store)); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	if b.strict {
		return b.runConditional(store, predicate, action)
	}
	id := strings.TrimSpace(b.captured.ID)
	if id == "" || store == nil {
		return false, fmt.Errorf("%w: automatic mutation requires a bead-backed session", session.ErrLiveBoundaryWitnessMismatch)
	}
	var ran bool
	err := session.WithSessionMutationLock(id, func() error {
		front := sessionFrontDoor(store)
		current, persisted, err := front.GetPersistedResponse(id)
		if err != nil {
			return err
		}
		if err := b.validateCurrent(current); err != nil {
			return err
		}
		if predicate != nil {
			allowed, err := predicate(current, persisted)
			if err != nil {
				return err
			}
			if !allowed {
				return nil
			}
		}
		if action != nil {
			if err := action(current, persisted, front); err != nil {
				return err
			}
		}
		ran = true
		return nil
	})
	return ran, err
}

func (b reconcilerMutationBoundary) runConditional(
	store beads.Store,
	predicate func(session.Info, session.PersistedResponse) (bool, error),
	action func(session.Info, session.PersistedResponse, *session.Store) error,
) (bool, error) {
	id := strings.TrimSpace(b.captured.ID)
	if id == "" || store == nil || !b.revisionCaptured {
		return false, fmt.Errorf("%w: strict automatic mutation requires a captured decision revision", session.ErrConditionalMutationInvalid)
	}
	actionName := "automatic-session-wake"
	if b.allowSuspended {
		actionName = "automatic-session-lifecycle"
	}
	predicates := []session.ConditionalMutationPredicate{
		{
			Identity: "automatic-trigger-state",
			Validate: func(current session.Info, _ session.PersistedResponse) error {
				return b.validateCurrent(current)
			},
		},
		{
			Identity: "automatic-session-identity",
			Validate: func(current session.Info, _ session.PersistedResponse) error {
				return validateCapturedSessionIdentity(b.captured, current)
			},
		},
	}
	if predicate != nil {
		predicates = append(predicates, session.ConditionalMutationPredicate{
			Identity: "automatic-action-predicate",
			Validate: func(current session.Info, persisted session.PersistedResponse) error {
				allowed, err := predicate(current, persisted)
				if err != nil {
					return err
				}
				if !allowed {
					return errReconcilerMutationPredicateFalse
				}
				return nil
			},
		})
	}
	front := sessionFrontDoor(store)
	var ran bool
	var acquired *session.ConditionalMutationLease
	err := front.WithConditionalMutation(session.ConditionalMutationRequest{
		SessionID:        id,
		ExpectedRevision: b.capturedRevision,
		Action:           actionName,
		Predicates:       predicates,
	}, func(lease *session.ConditionalMutationLease) error {
		acquired = lease
		current, persisted := lease.Current()
		leasedStore := &conditionalLeaseStore{Store: store, lease: lease}
		leasedFront := sessionFrontDoor(leasedStore)
		if action != nil {
			if err := action(current, persisted, leasedFront); err != nil {
				return err
			}
		}
		if err := lease.Commit(beads.UpdateOpts{}); err != nil {
			return err
		}
		ran = true
		return nil
	})
	if acquired != nil && !errors.Is(err, session.ErrConditionalMutationLost) && b.revisionSink != nil {
		b.revisionSink(acquired.Revision())
	}
	if errors.Is(err, errReconcilerMutationPredicateFalse) {
		return false, nil
	}
	return ran, err
}

func validateCapturedSessionIdentity(captured, current session.Info) error {
	if captured.ID != current.ID || captured.Type != current.Type ||
		captured.Template != current.Template || captured.AgentName != current.AgentName ||
		captured.ConfiguredNamedIdentity != current.ConfiguredNamedIdentity ||
		captured.ConfiguredNamedSession != current.ConfiguredNamedSession ||
		captured.Alias != current.Alias || captured.CommonName != current.CommonName ||
		captured.PoolManaged != current.PoolManaged || captured.PoolSlot != current.PoolSlot ||
		captured.CanonicalInstanceNameMetadata != current.CanonicalInstanceNameMetadata ||
		captured.CanonicalPoolSlotMetadata != current.CanonicalPoolSlotMetadata ||
		captured.SessionOrigin != current.SessionOrigin ||
		captured.SessionNameMetadata != current.SessionNameMetadata ||
		!equalPersistedSessionLabels(captured.Labels, current.Labels) {
		return fmt.Errorf("%w: automatic session identity changed", session.ErrLiveBoundaryWitnessMismatch)
	}
	return nil
}

func equalPersistedSessionLabels(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	left = slices.Clone(left)
	right = slices.Clone(right)
	sort.Strings(left)
	sort.Strings(right)
	return slices.Equal(left, right)
}

func mutateReconcilerInfoAtBoundary(
	info session.Info,
	cfg *config.City,
	store beads.Store,
	mutate func(session.Info, *session.Store) (session.Info, error),
	boundaries ...reconcilerMutationBoundary,
) (session.Info, error) {
	next := info
	boundary := selectedReconcilerMutationBoundary(info, cfg, boundaries...).lifecycleMutation()
	_, err := boundary.run(store, nil, func(current session.Info, front *session.Store) error {
		if mutate == nil {
			// A validation-only boundary must not replace the caller's coherent
			// same-tick projection with unrelated next-tick metadata from the
			// authoritative reload.
			next = info
			return nil
		}
		var mutateErr error
		mutated, mutateErr := mutate(current, front)
		if mutateErr == nil && !reflect.DeepEqual(mutated, current) {
			next = mutated
		}
		return mutateErr
	})
	return next, err
}

func observeSessionCircuitResetAtBoundary(
	info session.Info,
	cfg *config.City,
	store beads.Store,
	cb *sessionCircuitBreaker,
	identity string,
	boundaries ...reconcilerMutationBoundary,
) error {
	boundary := selectedReconcilerMutationBoundary(info, cfg, boundaries...)
	_, err := boundary.runPersisted(
		store,
		func(current session.Info, _ session.PersistedResponse) (bool, error) {
			return namedSessionIdentityInfo(current) == identity, nil
		},
		func(_ session.Info, persisted session.PersistedResponse, _ *session.Store) error {
			return cb.observeResetGenerationFromMetadata(identity, session.CircuitStateFromMetadata(persisted.Metadata))
		},
	)
	return err
}

func restoreSessionCircuitAtBoundary(
	info session.Info,
	cfg *config.City,
	store beads.Store,
	cb *sessionCircuitBreaker,
	identity string,
	now time.Time,
	boundaries ...reconcilerMutationBoundary,
) (bool, error) {
	var reset bool
	boundary := selectedReconcilerMutationBoundary(info, cfg, boundaries...)
	_, err := boundary.runPersisted(
		store,
		func(current session.Info, _ session.PersistedResponse) (bool, error) {
			return namedSessionIdentityInfo(current) == identity, nil
		},
		func(current session.Info, persisted session.PersistedResponse, front *session.Store) error {
			var err error
			reset, err = cb.restoreFromMetadata(identity, session.CircuitStateFromMetadata(persisted.Metadata), now)
			if err != nil || !reset {
				return err
			}
			return persistSessionCircuitBreakerMetadata(front, current.ID, cb, identity, now)
		},
	)
	return reset, err
}

func observeSessionCircuitProgressAtBoundary(
	info session.Info,
	cfg *config.City,
	store beads.Store,
	cb *sessionCircuitBreaker,
	identity string,
	signature string,
	now time.Time,
	boundaries ...reconcilerMutationBoundary,
) (bool, error) {
	var changed bool
	boundary := selectedReconcilerMutationBoundary(info, cfg, boundaries...)
	_, err := boundary.runPersisted(
		store,
		func(current session.Info, _ session.PersistedResponse) (bool, error) {
			return namedSessionIdentityInfo(current) == identity, nil
		},
		func(current session.Info, _ session.PersistedResponse, front *session.Store) error {
			changed = cb.ObserveProgressSignature(identity, signature, now)
			if !changed {
				return nil
			}
			return persistSessionCircuitBreakerMetadata(front, current.ID, cb, identity, now)
		},
	)
	return changed, err
}

func sessionCircuitOpenAtBoundary(
	info session.Info,
	cfg *config.City,
	store beads.Store,
	cb *sessionCircuitBreaker,
	identity string,
	now time.Time,
	boundaries ...reconcilerMutationBoundary,
) (bool, error) {
	var open bool
	boundary := selectedReconcilerMutationBoundary(info, cfg, boundaries...)
	_, err := boundary.run(store, func(current session.Info) (bool, error) {
		return namedSessionIdentityInfo(current) == identity, nil
	}, func(current session.Info, front *session.Store) error {
		open = cb.IsOpen(identity, now)
		if !open {
			return nil
		}
		return persistSessionCircuitBreakerMetadata(front, current.ID, cb, identity, now)
	})
	return open, err
}

func withOrderedSessionMutationLocks(ids []string, action func() error) error {
	ordered := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	var lockNext func(int) error
	lockNext = func(index int) error {
		if index == len(ordered) {
			return action()
		}
		return session.WithSessionMutationLock(ordered[index], func() error {
			return lockNext(index + 1)
		})
	}
	return lockNext(0)
}

// runReconcilerMutationPair provides the dual-row form used by duplicate and
// replacement retirement. Locks are acquired in stable ID order, then both
// rows are freshly loaded and validated before the predicate or any provider,
// work, or session mutation.
func runReconcilerMutationPair(
	left reconcilerMutationBoundary,
	right reconcilerMutationBoundary,
	store beads.Store,
	predicate func(session.Info, session.Info) (bool, error),
	action func(session.Info, session.Info, *session.Store) error,
) (bool, error) {
	if !left.enforce && !right.enforce {
		if predicate != nil {
			allowed, err := predicate(left.captured, right.captured)
			if err != nil || !allowed {
				return false, err
			}
		}
		if action != nil {
			if err := action(left.captured, right.captured, sessionFrontDoor(store)); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	leftID := strings.TrimSpace(left.captured.ID)
	rightID := strings.TrimSpace(right.captured.ID)
	if leftID == "" || rightID == "" || leftID == rightID || store == nil {
		return false, fmt.Errorf("%w: dual-row automatic mutation requires two bead-backed sessions", session.ErrLiveBoundaryWitnessMismatch)
	}
	var ran bool
	err := withOrderedSessionMutationLocks([]string{leftID, rightID}, func() error {
		front := sessionFrontDoor(store)
		currentLeft, _, err := front.GetPersistedResponse(leftID)
		if err != nil {
			return err
		}
		currentRight, _, err := front.GetPersistedResponse(rightID)
		if err != nil {
			return err
		}
		if err := left.validateCurrent(currentLeft); err != nil {
			return err
		}
		if err := right.validateCurrent(currentRight); err != nil {
			return err
		}
		if predicate != nil {
			allowed, err := predicate(currentLeft, currentRight)
			if err != nil {
				return err
			}
			if !allowed {
				return nil
			}
		}
		if action != nil {
			if err := action(currentLeft, currentRight, front); err != nil {
				return err
			}
		}
		ran = true
		return nil
	})
	return ran, err
}
