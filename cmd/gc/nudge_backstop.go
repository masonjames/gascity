package main

import (
	"context"
	"fmt"
	"io"
	"maps"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/worker"
)

// backstopPredicate adapts the shared nudge-backstop engine (observe → nudge
// → backoff → give-up, see decideBackstopAction) to one class of session.
// Each predicate owns its own eligibility test, outstanding-work resolution,
// nudge content, and persisted-metadata shape; the engine drives only the
// shared timing decision and the actual runtime.Provider.Nudge delivery.
//
// poolClaimBackstop and poolContinuationBackstop (idle_nudge.go) are the two
// predicates: initial trigger delivery and later graph-v2 successor delivery.
type backstopPredicate interface {
	// governs reports whether this predicate applies to the session bead at
	// all.
	governs(s beads.Bead) bool

	// resolve classifies the current evidence for sessName. Definite absence
	// returns backstopResolutionClear; incomplete or ambiguous evidence returns
	// backstopResolutionHold so persisted pacing state is not erased.
	resolve(s beads.Bead, work map[string]beads.Bead, sessName string) (target backstopTarget, resolution backstopResolution)

	// state reads the persisted pacing state for target. same is false when
	// target is an assignment not yet observed, in which case the engine calls
	// observe to (re)start the grace clock instead of consulting attempts.
	state(s beads.Bead, target backstopTarget) (same bool, attempts int, last time.Time)

	// content resolves the text to nudge with, or "" to skip silently.
	content(s beads.Bead) string

	// revalidate checks the exact target immediately before attempt reservation
	// and delivery. It closes the desired-state-snapshot race without treating
	// a read failure as proof that work disappeared.
	revalidate(target backstopTarget) backstopResolution

	// observe persists the start of a new assignment's grace window.
	observe(store beads.Store, s *beads.Bead, target backstopTarget, now time.Time, stdout io.Writer)

	// reserve durably records a nudge attempt before delivery. false means the
	// write failed and the provider must not be nudged.
	reserve(store beads.Store, s *beads.Bead, target backstopTarget, attempts int, now time.Time, stdout io.Writer) bool

	// reservationPatch returns the same write-ahead marker as reserve without
	// performing it. Strict automatic delivery passes this patch to the worker
	// boundary so marker reservation and exact-incarnation Nudge share one
	// conditional mutation lease.
	reservationPatch(target backstopTarget, attempts int, now time.Time) map[string]string

	// exhausted is invoked once attempts reach the shared max attempts.
	exhausted(store beads.Store, s *beads.Bead, stdout io.Writer)

	// clear wipes persisted state once nothing is outstanding.
	clear(store beads.Store, s *beads.Bead, stdout io.Writer)
}

// backstopTarget is the durable identity of one outstanding delivery target.
// ID is the human-facing work bead. RootID, StoreRef, and Generation are
// optional persisted provenance fields. Initial pool claims persist ID and
// StoreRef; continuation claims persist all four so same-ID rows in independent
// stores, recycled graph roots, and recycled pool generations never share
// pacing state. Assignee and Store retain the exact live-read authority used
// only for pre-delivery revalidation.
type backstopTarget struct {
	ID         string
	RootID     string
	StoreRef   string
	Generation string
	Assignee   string
	Store      beads.Store
}

// backstopMutationFence binds automatic strict-policy pacing writes to the
// exact trigger pair captured with the reconciler snapshot. The zero value is
// the established inherit-policy behavior.
type backstopMutationFence struct {
	strict    bool
	policyErr error
	witness   sessionpkg.LiveBoundaryWitness
	boundary  reconcilerMutationBoundary
}

func captureBackstopMutationFence(s beads.Bead, cfg *config.City) backstopMutationFence {
	boundary := captureReconcilerMutationBoundary(sessionpkg.InfoFromPersistedBead(s), cfg, s.Revision)
	return backstopMutationFence{
		strict:    boundary.strict,
		policyErr: boundary.policyErr,
		witness:   boundary.expected,
		boundary:  boundary,
	}
}

// mutate authorizes and executes one pacing-state mutation. Strict policy
// first proves that the session row is co-located with the canonical city work
// ledger, then serializes with every session trigger/lifecycle writer, reloads
// the authoritative persisted row through the live handle, and validates both
// the exact trigger pair and automatic-wake eligibility before the callback can
// reach SetMetadataBatch. Inherit policy deliberately preserves the previous
// direct callback path.
func (f backstopMutationFence) mutate(
	sessionStore beads.Store,
	canonicalWorkStore beads.Store,
	s *beads.Bead,
	label string,
	stdout io.Writer,
	mutation func(beads.Store, *beads.Bead) bool,
) bool {
	if mutation == nil || s == nil {
		return false
	}
	if f.policyErr != nil {
		if stdout != nil {
			fmt.Fprintf(stdout, "%s: refusing ambiguous pacing mutation for %s: %v\n", label, s.ID, f.policyErr) //nolint:errcheck // best-effort
		}
		return false
	}
	id := strings.TrimSpace(s.ID)
	if sessionStore == nil || id == "" {
		return false
	}
	if f.strict && (canonicalWorkStore == nil ||
		strings.TrimSpace(f.witness.TriggerBeadID) == "" ||
		strings.TrimSpace(f.witness.TriggerBeadStoreRef) == "") {
		return false
	}
	if f.strict && !beads.SameStoreIdentity(sessionStore, canonicalWorkStore) {
		return false
	}

	mutated := false
	_, err := f.boundary.runPersisted(sessionStore, nil, func(current sessionpkg.Info, persisted sessionpkg.PersistedResponse, front *sessionpkg.Store) error {
		currentBead := beads.Bead{
			ID:       current.ID,
			Type:     current.Type,
			Title:    current.Title,
			Status:   persisted.Status,
			Labels:   append([]string(nil), current.Labels...),
			Metadata: maps.Clone(persisted.Metadata),
			Revision: persisted.Revision,
		}
		mutated = mutation(front.Store().Store, &currentBead)
		if mutated {
			*s = currentBead
		}
		return nil
	})
	if err != nil {
		if stdout != nil {
			fmt.Fprintf(stdout, "%s: refusing stale pacing mutation for %s: %v\n", label, id, err) //nolint:errcheck // best-effort
		}
		return false
	}
	return mutated
}

// backstopResolution distinguishes definite completion from uncertainty.
// Conflating hold with clear resets persisted attempt caps during transient
// store or identity ambiguity and can turn a bounded backstop into churn.
type backstopResolution int

const (
	backstopResolutionClear backstopResolution = iota
	backstopResolutionHold
	backstopResolutionOutstanding
)

// backstopAction is the shared timing engine's verdict for one session on one
// reconcile tick.
type backstopAction int

const (
	backstopActionWait backstopAction = iota
	backstopActionNudge
	backstopActionExhausted
)

// decideBackstopAction is the observe(grace) → nudge → backoff → give-up
// timing rule shared by every backstop predicate, extracted unchanged from
// nudgeStalledPoolClaims. attempts is the number of delivery attempts already
// reserved for the current assignment; last is the time of the last attempt,
// or of first observation when attempts is 0. Pacing reuses the exact constants
// proven by the pool-claim backstop (idleClaimNudgeGrace/Backoff/MaxAttempts,
// idle_nudge.go).
func decideBackstopAction(attempts int, last, now time.Time) backstopAction {
	switch {
	case attempts == 0:
		if now.Sub(last) < idleClaimNudgeGrace {
			return backstopActionWait // still inside the observe-first grace
		}
	case attempts >= idleClaimNudgeMaxAttempts:
		return backstopActionExhausted // gave up; manual re-nudge is the escape hatch
	default:
		if now.Sub(last) < idleClaimNudgeBackoff {
			return backstopActionWait // waiting out the backoff before the next retry
		}
	}
	return backstopActionNudge
}

// runNudgeBackstop drives pred over sessionBeads: for each session it governs
// that is running and has outstanding work, it paces re-delivery of pred's
// nudge content through the shared grace → nudge → backoff → give-up engine,
// persisting all state via pred so a controller restart cannot replay it.
// label prefixes stdout diagnostics so multiple backstops stay distinguishable
// in logs.
func runNudgeBackstop(
	sp runtime.Provider,
	cfg *config.City,
	cityPath string,
	sessionStore beads.Store,
	canonicalWorkStore beads.Store,
	sessionBeads []beads.Bead,
	work []beads.Bead,
	now time.Time,
	stdout io.Writer,
	label string,
	pred backstopPredicate,
) {
	if sp == nil || sessionStore == nil {
		return // hot reconcile path: never panic on a half-built dependency
	}
	workByID := make(map[string]beads.Bead, len(work))
	for _, w := range work {
		workByID[w.ID] = w
	}

	for i := range sessionBeads {
		s := &sessionBeads[i]
		if !pred.governs(*s) {
			continue
		}
		sessName := strings.TrimSpace(s.Metadata["session_name"])
		if sessName == "" || !sp.IsRunning(sessName) {
			continue
		}
		mutationFence := captureBackstopMutationFence(*s, cfg)

		target, resolution := pred.resolve(*s, workByID, sessName)
		switch resolution {
		case backstopResolutionHold:
			continue
		case backstopResolutionClear:
			mutationFence.mutate(sessionStore, canonicalWorkStore, s, label, stdout, func(mutationStore beads.Store, current *beads.Bead) bool {
				pred.clear(mutationStore, current, stdout)
				return true
			})
			continue
		case backstopResolutionOutstanding:
			// Continue below.
		default:
			continue
		}

		same, attempts, last := pred.state(*s, target)
		if !same {
			// First observation of this assignment: start the grace clock,
			// don't nudge yet — a normal claim/confirmation almost always
			// lands within the grace window.
			mutationFence.mutate(sessionStore, canonicalWorkStore, s, label, stdout, func(mutationStore beads.Store, current *beads.Bead) bool {
				pred.observe(mutationStore, current, target, now, stdout)
				return true
			})
			continue
		}

		switch decideBackstopAction(attempts, last, now) {
		case backstopActionWait:
			continue
		case backstopActionExhausted:
			pred.exhausted(sessionStore, s, stdout)
			continue
		case backstopActionNudge:
			content := pred.content(*s)
			if content == "" {
				continue
			}
			switch pred.revalidate(target) {
			case backstopResolutionHold:
				continue
			case backstopResolutionClear:
				mutationFence.mutate(sessionStore, canonicalWorkStore, s, label, stdout, func(mutationStore beads.Store, current *beads.Bead) bool {
					pred.clear(mutationStore, current, stdout)
					return true
				})
				continue
			case backstopResolutionOutstanding:
				// Reserve below.
			default:
				continue
			}
			factory, err := workerFactoryWithConfig(cityPath, sessionStore, sp, cfg, canonicalWorkStore)
			if err != nil {
				fmt.Fprintf(stdout, "%s: %s failed: %v\n", label, sessName, err) //nolint:errcheck // best-effort
				continue
			}
			if mutationFence.strict {
				if canonicalWorkStore == nil || !beads.SameStoreIdentity(sessionStore, canonicalWorkStore) {
					continue
				}
				decision, decisionErr := mutationFence.boundary.automaticRuntimeDecision()
				if decisionErr != nil {
					fmt.Fprintf(stdout, "%s: %s failed: %v\n", label, sessName, decisionErr) //nolint:errcheck // best-effort
					continue
				}
				markerPatch := pred.reservationPatch(target, attempts+1, now)
				if len(markerPatch) == 0 {
					continue
				}
				delivered, commit, nudgeErr := factory.NudgeSessionForReconciler(
					context.Background(), s.ID, content, label, decision, markerPatch,
				)
				if nudgeErr != nil {
					fmt.Fprintf(stdout, "%s: %s failed: %v\n", label, sessName, nudgeErr) //nolint:errcheck // best-effort
					continue
				}
				postBoundary, commitErr := mutationFence.boundary.afterAutomaticRuntimeCommit(commit)
				if commitErr != nil {
					fmt.Fprintf(stdout, "%s: %s failed: %v\n", label, sessName, commitErr) //nolint:errcheck // best-effort
					continue
				}
				mutationFence.boundary = postBoundary
				s.Revision = commit.Persisted.Revision
				if s.Metadata == nil {
					s.Metadata = make(map[string]string, len(markerPatch))
				}
				for key, value := range markerPatch {
					s.Metadata[key] = value
				}
				if delivered {
					fmt.Fprintf(stdout, "%s: nudged %s for %s (attempt %d/%d)\n", label, sessName, target.ID, attempts+1, idleClaimNudgeMaxAttempts) //nolint:errcheck // best-effort
				}
				continue
			}
			expectedTriggerID := strings.TrimSpace(s.Metadata[beadmeta.TriggerBeadIDMetadataKey])
			expectedTriggerStoreRef := strings.TrimSpace(s.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey])
			if expectedTriggerID == "" || expectedTriggerStoreRef == "" {
				expectedTriggerID = ""
				expectedTriggerStoreRef = ""
			}
			// Write ahead of the external delivery. If the process crashes
			// after this point, an attempt may be consumed without delivery,
			// but a crash or store failure can never replay an unbounded nudge.
			if !mutationFence.mutate(sessionStore, canonicalWorkStore, s, label, stdout, func(mutationStore beads.Store, current *beads.Bead) bool {
				return pred.reserve(mutationStore, current, target, attempts+1, now, stdout)
			}) {
				continue
			}
			handle, err := factory.SessionByID(s.ID)
			if err != nil {
				fmt.Fprintf(stdout, "%s: %s failed: %v\n", label, sessName, err) //nolint:errcheck // best-effort
				continue
			}
			result, err := handle.Nudge(context.Background(), worker.NudgeRequest{
				Text:                        content,
				Source:                      label,
				Wake:                        worker.NudgeWakeLiveOnly,
				ExpectedTriggerBeadID:       expectedTriggerID,
				ExpectedTriggerBeadStoreRef: expectedTriggerStoreRef,
			})
			if err != nil {
				fmt.Fprintf(stdout, "%s: %s failed: %v\n", label, sessName, err) //nolint:errcheck // best-effort
				continue
			}
			if !result.Delivered {
				continue
			}
			fmt.Fprintf(stdout, "%s: nudged %s for %s (attempt %d/%d)\n", label, sessName, target.ID, attempts+1, idleClaimNudgeMaxAttempts) //nolint:errcheck // best-effort
		}
	}
}
