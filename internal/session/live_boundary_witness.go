package session

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
)

// ErrLiveBoundaryWitnessMismatch reports that a session changed after a
// higher layer authorized one exact trigger or is no longer launch-eligible.
var ErrLiveBoundaryWitnessMismatch = errors.New("session live-boundary witness mismatch")

// LiveBoundaryWitness binds one live operation to the exact trigger pair a
// higher layer authorized. The zero value preserves legacy/inherit behavior.
type LiveBoundaryWitness struct {
	TriggerBeadID       string
	TriggerBeadStoreRef string
}

func validateLiveBoundaryWitness(b beads.Bead, witness LiveBoundaryWitness) error {
	return ValidateLiveBoundaryInfo(infoFromPersistedBead(b), witness)
}

// ValidateLiveBoundaryInfo validates one freshly loaded persisted session
// projection against an exact live-boundary witness without performing any
// repair, routing, provider call, or store mutation.
func ValidateLiveBoundaryInfo(info Info, witness LiveBoundaryWitness) error {
	// The zero witness is the explicit/manual compatibility contract. Manual
	// lifecycle methods historically decide which states they admit themselves;
	// only witnessed and automatic reconciler operations apply this validator's
	// launch-state fence.
	if strings.TrimSpace(witness.TriggerBeadID) == "" && strings.TrimSpace(witness.TriggerBeadStoreRef) == "" {
		return nil
	}
	if err := ValidateLifecycleBoundaryInfo(info, witness); err != nil {
		return err
	}
	switch state := info.State; state {
	case StateNone, StateActive, StateAwake, StateAsleep, StateSuspended, StateStartPending, StateCreating:
		return nil
	default:
		return fmt.Errorf("%w: session %q state %q is not launch-eligible", ErrLiveBoundaryWitnessMismatch, info.ID, state)
	}
}

// ValidateLifecycleBoundaryInfo validates an exact persisted session witness
// for automatic containment work. It intentionally leaves state eligibility
// to the action-specific predicate so draining, suspended, quarantined, and
// other open lifecycle states can converge without becoming wake-eligible.
func ValidateLifecycleBoundaryInfo(info Info, witness LiveBoundaryWitness) error {
	expectedID := strings.TrimSpace(witness.TriggerBeadID)
	expectedRef := strings.TrimSpace(witness.TriggerBeadStoreRef)
	if expectedID == "" && expectedRef == "" {
		return nil
	}
	if expectedID == "" || expectedRef == "" {
		return fmt.Errorf("%w: incomplete expected trigger pair", ErrLiveBoundaryWitnessMismatch)
	}
	if info.Closed {
		return fmt.Errorf("%w: session %q is closed", ErrLiveBoundaryWitnessMismatch, info.ID)
	}
	actualID := strings.TrimSpace(info.TriggerBeadID)
	actualRef := strings.TrimSpace(info.TriggerBeadStoreRef)
	if actualID != expectedID || actualRef != expectedRef {
		return fmt.Errorf(
			"%w: session %q trigger changed from (%q, %q) to (%q, %q)",
			ErrLiveBoundaryWitnessMismatch,
			info.ID,
			expectedID,
			expectedRef,
			actualID,
			actualRef,
		)
	}
	return nil
}

// ValidateReconcilerLifecycleInfo validates an automatic lifecycle action's
// captured witness. Unlike the manual zero-witness contract, a zero expected
// pair is authority only for a row that still has no trigger pair; automatic
// work must never adopt a trigger attached after candidate capture.
func ValidateReconcilerLifecycleInfo(info Info, witness LiveBoundaryWitness) error {
	expectedID := strings.TrimSpace(witness.TriggerBeadID)
	expectedRef := strings.TrimSpace(witness.TriggerBeadStoreRef)
	if expectedID == "" && expectedRef == "" {
		if info.Closed {
			return fmt.Errorf("%w: session %q is closed", ErrLiveBoundaryWitnessMismatch, info.ID)
		}
		actualID := strings.TrimSpace(info.TriggerBeadID)
		actualRef := strings.TrimSpace(info.TriggerBeadStoreRef)
		if actualID != "" || actualRef != "" {
			return fmt.Errorf(
				"%w: session %q acquired trigger (%q, %q) after an unbound automatic decision",
				ErrLiveBoundaryWitnessMismatch,
				info.ID,
				actualID,
				actualRef,
			)
		}
		return nil
	}
	return ValidateLifecycleBoundaryInfo(info, witness)
}

// ValidateReconcilerWakeInfo applies the stricter eligibility required by an
// automatic wake. Explicit lifecycle operations may resume a suspended row,
// but the reconciler must never override an operator/session suspension.
func ValidateReconcilerWakeInfo(info Info, witness LiveBoundaryWitness) error {
	if err := ValidateReconcilerLifecycleInfo(info, witness); err != nil {
		return err
	}
	switch state := info.State; state {
	case StateNone, StateActive, StateAwake, StateAsleep, StateStartPending, StateCreating:
		return nil
	default:
		return fmt.Errorf("%w: session %q state %q is not automatic-wake-eligible", ErrLiveBoundaryWitnessMismatch, info.ID, state)
	}
}
