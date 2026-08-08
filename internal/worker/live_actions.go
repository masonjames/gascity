package worker

import (
	"context"
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

// RelaunchConfigPreparer builds a warm-relaunch config after the exact session
// witness has been revalidated under the session mutation lock.
type RelaunchConfigPreparer func() (runtime.Config, error)

// CloseDetailedWithWitness closes one bead-backed session only while the
// caller's captured trigger authority remains exact. The factory performs a
// side-effect-free persisted authorization read first; Manager then reloads
// and validates the same witness under the session mutation lock before any
// provider or session mutation. The returned Info and PersistedResponse are
// the authoritative pre-close row used for conditional work cleanup.
func (f *Factory) CloseDetailedWithWitness(
	ctx context.Context,
	id string,
	expected sessionpkg.LiveBoundaryWitness,
) (sessionpkg.CloseResult, sessionpkg.Info, sessionpkg.PersistedResponse, error) {
	info, persisted, err := f.manager.PersistedStore().GetPersistedResponse(id)
	if err != nil {
		return sessionpkg.CloseResult{}, sessionpkg.Info{}, sessionpkg.PersistedResponse{}, err
	}
	authorization, err := f.authorizePersistedRecordWithContext(ctx, info, persisted)
	if err != nil {
		return sessionpkg.CloseResult{}, sessionpkg.Info{}, sessionpkg.PersistedResponse{}, err
	}
	if err := requireReconcilerExpectedWitness(expected, authorization); err != nil {
		return sessionpkg.CloseResult{}, sessionpkg.Info{}, sessionpkg.PersistedResponse{}, err
	}
	result, current, currentPersisted, err := f.manager.CloseDetailedForLifecycleWithWitness(id, expected)
	if err != nil {
		return sessionpkg.CloseResult{}, sessionpkg.Info{}, sessionpkg.PersistedResponse{}, normalizeLiveBoundaryError(err)
	}
	return result, current, currentPersisted, nil
}

// RelaunchPrepared routes a warm provider relaunch through the same
// authoritative worker/session boundary as starts and turn delivery.
func (f *Factory) RelaunchPrepared(ctx context.Context, id string, expected sessionpkg.LiveBoundaryWitness, prepare RelaunchConfigPreparer) error {
	handle, err := f.SessionByID(id)
	if err != nil {
		return err
	}
	sessionHandle, ok := handle.(*SessionHandle)
	if !ok {
		return fmt.Errorf("%w: session-backed relaunch unavailable", ErrOperationUnsupported)
	}
	authorization, err := sessionHandle.authorizeLiveBoundary(ctx, id)
	if err != nil {
		return err
	}
	if err := requireReconcilerExpectedWitness(expected, authorization); err != nil {
		return err
	}
	err = sessionHandle.manager.RelaunchPreparedWithWitness(ctx, id, expected, func() (runtime.Config, error) {
		cfg, err := prepare()
		if err != nil {
			return runtime.Config{}, err
		}
		return sessionHandle.runtimeHintsWithAuthorization(cfg, authorization), nil
	})
	return normalizeLiveBoundaryError(err)
}

// RunLive re-applies a session's live commands through exact-trigger
// authorization and the session mutation lock.
func (f *Factory) RunLive(ctx context.Context, id string, cfg runtime.Config, expected sessionpkg.LiveBoundaryWitness) error {
	handle, err := f.SessionByID(id)
	if err != nil {
		return err
	}
	sessionHandle, ok := handle.(*SessionHandle)
	if !ok {
		return fmt.Errorf("%w: session-backed live config unavailable", ErrOperationUnsupported)
	}
	authorization, err := sessionHandle.authorizeLiveBoundary(ctx, id)
	if err != nil {
		return err
	}
	if err := requireReconcilerExpectedWitness(expected, authorization); err != nil {
		return err
	}
	err = sessionHandle.manager.RunLiveWithWitness(
		id,
		sessionHandle.runtimeHintsWithAuthorization(cfg, authorization),
		expected,
	)
	return normalizeLiveBoundaryError(err)
}

func requireReconcilerExpectedWitness(expected sessionpkg.LiveBoundaryWitness, authorization LaunchAuthorization) error {
	expectedID := strings.TrimSpace(expected.TriggerBeadID)
	expectedRef := strings.TrimSpace(expected.TriggerBeadStoreRef)
	actualID := strings.TrimSpace(authorization.TriggerBeadID)
	actualRef := strings.TrimSpace(authorization.TriggerBeadStoreRef)
	if expectedID == "" && expectedRef == "" && actualID == "" && actualRef == "" {
		return nil
	}
	if expectedID == "" || expectedRef == "" || actualID != expectedID || actualRef != expectedRef {
		return fmt.Errorf(
			"%w: %w: reconciler expected trigger (%q, %q) does not match current authorization (%q, %q)",
			ErrLaunchUnauthorized,
			sessionpkg.ErrLiveBoundaryWitnessMismatch,
			expectedID,
			expectedRef,
			actualID,
			actualRef,
		)
	}
	return nil
}
