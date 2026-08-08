package worker

import (
	"context"
	"fmt"
	"time"

	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

// LiveObservation is the worker-owned runtime observation surface used by API
// and CLI read models.
type LiveObservation struct {
	Running          bool
	Alive            bool
	Suspended        bool
	Attached         bool
	LastActivity     *time.Time
	RuntimeSessionID string
	SessionID        string
	SessionName      string
}

// ObserveHandle returns worker-owned runtime observations for handles that
// support them.
func ObserveHandle(ctx context.Context, h LiveObservationHandle) (LiveObservation, error) {
	if h == nil {
		return LiveObservation{}, fmt.Errorf("%w: live observation unavailable", ErrOperationUnsupported)
	}
	return h.LiveObservation(ctx)
}

// LiveObservation reports runtime presence and attachment metadata for a
// bead-backed session handle.
func (h *SessionHandle) LiveObservation(_ context.Context) (LiveObservation, error) {
	id := h.currentSessionID()
	if id == "" {
		return LiveObservation{}, nil
	}
	info, err := h.manager.Get(id)
	if err != nil {
		return LiveObservation{}, err
	}
	runtimeObs := h.manager.ObserveRuntimeForInfo(info, h.runtimeHints().ProcessNames)
	return liveObservationFromSession(info, runtimeObs), nil
}

// ObserveSessionForReconciler observes a bead-backed session only while the
// reconciler's originally captured trigger witness remains current and
// wake-eligible. The manager reloads and validates the persisted row under the
// session mutation lock before any repair, provider routing, or observation.
func (f *Factory) ObserveSessionForReconciler(
	ctx context.Context,
	id string,
	processNames []string,
	witness sessionpkg.LiveBoundaryWitness,
) (LiveObservation, error) {
	if err := ctx.Err(); err != nil {
		return LiveObservation{}, err
	}
	info, runtimeObs, err := f.manager.ObserveRuntimeForReconcilerWithWitness(id, processNames, witness)
	if err != nil {
		return LiveObservation{}, err
	}
	return liveObservationFromSession(info, runtimeObs), nil
}

// ObserveSessionForReconcilerLifecycle observes an exact bead-backed session
// for automatic drain/stop/retirement work. It retains the captured trigger
// witness while allowing that exact session to be suspended.
func (f *Factory) ObserveSessionForReconcilerLifecycle(
	ctx context.Context,
	id string,
	processNames []string,
	witness sessionpkg.LiveBoundaryWitness,
) (LiveObservation, error) {
	if err := ctx.Err(); err != nil {
		return LiveObservation{}, err
	}
	info, runtimeObs, err := f.manager.ObserveRuntimeForReconcilerLifecycleWithWitness(id, processNames, witness)
	if err != nil {
		return LiveObservation{}, err
	}
	return liveObservationFromSession(info, runtimeObs), nil
}

// PeekSessionForReconciler captures provider output only while the
// reconciler's originally captured trigger witness remains current and
// wake-eligible.
func (f *Factory) PeekSessionForReconciler(
	ctx context.Context,
	id string,
	lines int,
	witness sessionpkg.LiveBoundaryWitness,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return f.manager.PeekForReconcilerWithWitness(id, lines, witness)
}

func liveObservationFromSession(info sessionpkg.Info, runtimeObs sessionpkg.RuntimeObservation) LiveObservation {
	obs := LiveObservation{
		Running:          runtimeObs.Running,
		Alive:            runtimeObs.Alive,
		Suspended:        info.State == sessionpkg.StateSuspended,
		Attached:         runtimeObs.Attached,
		RuntimeSessionID: info.ID,
		SessionID:        info.ID,
		SessionName:      runtimeObs.SessionName,
	}
	if !runtimeObs.LastActive.IsZero() {
		last := runtimeObs.LastActive
		obs.LastActivity = &last
	}
	return obs
}
