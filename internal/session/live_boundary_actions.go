package session

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
)

// PreparedRuntimeStartResult reports whether a reconciler start attempted a
// provider launch and whether it first recycled a dead runtime container.
type PreparedRuntimeStartResult struct {
	Attempted       bool
	Recycled        bool
	RecycleDuration time.Duration
	Commit          AutomaticRuntimeCommit
}

// StartPreparedRuntimeOnlyWithWitness validates the exact captured trigger
// before observing, recycling, or starting provider runtime. The session lock
// stays held through the entire decision so an in-process trigger writer cannot
// repoint authority between the last validation and the provider boundary.
func (m *Manager) StartPreparedRuntimeOnlyWithWitness(
	ctx context.Context,
	id string,
	resumeCommand string,
	hints runtime.Config,
	witness LiveBoundaryWitness,
) (PreparedRuntimeStartResult, error) {
	var result PreparedRuntimeStartResult
	err := withSessionMutationLock(id, func() error {
		b, sessionName, err := m.reconcilerWakeSessionBeadWithWitness(id, witness)
		if err != nil {
			return err
		}
		liveness := runtime.ObserveLiveness(m.sp, sessionName, hints.ProcessNames)
		if liveness.Running {
			if liveness.Alive {
				if strings.TrimSpace(b.Metadata["pending_create_claim"]) == "true" &&
					!runningRuntimeMatchesPendingCreateBead(b, sessionName, m.sp) {
					return fmt.Errorf("%w: session %q", runtime.ErrSessionExists, sessionName)
				}
				return nil
			}
			recycleBegin := time.Now()
			stopErr := m.sp.Stop(sessionName)
			result.RecycleDuration = time.Since(recycleBegin)
			if stopErr != nil && !runtime.IsSessionGone(stopErr) {
				return fmt.Errorf("recycling session %q with dead agent process: %w", sessionName, stopErr)
			}
			result.Recycled = true
		}
		result.Attempted = true
		return m.ensureRunningRuntimeOnly(ctx, id, b, sessionName, resumeCommand, hints)
	})
	return result, err
}

func (m *Manager) reconcilerWakeSessionBeadWithWitness(id string, witness LiveBoundaryWitness) (beads.Bead, string, error) {
	return m.reconcilerSessionBeadWithWitness(id, witness, false)
}

func (m *Manager) reconcilerLifecycleSessionBeadWithWitness(id string, witness LiveBoundaryWitness) (beads.Bead, string, error) {
	return m.reconcilerSessionBeadWithWitness(id, witness, true)
}

func (m *Manager) reconcilerSessionBeadWithWitness(id string, witness LiveBoundaryWitness, lifecycle bool) (beads.Bead, string, error) {
	b, err := m.store.Get(id)
	if err != nil {
		return beads.Bead{}, "", fmt.Errorf("getting session: %w", err)
	}
	if !IsSessionBeadOrRepairable(b) {
		return beads.Bead{}, "", fmt.Errorf("%w: bead %s (type=%q)", ErrNotSession, id, b.Type)
	}
	if b.Status == "closed" {
		return beads.Bead{}, "", fmt.Errorf("%w: %s", ErrSessionClosed, id)
	}
	info := infoFromPersistedBead(b)
	var witnessErr error
	if lifecycle {
		witnessErr = ValidateReconcilerLifecycleInfo(info, witness)
	} else {
		witnessErr = ValidateReconcilerWakeInfo(info, witness)
	}
	if witnessErr != nil {
		return beads.Bead{}, "", witnessErr
	}
	RepairEmptyType(m.store, &b)
	sessionName := sessionName(id, b)
	transport, _ := m.transportForBead(b, sessionName)
	_ = m.routeACPIfNeeded(b.Metadata["provider"], transport, sessionName)
	return b, sessionName, nil
}

// ObserveRuntimeForReconcilerWithWitness reloads and validates the original
// trigger witness before any empty-type repair, ACP route mutation, or provider
// observation. The per-session lock remains held through the provider reads.
func (m *Manager) ObserveRuntimeForReconcilerWithWitness(
	id string,
	processNames []string,
	witness LiveBoundaryWitness,
) (Info, RuntimeObservation, error) {
	var info Info
	var observation RuntimeObservation
	err := withSessionMutationLock(id, func() error {
		b, sessionName, err := m.reconcilerWakeSessionBeadWithWitness(id, witness)
		if err != nil {
			return err
		}
		info = infoFromPersistedBead(b)
		info.SessionName = sessionName
		observation = m.ObserveRuntimeForInfo(info, processNames)
		return nil
	})
	return info, observation, err
}

// ObserveRuntimeForReconcilerLifecycleWithWitness reloads and validates the
// original trigger witness for automatic containment actions. Unlike the wake
// observer, it permits an exact suspended row so drain/stop convergence can
// still inspect the runtime selected by that row.
func (m *Manager) ObserveRuntimeForReconcilerLifecycleWithWitness(
	id string,
	processNames []string,
	witness LiveBoundaryWitness,
) (Info, RuntimeObservation, error) {
	var info Info
	var observation RuntimeObservation
	err := withSessionMutationLock(id, func() error {
		b, sessionName, err := m.reconcilerLifecycleSessionBeadWithWitness(id, witness)
		if err != nil {
			return err
		}
		info = infoFromPersistedBead(b)
		info.SessionName = sessionName
		observation = m.ObserveRuntimeForInfo(info, processNames)
		return nil
	})
	return info, observation, err
}

// PeekForReconcilerWithWitness captures output only while the original
// automatic-action witness remains current and wake-eligible. Validation runs
// before repair/routing and the lock stays held through provider Peek.
func (m *Manager) PeekForReconcilerWithWitness(id string, lines int, witness LiveBoundaryWitness) (string, error) {
	var output string
	err := withSessionMutationLock(id, func() error {
		_, sessionName, err := m.reconcilerWakeSessionBeadWithWitness(id, witness)
		if err != nil {
			return err
		}
		output, err = m.sp.Peek(sessionName, lines)
		return err
	})
	return output, err
}

func runningRuntimeMatchesPendingCreateBead(b beads.Bead, sessionName string, sp runtime.Provider) bool {
	if sp == nil {
		return false
	}
	liveID := ""
	if value, err := sp.GetMeta(sessionName, "GC_SESSION_ID"); err == nil {
		liveID = strings.TrimSpace(value)
		if liveID != "" && liveID != b.ID {
			return false
		}
	}
	expectedToken := strings.TrimSpace(b.Metadata["instance_token"])
	liveToken := ""
	if value, err := sp.GetMeta(sessionName, "GC_INSTANCE_TOKEN"); err == nil {
		liveToken = strings.TrimSpace(value)
		if liveToken != "" && liveToken != expectedToken {
			liveGeneration, _ := sp.GetMeta(sessionName, "GC_RUNTIME_EPOCH")
			expectedGeneration := strings.TrimSpace(b.Metadata["generation"])
			if strings.TrimSpace(liveGeneration) != "" && expectedGeneration != "" && strings.TrimSpace(liveGeneration) != expectedGeneration {
				return false
			}
			if liveID == "" {
				return false
			}
		}
	}
	if liveID != "" {
		return liveID == b.ID
	}
	if expectedToken == "" {
		return false
	}
	return liveToken == expectedToken
}

// RelaunchConfigPreparer builds the runtime config for a warm relaunch. The
// manager invokes it only after validating the live-boundary witness under the
// session mutation lock, so preparation may safely persist session-local start
// metadata without racing a trigger clear or repoint in this process.
type RelaunchConfigPreparer func() (runtime.Config, error)

// RelaunchPreparedWithWitness validates authority before config preparation or
// provider delivery and keeps the session mutation lock through both steps.
func (m *Manager) RelaunchPreparedWithWitness(ctx context.Context, id string, witness LiveBoundaryWitness, prepare RelaunchConfigPreparer) error {
	relauncher, ok := m.sp.(runtime.RelaunchProvider)
	if !ok {
		return runtime.ErrRelaunchUnsupported
	}
	return withSessionMutationLock(id, func() error {
		_, sessionName, err := m.reconcilerWakeSessionBeadWithWitness(id, witness)
		if err != nil {
			return err
		}
		cfg, err := prepare()
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
		return relauncher.Relaunch(ctx, sessionName, finalized)
	})
}

// RunLiveWithWitness re-applies live commands only while the authorized
// trigger remains current, finalizing and materializing isolation roots after
// validation and before the provider can execute anything.
func (m *Manager) RunLiveWithWitness(id string, cfg runtime.Config, witness LiveBoundaryWitness) error {
	return withSessionMutationLock(id, func() error {
		_, sessionName, err := m.reconcilerWakeSessionBeadWithWitness(id, witness)
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
		return m.sp.RunLive(sessionName, finalized)
	})
}
