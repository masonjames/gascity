package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

type strictPoolConcreteIdentityProjection struct {
	Type                          string
	Title                         string
	Template                      string
	CommonName                    string
	AgentName                     string
	Labels                        string
	Alias                         string
	AliasHistory                  string
	PoolAliasConflict             string
	PoolAliasConflictCount        string
	PoolAliasConflictAt           string
	PoolSlot                      string
	CanonicalInstanceNameMetadata string
	CanonicalPoolSlotMetadata     string
	SessionNameMetadata           string
	SessionNameExplicit           string
	InstanceToken                 string
	ConfiguredNamedIdentity       string
	ConfiguredNamedMode           string
	ConfiguredNamedSession        bool
	ManualSessionMetadata         string
	PoolManaged                   bool
	DependencyOnlyMetadata        string
	SessionOrigin                 string
	State                         session.State
	MetadataState                 string
	Closed                        bool
}

func strictPoolConcreteIdentity(info session.Info) strictPoolConcreteIdentityProjection {
	labels := make([]string, 0, len(info.Labels))
	for _, label := range info.Labels {
		label = strings.TrimSpace(label)
		if label != "" {
			labels = append(labels, label)
		}
	}
	sort.Strings(labels)
	return strictPoolConcreteIdentityProjection{
		Type:                          info.Type,
		Title:                         info.Title,
		Template:                      info.Template,
		CommonName:                    info.CommonName,
		AgentName:                     info.AgentName,
		Labels:                        strings.Join(labels, "\x00"),
		Alias:                         info.Alias,
		AliasHistory:                  strings.Join(info.AliasHistory, "\x00"),
		PoolAliasConflict:             info.PoolAliasConflict,
		PoolAliasConflictCount:        info.PoolAliasConflictCount,
		PoolAliasConflictAt:           info.PoolAliasConflictAt,
		PoolSlot:                      info.PoolSlot,
		CanonicalInstanceNameMetadata: info.CanonicalInstanceNameMetadata,
		CanonicalPoolSlotMetadata:     info.CanonicalPoolSlotMetadata,
		SessionNameMetadata:           info.SessionNameMetadata,
		SessionNameExplicit:           info.SessionNameExplicit,
		InstanceToken:                 info.InstanceToken,
		ConfiguredNamedIdentity:       info.ConfiguredNamedIdentity,
		ConfiguredNamedMode:           info.ConfiguredNamedMode,
		ConfiguredNamedSession:        info.ConfiguredNamedSession,
		ManualSessionMetadata:         info.ManualSessionMetadata,
		PoolManaged:                   info.PoolManaged,
		DependencyOnlyMetadata:        info.DependencyOnlyMetadata,
		SessionOrigin:                 info.SessionOrigin,
		State:                         info.State,
		MetadataState:                 info.MetadataState,
		Closed:                        info.Closed,
	}
}

type strictPoolCommitProjection struct {
	Identity            strictPoolConcreteIdentityProjection
	TriggerBeadID       string
	TriggerBeadStoreRef string
	BrainParentSID      string
	Pack                string
	PackWorkspace       string
	WorkDir             string
	WorkDirCanonical    string
	WorkerDir           string
}

func strictPoolCommitState(info session.Info) strictPoolCommitProjection {
	return strictPoolCommitProjection{
		Identity:            strictPoolConcreteIdentity(info),
		TriggerBeadID:       info.TriggerBeadID,
		TriggerBeadStoreRef: info.TriggerBeadStoreRef,
		BrainParentSID:      info.BrainParentSID,
		Pack:                info.Pack,
		PackWorkspace:       info.PackWorkspace,
		WorkDir:             info.WorkDir,
		WorkDirCanonical:    info.WorkDirCanonical,
		WorkerDir:           info.WorkerDir,
	}
}

// validateStrictCapturedPoolSession revalidates the exact ownership witness
// selected from the build snapshot against one fresh persisted read. It is
// read-only and must run while the caller holds SessionMutationLock.
func validateStrictCapturedPoolSession(
	bp *agentBuildParams,
	cfgAgent *config.Agent,
	template string,
	captured session.Info,
	current session.Info,
	dependencyOnly bool,
) error {
	if strings.TrimSpace(captured.ID) == "" || current.ID != captured.ID {
		return fmt.Errorf("strict pool reuse session identity changed from %q to %q", captured.ID, current.ID)
	}
	witness := session.LiveBoundaryWitness{
		TriggerBeadID:       captured.TriggerBeadID,
		TriggerBeadStoreRef: captured.TriggerBeadStoreRef,
	}
	if err := session.ValidateReconcilerWakeInfo(current, witness); err != nil {
		return err
	}
	if strictPoolConcreteIdentity(current) != strictPoolConcreteIdentity(captured) {
		return fmt.Errorf("%w: session %q concrete identity changed after strict pool selection", session.ErrLiveBoundaryWitnessMismatch, captured.ID)
	}
	if resolvedSessionTemplateInfo(current, reuseTemplateConfig(bp)) != template {
		return fmt.Errorf("%w: session %q no longer resolves to pool template %q", session.ErrLiveBoundaryWitnessMismatch, captured.ID, template)
	}
	if isFailedCreateSessionInfo(current) || isDrainedSessionInfo(current) || strings.TrimSpace(current.MetadataState) == string(session.StateDraining) {
		return fmt.Errorf("%w: session %q is no longer reusable", session.ErrLiveBoundaryWitnessMismatch, captured.ID)
	}
	if !strictSingletonActorIdentityStable(cfgAgent, current) {
		return fmt.Errorf("%w: session %q has stale singleton actor identity", session.ErrLiveBoundaryWitnessMismatch, captured.ID)
	}
	if dependencyOnly && !reusableDependencyPoolSessionInfo(bp, template, current) {
		return fmt.Errorf("%w: session %q is no longer an eligible dependency floor", session.ErrLiveBoundaryWitnessMismatch, captured.ID)
	}
	return nil
}

// finalizeStrictPoolSessionReuse keeps every SESSION-class mutation for an
// existing strict pool row behind one exact persisted witness. The guarded
// WORK claim may call into a different backend capability but remains inside
// the same session lock; a second persisted read catches any backend/external
// drift that occurred at the claim boundary before normalization or binding.
func finalizeStrictPoolSessionReuse(
	bp *agentBuildParams,
	cfgAgent *config.Agent,
	template string,
	captured session.Info,
	slot int,
	request SessionRequest,
) (session.Info, error) {
	if bp == nil || bp.beadStore == nil {
		return captured, fmt.Errorf("strict pool reuse session store unavailable")
	}
	if _, ok := beads.ConditionalWriterFor(bp.beadStore); !ok {
		return captured, beads.ErrConditionalWriteUnsupported
	}
	front := sessionFrontDoor(bp.beadStore)
	var finalized session.Info
	err := session.WithSessionMutationLock(captured.ID, func() error {
		current, persisted, err := front.GetPersistedResponse(captured.ID)
		if err != nil {
			return err
		}
		if err := session.RequireNoConditionalMutationLease(persisted); err != nil {
			return fmt.Errorf("strict pool reuse session %q: %w", captured.ID, err)
		}
		if err := validateStrictCapturedPoolSession(bp, cfgAgent, template, captured, current, false); err != nil {
			return err
		}

		manualSession := isManualSessionInfoForAgent(current, cfgAgent)
		qualifiedInstance := ""
		if manualSession {
			qualifiedInstance = sessionBeadQualifiedNameInfo(bp.cityPath, cfgAgent, bp.rigs, current)
		} else {
			_, qualifiedInstance, _ = poolDesiredRequestIdentity(cfgAgent, slot)
		}
		attestedWorkDir, err := preflightForbiddenPoolWorkDir(bp, cfgAgent, qualifiedInstance, &current)
		if err != nil {
			return err
		}

		if strings.TrimSpace(request.WorkBeadID) != "" {
			if _, err := claimPoolRequestWorkWithExactSession(bp, current, &persisted, request); err != nil {
				return err
			}
			// GuardedAssignmentClaimer is allowed to be a backend adapter. Reload
			// after it returns so a clear/repoint/suspend injected at that boundary
			// cannot reach either SESSION normalization or trigger binding.
			current, persisted, err = front.GetPersistedResponse(captured.ID)
			if err != nil {
				return err
			}
			if err := session.RequireNoConditionalMutationLease(persisted); err != nil {
				return fmt.Errorf("strict pool reuse session %q after work claim: %w", captured.ID, err)
			}
			if err := validateStrictCapturedPoolSession(bp, cfgAgent, template, captured, current, false); err != nil {
				return err
			}
		}

		normalizedExpected, err := normalizeNonExpandingPoolSessionInfoForSelectionAtPersistedLocked(bp, cfgAgent, current, persisted)
		if err != nil {
			return err
		}
		// Reload after the CAS (or no-op) so trigger binding carries the newest
		// authoritative revision and never writes from the folded projection.
		current, persisted, err = front.GetPersistedResponse(captured.ID)
		if err != nil {
			return err
		}
		if err := session.RequireNoConditionalMutationLease(persisted); err != nil {
			return fmt.Errorf("strict pool reuse session %q before trigger binding: %w", captured.ID, err)
		}
		if err := validateStrictCapturedPoolSession(bp, cfgAgent, template, normalizedExpected, current, false); err != nil {
			return err
		}
		boundExpected, err := bindPoolSessionTriggerBeadAtPersistedWithWorkDirLocked(bp, current, request, attestedWorkDir, persisted)
		if err != nil {
			return err
		}
		var finalizedPersisted session.PersistedResponse
		finalized, finalizedPersisted, err = front.GetPersistedResponse(captured.ID)
		if err != nil {
			return err
		}
		if err := session.RequireNoConditionalMutationLease(finalizedPersisted); err != nil {
			return fmt.Errorf("strict pool reuse session %q after trigger binding: %w", captured.ID, err)
		}
		if strictPoolCommitState(finalized) != strictPoolCommitState(boundExpected) {
			return fmt.Errorf("%w: session %q changed after strict trigger-binding commit", session.ErrLiveBoundaryWitnessMismatch, captured.ID)
		}
		return nil
	})
	if err != nil {
		return captured, err
	}
	return finalized, nil
}

// finalizeStrictDependencyPoolSession validates and normalizes an existing
// strict dependency-floor row under one SessionMutationLock. It performs no
// work claim or trigger bind: a dependency floor is authorized only by the
// exact complete trigger pair already captured on that row.
func finalizeStrictDependencyPoolSession(
	bp *agentBuildParams,
	cfg *config.City,
	cfgAgent *config.Agent,
	template string,
	captured session.Info,
) (session.Info, error) {
	if bp == nil || bp.beadStore == nil {
		return captured, fmt.Errorf("strict dependency-floor session store unavailable")
	}
	if _, ok := beads.ConditionalWriterFor(bp.beadStore); !ok {
		return captured, beads.ErrConditionalWriteUnsupported
	}
	front := sessionFrontDoor(bp.beadStore)
	var finalized session.Info
	err := session.WithSessionMutationLock(captured.ID, func() error {
		current, persisted, err := front.GetPersistedResponse(captured.ID)
		if err != nil {
			return err
		}
		if err := session.RequireNoConditionalMutationLease(persisted); err != nil {
			return fmt.Errorf("strict dependency-floor session %q: %w", captured.ID, err)
		}
		if err := validateStrictCapturedPoolSession(bp, cfgAgent, template, captured, current, true); err != nil {
			return err
		}
		if err := validateProjectHookSessionTrigger(current, canonicalCityDemandStoreRef(cfg, bp.cityPath, bp.cityName)); err != nil {
			return err
		}
		normalizedExpected, err := normalizeNonExpandingPoolSessionInfoForSelectionAtPersistedLocked(bp, cfgAgent, current, persisted)
		if err != nil {
			return err
		}
		var finalizedPersisted session.PersistedResponse
		finalized, finalizedPersisted, err = front.GetPersistedResponse(captured.ID)
		if err != nil {
			return err
		}
		if err := session.RequireNoConditionalMutationLease(finalizedPersisted); err != nil {
			return fmt.Errorf("strict dependency-floor session %q after normalization: %w", captured.ID, err)
		}
		return validateStrictCapturedPoolSession(bp, cfgAgent, template, normalizedExpected, finalized, true)
	})
	if err != nil {
		return captured, err
	}
	return finalized, nil
}
