package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

// AutomaticRuntimeDecision is the same-read persisted decision required by
// strict reconciler provider effects. Factory replaces ValidatePolicy with its
// own current-config resolver; callers cannot supply policy authority.
type AutomaticRuntimeDecision = sessionpkg.AutomaticRuntimeDecision

// AutomaticRuntimeCommit is the lease-advanced persisted snapshot returned by
// a successful strict reconciler provider effect.
type AutomaticRuntimeCommit = sessionpkg.AutomaticRuntimeCommit

// StartPreparedForReconciler crosses the provider start boundary only under an
// exact persisted revision, current configured policy, and runtime identity.
func (f *Factory) StartPreparedForReconciler(
	ctx context.Context,
	id string,
	command string,
	hints runtime.Config,
	decision AutomaticRuntimeDecision,
) (PreparedStartResult, error) {
	decision, authorization, err := f.prepareAutomaticRuntimeDecision(ctx, id, decision)
	if err != nil {
		return PreparedStartResult{}, err
	}
	started, err := f.manager.StartPreparedRuntimeOnlyForReconciler(
		ctx,
		id,
		command,
		automaticRuntimeConfigWithAuthorization(hints, authorization),
		decision,
	)
	return PreparedStartResult{
		Attempted:       started.Attempted,
		Recycled:        started.Recycled,
		RecycleDuration: started.RecycleDuration,
		Commit:          started.Commit,
	}, normalizeLiveBoundaryError(err)
}

// RunLiveForReconciler applies live configuration only to the exact persisted
// runtime selected by decision and returns its lease-advanced revision.
func (f *Factory) RunLiveForReconciler(
	ctx context.Context,
	id string,
	cfg runtime.Config,
	decision AutomaticRuntimeDecision,
) (AutomaticRuntimeCommit, error) {
	decision, authorization, err := f.prepareAutomaticRuntimeDecision(ctx, id, decision)
	if err != nil {
		return AutomaticRuntimeCommit{}, err
	}
	commit, err := f.manager.RunLiveForReconciler(
		ctx,
		id,
		automaticRuntimeConfigWithAuthorization(cfg, authorization),
		decision,
	)
	return commit, normalizeLiveBoundaryError(err)
}

// RelaunchForReconciler warm-relaunches only the exact persisted runtime
// selected by decision. prepared must be built before this call without a
// callback that reacquires the session mutation lock.
func (f *Factory) RelaunchForReconciler(
	ctx context.Context,
	id string,
	prepared runtime.Config,
	decision AutomaticRuntimeDecision,
) (AutomaticRuntimeCommit, error) {
	decision, authorization, err := f.prepareAutomaticRuntimeDecision(ctx, id, decision)
	if err != nil {
		return AutomaticRuntimeCommit{}, err
	}
	commit, err := f.manager.RelaunchForReconciler(
		ctx,
		id,
		automaticRuntimeConfigWithAuthorization(prepared, authorization),
		decision,
	)
	return commit, normalizeLiveBoundaryError(err)
}

// NudgeSessionForReconciler commits markerPatch before delivering one exact-
// incarnation live nudge, all under the same persisted decision lease.
func (f *Factory) NudgeSessionForReconciler(
	ctx context.Context,
	id string,
	text string,
	source string,
	decision AutomaticRuntimeDecision,
	markerPatch map[string]string,
) (bool, AutomaticRuntimeCommit, error) {
	_ = strings.TrimSpace(source) // retained for caller-side delivery attribution
	decision, _, err := f.prepareAutomaticRuntimeDecision(ctx, id, decision)
	if err != nil {
		return false, AutomaticRuntimeCommit{}, err
	}
	delivered, commit, err := f.manager.NudgeForReconciler(ctx, id, text, decision, markerPatch)
	return delivered, commit, normalizeLiveBoundaryError(err)
}

// SetRuntimeMetadataForReconciler mutates one exact runtime metadata key under
// the same current-config and persisted decision checks as other effects.
func (f *Factory) SetRuntimeMetadataForReconciler(
	ctx context.Context,
	id string,
	key string,
	value string,
	decision AutomaticRuntimeDecision,
) (AutomaticRuntimeCommit, error) {
	decision, _, err := f.prepareAutomaticRuntimeDecision(ctx, id, decision)
	if err != nil {
		return AutomaticRuntimeCommit{}, err
	}
	commit, err := f.manager.SetRuntimeMetadataForReconciler(ctx, id, key, value, decision)
	return commit, normalizeLiveBoundaryError(err)
}

// RemoveRuntimeMetadataForReconciler removes one exact runtime metadata key
// under the same current-config and persisted decision checks as other effects.
func (f *Factory) RemoveRuntimeMetadataForReconciler(
	ctx context.Context,
	id string,
	key string,
	decision AutomaticRuntimeDecision,
) (AutomaticRuntimeCommit, error) {
	decision, _, err := f.prepareAutomaticRuntimeDecision(ctx, id, decision)
	if err != nil {
		return AutomaticRuntimeCommit{}, err
	}
	commit, err := f.manager.RemoveRuntimeMetadataForReconciler(ctx, id, key, decision)
	return commit, normalizeLiveBoundaryError(err)
}

// ObserveSessionWithDecisionForReconciler performs a pure provider observation
// bracketed by the exact persisted decision. lifecycle admits containment
// states; false applies automatic wake eligibility.
func (f *Factory) ObserveSessionWithDecisionForReconciler(
	ctx context.Context,
	id string,
	processNames []string,
	decision AutomaticRuntimeDecision,
	lifecycle bool,
) (LiveObservation, error) {
	if err := ctx.Err(); err != nil {
		return LiveObservation{}, err
	}
	decision, _, err := f.prepareAutomaticRuntimeDecision(ctx, id, decision)
	if err != nil {
		return LiveObservation{}, err
	}
	info, runtimeObs, err := f.manager.ObserveRuntimeForReconciler(id, processNames, decision, lifecycle)
	if err != nil {
		return LiveObservation{}, normalizeLiveBoundaryError(err)
	}
	return liveObservationFromSession(info, runtimeObs), nil
}

// PeekSessionWithDecisionForReconciler performs a pure provider read and
// discards the output if the exact persisted decision changes during Peek.
func (f *Factory) PeekSessionWithDecisionForReconciler(
	ctx context.Context,
	id string,
	lines int,
	decision AutomaticRuntimeDecision,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	decision, _, err := f.prepareAutomaticRuntimeDecision(ctx, id, decision)
	if err != nil {
		return "", err
	}
	output, err := f.manager.PeekForReconciler(id, lines, decision)
	return output, normalizeLiveBoundaryError(err)
}

func (f *Factory) prepareAutomaticRuntimeDecision(
	ctx context.Context,
	id string,
	decision AutomaticRuntimeDecision,
) (AutomaticRuntimeDecision, LaunchAuthorization, error) {
	if f == nil || f.manager == nil {
		return AutomaticRuntimeDecision{}, LaunchAuthorization{}, fmt.Errorf("%w: automatic runtime factory is unavailable", ErrLaunchUnauthorized)
	}
	id = strings.TrimSpace(id)
	if id == "" || strings.TrimSpace(decision.Captured.ID) != id || !decision.RevisionCaptured {
		return AutomaticRuntimeDecision{}, LaunchAuthorization{}, fmt.Errorf("%w: incomplete automatic runtime decision for %q", ErrLaunchUnauthorized, id)
	}
	current, persisted, err := f.manager.PersistedStore().GetPersistedResponse(id)
	if err != nil {
		return AutomaticRuntimeDecision{}, LaunchAuthorization{}, err
	}
	if persisted.Revision != decision.ExpectedRevision {
		return AutomaticRuntimeDecision{}, LaunchAuthorization{}, fmt.Errorf(
			"%w: session %q revision changed from %d to %d",
			sessionpkg.ErrConditionalMutationLost,
			id,
			decision.ExpectedRevision,
			persisted.Revision,
		)
	}
	if err := sessionpkg.RequireNoConditionalMutationLease(persisted); err != nil {
		if errors.Is(err, sessionpkg.ErrConditionalMutationHeld) {
			err = errors.Join(sessionpkg.ErrAutomaticRuntimeRecoveryBlocked, err)
		}
		return AutomaticRuntimeDecision{}, LaunchAuthorization{}, err
	}
	authorization, err := f.validateAutomaticRuntimePolicy(ctx, current, persisted, decision)
	if err != nil {
		return AutomaticRuntimeDecision{}, LaunchAuthorization{}, err
	}
	decision.ValidatePolicy = func(info sessionpkg.Info, persisted sessionpkg.PersistedResponse, expectedStrict bool) error {
		currentDecision := decision
		currentDecision.ProjectHooksForbidden = expectedStrict
		currentAuthorization, err := f.validateAutomaticRuntimePolicy(ctx, info, persisted, currentDecision)
		if err != nil {
			return err
		}
		return validateAutomaticRuntimeAuthorizationUnchanged(info.ID, authorization, currentAuthorization)
	}
	return decision, authorization, nil
}

func (f *Factory) validateAutomaticRuntimePolicy(
	ctx context.Context,
	info sessionpkg.Info,
	persisted sessionpkg.PersistedResponse,
	decision AutomaticRuntimeDecision,
) (LaunchAuthorization, error) {
	resolution, err := agentutil.ResolvePersistedSessionAgent(f.cityConfig, info)
	if err != nil {
		return LaunchAuthorization{}, fmt.Errorf("%w: resolving current persisted session policy: %w", ErrLaunchUnauthorized, err)
	}
	if !resolution.Resolved {
		return LaunchAuthorization{}, fmt.Errorf("%w: session %q has no unique configured identity", ErrLaunchUnauthorized, info.ID)
	}
	if resolution.Strict != decision.ProjectHooksForbidden {
		return LaunchAuthorization{}, fmt.Errorf(
			"%w: session %q current project-hook policy strict=%v differs from captured strict=%v",
			ErrLaunchUnauthorized,
			info.ID,
			resolution.Strict,
			decision.ProjectHooksForbidden,
		)
	}
	authorization, err := f.authorizePersistedRecordWithContext(ctx, info, persisted)
	if err != nil {
		return LaunchAuthorization{}, err
	}
	if err := requireReconcilerExpectedWitness(decision.Witness, authorization); err != nil {
		return LaunchAuthorization{}, err
	}
	if enforcement := authorization.RuntimeEnforcement; enforcement != nil {
		if enforcement.ProjectHooksForbidden != decision.ProjectHooksForbidden {
			return LaunchAuthorization{}, fmt.Errorf("%w: session %q runtime enforcement policy changed", ErrLaunchUnauthorized, info.ID)
		}
	} else if decision.ProjectHooksForbidden {
		return LaunchAuthorization{}, fmt.Errorf("%w: session %q lacks strict runtime enforcement", ErrLaunchUnauthorized, info.ID)
	}
	return authorization, nil
}

func automaticRuntimeConfigWithAuthorization(cfg runtime.Config, authorization LaunchAuthorization) runtime.Config {
	cfg = cloneRuntimeConfig(cfg)
	if enforcement := authorization.RuntimeEnforcement; enforcement != nil {
		cfg.ProjectHooksForbidden = enforcement.ProjectHooksForbidden
		cfg.ProviderName = strings.TrimSpace(enforcement.ProviderName)
		cfg.ProviderOverlayName = strings.TrimSpace(enforcement.ProviderOverlayName)
		cfg.WorkDir = strings.TrimSpace(enforcement.WorkDir)
	}
	triggerID := strings.TrimSpace(authorization.TriggerBeadID)
	storeRef := strings.TrimSpace(authorization.TriggerBeadStoreRef)
	if triggerID == "" || storeRef == "" {
		return cfg
	}
	if cfg.Env == nil {
		cfg.Env = make(map[string]string)
	}
	cfg.Env["GC_TRIGGER_BEAD_ID"] = triggerID
	cfg.Env["GC_TRIGGER_WORK_BEAD_ID"] = triggerID
	cfg.Env["GC_TRIGGER_BEAD_STORE_REF"] = storeRef
	cfg.Env["GC_TRIGGER_WORK_STORE_REF"] = storeRef
	return cfg
}

func validateAutomaticRuntimeAuthorizationUnchanged(id string, expected, current LaunchAuthorization) error {
	if strings.TrimSpace(expected.TriggerBeadID) != strings.TrimSpace(current.TriggerBeadID) ||
		strings.TrimSpace(expected.TriggerBeadStoreRef) != strings.TrimSpace(current.TriggerBeadStoreRef) {
		return fmt.Errorf("%w: session %q trigger authorization changed", ErrLaunchUnauthorized, id)
	}
	if (expected.RuntimeEnforcement == nil) != (current.RuntimeEnforcement == nil) {
		return fmt.Errorf("%w: session %q runtime enforcement presence changed", ErrLaunchUnauthorized, id)
	}
	if expected.RuntimeEnforcement == nil {
		return nil
	}
	left := *expected.RuntimeEnforcement
	right := *current.RuntimeEnforcement
	left.ProviderName = strings.TrimSpace(left.ProviderName)
	left.ProviderOverlayName = strings.TrimSpace(left.ProviderOverlayName)
	left.WorkDir = strings.TrimSpace(left.WorkDir)
	right.ProviderName = strings.TrimSpace(right.ProviderName)
	right.ProviderOverlayName = strings.TrimSpace(right.ProviderOverlayName)
	right.WorkDir = strings.TrimSpace(right.WorkDir)
	if left != right {
		return fmt.Errorf("%w: session %q provider enforcement changed", ErrLaunchUnauthorized, id)
	}
	return nil
}
