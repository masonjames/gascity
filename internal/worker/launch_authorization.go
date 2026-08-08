package worker

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
)

// ErrLaunchUnauthorized reports that a live session boundary lacked the
// persisted authority required by its current configuration.
var ErrLaunchUnauthorized = errors.New("worker launch is unauthorized")

// RequireExactSessionTriggerAuthority validates that req names a persisted
// session carrying one complete trigger pair in the canonical city session
// store. It does not query or mutate the trigger bead; exact bead ownership is
// revalidated atomically by the claim boundary before any work mutation.
func RequireExactSessionTriggerAuthority(req LaunchAuthorizationRequest, cityStoreRef string) (LaunchAuthorization, error) {
	cityStoreRef = strings.TrimSpace(cityStoreRef)
	if req.Info == nil || strings.TrimSpace(req.Info.ID) == "" {
		return LaunchAuthorization{}, fmt.Errorf("%w: project-hooks isolation requires a persisted session row", ErrLaunchUnauthorized)
	}
	beadID := strings.TrimSpace(req.Info.TriggerBeadID)
	storeRef := strings.TrimSpace(req.Info.TriggerBeadStoreRef)
	if beadID == "" || storeRef == "" {
		return LaunchAuthorization{}, fmt.Errorf("%w: session %q lacks a complete exact trigger pair", ErrLaunchUnauthorized, req.Info.ID)
	}
	if strings.ContainsAny(beadID, " \t\r\n") {
		return LaunchAuthorization{}, fmt.Errorf("%w: session %q has a malformed trigger bead id", ErrLaunchUnauthorized, req.Info.ID)
	}
	if cityStoreRef == "" || storeRef != cityStoreRef {
		return LaunchAuthorization{}, fmt.Errorf(
			"%w: session %q trigger store %q is not the canonical city session store %q",
			ErrLaunchUnauthorized,
			req.Info.ID,
			storeRef,
			cityStoreRef,
		)
	}
	if !beads.SameStoreIdentity(req.SessionStore, req.CanonicalCityStore) {
		return LaunchAuthorization{}, fmt.Errorf(
			"%w: session %q is not persisted in the canonical city work store",
			ErrLaunchUnauthorized,
			req.Info.ID,
		)
	}
	return LaunchAuthorization{TriggerBeadID: beadID, TriggerBeadStoreRef: storeRef}, nil
}

// BindProjectHookIsolationRuntime binds the current resolved provider family
// and canonical cwd to an exact-trigger authorization. It fails closed when a
// forbid policy cannot produce a complete enforcement envelope.
func BindProjectHookIsolationRuntime(authorization LaunchAuthorization, resolved *ResolvedRuntime) (LaunchAuthorization, error) {
	if resolved == nil || !resolved.Hints.ProjectHooksForbidden {
		return LaunchAuthorization{}, fmt.Errorf("%w: current project-hook isolation runtime is unavailable", ErrLaunchUnauthorized)
	}
	providerName := strings.TrimSpace(resolved.Hints.ProviderName)
	workDir := strings.TrimSpace(resolved.WorkDir)
	if workDir == "" {
		workDir = strings.TrimSpace(resolved.Hints.WorkDir)
	}
	if providerName == "" || workDir == "" {
		return LaunchAuthorization{}, fmt.Errorf("%w: current project-hook isolation runtime lacks provider family or canonical work_dir", ErrLaunchUnauthorized)
	}
	authorization.RuntimeEnforcement = &LaunchRuntimeEnforcement{
		ProjectHooksForbidden: true,
		ProviderName:          providerName,
		ProviderOverlayName:   strings.TrimSpace(resolved.Hints.ProviderOverlayName),
		WorkDir:               workDir,
	}
	return authorization, nil
}
