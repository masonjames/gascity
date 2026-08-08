package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

var errStrictSessionWorkStoreMismatch = errors.New("project_hooks=forbid requires the session and canonical city work classes to share one physical store")

// validateStrictSessionWorkStore proves the physical co-location required by
// the strict project-hook contract. The configured storage binding names are
// not authority: two differently wrapped handles may share one carrier, while
// two handles bearing the same bead IDs may own independent ledgers.
func validateStrictSessionWorkStore(sessionStore beads.Store, workStore beads.WorkStore) error {
	if sessionStore == nil || workStore.Store == nil {
		return errStrictSessionWorkStoreMismatch
	}
	if !beads.SameStoreIdentity(sessionStore, workStore.Store) {
		return errStrictSessionWorkStoreMismatch
	}
	return nil
}

func (bp *agentBuildParams) validateStrictStoreIdentity(cfgAgent *config.Agent) error {
	if cfgAgent == nil || !cfgAgent.ForbidsProjectHooks() {
		return nil
	}
	if bp == nil {
		return errStrictSessionWorkStoreMismatch
	}
	return validateStrictSessionWorkStore(bp.beadStore, bp.canonicalCityWorkStore)
}

func cityHasStrictConfiguredAgent(cfg *config.City) bool {
	if cfg == nil {
		return false
	}
	for i := range cfg.Agents {
		if cfg.Agents[i].ForbidsProjectHooks() {
			return true
		}
	}
	return false
}

// strictConfiguredAgentForSession is the conservative boolean classifier used
// by legacy skip-only call sites. A conflict returns a synthetic forbidden
// marker so callers cannot interpret invalid identity evidence as inherit.
// Callers that need an exact configured identity must call
// ResolvePersistedSessionAgent directly and handle its error.
func strictConfiguredAgentForSession(cfg *config.City, info session.Info) *config.Agent {
	resolution, err := agentutil.ResolvePersistedSessionAgent(cfg, info)
	if err != nil {
		return &config.Agent{ProjectHooks: config.ProjectHooksForbid}
	}
	if !resolution.Resolved || !resolution.Strict {
		return nil
	}
	return &resolution.Agent
}

// strictSessionOwnershipAuthorized is the row-level ownership gate. Deferred
// strict rows without a trigger remain durable but inert; unrelated inherit
// rows continue to reconcile. The physical topology check is repeated here so
// direct helpers cannot bypass the composition-root preflight.
func strictSessionOwnershipAuthorized(
	cfg *config.City,
	cityPath, cityName string,
	sessionStore beads.Store,
	workStore beads.WorkStore,
	info session.Info,
) error {
	resolution, err := agentutil.ResolvePersistedSessionAgent(cfg, info)
	if err != nil {
		return fmt.Errorf("session %s configured identity: %w", info.ID, err)
	}
	if !resolution.Resolved || !resolution.Strict {
		return nil
	}
	if err := validateStrictSessionWorkStore(sessionStore, workStore); err != nil {
		return err
	}
	if err := validateProjectHookSessionTrigger(info, canonicalCityDemandStoreRef(cfg, cityPath, cityName)); err != nil {
		return fmt.Errorf("session %s strict trigger witness: %w", info.ID, err)
	}
	return nil
}

func strictStoreTopologyError(cfg *config.City, sessionStore beads.SessionStore, workStore beads.WorkStore) error {
	if !cityHasStrictConfiguredAgent(cfg) {
		return nil
	}
	return validateStrictSessionWorkStore(sessionStore.Store, workStore)
}

func configuredAgentForWorkRoute(cfg *config.City, route string) *config.Agent {
	route = strings.TrimSpace(route)
	if cfg == nil || route == "" {
		return nil
	}
	if normalized := normalizeAgentTemplateIdentity(cfg, route); normalized != "" {
		route = normalized
	}
	return findAgentByTemplate(cfg, route)
}

// strictWorkMutationAuthorized refuses every legacy work-side repair for a
// strict route. Physical co-location proves which ledger owns the row, but it
// does not make a stale List/Get followed by Update/SetMetadata atomic: a
// claim, repoint, or hold can land between those operations. Until each repair
// has an exact conditional-write predicate, strict work remains untouched.
// Unknown and inherit routes preserve their historical behavior.
//
// The typed store parameters remain explicit at the production boundary so a
// future CAS implementation cannot accidentally recover the former ambiguous
// store plumbing. They are intentionally not sufficient authorization today.
func strictWorkMutationAuthorized(
	cfg *config.City,
	route string,
	_ beads.SessionStore,
	_ beads.WorkStore,
	_ beads.Store,
) bool {
	agentCfg := configuredAgentForWorkRoute(cfg, route)
	if agentCfg == nil || !agentCfg.ForbidsProjectHooks() {
		return true
	}
	return false
}

func (bp *agentBuildParams) strictWorkMutationAuthorized(cfgAgent *config.Agent, candidateStore beads.Store) bool {
	if cfgAgent == nil || !cfgAgent.ForbidsProjectHooks() {
		return true
	}
	// The empty-name forbidden marker is returned only for conflicting
	// persisted session identity. It has no route that could safely authorize a
	// work mutation, so deny before configuredAgentForWorkRoute sees an empty
	// compatibility route as unmanaged.
	if strings.TrimSpace(cfgAgent.QualifiedName()) == "" {
		return false
	}
	if bp == nil {
		return false
	}
	return strictWorkMutationAuthorized(
		bp.city,
		cfgAgent.QualifiedName(),
		beads.SessionStore{Store: bp.beadStore},
		bp.canonicalCityWorkStore,
		candidateStore,
	)
}

func (bp *agentBuildParams) strictRouteMutationAuthorized(route string, candidateStore beads.Store) bool {
	if bp == nil {
		return true
	}
	return strictWorkMutationAuthorized(
		bp.city,
		route,
		beads.SessionStore{Store: bp.beadStore},
		bp.canonicalCityWorkStore,
		candidateStore,
	)
}

// filterStrictUnauthorizedSessionSnapshot removes configured strict rows that
// are not authorized for ownership/lifecycle mutation. It never removes
// inherit or unconfigured containment rows.
func filterStrictUnauthorizedSessionSnapshot(
	cfg *config.City,
	cityPath, cityName string,
	sessionStore beads.SessionStore,
	workStore beads.WorkStore,
	snapshot *sessionBeadSnapshot,
) *sessionBeadSnapshot {
	if snapshot == nil {
		return snapshot
	}
	rows := snapshot.OpenForReconcile()
	filtered := make([]session.ReconcileSession, 0, len(rows))
	withheld := false
	for _, row := range rows {
		info := row.Info
		if err := strictSessionOwnershipAuthorized(cfg, cityPath, cityName, sessionStore.Store, workStore, info); err != nil {
			withheld = true
			continue
		}
		filtered = append(filtered, row)
	}
	if !withheld {
		return snapshot
	}
	result := newSessionBeadSnapshotFromReconcileRows(filtered)
	snapshot.mu.RLock()
	result.loadErr = snapshot.loadErr
	result.fingerprint = snapshot.fingerprint
	snapshot.mu.RUnlock()
	return result
}

// filterStrictTopologyUnauthorizedSessionSnapshot is the build/refresh
// boundary: on a physical SESSION/WORK split it withholds only configured
// strict rows, while a co-located triggerless strict row remains visible to
// the later guarded-assignment finalizer. Exact trigger eligibility belongs to
// that per-row locked boundary; lifecycle callers retain the stricter filter
// above.
func filterStrictTopologyUnauthorizedSessionSnapshot(
	cfg *config.City,
	sessionStore beads.SessionStore,
	workStore beads.WorkStore,
	snapshot *sessionBeadSnapshot,
) (*sessionBeadSnapshot, bool) {
	if snapshot == nil {
		return snapshot, false
	}
	topologyErr := strictStoreTopologyError(cfg, sessionStore, workStore)
	rows := snapshot.OpenForReconcile()
	filtered := make([]session.ReconcileSession, 0, len(rows))
	withheld := false
	for _, row := range rows {
		resolution, err := agentutil.ResolvePersistedSessionAgent(cfg, row.Info)
		if err != nil || (resolution.Resolved && resolution.Strict && topologyErr != nil) {
			withheld = true
			continue
		}
		filtered = append(filtered, row)
	}
	if !withheld {
		return snapshot, false
	}
	result := newSessionBeadSnapshotFromReconcileRows(filtered)
	snapshot.mu.RLock()
	result.loadErr = snapshot.loadErr
	result.fingerprint = snapshot.fingerprint
	snapshot.mu.RUnlock()
	return result, true
}

func (cr *CityRuntime) strictOwnershipSnapshot(snapshot *sessionBeadSnapshot) *sessionBeadSnapshot {
	if cr == nil {
		return snapshot
	}
	return filterStrictUnauthorizedSessionSnapshot(
		cr.cfg,
		cr.cityPath,
		cr.cityName,
		cr.sessionsBeadStore(),
		cr.cityWorkStore(),
		snapshot,
	)
}
