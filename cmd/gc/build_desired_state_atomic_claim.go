package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

// strictSingletonActorIdentityStable rejects a legacy singleton whose
// normalization would rewrite an identity used to address or own work. The
// inherited policy retains legacy collapse behavior; strict routed work fails
// closed with both rows untouched.
func strictSingletonActorIdentityStable(cfgAgent *config.Agent, info session.Info) bool {
	if cfgAgent == nil || !cfgAgent.UsesCanonicalSingletonPoolIdentity() || isManualSessionInfoForAgent(info, cfgAgent) || isNamedSessionInfo(info) {
		return true
	}
	canonical := cfgAgent.QualifiedName()
	if slot := nonExpandingPoolIdentitySlot(cfgAgent, sessionBeadAgentNameInfo(info)); slot > 0 && strings.TrimSpace(info.AgentName) != canonical {
		return false
	}
	alias := strings.TrimSpace(info.Alias)
	if slot := nonExpandingPoolIdentitySlot(cfgAgent, alias); slot > 0 && alias != canonical {
		return false
	}
	if alias == "" && strings.TrimSpace(info.PoolAliasConflict) == canonical {
		return false
	}
	return true
}

func executePlannedForbiddenPoolSessionCreateAndClaim(
	bp *agentBuildParams,
	cfgAgent *config.Agent,
	template string,
	plan poolSessionCreatePlan,
	request SessionRequest,
) (session.Info, error) {
	fail := func(err error) (session.Info, error) {
		bp.releasePoolSessionCreate()
		return session.Info{}, err
	}
	if bp == nil || bp.beadStore == nil {
		return fail(fmt.Errorf("creating guarded pool session for %q: store unavailable", template))
	}
	if err := validateAgentSessionTransportForBuild(bp, cfgAgent, plan.qualifiedInstance); err != nil {
		return fail(err)
	}
	target, err := loadPoolRequestWorkTarget(bp, request)
	if err != nil {
		return fail(err)
	}
	if target.createClaimer == nil {
		return fail(beads.ErrCreateAssignmentClaimUnsupported)
	}
	resolvedTmuxAlias, err := bp.resolveTmuxAliasForAgent(cfgAgent)
	if err != nil {
		return fail(err)
	}
	resolvedTmuxAlias, err = validateResolvedPoolTmuxAlias(template, resolvedTmuxAlias)
	if err != nil {
		return fail(err)
	}
	instanceToken := session.NewInstanceToken()
	sessionName := pendingPoolSessionName(template, instanceToken)
	if resolvedTmuxAlias != "" {
		sessionName = resolvedTmuxAlias
	}
	alias := strings.TrimSpace(plan.qualifiedInstance)
	if alias == "" {
		return fail(errors.New("guarded pool session has an empty concrete alias"))
	}
	workDir := strings.TrimSpace(plan.workDir)
	if workDir == "" {
		return fail(errors.New("guarded pool session has an empty attested cwd"))
	}
	identityMetadata := maps.Clone(plan.metadata)
	if identityMetadata == nil {
		identityMetadata = make(map[string]string)
	}
	for key, value := range poolTriggerMetadataForWorkDir(request, workDir) {
		identityMetadata[key] = value
	}
	identity := poolSessionCreateIdentity{
		AgentName: plan.qualifiedInstance,
		Alias:     alias,
		Slot:      plan.slot,
		Metadata:  identityMetadata,
	}
	spec := preparePoolSessionCreateSpec(
		template,
		poolSessionCreateStartedAt(bp),
		identity,
		instanceToken,
		sessionName,
		"",
	)
	witness := session.BeadForCreateSpec(spec)
	claimReq := beads.CreateAssignmentClaimRequest{
		Claim: beads.AssignmentClaimRequest{
			ID:               strings.TrimSpace(request.WorkBeadID),
			Actor:            alias,
			ExpectedStatus:   "open",
			ExpectedAssignee: "",
			ExpectedMetadata: target.expectedMetadata,
			ForbiddenLabels:  beadmeta.DispatchHoldLabels,
			AssignmentMetadata: map[string]string{
				beadmeta.SessionNameMetadataKey:          sessionName,
				beadmeta.SessionInstanceTokenMetadataKey: instanceToken,
			},
		},
		Witness:                      witness,
		CreatedWitnessIDMetadataKeys: []string{beadmeta.SessionIDMetadataKey},
	}

	lockIDs := []string{alias, sessionName}
	var result beads.CreateAssignmentClaimResult
	var won bool
	err = session.WithCitySessionIdentifierLocks(bp.cityPath, lockIDs, func() error {
		if err := session.EnsureAliasAvailableWithConfig(bp.beadStore, bp.city, alias, ""); err != nil {
			return fmt.Errorf("guarded pool alias %q is unavailable: %w", alias, err)
		}
		if err := ensurePoolSessionNameAvailable(bp.beadStore, bp.city, bp.sessionBeads, sessionName, ""); err != nil {
			return fmt.Errorf("guarded pool session_name %q is unavailable: %w", sessionName, err)
		}
		var claimErr error
		result, won, claimErr = target.createClaimer.CreateAssignmentClaim(context.Background(), claimReq)
		return claimErr
	})
	if err != nil {
		return fail(err)
	}
	if !won {
		return fail(errors.New("atomic fresh witness/assignment predicates did not match"))
	}
	info := session.InfoFromPersistedBead(result.Created)
	if info.ID == "" || info.Type != session.BeadType || info.SessionNameMetadata != sessionName || info.InstanceToken != instanceToken || session.AssigneeIdentifier(info) != alias {
		return fail(fmt.Errorf("authoritative fresh session witness does not match requested identity"))
	}
	if info.TriggerBeadID != strings.TrimSpace(request.WorkBeadID) || info.TriggerBeadStoreRef != strings.TrimSpace(request.WorkStoreRef) {
		return fail(fmt.Errorf("authoritative fresh session witness does not match exact trigger/store"))
	}
	if result.Claimed.ID != strings.TrimSpace(request.WorkBeadID) || result.Claimed.Status != "in_progress" || strings.TrimSpace(result.Claimed.Assignee) != alias || result.Claimed.Metadata[beadmeta.SessionIDMetadataKey] != info.ID || result.Claimed.Metadata[beadmeta.SessionNameMetadataKey] != sessionName || result.Claimed.Metadata[beadmeta.SessionInstanceTokenMetadataKey] != instanceToken {
		return fail(fmt.Errorf("authoritative fresh work claim does not match created witness"))
	}
	if bp.sessionBeads != nil {
		bp.sessionBeads.addInfo(info)
	}
	return info, nil
}
