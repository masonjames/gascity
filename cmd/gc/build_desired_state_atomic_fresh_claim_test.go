package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/poolplan"
	"github.com/gastownhall/gascity/internal/session"
)

// holdBeforeAssignmentStore models a canonical hold added after the
// controller's advisory point read but before whichever atomic assignment
// primitive it invokes. It is transparent for physical-store identity.
type holdBeforeAssignmentStore struct {
	beads.Store
	workID           string
	holdLabel        string
	guardedCalls     int
	createClaimCalls int
	holdAdded        bool
}

func (s *holdBeforeAssignmentStore) StoreIdentityTarget() beads.Store { return s.Store }

func (s *holdBeforeAssignmentStore) addHold() error {
	if s.holdAdded {
		return nil
	}
	s.holdAdded = true
	return s.Update(s.workID, beads.UpdateOpts{Labels: []string{s.holdLabel}})
}

func (s *holdBeforeAssignmentStore) GuardedAssignmentClaimerHandle() (beads.GuardedAssignmentClaimer, bool) {
	return s, true
}

func (s *holdBeforeAssignmentStore) ClaimAssignment(ctx context.Context, req beads.AssignmentClaimRequest) (beads.Bead, bool, error) {
	s.guardedCalls++
	if err := s.addHold(); err != nil {
		return beads.Bead{}, false, err
	}
	claimer, ok := beads.GuardedAssignmentClaimerFor(s.Store)
	if !ok {
		return beads.Bead{}, false, beads.ErrGuardedAssignmentClaimUnsupported
	}
	return claimer.ClaimAssignment(ctx, req)
}

func (s *holdBeforeAssignmentStore) CreateAssignmentClaimerHandle() (beads.CreateAssignmentClaimer, bool) {
	return s, true
}

func (s *holdBeforeAssignmentStore) CreateAssignmentClaim(ctx context.Context, req beads.CreateAssignmentClaimRequest) (beads.CreateAssignmentClaimResult, bool, error) {
	s.createClaimCalls++
	if err := s.addHold(); err != nil {
		return beads.CreateAssignmentClaimResult{}, false, err
	}
	claimer, ok := beads.CreateAssignmentClaimerFor(s.Store)
	if !ok {
		return beads.CreateAssignmentClaimResult{}, false, beads.ErrCreateAssignmentClaimUnsupported
	}
	return claimer.CreateAssignmentClaim(ctx, req)
}

func TestForbiddenFreshPoolHoldAddedAfterAdvisoryReadCreatesNoSessionOrAssignment(t *testing.T) {
	cityPath := t.TempDir()
	externalRoot := filepath.Join(t.TempDir(), "isolated")
	backing := beads.NewMemStore()
	work := createGuardedClaimWork(t, backing, "worker")
	store := &holdBeforeAssignmentStore{
		Store: backing, workID: work.ID, holdLabel: beadmeta.HoldExternalLabel,
	}
	bp, cfg, fake, stderr := newForbiddenPoolBuildParams(
		t,
		cityPath,
		filepath.Join(externalRoot, "{{.AgentBase}}"),
		store,
	)
	bp.poolSessionCreateBudget = poolplan.NewCreateBudget(1)

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:     "worker",
			Tier:         "new",
			WorkBeadID:   work.ID,
			WorkStoreRef: "city:fixture-city",
			WorkRouteKey: beadmeta.RoutedToMetadataKey,
			WorkRoute:    "worker",
		}},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want zero; stderr=%q", len(desired), stderr.String())
	}
	if store.createClaimCalls != 1 {
		t.Fatalf("atomic fresh create/claim calls = %d, want 1", store.createClaimCalls)
	}
	current, err := backing.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != "open" || current.Assignee != "" || !beadmeta.HasDispatchHoldLabel(current.Labels) || current.Metadata[beadmeta.SessionIDMetadataKey] != "" {
		t.Fatalf("held work after rejected fresh claim = %+v, want unassigned external hold", current)
	}
	sessions, err := backing.List(beads.ListQuery{Type: sessionBeadType, IncludeClosed: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 0 {
		t.Fatalf("session rows after atomic predicate mismatch = %+v, want none", sessions)
	}
	if calls := fake.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls = %#v, want zero", calls)
	}
	attestedWorkDir := filepath.Join(externalRoot, "worker-1")
	if _, err := os.Lstat(attestedWorkDir); !os.IsNotExist(err) {
		t.Fatalf("attested cwd %q exists after atomic loser: err=%v", attestedWorkDir, err)
	}
	if !bp.tryClaimPoolSessionCreate("worker") {
		t.Fatal("fresh create budget was not refunded after atomic loser")
	}
	bp.releasePoolSessionCreate()
}

func TestForbiddenExistingSingletonClaimMismatchPrecedesNormalization(t *testing.T) {
	backing := beads.NewMemStore()
	store := &holdBeforeAssignmentStore{Store: backing, holdLabel: beadmeta.HoldMayorLabel}
	bp, cfg, info, stderr := guardedClaimBuildParams(t, store, store)
	cfg.Agents[0].MaxActiveSessions = intPtr(1)
	canonical := "worker"
	if err := backing.Update(info.ID, beads.UpdateOpts{
		Title: &canonical,
		Metadata: map[string]string{
			"agent_name": "worker",
			"alias":      "worker",
		},
		Labels: []string{"agent:worker"},
	}); err != nil {
		t.Fatal(err)
	}
	info, err := sessionFrontDoor(backing).Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	bp.sessionBeads = &sessionBeadSnapshot{}
	bp.sessionBeads.addInfo(info)
	work := createGuardedClaimWork(t, backing, "worker")
	store.workID = work.ID
	before, err := backing.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:      "worker",
			Tier:          "new",
			SessionBeadID: info.ID,
			WorkBeadID:    work.ID,
			WorkStoreRef:  "city:fixture-city",
			WorkRouteKey:  beadmeta.RoutedToMetadataKey,
			WorkRoute:     "worker",
		}},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want zero; stderr=%q", len(desired), stderr.String())
	}
	after, err := backing.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != before.Revision || after.Title != before.Title || after.Metadata["agent_name"] != before.Metadata["agent_name"] || after.Metadata["alias"] != before.Metadata["alias"] || after.Metadata["pool_slot"] != before.Metadata["pool_slot"] {
		t.Fatalf("claim loser normalized singleton before atomic predicate: before=%+v after=%+v", before, after)
	}
	currentWork, err := backing.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if currentWork.Status != "open" || currentWork.Assignee != "" || !beadmeta.HasDispatchHoldLabel(currentWork.Labels) {
		t.Fatalf("claim loser work = %+v, want held and unassigned", currentWork)
	}
}

func TestForbiddenExistingSingletonStaleActorIdentityMutatesNeitherRow(t *testing.T) {
	store := beads.NewMemStore()
	bp, cfg, info, stderr := guardedClaimBuildParams(t, store, store)
	cfg.Agents[0].MaxActiveSessions = intPtr(1)
	work := createGuardedClaimWork(t, store, "worker")
	beforeSession, err := store.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:      "worker",
			Tier:          "new",
			SessionBeadID: info.ID,
			WorkBeadID:    work.ID,
			WorkStoreRef:  "city:fixture-city",
			WorkRouteKey:  beadmeta.RoutedToMetadataKey,
			WorkRoute:     "worker",
		}},
	}, desired, stderr)
	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want zero for stale actor identity; stderr=%q", len(desired), stderr.String())
	}
	afterSession, err := store.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	afterWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterSession.Revision != beforeSession.Revision || afterSession.Title != beforeSession.Title || afterSession.Metadata["agent_name"] != beforeSession.Metadata["agent_name"] || afterSession.Metadata["alias"] != beforeSession.Metadata["alias"] || afterSession.Metadata["pool_slot"] != beforeSession.Metadata["pool_slot"] {
		t.Fatalf("stale actor identity mutated session: before=%+v after=%+v", beforeSession, afterSession)
	}
	if afterWork.Revision != beforeWork.Revision || afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee || afterWork.Metadata[beadmeta.SessionIDMetadataKey] != beforeWork.Metadata[beadmeta.SessionIDMetadataKey] {
		t.Fatalf("stale actor identity mutated work: before=%+v after=%+v", beforeWork, afterWork)
	}
}

func TestForbiddenExistingSingletonClaimKeepsSessionAndWorkActorConsistent(t *testing.T) {
	store := beads.NewMemStore()
	bp, cfg, info, stderr := guardedClaimBuildParams(t, store, store)
	cfg.Agents[0].MaxActiveSessions = intPtr(1)
	canonical := "worker"
	if err := store.Update(info.ID, beads.UpdateOpts{
		Title: &canonical,
		Metadata: map[string]string{
			"agent_name": "worker",
			"alias":      "worker",
		},
		Labels: []string{"agent:worker"},
	}); err != nil {
		t.Fatal(err)
	}
	info, err := sessionFrontDoor(store).Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	bp.sessionBeads = &sessionBeadSnapshot{}
	bp.sessionBeads.addInfo(info)
	work := createGuardedClaimWork(t, store, "worker")

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:      "worker",
			Tier:          "new",
			SessionBeadID: info.ID,
			WorkBeadID:    work.ID,
			WorkStoreRef:  "city:fixture-city",
			WorkRouteKey:  beadmeta.RoutedToMetadataKey,
			WorkRoute:     "worker",
		}},
	}, desired, stderr)
	if len(desired) != 1 {
		t.Fatalf("desired sessions = %d, want one; stderr=%q", len(desired), stderr.String())
	}
	storedInfo, err := sessionFrontDoor(store).Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if want := session.AssigneeIdentifier(storedInfo); claimed.Assignee != want {
		t.Fatalf("claimed assignee = %q, current session actor = %q; session=%+v work=%+v", claimed.Assignee, want, storedInfo, claimed)
	}
}

var (
	_ beads.StoreIdentityTargeter                  = (*holdBeforeAssignmentStore)(nil)
	_ beads.GuardedAssignmentClaimerHandleProvider = (*holdBeforeAssignmentStore)(nil)
	_ beads.CreateAssignmentClaimerHandleProvider  = (*holdBeforeAssignmentStore)(nil)
)
