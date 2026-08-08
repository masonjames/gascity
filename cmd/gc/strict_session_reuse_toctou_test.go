package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
)

type mutateStrictSessionAfterClaimStore struct {
	beads.Store
	sessionID string
	mutate    func(beads.Store, string) error
	claims    int
	mutated   beads.Bead
}

type mutateStrictSessionInsideClaimStore struct {
	*mutateStrictSessionAfterClaimStore
}

func (s *mutateStrictSessionInsideClaimStore) GuardedAssignmentClaimerHandle() (beads.GuardedAssignmentClaimer, bool) {
	return s, true
}

func (s *mutateStrictSessionInsideClaimStore) ClaimAssignment(ctx context.Context, req beads.AssignmentClaimRequest) (beads.Bead, bool, error) {
	s.claims++
	if err := s.mutate(s.Store, s.sessionID); err != nil {
		return beads.Bead{}, false, err
	}
	var err error
	s.mutated, err = s.Get(s.sessionID)
	if err != nil {
		return beads.Bead{}, false, err
	}
	claimer, ok := beads.GuardedAssignmentClaimerFor(s.Store)
	if !ok {
		return beads.Bead{}, false, beads.ErrGuardedAssignmentClaimUnsupported
	}
	return claimer.ClaimAssignment(ctx, req)
}

type mutateStrictSessionAtCASStore struct {
	*mutateStrictSessionAfterClaimStore
	casCalls int
}

type guardedOnlyStrictReuseStore struct {
	beads.Store
	claims int
}

func (s *guardedOnlyStrictReuseStore) GuardedAssignmentClaimerHandle() (beads.GuardedAssignmentClaimer, bool) {
	return s, true
}

func (s *guardedOnlyStrictReuseStore) ClaimAssignment(ctx context.Context, req beads.AssignmentClaimRequest) (beads.Bead, bool, error) {
	s.claims++
	claimer, ok := beads.GuardedAssignmentClaimerFor(s.Store)
	if !ok {
		return beads.Bead{}, false, beads.ErrGuardedAssignmentClaimUnsupported
	}
	return claimer.ClaimAssignment(ctx, req)
}

func (s *mutateStrictSessionAtCASStore) GuardedAssignmentClaimerHandle() (beads.GuardedAssignmentClaimer, bool) {
	return beads.GuardedAssignmentClaimerFor(s.Store)
}

func (s *mutateStrictSessionAtCASStore) ConditionalWriterHandle() (beads.ConditionalWriter, bool) {
	return s, true
}

func (s *mutateStrictSessionAtCASStore) UpdateIfMatch(id string, revision int64, opts beads.UpdateOpts) error {
	s.casCalls++
	if s.casCalls == 1 {
		if err := s.mutate(s.Store, s.sessionID); err != nil {
			return err
		}
		var err error
		s.mutated, err = s.Get(s.sessionID)
		if err != nil {
			return err
		}
	}
	writer, _ := beads.ConditionalWriterFor(s.Store)
	return writer.UpdateIfMatch(id, revision, opts)
}

func (s *mutateStrictSessionAfterClaimStore) GuardedAssignmentClaimerHandle() (beads.GuardedAssignmentClaimer, bool) {
	return s, true
}

func (s *mutateStrictSessionAfterClaimStore) ConditionalWriterHandle() (beads.ConditionalWriter, bool) {
	if _, ok := beads.ConditionalWriterFor(s.Store); !ok {
		return nil, false
	}
	return s, true
}

func (s *mutateStrictSessionAfterClaimStore) UpdateIfMatch(id string, revision int64, opts beads.UpdateOpts) error {
	writer, _ := beads.ConditionalWriterFor(s.Store)
	return writer.UpdateIfMatch(id, revision, opts)
}

func (s *mutateStrictSessionAfterClaimStore) CloseIfMatch(id string, revision int64) error {
	writer, _ := beads.ConditionalWriterFor(s.Store)
	return writer.CloseIfMatch(id, revision)
}

func (s *mutateStrictSessionAfterClaimStore) DeleteIfMatch(id string, revision int64) error {
	writer, _ := beads.ConditionalWriterFor(s.Store)
	return writer.DeleteIfMatch(id, revision)
}

func (s *mutateStrictSessionAfterClaimStore) CompareAndSetMetadataKey(id, key, expected, next string) (bool, error) {
	writer, _ := beads.ConditionalWriterFor(s.Store)
	return writer.CompareAndSetMetadataKey(id, key, expected, next)
}

func (s *mutateStrictSessionAfterClaimStore) ClaimAssignment(ctx context.Context, req beads.AssignmentClaimRequest) (beads.Bead, bool, error) {
	claimer, ok := beads.GuardedAssignmentClaimerFor(s.Store)
	if !ok {
		return beads.Bead{}, false, beads.ErrGuardedAssignmentClaimUnsupported
	}
	claimed, claimedOK, err := claimer.ClaimAssignment(ctx, req)
	if err != nil || !claimedOK {
		return claimed, claimedOK, err
	}
	s.claims++
	if err := s.mutate(s.Store, s.sessionID); err != nil {
		return beads.Bead{}, false, err
	}
	s.mutated, err = s.Get(s.sessionID)
	if err != nil {
		return beads.Bead{}, false, err
	}
	return claimed, true, nil
}

type strictReuseTOCTOUFixture struct {
	backing      beads.Store
	store        beads.Store
	bp           *agentBuildParams
	cfg          *config.City
	provider     *projectHookIsolationWorkerProvider
	stderr       *bytes.Buffer
	sessionID    string
	workID       string
	externalRoot string
}

func newStrictReuseTOCTOUFixture(t *testing.T, wrap func(beads.Store, string) beads.Store) strictReuseTOCTOUFixture {
	t.Helper()
	t.Setenv("GC_SESSION", config.SessionTransportTmux)
	cityPath := t.TempDir()
	externalRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize external workdir root: %v", err)
	}
	backing := beads.NewMemStore()
	work, err := backing.Create(beads.Bead{
		Title:  "strict routed work",
		Type:   "task",
		Status: "open",
		Metadata: map[string]string{
			beadmeta.RoutedToMetadataKey: "worker",
		},
	})
	if err != nil {
		t.Fatalf("create work: %v", err)
	}
	createdSession, err := backing.Create(beads.Bead{
		Title:  "worker",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession, "agent:worker", "template:worker"},
		Metadata: map[string]string{
			"template":                              "worker",
			"agent_name":                            "worker",
			"alias":                                 "worker",
			"session_name":                          "strict-reuse-runtime",
			"instance_token":                        "strict-reuse-token",
			"state":                                 string(session.StateCreating),
			"pool_slot":                             "1",
			poolManagedMetadataKey:                  "true",
			beadmeta.TriggerBeadIDMetadataKey:       work.ID,
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	sessionID := createdSession.ID
	workID := work.ID
	var store beads.Store = backing
	if wrap != nil {
		store = wrap(backing, sessionID)
	}
	maxActive := 1
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Agents: []config.Agent{{
			Name:              "worker",
			StartCommand:      "true",
			ProjectHooks:      config.ProjectHooksForbid,
			WorkDir:           filepath.Join(externalRoot, "{{.Agent}}"),
			MinActiveSessions: intPtr(0),
			MaxActiveSessions: &maxActive,
		}},
	}
	provider := &projectHookIsolationWorkerProvider{Fake: runtime.NewFake(), supported: true}
	stderr := &bytes.Buffer{}
	bp := newAgentBuildParams("fixture-city", cityPath, cfg, provider, time.Unix(1_700_000_000, 0).UTC(), store, stderr)
	snapshot, err := loadSessionBeadSnapshot(store)
	if err != nil {
		t.Fatalf("load session snapshot: %v", err)
	}
	bp.sessionBeads = snapshot
	return strictReuseTOCTOUFixture{
		backing:      backing,
		store:        store,
		bp:           bp,
		cfg:          cfg,
		provider:     provider,
		stderr:       stderr,
		sessionID:    sessionID,
		workID:       workID,
		externalRoot: externalRoot,
	}
}

func (f strictReuseTOCTOUFixture) realize(t *testing.T) map[string]TemplateParams {
	t.Helper()
	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(f.bp, &f.cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:      "worker",
			Tier:          "resume",
			SessionBeadID: f.sessionID,
			WorkBeadID:    f.workID,
			WorkStoreRef:  "city:fixture-city",
			WorkRouteKey:  beadmeta.RoutedToMetadataKey,
			WorkRoute:     "worker",
		}},
	}, desired, f.stderr)
	return desired
}

func strictReuseSessionDrifts() []struct {
	name   string
	mutate func(beads.Store, string) error
} {
	return []struct {
		name   string
		mutate func(beads.Store, string) error
	}{
		{
			name: "trigger cleared",
			mutate: func(store beads.Store, id string) error {
				return store.Update(id, beads.UpdateOpts{Metadata: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       "",
					beadmeta.TriggerBeadStoreRefMetadataKey: "",
				}})
			},
		},
		{
			name: "trigger repointed",
			mutate: func(store beads.Store, id string) error {
				return store.Update(id, beads.UpdateOpts{Metadata: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       "foreign-work",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				}})
			},
		},
		{
			name: "session suspended",
			mutate: func(store beads.Store, id string) error {
				return store.SetMetadata(id, "state", string(session.StateSuspended))
			},
		},
		{
			name: "pool slot changed",
			mutate: func(store beads.Store, id string) error {
				return store.SetMetadata(id, "pool_slot", "2")
			},
		},
		{
			name: "agent member changed behind stable alias",
			mutate: func(store beads.Store, id string) error {
				return store.SetMetadata(id, "agent_name", "foreign-member")
			},
		},
		{
			name: "title identity changed",
			mutate: func(store beads.Store, id string) error {
				title := "worker-2"
				return store.Update(id, beads.UpdateOpts{Title: &title})
			},
		},
	}
}

func assertStrictReuseStoppedAfterDrift(t *testing.T, f strictReuseTOCTOUFixture, baseline beads.Bead, desired map[string]TemplateParams) {
	t.Helper()
	if len(desired) != 0 {
		t.Fatalf("desired sessions = %+v, want none; stderr=%q", desired, f.stderr.String())
	}
	after, err := f.backing.Get(f.sessionID)
	if err != nil {
		t.Fatalf("Get(session after build): %v", err)
	}
	if after.Revision != baseline.Revision {
		t.Fatalf("strict drift received normalization or trigger binding: baseline revision=%d after=%d\nbaseline=%+v\nafter=%+v\nstderr=%q", baseline.Revision, after.Revision, baseline, after, f.stderr.String())
	}
	if calls := f.provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls after strict drift = %+v, want none", calls)
	}
	if _, err := os.Lstat(filepath.Join(f.externalRoot, "worker")); !os.IsNotExist(err) {
		t.Fatalf("strict worker cwd was created after drift: err=%v", err)
	}
}

func TestStrictPoolReuseReloadsCapturedWitnessBeforeClaimOrSessionMutation(t *testing.T) {
	for _, tc := range strictReuseSessionDrifts() {
		t.Run(tc.name, func(t *testing.T) {
			f := newStrictReuseTOCTOUFixture(t, nil)
			if err := tc.mutate(f.backing, f.sessionID); err != nil {
				t.Fatalf("mutate session after snapshot: %v", err)
			}
			baseline, err := f.backing.Get(f.sessionID)
			if err != nil {
				t.Fatal(err)
			}
			beforeWork, err := f.backing.Get(f.workID)
			if err != nil {
				t.Fatal(err)
			}

			desired := f.realize(t)

			assertStrictReuseStoppedAfterDrift(t, f, baseline, desired)
			afterWork, err := f.backing.Get(f.workID)
			if err != nil {
				t.Fatal(err)
			}
			if afterWork.Revision != beforeWork.Revision || afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee {
				t.Fatalf("post-snapshot session drift still claimed work: before=%+v after=%+v", beforeWork, afterWork)
			}
		})
	}
}

func TestStrictPoolReuseReloadsWitnessAfterGuardedClaimBeforeNormalizeOrBind(t *testing.T) {
	for _, tc := range strictReuseSessionDrifts() {
		t.Run(tc.name, func(t *testing.T) {
			var mutating *mutateStrictSessionAfterClaimStore
			f := newStrictReuseTOCTOUFixture(t, func(backing beads.Store, sessionID string) beads.Store {
				mutating = &mutateStrictSessionAfterClaimStore{Store: backing, sessionID: sessionID, mutate: tc.mutate}
				return mutating
			})

			desired := f.realize(t)

			if mutating.claims != 1 {
				t.Fatalf("guarded claims = %d, want one", mutating.claims)
			}
			assertStrictReuseStoppedAfterDrift(t, f, mutating.mutated, desired)
			work, err := f.backing.Get(f.workID)
			if err != nil {
				t.Fatal(err)
			}
			if work.Status != "in_progress" || work.Assignee != "worker" {
				t.Fatalf("authoritative claim did not commit before injected session drift: %+v", work)
			}
		})
	}
}

func TestStrictPoolReuseExactCoLocatedWitnessRejectsDriftInsideClaim(t *testing.T) {
	for _, tc := range strictReuseSessionDrifts() {
		if tc.name != "trigger repointed" && tc.name != "pool slot changed" && tc.name != "agent member changed behind stable alias" && tc.name != "title identity changed" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			var mutating *mutateStrictSessionInsideClaimStore
			f := newStrictReuseTOCTOUFixture(t, func(backing beads.Store, sessionID string) beads.Store {
				base := &mutateStrictSessionAfterClaimStore{Store: backing, sessionID: sessionID, mutate: tc.mutate}
				mutating = &mutateStrictSessionInsideClaimStore{mutateStrictSessionAfterClaimStore: base}
				return mutating
			})
			beforeWork, err := f.backing.Get(f.workID)
			if err != nil {
				t.Fatal(err)
			}

			desired := f.realize(t)

			if mutating.claims != 1 {
				t.Fatalf("guarded claim invocations = %d, want one", mutating.claims)
			}
			assertStrictReuseStoppedAfterDrift(t, f, mutating.mutated, desired)
			afterWork, err := f.backing.Get(f.workID)
			if err != nil {
				t.Fatal(err)
			}
			if afterWork.Revision != beforeWork.Revision || afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee {
				t.Fatalf("in-transaction session drift still committed work: before=%+v after=%+v", beforeWork, afterWork)
			}
		})
	}
}

func TestStrictPoolReuseSessionCASRejectsExternalWriteWithoutOverwrite(t *testing.T) {
	for _, tc := range []struct {
		name              string
		skipNormalization bool
	}{
		{name: "normalization CAS"},
		{name: "trigger binding CAS", skipNormalization: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mutating *mutateStrictSessionAtCASStore
			f := newStrictReuseTOCTOUFixture(t, func(backing beads.Store, sessionID string) beads.Store {
				base := &mutateStrictSessionAfterClaimStore{
					Store: backing, sessionID: sessionID,
					mutate: func(store beads.Store, id string) error {
						return store.Update(id, beads.UpdateOpts{Metadata: map[string]string{
							beadmeta.TriggerBeadIDMetadataKey:       "external-winner",
							beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
						}})
					},
				}
				mutating = &mutateStrictSessionAtCASStore{mutateStrictSessionAfterClaimStore: base}
				return mutating
			})
			if tc.skipNormalization {
				if err := f.backing.SetMetadata(f.sessionID, "pool_slot", ""); err != nil {
					t.Fatal(err)
				}
				snapshot, err := loadSessionBeadSnapshot(f.store)
				if err != nil {
					t.Fatal(err)
				}
				f.bp.sessionBeads = snapshot
			}

			desired := f.realize(t)

			if mutating.casCalls != 1 {
				t.Fatalf("conditional SESSION writes = %d, want one losing CAS", mutating.casCalls)
			}
			assertStrictReuseStoppedAfterDrift(t, f, mutating.mutated, desired)
			work, err := f.backing.Get(f.workID)
			if err != nil {
				t.Fatal(err)
			}
			if work.Status != "in_progress" || work.Assignee != "worker" {
				t.Fatalf("work claim before external post-claim drift = %+v, want committed exact owner", work)
			}
		})
	}
}

func TestStrictPoolReuseRequiresConditionalSessionWriterBeforeClaim(t *testing.T) {
	var guarded *guardedOnlyStrictReuseStore
	f := newStrictReuseTOCTOUFixture(t, func(backing beads.Store, _ string) beads.Store {
		guarded = &guardedOnlyStrictReuseStore{Store: backing}
		return guarded
	})
	beforeSession, err := f.backing.Get(f.sessionID)
	if err != nil {
		t.Fatal(err)
	}
	beforeWork, err := f.backing.Get(f.workID)
	if err != nil {
		t.Fatal(err)
	}

	desired := f.realize(t)

	assertStrictReuseStoppedAfterDrift(t, f, beforeSession, desired)
	if guarded.claims != 0 {
		t.Fatalf("guarded claim calls = %d, want zero without conditional SESSION capability", guarded.claims)
	}
	afterWork, err := f.backing.Get(f.workID)
	if err != nil {
		t.Fatal(err)
	}
	if afterWork.Revision != beforeWork.Revision || afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee {
		t.Fatalf("unsupported SESSION CAS mutated work: before=%+v after=%+v", beforeWork, afterWork)
	}
}

func TestStrictPoolReuseRejectsActiveConditionalMutationLeaseBeforeClaim(t *testing.T) {
	f := newStrictReuseTOCTOUFixture(t, nil)
	if err := f.backing.SetMetadata(f.sessionID, session.ConditionalMutationLeaseMetadataKey, "foreign-active-lease"); err != nil {
		t.Fatal(err)
	}
	beforeSession, err := f.backing.Get(f.sessionID)
	if err != nil {
		t.Fatal(err)
	}
	beforeWork, err := f.backing.Get(f.workID)
	if err != nil {
		t.Fatal(err)
	}

	desired := f.realize(t)

	assertStrictReuseStoppedAfterDrift(t, f, beforeSession, desired)
	afterWork, err := f.backing.Get(f.workID)
	if err != nil {
		t.Fatal(err)
	}
	if afterWork.Revision != beforeWork.Revision || afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee {
		t.Fatalf("active SESSION mutation lease still permitted work claim: before=%+v after=%+v", beforeWork, afterWork)
	}
}

func TestStrictDependencyFloorRejectsActiveConditionalMutationLeaseBeforeNormalize(t *testing.T) {
	f := newStrictReuseTOCTOUFixture(t, nil)
	if err := f.backing.SetMetadata(f.sessionID, "dependency_only", "true"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := loadSessionBeadSnapshot(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.bp.sessionBeads = snapshot
	if err := f.backing.SetMetadata(f.sessionID, session.ConditionalMutationLeaseMetadataKey, "foreign-active-lease"); err != nil {
		t.Fatal(err)
	}
	beforeSession, err := f.backing.Get(f.sessionID)
	if err != nil {
		t.Fatal(err)
	}
	desired := map[string]TemplateParams{}

	ensureDependencyOnlyTemplate(f.bp, f.cfg, &f.cfg.Agents[0], desired, f.stderr)

	assertStrictReuseStoppedAfterDrift(t, f, beforeSession, desired)
}

func TestStrictDependencyFloorReloadsCapturedWitnessBeforeNormalizeOrDesired(t *testing.T) {
	for _, tc := range strictReuseSessionDrifts() {
		t.Run(tc.name, func(t *testing.T) {
			f := newStrictReuseTOCTOUFixture(t, nil)
			if err := f.backing.SetMetadata(f.sessionID, "dependency_only", "true"); err != nil {
				t.Fatal(err)
			}
			// Capture the dependency-only row, then drift it behind that snapshot.
			snapshot, err := loadSessionBeadSnapshot(f.store)
			if err != nil {
				t.Fatal(err)
			}
			f.bp.sessionBeads = snapshot
			if err := tc.mutate(f.backing, f.sessionID); err != nil {
				t.Fatalf("mutate dependency row after snapshot: %v", err)
			}
			baseline, err := f.backing.Get(f.sessionID)
			if err != nil {
				t.Fatal(err)
			}
			desired := map[string]TemplateParams{}

			ensureDependencyOnlyTemplate(f.bp, f.cfg, &f.cfg.Agents[0], desired, f.stderr)

			assertStrictReuseStoppedAfterDrift(t, f, baseline, desired)
		})
	}
}

func TestStrictPoolReuseExactWitnessClaimsNormalizesBindsAndPublishes(t *testing.T) {
	f := newStrictReuseTOCTOUFixture(t, nil)

	desired := f.realize(t)

	if len(desired) != 1 {
		t.Fatalf("desired sessions = %+v, want one exact strict reuse; stderr=%q", desired, f.stderr.String())
	}
	work, err := f.backing.Get(f.workID)
	if err != nil {
		t.Fatal(err)
	}
	if work.Status != "in_progress" || work.Assignee != "worker" {
		t.Fatalf("exact strict work assignment = %+v, want worker ownership", work)
	}
	info, err := sessionFrontDoor(f.backing).Get(f.sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if info.PoolSlot != "" || info.TriggerBeadID != f.workID || info.TriggerBeadStoreRef != "city:fixture-city" {
		t.Fatalf("final strict session = %+v, want normalized singleton bound to exact work", info)
	}
}
