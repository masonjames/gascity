package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
)

func strictSplitStoreFixture(t *testing.T) (*agentBuildParams, *config.City, beads.Store, beads.Store, *bytes.Buffer, string) {
	t.Helper()
	cityPath := t.TempDir()
	externalRoot := t.TempDir()
	sessionStore := beads.NewMemStore()
	workStore := beads.NewMemStore()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Agents: []config.Agent{{
			Name:              "worker",
			StartCommand:      "true",
			WorkDir:           filepath.Join(externalRoot, "{{.AgentBase}}"),
			ProjectHooks:      config.ProjectHooksForbid,
			MinActiveSessions: intPtr(0),
			MaxActiveSessions: intPtr(2),
		}},
	}
	provider := &projectHookIsolationWorkerProvider{Fake: runtime.NewFake(), supported: true}
	stderr := &bytes.Buffer{}
	bp := newAgentBuildParamsWithStores(
		"fixture-city",
		cityPath,
		cfg,
		provider,
		time.Unix(1_700_000_000, 0).UTC(),
		beads.SessionStore{Store: sessionStore},
		beads.WorkStore{Store: workStore},
		stderr,
	)
	bp.sessionBeads = &sessionBeadSnapshot{}
	return bp, cfg, sessionStore, workStore, stderr, filepath.Join(externalRoot, "worker-1")
}

func TestStrictFreshPoolDemandRejectsSplitPhysicalStoresBeforeAnyMutation(t *testing.T) {
	bp, cfg, sessionStore, workStore, stderr, workDir := strictSplitStoreFixture(t)
	wrong := createGuardedClaimWork(t, sessionStore, "worker")
	actualWork := createGuardedClaimWork(t, workStore, "worker")
	if wrong.ID != actualWork.ID {
		t.Fatalf("fixture IDs differ: session=%q work=%q", wrong.ID, actualWork.ID)
	}
	beforeSession := listAllBeadsForStrictStoreFence(t, sessionStore)
	beforeWork := listAllBeadsForStrictStoreFence(t, workStore)

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:     "worker",
			Tier:         "new",
			WorkBeadID:   actualWork.ID,
			WorkStoreRef: "city:fixture-city",
			WorkRouteKey: beadmeta.RoutedToMetadataKey,
			WorkRoute:    "worker",
		}},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Fatalf("desired=%+v, want no strict session across split stores; stderr=%q", desired, stderr.String())
	}
	assertStrictStoreFenceUnchanged(t, "session", sessionStore, beforeSession)
	assertStrictStoreFenceUnchanged(t, "work", workStore, beforeWork)
	if _, err := os.Lstat(workDir); !os.IsNotExist(err) {
		t.Fatalf("strict cwd %q was materialized: %v", workDir, err)
	}
	if calls := bp.sp.(*projectHookIsolationWorkerProvider).SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls=%#v, want none", calls)
	}
}

func TestBuildDesiredStateWithStoresRejectsFreshStrictDemandAcrossSameIDSplitStores(t *testing.T) {
	bp, cfg, sessionStore, workStore, stderr, workDir := strictSplitStoreFixture(t)
	scaleCheckMarker := filepath.Join(t.TempDir(), "strict-scale-check-ran")
	cfg.Agents[0].ScaleCheck = "touch " + scaleCheckMarker + "; printf 1"
	wrong := createGuardedClaimWork(t, sessionStore, "worker")
	actualWork := createGuardedClaimWork(t, workStore, "worker")
	if wrong.ID != actualWork.ID {
		t.Fatalf("fixture IDs differ: session=%q work=%q", wrong.ID, actualWork.ID)
	}
	snapshot, err := loadSessionBeadSnapshot(sessionStore)
	if err != nil {
		t.Fatal(err)
	}
	beforeSession := listAllBeadsForStrictStoreFence(t, sessionStore)
	beforeWork := listAllBeadsForStrictStoreFence(t, workStore)

	result := buildDesiredStateWithStores(
		"fixture-city",
		bp.cityPath,
		bp.beaconTime,
		cfg,
		bp.sp,
		beads.SessionStore{Store: sessionStore},
		beads.WorkStore{Store: workStore},
		nil,
		snapshot,
		nil,
		stderr,
	)

	if len(result.State) != 0 {
		t.Fatalf("desired=%+v, want no strict session across split stores; stderr=%q", result.State, stderr.String())
	}
	assertStrictStoreFenceUnchanged(t, "session", sessionStore, beforeSession)
	assertStrictStoreFenceUnchanged(t, "work", workStore, beforeWork)
	if _, err := os.Lstat(workDir); !os.IsNotExist(err) {
		t.Fatalf("strict cwd %q was materialized: %v", workDir, err)
	}
	if calls := bp.sp.(*projectHookIsolationWorkerProvider).SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls=%#v, want none", calls)
	}
	if _, err := os.Lstat(scaleCheckMarker); !os.IsNotExist(err) {
		t.Fatalf("strict scale_check ran before split-store topology refusal: %v", err)
	}
}

func TestAdoptionBarrierRejectsExistingStrictRuntimeAcrossSameIDSplitStores(t *testing.T) {
	sessionStore := beads.NewMemStore()
	workStore := beads.NewMemStore()
	work, err := workStore.Create(beads.Bead{Title: "strict work", Type: "task", Status: "open"})
	if err != nil {
		t.Fatal(err)
	}
	row, err := sessionStore.Create(beads.Bead{
		Title:  "worker",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession, "template:worker"},
		Metadata: map[string]string{
			"template":                              "worker",
			"agent_name":                            "worker",
			"session_name":                          "test-city-worker",
			"state":                                 string(session.StateActive),
			beadmeta.TriggerBeadIDMetadataKey:       work.ID,
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:test-city",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if row.ID != work.ID {
		t.Fatalf("fixture IDs differ: session=%q work=%q", row.ID, work.ID)
	}
	beforeSession := listAllBeadsForStrictStoreFence(t, sessionStore)
	beforeWork := listAllBeadsForStrictStoreFence(t, workStore)
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents:    []config.Agent{{Name: "worker", ProjectHooks: config.ProjectHooksForbid}},
	}
	sp := &fakeAdoptionProvider{running: []string{"test-city-worker"}}
	stderr := &bytes.Buffer{}

	result, passed := runAdoptionBarrierWithWorkStore(
		"",
		sessionFrontDoor(sessionStore),
		beads.WorkStore{Store: workStore},
		sp,
		cfg,
		"test-city",
		clock.Real{},
		stderr,
		false,
	)

	if passed || result.Skipped != 1 || result.Adopted != 0 || result.AlreadyHadBead != 0 {
		t.Fatalf("result=%+v passed=%v, want one strict refusal; stderr=%q", result, passed, stderr.String())
	}
	assertStrictStoreFenceUnchanged(t, "session", sessionStore, beforeSession)
	assertStrictStoreFenceUnchanged(t, "work", workStore, beforeWork)
}

func TestStrictExistingRowsRejectSplitPhysicalStoresAcrossAllBuildOverlays(t *testing.T) {
	for _, shape := range []string{"named", "manual", "dependency"} {
		t.Run(shape, func(t *testing.T) {
			bp, cfg, sessionStore, workStore, stderr, workDir := strictSplitStoreFixture(t)
			work := createGuardedClaimWork(t, workStore, "worker")
			metadata := map[string]string{
				"template":                              "worker",
				"agent_name":                            "worker-1",
				"alias":                                 "worker-1",
				"session_name":                          "strict-existing-1",
				"instance_token":                        "strict-instance-1",
				"state":                                 string(session.StateActive),
				"pool_slot":                             "1",
				poolManagedMetadataKey:                  "true",
				beadmeta.TriggerBeadIDMetadataKey:       work.ID,
				beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
			}
			switch shape {
			case "named":
				cfg.NamedSessions = []config.NamedSession{{Template: "worker", Mode: "always"}}
				metadata["agent_name"] = "worker"
				metadata["alias"] = "worker"
				metadata[namedSessionMetadataKey] = "true"
				metadata[namedSessionIdentityMetadata] = "worker"
				metadata[namedSessionModeMetadata] = "always"
				delete(metadata, "pool_slot")
				delete(metadata, poolManagedMetadataKey)
			case "manual":
				metadata["session_origin"] = "manual"
				metadata["manual"] = "true"
			case "dependency":
				metadata["dependency_only"] = "true"
			}
			created, err := sessionStore.Create(beads.Bead{
				Title:    metadata["agent_name"],
				Type:     session.BeadType,
				Status:   "open",
				Labels:   []string{session.LabelSession, "template:worker"},
				Metadata: metadata,
			})
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := loadSessionBeadSnapshot(sessionStore)
			if err != nil {
				t.Fatal(err)
			}
			bp.sessionBeads = snapshot
			beforeSession := listAllBeadsForStrictStoreFence(t, sessionStore)
			beforeWork := listAllBeadsForStrictStoreFence(t, workStore)
			desired := map[string]TemplateParams{}

			switch shape {
			case "named", "manual":
				discoverSessionBeads(bp, cfg, desired, stderr)
			case "dependency":
				ensureDependencyOnlyTemplate(bp, cfg, &cfg.Agents[0], desired, stderr)
			}

			if len(desired) != 0 {
				t.Fatalf("shape=%s desired=%+v, want none; stderr=%q", shape, desired, stderr.String())
			}
			assertStrictStoreFenceUnchanged(t, "session", sessionStore, beforeSession)
			assertStrictStoreFenceUnchanged(t, "work", workStore, beforeWork)
			if _, err := os.Lstat(workDir); !os.IsNotExist(err) {
				t.Fatalf("shape=%s strict cwd %q was materialized: %v", shape, workDir, err)
			}
			if calls := bp.sp.(*projectHookIsolationWorkerProvider).SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("shape=%s provider calls=%#v, want none", shape, calls)
			}
			if got, err := sessionStore.Get(created.ID); err != nil || got.Revision != beforeSession[0].Revision {
				t.Fatalf("shape=%s session revision changed: got=%+v err=%v before=%+v", shape, got, err, beforeSession[0])
			}
		})
	}
}

func TestInheritBuildRemainsCompatibleAcrossSplitPhysicalStores(t *testing.T) {
	bp, cfg, _, _, stderr, _ := strictSplitStoreFixture(t)
	cfg.Agents[0].ProjectHooks = config.ProjectHooksInherit
	bp.city.Agents[0].ProjectHooks = config.ProjectHooksInherit
	desired := map[string]TemplateParams{}
	ensureDependencyOnlyTemplate(bp, cfg, &cfg.Agents[0], desired, stderr)
	if len(desired) != 1 {
		t.Fatalf("inherit desired=%+v, want one dependency floor; stderr=%q", desired, stderr.String())
	}
}

func TestSplitStoreSyncLeavesDeferredStrictRowInertWhileInheritConverges(t *testing.T) {
	bp, cfg, sessionStore, workStore, stderr, _ := strictSplitStoreFixture(t)
	cfg.Agents = append(cfg.Agents, config.Agent{
		Name:              "helper",
		StartCommand:      "true",
		ProjectHooks:      config.ProjectHooksInherit,
		MinActiveSessions: intPtr(0),
		MaxActiveSessions: intPtr(1),
	})
	strictRow, err := sessionStore.Create(beads.Bead{
		Title:  "worker-1",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession, "template:worker"},
		Metadata: map[string]string{
			"template":     "worker",
			"agent_name":   "worker-1",
			"session_name": "strict-deferred-1",
			"state":        string(session.StateActive),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	workTwin, err := workStore.Create(beads.Bead{Title: "same-id work twin", Type: "task", Status: "open"})
	if err != nil {
		t.Fatal(err)
	}
	if strictRow.ID != workTwin.ID {
		t.Fatalf("fixture IDs differ: session=%q work=%q", strictRow.ID, workTwin.ID)
	}
	snapshot, err := loadSessionBeadSnapshot(sessionStore)
	if err != nil {
		t.Fatal(err)
	}
	beforeStrict, err := sessionStore.Get(strictRow.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeWork := listAllBeadsForStrictStoreFence(t, workStore)
	desired := map[string]TemplateParams{
		"strict-deferred-1": {TemplateName: "worker", InstanceName: "worker-1", Command: "true"},
		"inherit-session-1": {TemplateName: "helper", InstanceName: "helper", Command: "true"},
	}

	_, updated := syncSessionBeadsWithStores(
		bp.cityPath,
		beads.SessionStore{Store: sessionStore},
		beads.WorkStore{Store: workStore},
		nil,
		desired,
		bp.sp,
		map[string]bool{"strict-deferred-1": true, "inherit-session-1": true},
		cfg,
		&clock.Fake{Time: bp.beaconTime},
		stderr,
		true,
		snapshot,
	)

	afterStrict, err := sessionStore.Get(strictRow.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterStrict, beforeStrict) {
		t.Fatalf("strict row mutated across split-store sync:\nbefore=%+v\nafter=%+v\nstderr=%q", beforeStrict, afterStrict, stderr.String())
	}
	assertStrictStoreFenceUnchanged(t, "work", workStore, beforeWork)
	inheritCount := 0
	for _, info := range updated.OpenInfos() {
		if info.SessionNameMetadata == "inherit-session-1" && info.Template == "helper" {
			inheritCount++
		}
	}
	if inheritCount != 1 {
		t.Fatalf("inherit session count=%d, want 1; infos=%+v stderr=%q", inheritCount, updated.OpenInfos(), stderr.String())
	}
}

func TestSplitStoreRouteRecoverySkipsStrictRouteWhileInheritConverges(t *testing.T) {
	sessionStore := beads.NewMemStore()
	workStore := beads.NewMemStore()
	strictTwin, err := sessionStore.Create(beads.Bead{Title: "same-id session twin", Type: "task", Status: "open"})
	if err != nil {
		t.Fatal(err)
	}
	strictWork, err := workStore.Create(beads.Bead{
		Title:  "strict unrouted",
		Type:   "task",
		Status: "open",
		Metadata: map[string]string{
			beadmeta.RunTargetMetadataKey: "worker",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strictTwin.ID != strictWork.ID {
		t.Fatalf("fixture IDs differ: session=%q work=%q", strictTwin.ID, strictWork.ID)
	}
	inheritWork, err := workStore.Create(beads.Bead{
		Title:  "inherit unrouted",
		Type:   "task",
		Status: "open",
		Metadata: map[string]string{
			beadmeta.RunTargetMetadataKey: "helper",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{Agents: []config.Agent{
		{Name: "worker", ProjectHooks: config.ProjectHooksForbid},
		{Name: "helper", ProjectHooks: config.ProjectHooksInherit},
	}}

	restored, err := restoreCarriedWorkRoutesWithPredicate(workStore, func(route string, candidateStore beads.Store) bool {
		return strictWorkMutationAuthorized(
			cfg,
			route,
			beads.SessionStore{Store: sessionStore},
			beads.WorkStore{Store: workStore},
			candidateStore,
		)
	})
	if err != nil {
		t.Fatal(err)
	}
	if restored != 1 {
		t.Fatalf("restored=%d, want only inherit route", restored)
	}
	gotStrict, err := workStore.Get(strictWork.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := gotStrict.Metadata[beadmeta.RoutedToMetadataKey]; got != "" {
		t.Fatalf("strict routed_to=%q, want empty", got)
	}
	gotInherit, err := workStore.Get(inheritWork.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := gotInherit.Metadata[beadmeta.RoutedToMetadataKey]; got != "helper" {
		t.Fatalf("inherit routed_to=%q, want helper", got)
	}
}

func TestSplitStoreOrphanReleaseSkipsStrictWorkWhileInheritConverges(t *testing.T) {
	sessionStore := beads.NewMemStore()
	workStore := beads.NewMemStore()
	strictTwin, err := sessionStore.Create(beads.Bead{Title: "same-id session twin", Type: "task", Status: "open"})
	if err != nil {
		t.Fatal(err)
	}
	strictWork, err := workStore.Create(beads.Bead{
		Title:    "strict orphan",
		Type:     "task",
		Status:   "in_progress",
		Assignee: "missing-strict-session",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strictTwin.ID != strictWork.ID {
		t.Fatalf("fixture IDs differ: session=%q work=%q", strictTwin.ID, strictWork.ID)
	}
	inheritWork, err := workStore.Create(beads.Bead{
		Title:    "inherit orphan",
		Type:     "task",
		Status:   "in_progress",
		Assignee: "missing-inherit-session",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "helper"},
	})
	if err != nil {
		t.Fatal(err)
	}
	inProgress := "in_progress"
	for _, id := range []string{strictWork.ID, inheritWork.ID} {
		if err := workStore.Update(id, beads.UpdateOpts{Status: &inProgress}); err != nil {
			t.Fatal(err)
		}
	}
	strictWork, err = workStore.Get(strictWork.ID)
	if err != nil {
		t.Fatal(err)
	}
	inheritWork, err = workStore.Get(inheritWork.ID)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{Agents: []config.Agent{
		{Name: "worker", ProjectHooks: config.ProjectHooksForbid, MinActiveSessions: intPtr(0), MaxActiveSessions: intPtr(2)},
		{Name: "helper", ProjectHooks: config.ProjectHooksInherit, MinActiveSessions: intPtr(0), MaxActiveSessions: intPtr(2)},
	}}
	result := DesiredStateResult{
		AssignedWorkBeads:     []beads.Bead{strictWork, inheritWork},
		AssignedWorkStores:    []beads.Store{workStore, workStore},
		AssignedWorkStoreRefs: []string{"", ""},
	}

	released := releaseOrphanedPoolAssignmentsWhenSnapshotsCompleteWithStores(
		beads.SessionStore{Store: sessionStore},
		beads.WorkStore{Store: workStore},
		cfg,
		"",
		nil,
		result,
		nil,
	)
	if len(released) != 1 || released[0].ID != inheritWork.ID {
		t.Fatalf("released=%+v, want inherit work %s only", released, inheritWork.ID)
	}
	gotStrict, err := workStore.Get(strictWork.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotStrict.Status != "in_progress" || gotStrict.Assignee != "missing-strict-session" {
		t.Fatalf("strict orphan mutated: %+v", gotStrict)
	}
	gotInherit, err := workStore.Get(inheritWork.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotInherit.Status != "open" || gotInherit.Assignee != "" {
		t.Fatalf("inherit orphan did not converge: %+v", gotInherit)
	}
}

func listAllBeadsForStrictStoreFence(t *testing.T, store beads.Store) []beads.Bead {
	t.Helper()
	got, err := store.List(beads.ListQuery{IncludeClosed: true, AllowScan: true})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func assertStrictStoreFenceUnchanged(t *testing.T, name string, store beads.Store, before []beads.Bead) {
	t.Helper()
	after := listAllBeadsForStrictStoreFence(t, store)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("%s store mutated across strict split-store refusal:\nbefore=%+v\nafter=%+v", name, before, after)
	}
}
