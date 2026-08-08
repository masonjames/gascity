package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/pathutil"
	"github.com/gastownhall/gascity/internal/runtime"
)

func newForbiddenPoolBuildParams(
	t *testing.T,
	cityPath string,
	workDir string,
	store beads.Store,
) (*agentBuildParams, *config.City, *runtime.Fake, *bytes.Buffer) {
	t.Helper()
	t.Setenv("GC_SESSION", config.SessionTransportTmux)
	maxActive := 2
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Agents: []config.Agent{{
			Name:              "worker",
			StartCommand:      "true",
			WorkDir:           workDir,
			ProjectHooks:      config.ProjectHooksForbid,
			MinActiveSessions: intPtr(0),
			MaxActiveSessions: &maxActive,
		}},
	}
	fake := runtime.NewFake()
	provider := &projectHookIsolationWorkerProvider{Fake: fake, supported: true}
	stderr := &bytes.Buffer{}
	bp := newAgentBuildParams("fixture-city", cityPath, cfg, provider, time.Unix(1_700_000_000, 0).UTC(), store, stderr)
	bp.sessionBeads = &sessionBeadSnapshot{}
	return bp, cfg, fake, stderr
}

func TestForbiddenRoutedClaimFailureHasNoPreclaimWorkDirOrLaunchSideEffects(t *testing.T) {
	cityPath := t.TempDir()
	externalRoot := filepath.Join(t.TempDir(), "isolated")
	attestedWorkDir := filepath.Join(externalRoot, "worker-1")
	store := beads.NewMemStore()
	work := createGuardedClaimWork(t, store, "other-route")
	bp, cfg, fake, stderr := newForbiddenPoolBuildParams(
		t,
		cityPath,
		filepath.Join(externalRoot, "{{.AgentBase}}"),
		store,
	)
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
			WorkBeadID:    work.ID,
			WorkStoreRef:  "city:fixture-city",
			WorkRouteKey:  beadmeta.RoutedToMetadataKey,
			WorkRoute:     "worker",
			WorkPack:      "packer",
			WorkWorkspace: "exact-workspace",
		}},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want zero after exact claim mismatch; stderr=%q", len(desired), stderr.String())
	}
	if _, err := os.Lstat(attestedWorkDir); !os.IsNotExist(err) {
		t.Fatalf("attested cwd %q exists before a successful claim: err=%v", attestedWorkDir, err)
	}
	if calls := fake.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("runtime provider calls = %#v, want zero before successful claim", calls)
	}
	afterWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterWork.Revision != beforeWork.Revision || afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee {
		t.Fatalf("failed exact claim mutated work: before=%+v after=%+v", beforeWork, afterWork)
	}
	sessions, err := store.List(beads.ListQuery{Type: sessionBeadType, IncludeClosed: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 0 {
		t.Fatalf("failed exact target preflight created or closed session rows: %+v", sessions)
	}
}

func TestForbiddenExactTargetPreflightRejectsBeforeAnySessionRowMutation(t *testing.T) {
	tests := []struct {
		name       string
		workID     string
		storeRef   string
		workRoute  string
		requestKey string
		requestTo  string
		labels     []string
		hideGuard  bool
	}{
		{
			name:       "missing exact bead",
			workID:     "missing-work-id",
			storeRef:   "city:fixture-city",
			workRoute:  "worker",
			requestKey: beadmeta.RoutedToMetadataKey,
			requestTo:  "worker",
		},
		{
			name:       "wrong exact city store",
			storeRef:   "city:other-city",
			workRoute:  "worker",
			requestKey: beadmeta.RoutedToMetadataKey,
			requestTo:  "worker",
		},
		{
			name:       "unqualified legacy city store",
			storeRef:   "city",
			workRoute:  "worker",
			requestKey: beadmeta.RoutedToMetadataKey,
			requestTo:  "worker",
		},
		{
			name:       "route drift",
			storeRef:   "city:fixture-city",
			workRoute:  "other-route",
			requestKey: beadmeta.RoutedToMetadataKey,
			requestTo:  "worker",
		},
		{
			name:       "canonical hold",
			storeRef:   "city:fixture-city",
			workRoute:  "worker",
			requestKey: beadmeta.RoutedToMetadataKey,
			requestTo:  "worker",
			labels:     []string{beadmeta.HoldMayorLabel},
		},
		{
			name:       "unsupported atomic store",
			storeRef:   "city:fixture-city",
			workRoute:  "worker",
			requestKey: beadmeta.RoutedToMetadataKey,
			requestTo:  "worker",
			hideGuard:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cityPath := t.TempDir()
			externalRoot := filepath.Join(t.TempDir(), "isolated")
			backing := beads.NewMemStore()
			work := createGuardedClaimWork(t, backing, tt.workRoute, tt.labels...)
			beforeWork, err := backing.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			var store beads.Store = backing
			if tt.hideGuard {
				store = &storeWithoutGuard{Store: backing}
			}
			bp, cfg, fake, stderr := newForbiddenPoolBuildParams(
				t,
				cityPath,
				filepath.Join(externalRoot, "{{.AgentBase}}"),
				store,
			)
			workID := tt.workID
			if workID == "" {
				workID = work.ID
			}

			desired := map[string]TemplateParams{}
			realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
				Template: "worker",
				Requests: []SessionRequest{{
					Template:     "worker",
					Tier:         "new",
					WorkBeadID:   workID,
					WorkStoreRef: tt.storeRef,
					WorkRouteKey: tt.requestKey,
					WorkRoute:    tt.requestTo,
				}},
			}, desired, stderr)

			if len(desired) != 0 {
				t.Fatalf("desired sessions = %d, want zero; stderr=%q", len(desired), stderr.String())
			}
			sessions, err := store.List(beads.ListQuery{Type: sessionBeadType, IncludeClosed: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(sessions) != 0 {
				t.Fatalf("failed target preflight created or closed session rows: %+v", sessions)
			}
			afterWork, err := backing.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			if afterWork.Revision != beforeWork.Revision || afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee {
				t.Fatalf("failed target preflight mutated work: before=%+v after=%+v", beforeWork, afterWork)
			}
			if calls := fake.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("runtime provider calls = %#v, want zero", calls)
			}
		})
	}
}

func TestForbiddenCompleteOldTriggerCannotBypassExactResumeTargetFence(t *testing.T) {
	cityPath := t.TempDir()
	externalRoot := filepath.Join(t.TempDir(), "isolated")
	store := beads.NewMemStore()
	info := seedGuardedClaimPoolSession(t, store)
	oldWork := createGuardedClaimWork(t, store, "worker")
	if err := store.Update(info.ID, beads.UpdateOpts{Metadata: map[string]string{
		beadmeta.TriggerBeadIDMetadataKey:       oldWork.ID,
		beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
	}}); err != nil {
		t.Fatal(err)
	}
	info, err := sessionFrontDoor(store).Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	newWork := createGuardedClaimWork(t, store, "other-route")
	beforeSession, err := store.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeWork, err := store.Get(newWork.ID)
	if err != nil {
		t.Fatal(err)
	}
	bp, cfg, fake, stderr := newForbiddenPoolBuildParams(
		t,
		cityPath,
		filepath.Join(externalRoot, "{{.AgentBase}}"),
		store,
	)
	bp.sessionBeads.addInfo(info)

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:      "worker",
			Tier:          "resume",
			SessionBeadID: info.ID,
			WorkBeadID:    newWork.ID,
			WorkStoreRef:  "city:fixture-city",
			WorkRouteKey:  beadmeta.RoutedToMetadataKey,
			WorkRoute:     "worker",
		}},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want zero for drifted resume target; stderr=%q", len(desired), stderr.String())
	}
	afterSession, err := store.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterSession.Revision != beforeSession.Revision ||
		afterSession.Metadata[beadmeta.TriggerBeadIDMetadataKey] != oldWork.ID ||
		afterSession.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey] != "city:fixture-city" {
		t.Fatalf("drifted resume target mutated session: before=%+v after=%+v", beforeSession, afterSession)
	}
	afterWork, err := store.Get(newWork.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterWork.Revision != beforeWork.Revision || afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee {
		t.Fatalf("drifted resume target mutated work: before=%+v after=%+v", beforeWork, afterWork)
	}
	if calls := fake.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("runtime provider calls = %#v, want zero", calls)
	}
}

func TestForbiddenUnsafeWorkDirPreflightPrecedesWorkAndSessionMutation(t *testing.T) {
	cityPath := t.TempDir()
	store := beads.NewMemStore()
	work := createGuardedClaimWork(t, store, "worker")
	bp, cfg, fake, stderr := newForbiddenPoolBuildParams(
		t,
		cityPath,
		filepath.Join(cityPath, "unsafe", "{{.AgentBase}}"),
		store,
	)
	beforeWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}

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
		t.Fatalf("desired sessions = %d, want zero for unsafe cwd; stderr=%q", len(desired), stderr.String())
	}
	afterWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterWork.Revision != beforeWork.Revision || afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee {
		t.Fatalf("unsafe cwd preflight mutated work: before=%+v after=%+v", beforeWork, afterWork)
	}
	sessions, err := store.List(beads.ListQuery{Type: sessionBeadType, IncludeClosed: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 0 {
		t.Fatalf("unsafe cwd preflight created session rows: %+v", sessions)
	}
	if calls := fake.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("runtime provider calls = %#v, want zero for unsafe cwd", calls)
	}
}

func TestForbiddenPackWorkspaceDemandKeepsAttestedInstanceWorkDir(t *testing.T) {
	cityPath := t.TempDir()
	externalRoot := filepath.Join(t.TempDir(), "isolated")
	wantWorkDir := filepath.Join(externalRoot, "worker-1")
	store := beads.NewMemStore()
	work := createGuardedClaimWork(t, store, "worker")
	bp, cfg, _, stderr := newForbiddenPoolBuildParams(
		t,
		cityPath,
		filepath.Join(externalRoot, "{{.AgentBase}}"),
		store,
	)

	desired := map[string]TemplateParams{}
	realizePoolDesiredSessions(bp, &cfg.Agents[0], PoolDesiredState{
		Template: "worker",
		Requests: []SessionRequest{{
			Template:      "worker",
			Tier:          "new",
			WorkBeadID:    work.ID,
			WorkStoreRef:  "city:fixture-city",
			WorkRouteKey:  beadmeta.RoutedToMetadataKey,
			WorkRoute:     "worker",
			WorkPack:      "packer",
			WorkWorkspace: "exact-workspace",
		}},
	}, desired, stderr)

	if len(desired) != 1 {
		t.Fatalf("desired sessions = %d, want one after exact claim; stderr=%q", len(desired), stderr.String())
	}
	infos := bp.sessionBeads.OpenInfos()
	if len(infos) != 1 {
		t.Fatalf("session infos = %+v, want one", infos)
	}
	stored, err := store.Get(infos[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.Metadata[beadmeta.WorkDirMetadataKey]; !pathutil.SamePath(got, wantWorkDir) {
		t.Fatalf("forbid session gc.work_dir = %q, want attested instance cwd %q", got, wantWorkDir)
	}
	if got := stored.Metadata[beadmeta.LegacyWorkDirMetadataKey]; !pathutil.SamePath(got, wantWorkDir) {
		t.Fatalf("forbid session work_dir = %q, want attested instance cwd %q", got, wantWorkDir)
	}
	for _, tp := range desired {
		if !pathutil.SamePath(tp.WorkDir, wantWorkDir) {
			t.Fatalf("desired workdir = %q, want attested instance cwd %q", tp.WorkDir, wantWorkDir)
		}
	}
	claimed, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status != "in_progress" || claimed.Assignee != "worker-1" {
		t.Fatalf("claimed work = status %q assignee %q, want in_progress/worker-1", claimed.Status, claimed.Assignee)
	}
}

func TestForbiddenAttestedWorkDirDriftFailsBeforeWorkClaim(t *testing.T) {
	cityPath := t.TempDir()
	externalRoot := filepath.Join(t.TempDir(), "isolated")
	store := beads.NewMemStore()
	info := seedGuardedClaimPoolSession(t, store)
	driftedWorkDir := filepath.Join(t.TempDir(), "other", "worker-1")
	if err := store.Update(info.ID, beads.UpdateOpts{Metadata: map[string]string{
		beadmeta.WorkDirMetadataKey:       driftedWorkDir,
		beadmeta.LegacyWorkDirMetadataKey: driftedWorkDir,
	}}); err != nil {
		t.Fatal(err)
	}
	info, err := sessionFrontDoor(store).Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	work := createGuardedClaimWork(t, store, "worker")
	beforeWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	bp, cfg, _, stderr := newForbiddenPoolBuildParams(
		t,
		cityPath,
		filepath.Join(externalRoot, "{{.AgentBase}}"),
		store,
	)
	cfg.Agents[0].MaxActiveSessions = intPtr(1)
	bp.sessionBeads.addInfo(info)
	beforeSession, err := store.Get(info.ID)
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
			WorkPack:      "packer",
			WorkWorkspace: "exact-workspace",
		}},
	}, desired, stderr)

	if len(desired) != 0 {
		t.Fatalf("desired sessions = %d, want zero for attested cwd drift; stderr=%q", len(desired), stderr.String())
	}
	afterWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterWork.Revision != beforeWork.Revision || afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee {
		t.Fatalf("attested cwd drift mutated work: before=%+v after=%+v", beforeWork, afterWork)
	}
	afterSession, err := store.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := afterSession.Metadata[beadmeta.TriggerBeadIDMetadataKey]; got != "" {
		t.Fatalf("attested cwd drift bound trigger %q before claim", got)
	}
	if afterSession.Revision != beforeSession.Revision ||
		afterSession.Metadata["agent_name"] != beforeSession.Metadata["agent_name"] ||
		afterSession.Metadata["alias"] != beforeSession.Metadata["alias"] {
		t.Fatalf("attested cwd drift normalized session before preflight: before=%+v after=%+v", beforeSession, afterSession)
	}
	if !pathutil.SamePath(afterSession.Metadata[beadmeta.WorkDirMetadataKey], driftedWorkDir) {
		t.Fatalf("attested cwd changed from %q to %q", driftedWorkDir, afterSession.Metadata[beadmeta.WorkDirMetadataKey])
	}
}
