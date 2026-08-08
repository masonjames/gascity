package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/worker"
)

func TestWorkerLaunchAuthorizerWithConfigFencesOnlyCurrentForbiddenAgents(t *testing.T) {
	maxSessions := 3
	workRoot := t.TempDir()
	workDirTemplate := filepath.Join(workRoot, "{{.AgentBase}}")
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Providers: map[string]config.ProviderSpec{"codex": {Command: "codex", PathCheck: "true"}},
		Agents: []config.Agent{
			{Name: "strict", Provider: "codex", WorkDir: workDirTemplate, ProjectHooks: config.ProjectHooksForbid, MaxActiveSessions: &maxSessions},
			{Name: "legacy", ProjectHooks: config.ProjectHooksInherit},
		},
	}
	authorize := workerLaunchAuthorizerWithConfig("/cities/fixture-city", cfg)
	cityStore := beads.NewMemStore()
	exact := &session.Info{
		ID:                  "session-strict",
		Template:            "strict",
		Provider:            "codex",
		WorkDir:             filepath.Join(workRoot, "strict"),
		TriggerBeadID:       "work-exact",
		TriggerBeadStoreRef: "city:fixture-city",
	}

	for _, tc := range []struct {
		name         string
		req          worker.LaunchAuthorizationRequest
		wantTrigger  string
		wantStoreRef string
		wantErr      bool
	}{
		{name: "new forbidden direct start", req: worker.LaunchAuthorizationRequest{Session: worker.SessionSpec{Template: "strict"}}, wantErr: true},
		{name: "deferred forbidden row without trigger", req: worker.LaunchAuthorizationRequest{Info: &session.Info{ID: "session-strict", Template: "strict"}}, wantErr: true},
		{name: "forbidden noncolocated pair", req: worker.LaunchAuthorizationRequest{Info: &session.Info{ID: "session-strict", Template: "strict", TriggerBeadID: "work-exact", TriggerBeadStoreRef: "rig:fixture"}}, wantErr: true},
		{name: "forbidden exact trigger", req: worker.LaunchAuthorizationRequest{Info: exact, SessionStore: cityStore, CanonicalCityStore: cityStore}, wantTrigger: "work-exact", wantStoreRef: "city:fixture-city"},
		{name: "forbidden synthesized pool member exact trigger", req: worker.LaunchAuthorizationRequest{Info: &session.Info{ID: "session-pool", Template: "strict-2", Provider: "codex", WorkDir: filepath.Join(workRoot, "strict"), TriggerBeadID: "work-pool", TriggerBeadStoreRef: "city:fixture-city"}, SessionStore: cityStore, CanonicalCityStore: cityStore}, wantTrigger: "work-pool", wantStoreRef: "city:fixture-city"},
		{name: "inherit new direct start", req: worker.LaunchAuthorizationRequest{Session: worker.SessionSpec{Template: "legacy"}}},
		{
			name: "inherit persisted trigger remains exact delivery authority",
			req: worker.LaunchAuthorizationRequest{Info: &session.Info{
				ID:                  "session-legacy",
				Template:            "legacy",
				TriggerBeadID:       "work-rig",
				TriggerBeadStoreRef: "rig:fixture",
			}},
			wantTrigger:  "work-rig",
			wantStoreRef: "rig:fixture",
		},
		{name: "unconfigured provider session", req: worker.LaunchAuthorizationRequest{Session: worker.SessionSpec{Template: "adhoc-provider"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := authorize(context.Background(), tc.req)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, worker.ErrLaunchUnauthorized) {
				t.Fatalf("error = %v, want ErrLaunchUnauthorized", err)
			}
			if err == nil && (got.TriggerBeadID != tc.wantTrigger || got.TriggerBeadStoreRef != tc.wantStoreRef) {
				t.Fatalf("authorization trigger = (%q, %q), want (%q, %q)", got.TriggerBeadID, got.TriggerBeadStoreRef, tc.wantTrigger, tc.wantStoreRef)
			}
		})
	}
}

func TestCLIWorkerFactoryRejectsImmediateForbiddenCreateAndAllowsOnlyInertDeferredRow(t *testing.T) {
	maxSessions := 1
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Agents: []config.Agent{{
			Name:              "strict",
			Provider:          "codex",
			ProjectHooks:      config.ProjectHooksForbid,
			MaxActiveSessions: &maxSessions,
		}},
	}
	store := beads.NewMemStore()
	provider := runtime.NewFake()
	factory, err := workerFactoryWithConfig("/cities/fixture-city", store, provider, cfg)
	if err != nil {
		t.Fatalf("workerFactoryWithConfig: %v", err)
	}
	handle, err := factory.Session(worker.SessionSpec{
		Template: "strict",
		Command:  "codex",
		WorkDir:  t.TempDir(),
		Provider: "codex",
	})
	if err != nil {
		t.Fatalf("Session: %v", err)
	}

	if _, err := handle.Create(context.Background(), worker.CreateModeStarted); !errors.Is(err, worker.ErrLaunchUnauthorized) {
		t.Fatalf("Create(started) error = %v, want ErrLaunchUnauthorized", err)
	}
	all, err := store.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("List(after started rejection): %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("immediate create persisted rows before trigger binding: %+v", all)
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls before immediate-create rejection: %+v", calls)
	}

	info, err := handle.Create(context.Background(), worker.CreateModeDeferred)
	if err != nil {
		t.Fatalf("Create(deferred): %v", err)
	}
	if info.ID == "" || info.State != session.StateStartPending {
		t.Fatalf("deferred info = %+v, want inert start-pending row", info)
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls during inert deferred create: %+v", calls)
	}
}

func TestCLIWorkerFactoryRejectsStrictSessionInRelocatedPhysicalStore(t *testing.T) {
	maxSessions := 1
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Agents: []config.Agent{{
			Name:              "strict",
			Provider:          "codex",
			ProjectHooks:      config.ProjectHooksForbid,
			MaxActiveSessions: &maxSessions,
		}},
	}
	cityStore := beads.NewMemStore()
	sessionStore := beads.NewMemStoreFrom(1, []beads.Bead{{
		ID:     "session-strict",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"session_name":              "s-session-strict",
			"template":                  "strict",
			"state":                     string(session.StateActive),
			"provider":                  "codex",
			"command":                   "codex",
			"work_dir":                  t.TempDir(),
			"gc.trigger_bead_id":        "work-exact",
			"gc.trigger_bead_store_ref": "city:fixture-city",
		},
	}}, nil)
	provider := runtime.NewFake()
	factory, err := workerFactoryWithConfig("/cities/fixture-city", sessionStore, provider, cfg, cityStore)
	if err != nil {
		t.Fatalf("workerFactoryWithConfig: %v", err)
	}

	if _, err := factory.SessionByID("session-strict"); !errors.Is(err, worker.ErrLaunchUnauthorized) {
		t.Fatalf("SessionByID error = %v, want ErrLaunchUnauthorized", err)
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls = %+v, want none", calls)
	}
}

func TestCLIWorkerFactoryStrictSessionLookupDoesNotMaterializeResumeWorkDirBeforeStartAuthorization(t *testing.T) {
	cityPath := t.TempDir()
	externalParent := t.TempDir()
	workDir := filepath.Join(externalParent, "strict-session-cwd")
	maxSessions := 1
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Providers: map[string]config.ProviderSpec{
			"codex": {Command: "codex", PathCheck: "true"},
		},
		Agents: []config.Agent{{
			Name:              "strict",
			Provider:          "codex",
			ProjectHooks:      config.ProjectHooksForbid,
			WorkDir:           workDir,
			MaxActiveSessions: &maxSessions,
		}},
	}
	stored := beads.Bead{
		ID:     "session-strict",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"session_name":              "s-session-strict",
			"template":                  "strict",
			"state":                     string(session.StateActive),
			"provider":                  "codex",
			"command":                   "codex",
			"work_dir":                  workDir,
			"gc.trigger_bead_id":        "work-exact",
			"gc.trigger_bead_store_ref": "city:fixture-city",
		},
	}
	store := beads.NewMemStoreFrom(1, []beads.Bead{stored}, nil)
	provider := &projectHookIsolationWorkerProvider{Fake: runtime.NewFake(), supported: true}
	factory, err := workerFactoryWithConfig(cityPath, store, provider, cfg)
	if err != nil {
		t.Fatalf("workerFactoryWithConfig: %v", err)
	}

	before, err := store.Get(stored.ID)
	if err != nil {
		t.Fatalf("Get(before): %v", err)
	}
	handle, err := factory.SessionByID(stored.ID)
	if err != nil {
		t.Fatalf("SessionByID: %v", err)
	}
	if handle == nil {
		t.Fatal("SessionByID handle = nil")
	}
	if _, err := os.Stat(workDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("strict resume work_dir stat error = %v, want absent before Start authorization", err)
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls during strict handle lookup = %+v, want none", calls)
	}
	after, err := store.Get(stored.ID)
	if err != nil {
		t.Fatalf("Get(after): %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("persisted session mutated during strict handle lookup:\nafter=%+v\nbefore=%+v", after, before)
	}
}
