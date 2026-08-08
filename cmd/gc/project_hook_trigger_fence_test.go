package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/session/sessiontest"
)

func TestEnsureDependencyOnlyTemplateProjectHooksPolicyTriggerFence(t *testing.T) {
	for _, tc := range []struct {
		name         string
		policy       config.ProjectHooksPolicy
		wantDesired  int
		wantSessions int
	}{
		{
			name:         "forbid requires exact trigger before reservation",
			policy:       config.ProjectHooksForbid,
			wantDesired:  0,
			wantSessions: 0,
		},
		{
			name:         "inherit preserves ordinary dependency floor",
			policy:       config.ProjectHooksInherit,
			wantDesired:  1,
			wantSessions: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GC_SESSION", config.SessionTransportTmux)
			cityPath := t.TempDir()
			externalRoot := t.TempDir()
			store := beads.NewMemStore()
			cfg := &config.City{
				Workspace: config.Workspace{Name: "fixture-city"},
				Agents: []config.Agent{{
					Name:              "worker",
					StartCommand:      "true",
					WorkDir:           filepath.Join(externalRoot, "{{.AgentBase}}"),
					ProjectHooks:      tc.policy,
					MinActiveSessions: intPtr(0),
					MaxActiveSessions: intPtr(2),
				}},
			}
			fake := runtime.NewFake()
			provider := &projectHookIsolationWorkerProvider{Fake: fake, supported: true}
			stderr := &bytes.Buffer{}
			bp := newAgentBuildParams("fixture-city", cityPath, cfg, provider, time.Unix(1_700_000_000, 0).UTC(), store, stderr)
			bp.sessionBeads = &sessionBeadSnapshot{}

			desired := map[string]TemplateParams{}
			ensureDependencyOnlyTemplate(bp, cfg, &cfg.Agents[0], desired, stderr)

			if got := len(desired); got != tc.wantDesired {
				t.Fatalf("desired dependency floors = %d, want %d; stderr=%q", got, tc.wantDesired, stderr.String())
			}
			sessions, err := store.List(beads.ListQuery{Type: sessionBeadType, IncludeClosed: true})
			if err != nil {
				t.Fatal(err)
			}
			if got := len(sessions); got != tc.wantSessions {
				t.Fatalf("persisted dependency sessions = %d, want %d; sessions=%+v stderr=%q", got, tc.wantSessions, sessions, stderr.String())
			}
		})
	}
}

func TestBuildDesiredStateNamedAlwaysProjectHooksPolicyTriggerFence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		policy      config.ProjectHooksPolicy
		wantDesired bool
	}{
		{name: "forbid requires exact trigger before desired", policy: config.ProjectHooksForbid},
		{name: "inherit preserves always named session", policy: config.ProjectHooksInherit, wantDesired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GC_SESSION", config.SessionTransportTmux)
			cityPath := t.TempDir()
			externalRoot := t.TempDir()
			cfg := &config.City{
				Workspace: config.Workspace{Name: "fixture-city"},
				Agents: []config.Agent{{
					Name:              "worker",
					StartCommand:      "true",
					WorkDir:           filepath.Join(externalRoot, "{{.AgentBase}}"),
					ProjectHooks:      tc.policy,
					MinActiveSessions: intPtr(0),
					MaxActiveSessions: intPtr(1),
				}},
				NamedSessions: []config.NamedSession{{Template: "worker", Mode: "always"}},
			}
			provider := &projectHookIsolationWorkerProvider{Fake: runtime.NewFake(), supported: true}
			var stderr bytes.Buffer

			result := buildDesiredState("fixture-city", cityPath, time.Unix(1_700_000_000, 0).UTC(), cfg, provider, beads.NewMemStore(), &stderr)

			found := false
			for _, tp := range result.State {
				if tp.ConfiguredNamedIdentity == "worker" {
					found = true
					break
				}
			}
			if found != tc.wantDesired {
				t.Fatalf("named always desired = %v, want %v; state=%+v stderr=%q", found, tc.wantDesired, result.State, stderr.String())
			}
		})
	}
}

func TestProjectHooksForbidControllerManagedExactCityTriggerRemainsDesired(t *testing.T) {
	t.Run("dependency floor", func(t *testing.T) {
		t.Setenv("GC_SESSION", config.SessionTransportTmux)
		cityPath := t.TempDir()
		store := beads.NewMemStore()
		cfg := &config.City{
			Workspace: config.Workspace{Name: "fixture-city"},
			Agents: []config.Agent{{
				Name:              "worker",
				StartCommand:      "true",
				WorkDir:           filepath.Join(t.TempDir(), "{{.AgentBase}}"),
				ProjectHooks:      config.ProjectHooksForbid,
				MinActiveSessions: intPtr(0),
				MaxActiveSessions: intPtr(2),
			}},
		}
		work, err := store.Create(beads.Bead{
			ID:       "work-dependency-trigger",
			Title:    "exact dependency work",
			Type:     "task",
			Status:   "in_progress",
			Assignee: "worker-1",
			Metadata: map[string]string{
				beadmeta.RoutedToMetadataKey:             "worker",
				beadmeta.SessionIDMetadataKey:            "session-dependency-trigger",
				beadmeta.SessionNameMetadataKey:          "worker-dependency-trigger",
				beadmeta.SessionInstanceTokenMetadataKey: "instance-dependency-trigger",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Create(beads.Bead{
			ID:     "session-dependency-trigger",
			Title:  "worker-1",
			Type:   session.BeadType,
			Status: "open",
			Labels: []string{session.LabelSession, "agent:worker-1", "template:worker"},
			Metadata: map[string]string{
				"template":                              "worker",
				"agent_name":                            "worker-1",
				"alias":                                 "worker-1",
				"session_name":                          "worker-dependency-trigger",
				"instance_token":                        "instance-dependency-trigger",
				"state":                                 string(session.StateActive),
				"dependency_only":                       "true",
				"pool_slot":                             "1",
				poolManagedMetadataKey:                  "true",
				beadmeta.TriggerBeadIDMetadataKey:       work.ID,
				beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
			},
		}); err != nil {
			t.Fatal(err)
		}
		snapshot, err := loadSessionBeadSnapshot(store)
		if err != nil {
			t.Fatal(err)
		}
		provider := &projectHookIsolationWorkerProvider{Fake: runtime.NewFake(), supported: true}
		var stderr bytes.Buffer
		bp := newAgentBuildParams("fixture-city", cityPath, cfg, provider, time.Unix(1_700_000_000, 0).UTC(), store, &stderr)
		bp.sessionBeads = snapshot
		desired := map[string]TemplateParams{}

		ensureDependencyOnlyTemplate(bp, cfg, &cfg.Agents[0], desired, &stderr)

		if len(desired) != 1 {
			t.Fatalf("desired dependency floors = %d, want 1; state=%+v stderr=%q", len(desired), desired, stderr.String())
		}
		for _, tp := range desired {
			if tp.Env["GC_TRIGGER_BEAD_ID"] != work.ID || tp.Env["GC_TRIGGER_BEAD_STORE_REF"] != "city:fixture-city" {
				t.Fatalf("trigger env = (%q, %q), want (%q, city:fixture-city)", tp.Env["GC_TRIGGER_BEAD_ID"], tp.Env["GC_TRIGGER_BEAD_STORE_REF"], work.ID)
			}
		}
	})

	t.Run("named always", func(t *testing.T) {
		t.Setenv("GC_SESSION", config.SessionTransportTmux)
		cityPath := t.TempDir()
		store := beads.NewMemStore()
		cfg := &config.City{
			Workspace: config.Workspace{Name: "fixture-city"},
			Agents: []config.Agent{{
				Name:              "worker",
				StartCommand:      "true",
				WorkDir:           filepath.Join(t.TempDir(), "{{.AgentBase}}"),
				ProjectHooks:      config.ProjectHooksForbid,
				MinActiveSessions: intPtr(0),
				MaxActiveSessions: intPtr(1),
			}},
			NamedSessions: []config.NamedSession{{Template: "worker", Mode: "always"}},
		}
		work, err := store.Create(beads.Bead{
			ID:       "work-named-trigger",
			Title:    "exact named work",
			Type:     "task",
			Status:   "in_progress",
			Assignee: "worker",
			Metadata: map[string]string{
				beadmeta.RoutedToMetadataKey:             "worker",
				beadmeta.SessionIDMetadataKey:            "session-named-trigger",
				beadmeta.SessionNameMetadataKey:          "worker-named-trigger",
				beadmeta.SessionInstanceTokenMetadataKey: "instance-named-trigger",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Create(beads.Bead{
			ID:     "session-named-trigger",
			Title:  "worker",
			Type:   session.BeadType,
			Status: "open",
			Labels: []string{session.LabelSession, "agent:worker", "template:worker"},
			Metadata: map[string]string{
				"template":                              "worker",
				"agent_name":                            "worker",
				"alias":                                 "worker",
				"session_name":                          "worker-named-trigger",
				"instance_token":                        "instance-named-trigger",
				"state":                                 string(session.StateActive),
				namedSessionMetadataKey:                 "true",
				namedSessionIdentityMetadata:            "worker",
				namedSessionModeMetadata:                "always",
				"session_origin":                        "named",
				beadmeta.TriggerBeadIDMetadataKey:       work.ID,
				beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
			},
		}); err != nil {
			t.Fatal(err)
		}
		provider := &projectHookIsolationWorkerProvider{Fake: runtime.NewFake(), supported: true}
		var stderr bytes.Buffer

		result := buildDesiredState("fixture-city", cityPath, time.Unix(1_700_000_000, 0).UTC(), cfg, provider, store, &stderr)

		var found bool
		for _, tp := range result.State {
			if tp.ConfiguredNamedIdentity != "worker" {
				continue
			}
			found = true
			if tp.Env["GC_TRIGGER_BEAD_ID"] != work.ID || tp.Env["GC_TRIGGER_BEAD_STORE_REF"] != "city:fixture-city" {
				t.Fatalf("trigger env = (%q, %q), want (%q, city:fixture-city)", tp.Env["GC_TRIGGER_BEAD_ID"], tp.Env["GC_TRIGGER_BEAD_STORE_REF"], work.ID)
			}
		}
		if !found {
			t.Fatalf("triggered named session missing from desired state; state=%+v stderr=%q", result.State, stderr.String())
		}
	})
}

func TestValidateProjectHookSessionTriggerRequiresCompleteCoLocatedPair(t *testing.T) {
	const cityRef = "city:fixture-city"
	for _, tc := range []struct {
		name    string
		info    session.Info
		wantErr bool
	}{
		{name: "missing pair", wantErr: true},
		{name: "partial pair", info: session.Info{TriggerBeadID: "work-1"}, wantErr: true},
		{name: "rig store is not co-located", info: session.Info{TriggerBeadID: "work-1", TriggerBeadStoreRef: "rig:fixture"}, wantErr: true},
		{name: "other city is not co-located", info: session.Info{TriggerBeadID: "work-1", TriggerBeadStoreRef: "city:other"}, wantErr: true},
		{name: "exact city pair", info: session.Info{TriggerBeadID: "work-1", TriggerBeadStoreRef: cityRef}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateProjectHookSessionTrigger(tc.info, cityRef)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestBuildAwakeInputProjectHooksPolicyTriggerFence(t *testing.T) {
	const cityRef = "city:fixture-city"
	now := time.Unix(1_700_000_000, 0).UTC()
	for _, tc := range []struct {
		name        string
		policy      config.ProjectHooksPolicy
		shape       string
		triggered   bool
		wantSession bool
		wantWake    bool
		wantReason  string
	}{
		{
			name:   "forbid named always without trigger cannot launch",
			policy: config.ProjectHooksForbid,
			shape:  "named",
		},
		{
			name:   "forbid dependency floor without trigger cannot launch",
			policy: config.ProjectHooksForbid,
			shape:  "dependency",
		},
		{
			name:   "forbid manual session without trigger cannot launch",
			policy: config.ProjectHooksForbid,
			shape:  "manual",
		},
		{
			name:   "forbid ordinary explicit wake without trigger cannot launch",
			policy: config.ProjectHooksForbid,
			shape:  "explicit",
		},
		{
			name:   "forbid ordinary pending create without trigger cannot launch",
			policy: config.ProjectHooksForbid,
			shape:  "pending",
		},
		{
			name:        "inherit named always keeps ordinary launch",
			policy:      config.ProjectHooksInherit,
			shape:       "named",
			wantSession: true,
			wantWake:    true,
			wantReason:  "named-always",
		},
		{
			name:        "inherit dependency floor keeps pending create launch",
			policy:      config.ProjectHooksInherit,
			shape:       "dependency",
			wantSession: true,
			wantWake:    true,
			wantReason:  "pending-create",
		},
		{
			name:        "inherit ordinary explicit wake keeps launch",
			policy:      config.ProjectHooksInherit,
			shape:       "explicit",
			wantSession: true,
			wantWake:    true,
			wantReason:  "explicit-wake",
		},
		{
			name:        "inherit ordinary pending create keeps launch",
			policy:      config.ProjectHooksInherit,
			shape:       "pending",
			wantSession: true,
			wantWake:    true,
			wantReason:  "pending-create",
		},
		{
			name:        "forbid named always with exact trigger may launch",
			policy:      config.ProjectHooksForbid,
			shape:       "named",
			triggered:   true,
			wantSession: true,
			wantWake:    true,
			wantReason:  "named-always",
		},
		{
			name:        "forbid dependency floor with exact trigger may launch",
			policy:      config.ProjectHooksForbid,
			shape:       "dependency",
			triggered:   true,
			wantSession: true,
			wantWake:    true,
			wantReason:  "pending-create",
		},
		{
			name:        "forbid manual session with exact trigger may launch",
			policy:      config.ProjectHooksForbid,
			shape:       "manual",
			triggered:   true,
			wantSession: true,
			wantWake:    true,
			wantReason:  "manual",
		},
		{
			name:        "forbid ordinary explicit wake with exact trigger may launch",
			policy:      config.ProjectHooksForbid,
			shape:       "explicit",
			triggered:   true,
			wantSession: true,
			wantWake:    true,
			wantReason:  "explicit-wake",
		},
		{
			name:        "forbid ordinary pending create with exact trigger may launch",
			policy:      config.ProjectHooksForbid,
			shape:       "pending",
			triggered:   true,
			wantSession: true,
			wantWake:    true,
			wantReason:  "pending-create",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cityPath := t.TempDir()
			cfg := &config.City{
				Workspace: config.Workspace{Name: "fixture-city"},
				Agents: []config.Agent{{
					Name:         "worker",
					ProjectHooks: tc.policy,
				}},
			}
			metadata := map[string]string{
				"state":          string(session.StateCreating),
				"session_name":   "worker-session",
				"template":       "worker",
				"instance_token": "instance-current",
			}
			switch tc.shape {
			case "named":
				cfg.NamedSessions = []config.NamedSession{{Template: "worker", Mode: "always"}}
				metadata["state"] = "stopped"
				metadata[namedSessionMetadataKey] = "true"
				metadata[namedSessionIdentityMetadata] = "worker"
				metadata[namedSessionModeMetadata] = "always"
			case "dependency":
				metadata["dependency_only"] = "true"
				metadata["pending_create_claim"] = "true"
			case "manual":
				metadata["state"] = "stopped"
				metadata["manual_session"] = "true"
			case "explicit":
				metadata["state"] = "stopped"
				metadata["wake_request"] = string(session.WakeCauseExplicit)
			case "pending":
				metadata["pending_create_claim"] = "true"
			}
			if tc.triggered {
				metadata[beadmeta.TriggerBeadIDMetadataKey] = "work-exact"
				metadata[beadmeta.TriggerBeadStoreRefMetadataKey] = cityRef
			}
			info := sessiontest.SeedBead(t, beads.Bead{
				ID:       "session-controlled",
				Type:     session.BeadType,
				Status:   "open",
				Labels:   []string{session.LabelSession, "agent:worker"},
				Metadata: metadata,
			})

			input := buildAwakeInputFromReconciler(
				cfg,
				cityPath,
				[]session.Info{info},
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				runtime.NewFake(),
				now,
			)
			if got := len(input.SessionBeads); (got == 1) != tc.wantSession {
				t.Fatalf("awake input session count = %d, want present=%v; input=%+v", got, tc.wantSession, input.SessionBeads)
			}
			decision, ok := ComputeAwakeSet(input)["worker-session"]
			if ok != tc.wantSession {
				t.Fatalf("awake decision present = %v, want %v; decision=%+v", ok, tc.wantSession, decision)
			}
			if decision.ShouldWake != tc.wantWake || decision.Reason != tc.wantReason {
				t.Fatalf("awake decision = %+v, want wake=%v reason=%q", decision, tc.wantWake, tc.wantReason)
			}
		})
	}
}

func TestDiscoverSessionBeadsProjectHooksPolicyTriggerFenceCoversManualSessions(t *testing.T) {
	for _, tc := range []struct {
		name        string
		policy      config.ProjectHooksPolicy
		triggered   bool
		preDesired  bool
		wantDesired bool
	}{
		{name: "forbid without trigger is not desired", policy: config.ProjectHooksForbid},
		{name: "forbid without trigger removes preselected desired row", policy: config.ProjectHooksForbid, preDesired: true},
		{name: "inherit without trigger remains desired", policy: config.ProjectHooksInherit, wantDesired: true},
		{name: "forbid with exact trigger is desired", policy: config.ProjectHooksForbid, triggered: true, wantDesired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cityPath := t.TempDir()
			cityName := "fixture-city"
			cfg := &config.City{
				Workspace: config.Workspace{Name: cityName},
				Agents: []config.Agent{{
					Name:              "worker",
					StartCommand:      "true",
					WorkDir:           filepath.Join(t.TempDir(), "{{.AgentBase}}"),
					ProjectHooks:      tc.policy,
					MinActiveSessions: intPtr(0),
					MaxActiveSessions: intPtr(1),
				}},
			}
			metadata := map[string]string{
				"state":          string(session.StateActive),
				"session_name":   "worker-manual",
				"template":       "worker",
				"agent_name":     "worker",
				"alias":          "worker-manual",
				"manual_session": "true",
				"instance_token": "instance-current",
			}
			if tc.triggered {
				metadata[beadmeta.TriggerBeadIDMetadataKey] = "work-exact"
				metadata[beadmeta.TriggerBeadStoreRefMetadataKey] = "city:" + cityName
			}
			info := sessiontest.SeedBead(t, beads.Bead{
				ID:       "session-manual",
				Type:     session.BeadType,
				Status:   "open",
				Labels:   []string{session.LabelSession, "agent:worker"},
				Metadata: metadata,
			})
			provider := &projectHookIsolationWorkerProvider{Fake: runtime.NewFake(), supported: true}
			var stderr bytes.Buffer
			bp := newAgentBuildParams(cityName, cityPath, cfg, provider, time.Unix(1_700_000_000, 0).UTC(), beads.NewMemStore(), &stderr)
			bp.sessionBeads = newSessionBeadSnapshotFromInfos([]session.Info{info})
			desired := map[string]TemplateParams{}
			if tc.preDesired {
				desired["worker-manual"] = TemplateParams{SessionName: "worker-manual", TemplateName: "worker"}
			}

			discoverSessionBeadsWithRoots(bp, cfg, desired, nil, nil, nil, &stderr)

			_, found := desired["worker-manual"]
			if found != tc.wantDesired {
				t.Fatalf("manual desired = %v, want %v; desired=%+v stderr=%q", found, tc.wantDesired, desired, stderr.String())
			}
		})
	}
}

func TestHookCommandClaimProjectHooksForbidManagedNoTriggerFailsBeforeQueryOrMutation(t *testing.T) {
	clearGCEnv(t)
	disableManagedDoltRecoveryForTest(t)
	t.Setenv("GC_BEADS", "file")

	cityDir := t.TempDir()
	workDir := t.TempDir()
	queryMarker := filepath.Join(t.TempDir(), "work-query-ran")
	if err := os.MkdirAll(filepath.Join(cityDir, ".gc"), 0o755); err != nil {
		t.Fatal(err)
	}
	workQuery := fmt.Sprintf(
		"printf ran > %s; printf '[{\"id\":\"work-post-build\",\"status\":\"open\",\"metadata\":{\"gc.routed_to\":\"worker\"}}]'",
		queryMarker,
	)
	cityTOML := fmt.Sprintf(`[workspace]
name = "fixture-city"

[[agent]]
name = "worker"
work_dir = %q
work_query = %q
project_hooks = "forbid"
`, workDir, workQuery)
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(cityTOML), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatal(err)
	}
	sessionBead, err := store.Create(beads.Bead{
		Title:  "worker dependency floor",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession, "agent:worker"},
		Metadata: map[string]string{
			"template":             "worker",
			"agent_name":           "worker",
			"alias":                "worker",
			"session_name":         "worker-dependency",
			"instance_token":       "instance-current",
			"state":                string(session.StateActive),
			"dependency_only":      "true",
			poolManagedMetadataKey: "true",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	work, err := store.Create(beads.Bead{
		ID:     "work-post-build",
		Title:  "routed after desired-state build",
		Type:   "task",
		Status: "open",
		Metadata: map[string]string{
			beadmeta.RoutedToMetadataKey: "worker",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}

	fakeBin := t.TempDir()
	claimLog := filepath.Join(t.TempDir(), "bd-claim.log")
	fakeBD := filepath.Join(fakeBin, "bd")
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" >> %q\nprintf '[]'\n", claimLog)
	if err := os.WriteFile(fakeBD, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_TEMPLATE", "worker")
	t.Setenv("GC_AGENT", "worker")
	t.Setenv("GC_ALIAS", "worker")
	t.Setenv("GC_SESSION_ID", sessionBead.ID)
	t.Setenv("GC_SESSION_NAME", "worker-dependency")
	t.Setenv("GC_SESSION_ORIGIN", "ephemeral")
	t.Setenv("GC_INSTANCE_TOKEN", "instance-current")
	t.Setenv("GC_TRIGGER_BEAD_ID", "")
	t.Setenv("GC_TRIGGER_BEAD_STORE_REF", "")

	var stdout, stderr bytes.Buffer
	code := cmdHookWithOptions(nil, hookCommandOptions{Claim: true, DrainAck: true, JSON: true}, &stdout, &stderr)

	if code != 1 {
		t.Fatalf("code = %d, want fail-closed 1; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want no work/drain output", stdout.String())
	}
	if !strings.Contains(stderr.String(), "project_hooks=forbid") || !strings.Contains(stderr.String(), "exact trigger") {
		t.Fatalf("stderr = %q, want project_hooks=forbid exact-trigger refusal", stderr.String())
	}
	if _, err := os.Stat(queryMarker); !os.IsNotExist(err) {
		t.Fatalf("work query ran before exact trigger fence: stat err=%v", err)
	}
	if _, err := os.Stat(claimLog); !os.IsNotExist(err) {
		data, _ := os.ReadFile(claimLog)
		t.Fatalf("bd mutation surface reached before exact trigger fence: %q", data)
	}
	after, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != before.Revision || after.Status != before.Status || after.Assignee != before.Assignee {
		t.Fatalf("routed work mutated without exact trigger: before=%+v after=%+v", before, after)
	}
	if got := after.Metadata[beadmeta.SessionIDMetadataKey]; got != "" {
		t.Fatalf("routed work session witness = %q, want empty", got)
	}
}
