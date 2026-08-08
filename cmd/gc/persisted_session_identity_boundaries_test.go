package main

import (
	"context"
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/worker"
)

func TestPersistedSessionIdentityConflictDeniesCLIBoundaries(t *testing.T) {
	cfg := persistedBoundaryIdentityConfig()
	authorize := workerLaunchAuthorizerWithConfig("/cities/fixture-city", cfg)
	tests := []struct {
		name string
		info session.Info
	}{
		{name: "agent name", info: session.Info{Template: "inherit", AgentName: "strict"}},
		{name: "agent label", info: session.Info{Template: "inherit", Labels: []string{"agent:strict"}}},
		{name: "alias", info: session.Info{Template: "inherit", Alias: "strict"}},
		{name: "common name", info: session.Info{Template: "inherit", CommonName: "strict"}},
		{name: "configured named identity", info: session.Info{Template: "inherit", ConfiguredNamedIdentity: "strict-alias"}},
		{name: "canonical pool member", info: session.Info{
			Template:                      "inherit",
			CanonicalInstanceNameMetadata: "rig/strict-pool-2",
			CanonicalPoolSlotMetadata:     "2",
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info := tc.info
			info.ID = "session-conflict"

			if _, err := authorize(context.Background(), worker.LaunchAuthorizationRequest{
				Session: worker.SessionSpec{Template: info.Template},
				Info:    &info,
			}); !errors.Is(err, worker.ErrLaunchUnauthorized) || !errors.Is(err, agentutil.ErrPersistedSessionIdentityConflict) {
				t.Fatalf("launch authorization error = %v, want unauthorized identity conflict", err)
			}
			if _, err := resolvedWorkerRuntimeWithConfigAndMetadata("/cities/fixture-city", cfg, info, "", nil); !errors.Is(err, agentutil.ErrPersistedSessionIdentityConflict) {
				t.Fatalf("runtime resolution error = %v, want identity conflict", err)
			}
			if projectHookControllerSessionLaunchAuthorized(cfg, "/cities/fixture-city", info) {
				t.Fatal("awake bridge authorized conflicting identity")
			}
			if strict, err := hookClaimSessionRequiresExactTrigger(cfg, info); strict || !errors.Is(err, agentutil.ErrPersistedSessionIdentityConflict) {
				t.Fatalf("hook strict classification = (%v, %v), want conflict refusal", strict, err)
			}
			if strictAgent := strictConfiguredAgentForSession(cfg, info); strictAgent == nil || !strictAgent.ForbidsProjectHooks() {
				t.Fatalf("strict store classifier laundered conflict: %+v", strictAgent)
			}
			if err := strictSessionOwnershipAuthorized(cfg, "/cities/fixture-city", "fixture-city", nil, beads.WorkStore{}, info); !errors.Is(err, agentutil.ErrPersistedSessionIdentityConflict) {
				t.Fatalf("strict ownership error = %v, want identity conflict", err)
			}

			bead := persistedBoundarySessionBead(info)
			fence := captureBackstopMutationFence(bead, cfg)
			called := false
			if fence.mutate(nil, nil, &bead, "test", nil, func(beads.Store, *beads.Bead) bool {
				called = true
				return true
			}) || called {
				t.Fatal("backstop conflict reached pacing mutation")
			}
			if !errors.Is(fence.policyErr, agentutil.ErrPersistedSessionIdentityConflict) {
				t.Fatalf("backstop policy error = %v, want identity conflict", fence.policyErr)
			}
		})
	}
}

func TestStrictSnapshotFilterWithholdsConflictEvenWhenAllConfiguredPoliciesInherit(t *testing.T) {
	cfg := &config.City{Agents: []config.Agent{
		{Name: "first", ProjectHooks: config.ProjectHooksInherit},
		{Name: "second", ProjectHooks: config.ProjectHooksInherit},
	}}
	snapshot := newSessionBeadSnapshotFromReconcileRows([]session.ReconcileSession{{
		Info: session.Info{ID: "session-conflict", Template: "first", AgentName: "second"},
	}})
	store := beads.NewMemStore()
	filtered, withheld := filterStrictTopologyUnauthorizedSessionSnapshot(
		cfg,
		beads.SessionStore{Store: store},
		beads.WorkStore{Store: store},
		snapshot,
	)
	if !withheld {
		t.Fatal("conflicting inherit row was not withheld")
	}
	if rows := filtered.OpenForReconcile(); len(rows) != 0 {
		t.Fatalf("filtered rows = %+v, want none", rows)
	}
}

func persistedBoundaryIdentityConfig() *config.City {
	maxThree := 3
	return &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Rigs:      []config.Rig{{Name: "rig"}},
		Agents: []config.Agent{
			{Name: "inherit", ProjectHooks: config.ProjectHooksInherit},
			{Name: "strict", ProjectHooks: config.ProjectHooksForbid},
			{Name: "strict-pool", Dir: "rig", MaxActiveSessions: &maxThree, ProjectHooks: config.ProjectHooksForbid},
		},
		NamedSessions: []config.NamedSession{{Name: "strict-alias", Template: "strict"}},
	}
}

func persistedBoundarySessionBead(info session.Info) beads.Bead {
	metadata := map[string]string{
		"template":                            info.Template,
		"agent_name":                          info.AgentName,
		"alias":                               info.Alias,
		"common_name":                         info.CommonName,
		session.NamedSessionIdentityMetadata:  info.ConfiguredNamedIdentity,
		session.CanonicalInstanceNameMetadata: info.CanonicalInstanceNameMetadata,
		session.CanonicalPoolSlotMetadata:     info.CanonicalPoolSlotMetadata,
	}
	return beads.Bead{
		ID:       info.ID,
		Type:     session.BeadType,
		Status:   "open",
		Labels:   append([]string{session.LabelSession}, info.Labels...),
		Metadata: metadata,
	}
}
