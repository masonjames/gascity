package api

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/worker"
)

func TestPersistedSessionIdentityConflictDeniesAPIWorkerBoundaries(t *testing.T) {
	fs := newSessionFakeState(t)
	maxThree := 3
	fs.cfg = &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Rigs:      []config.Rig{{Name: "rig"}},
		Agents: []config.Agent{
			{Name: "inherit", ProjectHooks: config.ProjectHooksInherit},
			{Name: "strict", ProjectHooks: config.ProjectHooksForbid},
			{Name: "strict-pool", Dir: "rig", MaxActiveSessions: &maxThree, ProjectHooks: config.ProjectHooksForbid},
		},
		NamedSessions: []config.NamedSession{{Name: "strict-alias", Template: "strict"}},
	}
	srv := New(fs)
	authorize := srv.workerLaunchAuthorizer()
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
			if _, err := srv.resolveWorkerSessionRuntimeWithMetadata(info, "", nil); !errors.Is(err, agentutil.ErrPersistedSessionIdentityConflict) {
				t.Fatalf("runtime resolution error = %v, want identity conflict", err)
			}
			if calls := fs.sp.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("provider calls = %+v, want zero", calls)
			}
		})
	}
}

func TestProviderShapedPersistedSessionUnknownTemplateRemainsProviderBacked(t *testing.T) {
	tests := []struct {
		name        string
		sessionKind string
		metadata    map[string]string
	}{
		{
			name:        "explicit provider kind",
			sessionKind: "provider",
			metadata:    map[string]string{"real_world_app_session_kind": "provider"},
		},
		{
			name:     "legacy manual provider session",
			metadata: map[string]string{"session_origin": "manual"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newSessionFakeState(t)
			fs.cfg.Providers["provider-only"] = config.ProviderSpec{
				Command:   "/bin/echo",
				PathCheck: "true",
			}
			srv := New(fs)
			info := session.Info{
				ID:       "provider-session",
				Template: "provider-only",
				Provider: "provider-only",
				WorkDir:  t.TempDir(),
			}

			if _, err := srv.workerLaunchAuthorizer()(context.Background(), worker.LaunchAuthorizationRequest{
				Session:  worker.SessionSpec{Template: info.Template, Provider: info.Provider},
				Info:     &info,
				Metadata: tc.metadata,
			}); err != nil {
				t.Fatalf("provider launch authorization: %v", err)
			}
			resolved, err := srv.resolveWorkerSessionRuntimeWithMetadata(info, tc.sessionKind, tc.metadata)
			if err != nil {
				t.Fatalf("provider runtime resolution: %v", err)
			}
			if resolved == nil || resolved.Provider != "provider-only" {
				t.Fatalf("provider runtime = %+v, want provider-only", resolved)
			}
		})
	}
}

func TestProviderShapedPersistedSessionTemplateCollisionDoesNotInheritAgentPolicy(t *testing.T) {
	fs := newSessionFakeState(t)
	fs.cfg.Agents = append(fs.cfg.Agents, config.Agent{
		Name:         "provider-only",
		Provider:     "provider-only",
		ProjectHooks: config.ProjectHooksForbid,
	})
	fs.cfg.Providers["provider-only"] = config.ProviderSpec{
		Command:   "/bin/echo",
		PathCheck: "true",
	}
	srv := New(fs)
	info := session.Info{
		ID:       "provider-session",
		Template: "provider-only",
		Provider: "provider-only",
		WorkDir:  t.TempDir(),
	}
	tests := []struct {
		name        string
		sessionKind string
		metadata    map[string]string
	}{
		{
			name:        "explicit provider kind",
			sessionKind: "provider",
			metadata:    map[string]string{"real_world_app_session_kind": "provider"},
		},
		{
			name:     "legacy manual provider session",
			metadata: map[string]string{"session_origin": "manual"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := srv.workerLaunchAuthorizer()(context.Background(), worker.LaunchAuthorizationRequest{
				Session:  worker.SessionSpec{Template: info.Template, Provider: info.Provider},
				Info:     &info,
				Metadata: tc.metadata,
			}); err != nil {
				t.Fatalf("provider launch authorization inherited colliding agent policy: %v", err)
			}
			resolved, err := srv.resolveWorkerSessionRuntimeWithMetadata(info, tc.sessionKind, tc.metadata)
			if err != nil {
				t.Fatalf("provider runtime resolution: %v", err)
			}
			if resolved == nil || resolved.Provider != "provider-only" {
				t.Fatalf("provider runtime = %+v, want provider-only", resolved)
			}
			if resolved.Hints.ProjectHooksForbidden {
				t.Fatalf("provider runtime inherited project-hook isolation from colliding agent: %+v", resolved)
			}
		})
	}
}

func TestProviderShapedPersistedSessionKeepsOtherAuthoritativeSignals(t *testing.T) {
	fs := newSessionFakeState(t)
	srv := New(fs)
	tests := []struct {
		name       string
		wantSource string
		mutate     func(*session.Info)
	}{
		{name: "agent name", wantSource: "agent_name", mutate: func(info *session.Info) { info.AgentName = "missing" }},
		{name: "agent label", wantSource: "agent_label", mutate: func(info *session.Info) { info.Labels = []string{"agent:missing"} }},
		{name: "configured named identity", wantSource: "configured_named_identity", mutate: func(info *session.Info) { info.ConfiguredNamedIdentity = "missing" }},
		{name: "canonical identity", wantSource: "canonical identity", mutate: func(info *session.Info) { info.CanonicalInstanceNameMetadata = "missing" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info := session.Info{ID: "provider-session", Template: "provider-only", Provider: "provider-only"}
			tc.mutate(&info)
			metadata := map[string]string{"real_world_app_session_kind": "provider"}

			if _, err := srv.workerLaunchAuthorizer()(context.Background(), worker.LaunchAuthorizationRequest{
				Session:  worker.SessionSpec{Template: info.Template, Provider: info.Provider},
				Info:     &info,
				Metadata: metadata,
			}); !errors.Is(err, worker.ErrLaunchUnauthorized) || !errors.Is(err, agentutil.ErrPersistedSessionIdentityConflict) {
				t.Fatalf("launch authorization error = %v, want unauthorized identity conflict", err)
			} else if !strings.Contains(err.Error(), tc.wantSource) {
				t.Fatalf("launch authorization error = %v, want source %q (Template must be excluded)", err, tc.wantSource)
			}
			if _, err := srv.resolveWorkerSessionRuntimeWithMetadata(info, "provider", metadata); !errors.Is(err, agentutil.ErrPersistedSessionIdentityConflict) {
				t.Fatalf("runtime resolution error = %v, want identity conflict", err)
			} else if !strings.Contains(err.Error(), tc.wantSource) {
				t.Fatalf("runtime resolution error = %v, want source %q (Template must be excluded)", err, tc.wantSource)
			}
		})
	}
}

func TestAgentShapedPersistedSessionUnknownTemplateStillFailsClosed(t *testing.T) {
	tests := []struct {
		name        string
		sessionKind string
		metadata    map[string]string
	}{
		{
			name:        "explicit agent kind",
			sessionKind: "agent",
			metadata:    map[string]string{"real_world_app_session_kind": "agent"},
		},
		{name: "automatic legacy row"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newSessionFakeState(t)
			srv := New(fs)
			info := session.Info{ID: "agent-session", Template: "missing", Provider: "test-agent"}

			if _, err := srv.workerLaunchAuthorizer()(context.Background(), worker.LaunchAuthorizationRequest{
				Session:  worker.SessionSpec{Template: info.Template, Provider: info.Provider},
				Info:     &info,
				Metadata: tc.metadata,
			}); !errors.Is(err, worker.ErrLaunchUnauthorized) || !errors.Is(err, agentutil.ErrPersistedSessionIdentityConflict) {
				t.Fatalf("launch authorization error = %v, want unauthorized identity conflict", err)
			}
			if _, err := srv.resolveWorkerSessionRuntimeWithMetadata(info, tc.sessionKind, tc.metadata); !errors.Is(err, agentutil.ErrPersistedSessionIdentityConflict) {
				t.Fatalf("runtime resolution error = %v, want identity conflict", err)
			}
		})
	}
}
