package config

import (
	"strings"
	"testing"
)

func TestAgentProjectHooksPolicyPropagationAndConflicts(t *testing.T) {
	t.Run("TOML parses exact policy and Agent.Clone preserves it", func(t *testing.T) {
		cfg, err := Parse([]byte(`
[[agent]]
name = "worker"
project_hooks = "forbid"
`))
		if err != nil {
			t.Fatalf("Parse(project_hooks=forbid): %v", err)
		}
		if len(cfg.Agents) != 1 {
			t.Fatalf("parsed agents = %d, want 1", len(cfg.Agents))
		}
		if got := cfg.Agents[0].ProjectHooks; got != ProjectHooksForbid {
			t.Fatalf("parsed ProjectHooks = %q, want %q", got, ProjectHooksForbid)
		}

		clone := cfg.Agents[0].Clone()
		if got := clone.ProjectHooks; got != ProjectHooksForbid {
			t.Fatalf("cloned ProjectHooks = %q, want %q", got, ProjectHooksForbid)
		}
		clone.ProjectHooks = ProjectHooksInherit
		if got := cfg.Agents[0].ProjectHooks; got != ProjectHooksForbid {
			t.Fatalf("original ProjectHooks after clone mutation = %q, want %q", got, ProjectHooksForbid)
		}
	})

	t.Run("omitted policy inherits", func(t *testing.T) {
		agent := Agent{Name: "worker"}
		if got := agent.EffectiveProjectHooksPolicy(); got != ProjectHooksInherit {
			t.Fatalf("EffectiveProjectHooksPolicy() = %q, want %q", got, ProjectHooksInherit)
		}
		if err := ValidateAgents([]Agent{agent}); err != nil {
			t.Fatalf("ValidateAgents() error = %v, want nil", err)
		}
	})

	t.Run("agent patch propagates typed policy", func(t *testing.T) {
		policy := ProjectHooksForbid
		cfg := &City{Agents: []Agent{{Name: "worker"}}}
		if err := ApplyPatches(cfg, Patches{Agents: []AgentPatch{{
			Name:         "worker",
			ProjectHooks: &policy,
		}}}); err != nil {
			t.Fatalf("ApplyPatches() error = %v, want nil", err)
		}
		if got := cfg.Agents[0].ProjectHooks; got != ProjectHooksForbid {
			t.Fatalf("patched ProjectHooks = %q, want %q", got, ProjectHooksForbid)
		}
	})

	t.Run("rig override propagates typed policy", func(t *testing.T) {
		policy := ProjectHooksForbid
		agent := Agent{Name: "worker"}
		applyAgentOverride(&agent, &AgentOverride{Agent: "worker", ProjectHooks: &policy})
		if got := agent.ProjectHooks; got != ProjectHooksForbid {
			t.Fatalf("overridden ProjectHooks = %q, want %q", got, ProjectHooksForbid)
		}
	})

	t.Run("unknown policy is rejected", func(t *testing.T) {
		err := ValidateAgents([]Agent{{Name: "worker", ProjectHooks: ProjectHooksPolicy("discover")}})
		if err == nil {
			t.Fatal("ValidateAgents() error = nil, want invalid project_hooks error")
		}
		for _, want := range []string{"project_hooks", "discover"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q missing %q", err, want)
			}
		}
	})

	for _, tc := range []struct {
		name  string
		agent Agent
		want  string
	}{
		{
			name: "forbid conflicts with installed hooks",
			agent: Agent{
				Name:           "worker",
				ProjectHooks:   ProjectHooksForbid,
				HooksInstalled: boolValue(true),
			},
			want: "hooks_installed",
		},
		{
			name: "forbid conflicts with hook installers",
			agent: Agent{
				Name:              "worker",
				ProjectHooks:      ProjectHooksForbid,
				InstallAgentHooks: []string{"provider-a"},
			},
			want: "install_agent_hooks",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAgents([]Agent{tc.agent})
			if err == nil {
				t.Fatalf("ValidateAgents() error = nil, want conflict with %s", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want %q context", err, tc.want)
			}
		})
	}

	t.Run("explicit false installed marker does not conflict", func(t *testing.T) {
		agent := Agent{
			Name:           "worker",
			ProjectHooks:   ProjectHooksForbid,
			HooksInstalled: boolValue(false),
		}
		if err := ValidateAgents([]Agent{agent}); err != nil {
			t.Fatalf("ValidateAgents() error = %v, want nil", err)
		}
	})
}

func TestValidateCityAgentsRejectsInheritedHooksUnderProjectHooksForbid(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{InstallAgentHooks: []string{"codex"}},
		Agents: []Agent{{
			Name:         "worker",
			ProjectHooks: ProjectHooksForbid,
		}},
	}
	err := ValidateCityAgents(cfg)
	if err == nil || !strings.Contains(err.Error(), "workspace install_agent_hooks") {
		t.Fatalf("ValidateCityAgents() error = %v, want inherited hook conflict", err)
	}
	if got := ResolveInstallHooks(&cfg.Agents[0], &cfg.Workspace); len(got) != 0 {
		t.Fatalf("ResolveInstallHooks(forbid) = %v, want no effective hook writers", got)
	}
	if AgentHasHooks(&cfg.Agents[0], &cfg.Workspace, "codex", nil) {
		t.Fatal("AgentHasHooks(forbid) = true, want false")
	}
}

func boolValue(value bool) *bool {
	return &value
}
