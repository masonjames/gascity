package agentutil

import (
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

func TestResolvePersistedSessionAgentStrictSignalMatrix(t *testing.T) {
	cfg := persistedIdentityTestConfig()
	tests := []struct {
		name string
		info session.Info
	}{
		{name: "template", info: session.Info{Template: "strict"}},
		{name: "agent name", info: session.Info{AgentName: "strict"}},
		{name: "agent label", info: session.Info{Labels: []string{"gc:session", "agent:strict"}}},
		{name: "alias", info: session.Info{Alias: "strict"}},
		{name: "common name", info: session.Info{CommonName: "strict"}},
		{name: "configured named identity", info: session.Info{ConfiguredNamedIdentity: "strict-alias"}},
		{name: "canonical pool member", info: session.Info{
			CanonicalInstanceNameMetadata: "rig/strict-pool-2",
			CanonicalPoolSlotMetadata:     "2",
		}},
		{name: "namepool member", info: session.Info{AgentName: "rig/beta"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolution, err := ResolvePersistedSessionAgent(cfg, tc.info)
			if err != nil {
				t.Fatalf("ResolvePersistedSessionAgent: %v", err)
			}
			if !resolution.Resolved || !resolution.Strict || !resolution.Agent.ForbidsProjectHooks() {
				t.Fatalf("resolution = %+v, want strict configured agent", resolution)
			}
			if len(resolution.Signals) == 0 {
				t.Fatal("resolution omitted signal evidence")
			}
		})
	}
}

func TestResolvePersistedSessionAgentTemplateInheritConflictsWithEveryStrictSignal(t *testing.T) {
	cfg := persistedIdentityTestConfig()
	tests := []struct {
		name   string
		mutate func(*session.Info)
	}{
		{name: "agent name", mutate: func(i *session.Info) { i.AgentName = "strict" }},
		{name: "agent label", mutate: func(i *session.Info) { i.Labels = []string{"agent:strict"} }},
		{name: "alias", mutate: func(i *session.Info) { i.Alias = "strict" }},
		{name: "common name", mutate: func(i *session.Info) { i.CommonName = "strict" }},
		{name: "configured named", mutate: func(i *session.Info) { i.ConfiguredNamedIdentity = "strict-alias" }},
		{name: "canonical pool", mutate: func(i *session.Info) {
			i.CanonicalInstanceNameMetadata = "rig/strict-pool-2"
			i.CanonicalPoolSlotMetadata = "2"
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info := session.Info{Template: "inherit"}
			tc.mutate(&info)
			resolution, err := ResolvePersistedSessionAgent(cfg, info)
			if !errors.Is(err, ErrPersistedSessionIdentityConflict) {
				t.Fatalf("ResolvePersistedSessionAgent = (%+v, %v), want typed conflict", resolution, err)
			}
			if resolution.Resolved {
				t.Fatalf("conflicting resolution chose an agent: %+v", resolution)
			}
		})
	}
}

func TestResolvePersistedSessionAgentCanonicalizesPoolMembersToBase(t *testing.T) {
	cfg := persistedIdentityTestConfig()
	resolution, err := ResolvePersistedSessionAgent(cfg, session.Info{
		Template:                      "rig/strict-pool",
		AgentName:                     "rig/strict-pool-2",
		CanonicalInstanceNameMetadata: "rig/strict-pool-2",
		CanonicalPoolSlotMetadata:     "2",
	})
	if err != nil {
		t.Fatalf("ResolvePersistedSessionAgent: %v", err)
	}
	if got := resolution.Agent.QualifiedName(); got != "rig/strict-pool" {
		t.Fatalf("resolved base = %q, want rig/strict-pool", got)
	}
	for _, signal := range resolution.Signals {
		if signal.ConcreteIdentity != "" && signal.ConcreteIdentity != "rig/strict-pool-2" {
			t.Fatalf("concrete identity = %q, want member 2 or neutral", signal.ConcreteIdentity)
		}
	}
}

func TestResolvePersistedSessionAgentCanonicalizesLegacyDecoratedPoolMember(t *testing.T) {
	cfg := &config.City{Agents: []config.Agent{{
		Name:         "strict-pool",
		Dir:          "rig",
		ProjectHooks: config.ProjectHooksForbid,
	}}}
	resolution, err := ResolvePersistedSessionAgent(cfg, session.Info{
		Template:  "rig/strict-pool",
		AgentName: "rig/strict-pool__themed-1",
	})
	if err != nil {
		t.Fatalf("ResolvePersistedSessionAgent: %v", err)
	}
	if !resolution.Resolved || !resolution.Strict || resolution.Agent.QualifiedName() != "rig/strict-pool" {
		t.Fatalf("resolution = %+v, want decorated member canonicalized to strict base", resolution)
	}
	if got := resolution.Signals[1].ConcreteIdentity; got != "rig/strict-pool__themed-1" {
		t.Fatalf("concrete identity = %q", got)
	}
}

func TestResolvePersistedSessionAgentRejectsConflictingPoolMembers(t *testing.T) {
	cfg := persistedIdentityTestConfig()
	_, err := ResolvePersistedSessionAgent(cfg, session.Info{
		Template:                      "rig/strict-pool",
		AgentName:                     "rig/strict-pool-1",
		Labels:                        []string{"agent:rig/strict-pool-2"},
		CanonicalInstanceNameMetadata: "rig/strict-pool-3",
		CanonicalPoolSlotMetadata:     "3",
	})
	if !errors.Is(err, ErrPersistedSessionIdentityConflict) {
		t.Fatalf("ResolvePersistedSessionAgent error = %v, want member conflict", err)
	}
}

func TestResolvePersistedSessionAgentValidatesCanonicalRecord(t *testing.T) {
	cfg := persistedIdentityTestConfig()
	tests := []session.Info{
		{CanonicalInstanceNameMetadata: "rig/strict-pool-2", CanonicalPoolSlotMetadata: "bad"},
		{CanonicalInstanceNameMetadata: "rig/strict-pool-2", CanonicalPoolSlotMetadata: "1"},
		{CanonicalInstanceNameMetadata: "strict", CanonicalPoolSlotMetadata: "2"},
		{CanonicalInstanceNameMetadata: "unknown", CanonicalPoolSlotMetadata: ""},
	}
	for _, info := range tests {
		if _, err := ResolvePersistedSessionAgent(cfg, info); !errors.Is(err, ErrPersistedSessionIdentityConflict) {
			t.Errorf("ResolvePersistedSessionAgent(%+v) error = %v, want canonical conflict", info, err)
		}
	}
}

func TestResolvePersistedSessionAgentUsesSessionNameOnlyWhenUnique(t *testing.T) {
	cfg := persistedIdentityTestConfig()
	namedRuntime := config.NamedSessionRuntimeName(cfg.EffectiveCityName(), cfg.Workspace, "strict-alias")
	resolution, err := ResolvePersistedSessionAgent(cfg, session.Info{SessionNameMetadata: namedRuntime})
	if err != nil || !resolution.Strict || resolution.Agent.QualifiedName() != "strict" {
		t.Fatalf("named runtime resolution = (%+v, %v)", resolution, err)
	}

	poolRuntime := agent.SessionNameFor(cfg.EffectiveCityName(), "rig/strict-pool-2", cfg.Workspace.SessionTemplate)
	resolution, err = ResolvePersistedSessionAgent(cfg, session.Info{SessionNameMetadata: poolRuntime})
	if err != nil || !resolution.Strict || resolution.Agent.QualifiedName() != "rig/strict-pool" {
		t.Fatalf("pool runtime resolution = (%+v, %v)", resolution, err)
	}

	collision := persistedIdentityTestConfig()
	collision.Workspace.SessionTemplate = "constant"
	resolution, err = ResolvePersistedSessionAgent(collision, session.Info{SessionNameMetadata: "constant"})
	if err != nil {
		t.Fatalf("ambiguous SessionName should be ignored, got %v", err)
	}
	if resolution.Resolved {
		t.Fatalf("ambiguous SessionName resolved: %+v", resolution)
	}

	resolution, err = ResolvePersistedSessionAgent(cfg, session.Info{
		ID:          "derived",
		SessionName: namedRuntime,
	})
	if err != nil {
		t.Fatalf("non-persisted SessionName should be ignored, got %v", err)
	}
	if resolution.Resolved {
		t.Fatalf("non-persisted SessionName resolved: %+v", resolution)
	}
}

func TestResolvePersistedSessionAgentAmbiguousBareConfiguredIdentityFailsClosed(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "worker", Dir: "one"},
			{Name: "worker", Dir: "two", ProjectHooks: config.ProjectHooksForbid},
		},
	}
	_, err := ResolvePersistedSessionAgent(cfg, session.Info{AgentName: "worker"})
	if !errors.Is(err, ErrPersistedSessionIdentityConflict) {
		t.Fatalf("ambiguous bare identity error = %v, want conflict", err)
	}
}

func TestResolvePersistedSessionAgentConfiguredNamedIdentityIsAuthoritative(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.City
		info session.Info
	}{
		{
			name: "unknown named identity",
			cfg:  persistedIdentityTestConfig(),
			info: session.Info{ConfiguredNamedIdentity: "missing"},
		},
		{
			name: "named identity cannot fall through to agent",
			cfg:  persistedIdentityTestConfig(),
			info: session.Info{ConfiguredNamedIdentity: "strict"},
		},
		{
			name: "ambiguous named identity",
			cfg: func() *config.City {
				cfg := persistedIdentityTestConfig()
				cfg.NamedSessions = append(cfg.NamedSessions,
					config.NamedSession{Name: "shared", Dir: "one", Template: "strict"},
					config.NamedSession{Name: "shared", Dir: "two", Template: "strict"},
				)
				return cfg
			}(),
			info: session.Info{ConfiguredNamedIdentity: "shared"},
		},
		{
			name: "unresolved backing template",
			cfg: func() *config.City {
				cfg := persistedIdentityTestConfig()
				cfg.NamedSessions = append(cfg.NamedSessions,
					config.NamedSession{Name: "broken", Template: "missing"},
				)
				return cfg
			}(),
			info: session.Info{ConfiguredNamedIdentity: "broken"},
		},
		{
			name: "ambiguous backing template",
			cfg: func() *config.City {
				cfg := persistedIdentityTestConfig()
				cfg.Agents = append(cfg.Agents,
					config.Agent{Name: "shared-backing", Dir: "one"},
					config.Agent{Name: "shared-backing", Dir: "two", ProjectHooks: config.ProjectHooksForbid},
				)
				cfg.NamedSessions = append(cfg.NamedSessions,
					config.NamedSession{Name: "broken", Template: "shared-backing"},
				)
				return cfg
			}(),
			info: session.Info{ConfiguredNamedIdentity: "broken"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolution, err := ResolvePersistedSessionAgent(tc.cfg, tc.info)
			if !errors.Is(err, ErrPersistedSessionIdentityConflict) {
				t.Fatalf("ResolvePersistedSessionAgent = (%+v, %v), want authoritative named conflict", resolution, err)
			}
			if resolution.Resolved || resolution.Strict {
				t.Fatalf("conflicting resolution returned usable policy: %+v", resolution)
			}
		})
	}
}

func TestResolvePersistedSessionAgentReturnsIndependentAgentCopy(t *testing.T) {
	cfg := persistedIdentityTestConfig()
	cfg.Agents[3].Env = map[string]string{"unchanged": "true"}
	resolution, err := ResolvePersistedSessionAgent(cfg, session.Info{AgentName: "rig/beta"})
	if err != nil {
		t.Fatalf("ResolvePersistedSessionAgent: %v", err)
	}
	resolution.Agent.NamepoolNames[0] = "mutated"
	resolution.Agent.Env["unchanged"] = "false"
	if got := cfg.Agents[3].NamepoolNames[0]; got != "alpha" {
		t.Fatalf("configured NamepoolNames mutated through resolution: %q", got)
	}
	if got := cfg.Agents[3].Env["unchanged"]; got != "true" {
		t.Fatalf("configured Env mutated through resolution: %q", got)
	}
}

func TestResolvePersistedSessionAgentAuthoritativeUnknownSignalFailsClosed(t *testing.T) {
	cfg := persistedIdentityTestConfig()
	tests := []struct {
		name string
		info session.Info
	}{
		{name: "template", info: session.Info{Template: "missing"}},
		{name: "agent name", info: session.Info{AgentName: "missing"}},
		{name: "agent label", info: session.Info{Labels: []string{"agent:missing"}}},
		{name: "inherit plus unknown agent name", info: session.Info{Template: "inherit", AgentName: "missing"}},
		{name: "inherit plus unknown agent label", info: session.Info{Template: "inherit", Labels: []string{"agent:missing"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolution, err := ResolvePersistedSessionAgent(cfg, tc.info)
			if !errors.Is(err, ErrPersistedSessionIdentityConflict) {
				t.Fatalf("ResolvePersistedSessionAgent = (%+v, %v), want unknown authoritative conflict", resolution, err)
			}
			if resolution.Resolved || resolution.Strict {
				t.Fatalf("unknown authoritative signal returned usable policy: %+v", resolution)
			}
		})
	}
}

func TestResolvePersistedSessionAgentIgnoresUnknownLegacyHints(t *testing.T) {
	cfg := persistedIdentityTestConfig()
	resolution, err := ResolvePersistedSessionAgent(cfg, session.Info{
		Template:   "inherit",
		Alias:      "legacy-alias",
		CommonName: "legacy-common-name",
	})
	if err != nil {
		t.Fatalf("ResolvePersistedSessionAgent: %v", err)
	}
	if !resolution.Resolved || resolution.Strict || resolution.Agent.QualifiedName() != "inherit" {
		t.Fatalf("resolution = %+v, want configured inherit agent", resolution)
	}
}

func TestResolvePersistedSessionAgentRecognizesConfiguredAdhocIdentity(t *testing.T) {
	cfg := persistedIdentityTestConfig()
	resolution, err := ResolvePersistedSessionAgent(cfg, session.Info{
		Template:  "rig/named-pool",
		AgentName: "rig/named-pool-adhoc-abc123",
	})
	if err != nil {
		t.Fatalf("ResolvePersistedSessionAgent: %v", err)
	}
	if !resolution.Resolved || !resolution.Strict || resolution.Agent.QualifiedName() != "rig/named-pool" {
		t.Fatalf("resolution = %+v, want strict configured base", resolution)
	}
	var concrete string
	for _, signal := range resolution.Signals {
		if signal.Source == PersistedIdentityAgentName {
			concrete = signal.ConcreteIdentity
		}
	}
	if concrete != "rig/named-pool-adhoc-abc123" {
		t.Fatalf("adhoc concrete identity = %q", concrete)
	}

	_, err = ResolvePersistedSessionAgent(cfg, session.Info{
		Template:  "rig/named-pool",
		AgentName: "rig/named-pool-adhoc-one",
		Labels:    []string{"agent:rig/named-pool-adhoc-two"},
	})
	if !errors.Is(err, ErrPersistedSessionIdentityConflict) {
		t.Fatalf("incompatible adhoc identities error = %v, want conflict", err)
	}
}

func TestResolvePersistedSessionAgentAcceptsCanonicalAdhocIdentityWithoutPoolSlot(t *testing.T) {
	cfg := persistedIdentityTestConfig()
	resolution, err := ResolvePersistedSessionAgent(cfg, session.Info{
		CanonicalInstanceNameMetadata: "rig/named-pool-adhoc-abc123",
	})
	if err != nil {
		t.Fatalf("ResolvePersistedSessionAgent: %v", err)
	}
	if !resolution.Resolved || !resolution.Strict || resolution.Agent.QualifiedName() != "rig/named-pool" {
		t.Fatalf("resolution = %+v, want strict configured adhoc base", resolution)
	}
	if got := resolution.Signals[0].ConcreteIdentity; got != "rig/named-pool-adhoc-abc123" {
		t.Fatalf("canonical adhoc concrete identity = %q", got)
	}

	_, err = ResolvePersistedSessionAgent(cfg, session.Info{
		CanonicalInstanceNameMetadata: "rig/named-pool-adhoc-abc123",
		CanonicalPoolSlotMetadata:     "1",
	})
	if !errors.Is(err, ErrPersistedSessionIdentityConflict) {
		t.Fatalf("canonical adhoc identity with pool slot error = %v, want conflict", err)
	}
}

func persistedIdentityTestConfig() *config.City {
	maxThree := 3
	return &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "rig"}},
		Agents: []config.Agent{
			{Name: "inherit", ProjectHooks: config.ProjectHooksInherit},
			{Name: "strict", ProjectHooks: config.ProjectHooksForbid},
			{Name: "strict-pool", Dir: "rig", MaxActiveSessions: &maxThree, ProjectHooks: config.ProjectHooksForbid},
			{Name: "named-pool", Dir: "rig", MaxActiveSessions: &maxThree, NamepoolNames: []string{"alpha", "beta", "gamma"}, ProjectHooks: config.ProjectHooksForbid},
		},
		NamedSessions: []config.NamedSession{{Name: "strict-alias", Template: "strict"}},
	}
}
