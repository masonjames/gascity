package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

func TestAutomaticRuntimeDecisionUsesFreshConfiguredPolicyNotCallerValidator(t *testing.T) {
	store := beads.NewMemStore()
	provider := runtime.NewFake()
	manager := sessionpkg.NewManagerWithOptions(store, provider)
	created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
		BeadOnly: true,
		Template: "strict-worker",
		Title:    "Strict Worker",
		Command:  "codex",
		WorkDir:  t.TempDir(),
		Provider: "codex",
		ExtraMeta: map[string]string{
			"agent_name":                            "strict-worker",
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	info, persisted, err := manager.PersistedStore().GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("GetPersistedResponse: %v", err)
	}
	cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
	enforcedWorkDir := created.WorkDir
	factory, err := NewFactory(FactoryConfig{
		Store:              store,
		CanonicalCityStore: store,
		Provider:           provider,
		CityConfig:         cfg,
		AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
			authorization, err := RequireExactSessionTriggerAuthority(req, "city:fixture-city")
			if err != nil {
				return LaunchAuthorization{}, err
			}
			authorization.RuntimeEnforcement = &LaunchRuntimeEnforcement{
				ProjectHooksForbidden: true,
				ProviderName:          "codex",
				WorkDir:               enforcedWorkDir,
			}
			return authorization, nil
		},
	})
	if err != nil {
		t.Fatalf("NewFactory: %v", err)
	}
	decision := AutomaticRuntimeDecision{
		Captured:              info,
		ExpectedRevision:      persisted.Revision,
		RevisionCaptured:      true,
		Witness:               sessionpkg.LiveBoundaryWitness{TriggerBeadID: info.TriggerBeadID, TriggerBeadStoreRef: info.TriggerBeadStoreRef},
		ProjectHooksForbidden: true,
		ValidatePolicy: func(sessionpkg.Info, sessionpkg.PersistedResponse, bool) error {
			return nil
		},
	}
	prepared, _, err := factory.prepareAutomaticRuntimeDecision(context.Background(), info.ID, decision)
	if err != nil {
		t.Fatalf("prepareAutomaticRuntimeDecision: %v", err)
	}

	enforcedWorkDir = t.TempDir()
	if err := prepared.ValidatePolicy(info, persisted, true); !errors.Is(err, ErrLaunchUnauthorized) {
		t.Fatalf("fresh enforcement validator error = %v, want launch unauthorized after cwd drift", err)
	}
	enforcedWorkDir = created.WorkDir
	cfg.Agents[0].ProjectHooks = config.ProjectHooksInherit
	if err := prepared.ValidatePolicy(info, persisted, true); !errors.Is(err, ErrLaunchUnauthorized) {
		t.Fatalf("fresh policy validator error = %v, want launch unauthorized after config drift", err)
	}
}

func TestAutomaticRuntimeDecisionRejectsCallerStrictnessMismatchBeforeProvider(t *testing.T) {
	store := beads.NewMemStore()
	provider := runtime.NewFake()
	manager := sessionpkg.NewManagerWithOptions(store, provider)
	created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
		BeadOnly: true,
		Template: "strict-worker",
		Title:    "Strict Worker",
		Command:  "codex",
		WorkDir:  t.TempDir(),
		Provider: "codex",
		ExtraMeta: map[string]string{
			"agent_name":                            "strict-worker",
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	info, persisted, err := manager.PersistedStore().GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("GetPersistedResponse: %v", err)
	}
	factory, err := NewFactory(FactoryConfig{
		Store:              store,
		CanonicalCityStore: store,
		Provider:           provider,
		CityConfig:         &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}},
	})
	if err != nil {
		t.Fatalf("NewFactory: %v", err)
	}
	decision := AutomaticRuntimeDecision{
		Captured:              info,
		ExpectedRevision:      persisted.Revision,
		RevisionCaptured:      true,
		Witness:               sessionpkg.LiveBoundaryWitness{TriggerBeadID: info.TriggerBeadID, TriggerBeadStoreRef: info.TriggerBeadStoreRef},
		ProjectHooksForbidden: false,
	}

	if _, _, err := factory.prepareAutomaticRuntimeDecision(context.Background(), info.ID, decision); !errors.Is(err, ErrLaunchUnauthorized) {
		t.Fatalf("prepareAutomaticRuntimeDecision error = %v, want launch unauthorized", err)
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls = %+v, want none", calls)
	}
}
