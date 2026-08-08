package api

import (
	"context"
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/worker"
)

func (s *Server) workerFactory(store beads.Store) (*worker.Factory, error) {
	cfg := s.state.Config()
	var resolveTransport func(template, provider string) string
	if cfg != nil {
		resolveTransport = func(template, provider string) string {
			return configuredSessionTransport(cfg, template, provider)
		}
	}
	return worker.NewFactory(worker.FactoryConfig{
		Store:                 store,
		CanonicalCityStore:    s.state.CityBeadStore(),
		CityConfig:            cfg,
		Provider:              s.state.SessionProvider(),
		CityPath:              s.state.CityPath(),
		SearchPaths:           s.sessionLogPaths(),
		Recorder:              s.state.EventProvider(),
		UsageSink:             s.state.UsageSink(),
		ResolveTransport:      resolveTransport,
		ResolveSessionRuntime: s.resolveWorkerSessionRuntimeWithMetadata,
		AuthorizeLaunch:       s.workerLaunchAuthorizer(),
		Pricing:               cfg.PricingRegistry(),
	})
}

// workerLaunchAuthorizer reads the current config at invocation time so a
// handle built before a config reload never launches under stale policy.
func (s *Server) workerLaunchAuthorizer() worker.LaunchAuthorizer {
	return func(_ context.Context, req worker.LaunchAuthorizationRequest) (worker.LaunchAuthorization, error) {
		cfg := s.state.Config()
		if cfg == nil {
			return worker.LaunchAuthorization{}, nil
		}
		template := strings.TrimSpace(req.Session.Template)
		if req.Info != nil && strings.TrimSpace(req.Info.Template) != "" {
			template = strings.TrimSpace(req.Info.Template)
		}
		var (
			agentCfg                config.Agent
			ok                      bool
			templateIdentifiesAgent = true
		)
		if req.Info != nil {
			resolution, identifiesAgent, resolveErr := resolvePersistedSessionAgentForRuntime(
				cfg,
				*req.Info,
				strings.TrimSpace(req.Metadata["real_world_app_session_kind"]),
				req.Metadata,
			)
			if resolveErr != nil {
				return worker.LaunchAuthorization{}, fmt.Errorf("%w: resolving persisted session identity: %w", worker.ErrLaunchUnauthorized, resolveErr)
			}
			templateIdentifiesAgent = identifiesAgent
			if resolution.Resolved {
				agentCfg = resolution.Agent
				ok = true
			}
		}
		if !ok && templateIdentifiesAgent {
			agentCfg, ok = agentutil.ResolveAgent(cfg, template, agentutil.ResolveOpts{AllowPoolMembers: true})
		}
		if !ok || !agentCfg.ForbidsProjectHooks() {
			return worker.LaunchAuthorization{}, nil
		}
		cityName := config.EffectiveCityName(cfg, apiCityName(cfg, s.state.CityPath()))
		authorization, err := worker.RequireExactSessionTriggerAuthority(req, "city:"+cityName)
		if err != nil {
			return worker.LaunchAuthorization{}, err
		}
		resolved, err := s.resolveWorkerSessionRuntimeWithMetadata(
			*req.Info,
			strings.TrimSpace(req.Metadata["real_world_app_session_kind"]),
			req.Metadata,
		)
		if err != nil {
			return worker.LaunchAuthorization{}, err
		}
		return worker.BindProjectHookIsolationRuntime(authorization, resolved)
	}
}

func (s *Server) workerSessionCatalog(store beads.Store) (*worker.SessionCatalog, error) {
	factory, err := s.workerFactory(store)
	if err != nil {
		return nil, err
	}
	return factory.Catalog()
}
