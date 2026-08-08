package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/materialize"
	"github.com/gastownhall/gascity/internal/processenv"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
	workdirutil "github.com/gastownhall/gascity/internal/workdir"
	"github.com/gastownhall/gascity/internal/worker"
)

func workerSessionCatalogWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City) (*worker.SessionCatalog, error) {
	factory, err := workerFactoryWithConfig(cityPath, store, sp, cfg)
	if err != nil {
		return nil, err
	}
	return factory.Catalog()
}

func workerFactoryWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, canonicalStores ...beads.Store) (*worker.Factory, error) {
	canonicalStore := canonicalSessionWorkStore(store, cfg, canonicalStores...)
	return workerFactoryWithStaleKeyDetectionWaiter(cityPath, store, canonicalStore, sp, cfg, nil)
}

func canonicalSessionWorkStore(store beads.Store, cfg *config.City, explicit ...beads.Store) beads.Store {
	if len(explicit) > 0 {
		return explicit[0]
	}
	if cfg != nil {
		storage := cfg.EffectiveStorage()
		if storage.Classes.BindingFor(config.StorageClassSessions) != storage.Classes.BindingFor(config.StorageClassWork) {
			return nil
		}
	}
	return store
}

func workerFactoryWithStaleKeyDetectionWaiter(
	cityPath string,
	store beads.Store,
	canonicalStore beads.Store,
	sp runtime.Provider,
	cfg *config.City,
	waiter session.StaleKeyDetectionWaiter,
) (*worker.Factory, error) {
	var (
		resolveTransport func(template, provider string) string
		searchPaths      []string
	)
	if cfg != nil {
		rigContext := currentRigContext(cfg)
		resolveTransport = func(template, provider string) string {
			agentCfg, ok := resolveAgentIdentity(cfg, template, rigContext)
			if ok {
				resolved, err := config.ResolveProvider(
					&agentCfg,
					&cfg.Workspace,
					cfg.Providers,
					func(name string) (string, error) { return name, nil },
				)
				if err != nil {
					return agentCfg.Session
				}
				return config.ResolveSessionCreateTransport(agentCfg.Session, resolved)
			}
			provider = strings.TrimSpace(provider)
			if provider == "" {
				provider = strings.TrimSpace(template)
			}
			if provider == "" {
				return ""
			}
			resolved, err := config.ResolveProvider(
				&config.Agent{Provider: provider},
				&cfg.Workspace,
				cfg.Providers,
				func(name string) (string, error) { return name, nil },
			)
			if err != nil {
				return ""
			}
			return strings.TrimSpace(resolved.ProviderSessionCreateTransport())
		}
		searchPaths = worker.MergeSearchPaths(cfg.Daemon.ObservePaths)
	}
	return worker.NewFactory(worker.FactoryConfig{
		Store:                   store,
		CanonicalCityStore:      canonicalStore,
		CityConfig:              cfg,
		Provider:                sp,
		CityPath:                cityPath,
		SearchPaths:             searchPaths,
		UsageSink:               usageSinkForCity(cfg, cityPath),
		ResolveTransport:        resolveTransport,
		ResolveSessionRuntime:   workerSessionRuntimeResolverWithConfig(cityPath, cfg, sp),
		AuthorizeLaunch:         workerLaunchAuthorizerWithConfig(cityPath, cfg, sp),
		StaleKeyDetectionWaiter: waiter,
		Pricing:                 cfg.PricingRegistry(),
	})
}

// workerLaunchAuthorizerWithConfig keeps direct CLI lifecycle operations on
// the same configured-agent identity seam as runtime resolution. Ordinary
// inherit agents and unconfigured provider sessions preserve their existing
// behavior; project-hooks-isolated agents require an authoritative persisted
// trigger pair in the canonical city session store before any live boundary.
func workerLaunchAuthorizerWithConfig(cityPath string, cfg *config.City, providers ...runtime.Provider) worker.LaunchAuthorizer {
	if cfg == nil {
		return nil
	}
	cityStoreRef := "city:" + loadedCityName(cfg, cityPath)
	return func(_ context.Context, req worker.LaunchAuthorizationRequest) (worker.LaunchAuthorization, error) {
		// Expected-trigger delivery fences are useful for every configured
		// session, including inherited project-hook policy. Bind a complete pair
		// from the freshly reloaded persisted row so the worker boundary can
		// reject a stale backstop request before provider mutation. A missing or
		// half-written pair preserves ordinary inherited behavior; an operation
		// that supplied an explicit expectation will fail its exact comparison.
		authorization := worker.LaunchAuthorization{}
		if req.Info != nil {
			triggerID := strings.TrimSpace(req.Info.TriggerBeadID)
			triggerStoreRef := strings.TrimSpace(req.Info.TriggerBeadStoreRef)
			if triggerID != "" && triggerStoreRef != "" {
				authorization.TriggerBeadID = triggerID
				authorization.TriggerBeadStoreRef = triggerStoreRef
			}
		}
		template := strings.TrimSpace(req.Session.Template)
		if req.Info != nil && strings.TrimSpace(req.Info.Template) != "" {
			template = strings.TrimSpace(req.Info.Template)
		}
		var (
			agentCfg config.Agent
			ok       bool
		)
		if req.Info != nil {
			resolution, resolveErr := agentutil.ResolvePersistedSessionAgent(cfg, *req.Info)
			if resolveErr != nil {
				return worker.LaunchAuthorization{}, fmt.Errorf("%w: resolving persisted session identity: %w", worker.ErrLaunchUnauthorized, resolveErr)
			}
			if resolution.Resolved {
				agentCfg = resolution.Agent
				ok = true
			}
		}
		if !ok {
			agentCfg, ok = resolveWorkerConfigAgent(cfg, template)
		}
		if !ok || !agentCfg.ForbidsProjectHooks() {
			return authorization, nil
		}
		authorization, err := worker.RequireExactSessionTriggerAuthority(req, cityStoreRef)
		if err != nil {
			return worker.LaunchAuthorization{}, err
		}
		resolved, err := resolvedWorkerRuntimeWithConfigAndMetadata(
			cityPath,
			cfg,
			*req.Info,
			strings.TrimSpace(req.Metadata["real_world_app_session_kind"]),
			req.Metadata,
			providers...,
		)
		if err != nil {
			return worker.LaunchAuthorization{}, fmt.Errorf("resolving current isolated runtime: %w", err)
		}
		return worker.BindProjectHookIsolationRuntime(authorization, resolved)
	}
}

func workerSessionRuntimeResolverWithConfig(cityPath string, cfg *config.City, providers ...runtime.Provider) worker.SessionRuntimeResolver {
	if cfg == nil {
		return nil
	}
	return func(info session.Info, sessionKind string, metadata map[string]string) (*worker.ResolvedRuntime, error) {
		runtimeCfg, err := resolvedWorkerRuntimeWithConfigAndMetadata(cityPath, cfg, info, sessionKind, metadata, providers...)
		if err != nil {
			return nil, err
		}
		if runtimeCfg == nil {
			return nil, nil
		}
		normalized, err := worker.NormalizeResolvedRuntime(*runtimeCfg)
		if err != nil {
			return nil, err
		}
		return &normalized, nil
	}
}

func workerSessionCreateHints(resolved *config.ResolvedProvider) runtime.Config {
	if resolved == nil {
		return runtime.Config{}
	}
	hints := agent.StartupHints{
		Lifecycle:              runtime.Lifecycle(resolved.Lifecycle),
		ReadyPromptPrefix:      resolved.ReadyPromptPrefix,
		ReadyDelayMs:           resolved.ReadyDelayMs,
		ProcessNames:           resolved.ProcessNames,
		EmitsPermissionWarning: resolved.EmitsPermissionWarning,
		AcceptStartupDialogs:   resolved.AcceptStartupDialogs,
		// ga-c4w: the unmanaged `gc session new` direct-start path (controller
		// down) builds its runtime hints here. Default interactive CLI creates to
		// mouse-on so the tmux wheel drives copy-mode scrollback. Pool/headless
		// agents never reach this function — they resolve MouseOn via the
		// reconciler's templateParamsToConfig (cfgAgent.MouseModeOn()=false), so
		// this does not weaken controller-poll safety.
		MouseOn: true,
	}
	// Project through the single StartupHints → runtime.Config mapping so this
	// CLI create path can never silently drop a hint field the reconciler
	// threads (gc-0tna7). It still populates only the provider-resolvable subset
	// above; closing the remaining create-vs-resume population gap is the
	// internal/worker.Factory follow-up.
	return hints.ToRuntimeConfig()
}

// resolveWorkerConfigAgent keeps worker lifecycle policy and runtime
// resolution on the same configured-agent identity seam. In particular,
// resolveAgentIdentity understands synthesized pool instance names such as
// rig/worker-1 and worker-1; an exact-only lookup would treat those configured
// sessions as synthetic and silently omit their launch policy.
func resolveWorkerConfigAgent(cfg *config.City, template string) (config.Agent, bool) {
	if cfg == nil {
		return config.Agent{}, false
	}
	if agentCfg, ok := resolveAgentIdentity(cfg, strings.TrimSpace(template), ""); ok {
		return agentCfg, true
	}
	// Preserve the legacy bound-to-unbound identity fallback used by stored
	// sessions created before a binding migration.
	if agentCfg := findAgentByTemplate(cfg, template); agentCfg != nil {
		return agentCfg.Clone(), true
	}
	return config.Agent{}, false
}

// applyWorkerOverlayHints populates the provider-overlay staging fields
// (ProviderName/ProviderOverlayName/InstallAgentHooks/PackOverlayDirs) on a
// worker create/resume runtime.Config, mirroring the canonical create-time
// sourcing in cmd/gc/template_resolve.go (resolveTemplate). The worker.Factory
// create and resume paths build runtime.Config directly and never route through
// resolveTemplate, so without this they leave these fields empty:
// OverlayProviderNames then falls back to ProviderName="" and the per-provider
// overlay (e.g. core/overlay/per-provider/pi/.pi/extensions/gc-hooks.js for a
// custom base="builtin:pi" provider) is never staged, the harness never signals
// ready, and the controller churns into a fall-back-to-claude loop (gc-6bw8o).
// Best-effort: a missing cfg/resolved (CLI direct-start fallback) leaves the
// config untouched rather than failing the start.
func applyWorkerOverlayHints(hints *runtime.Config, cfg *config.City, cityPath, template string, resolved *config.ResolvedProvider) {
	if hints == nil || cfg == nil || resolved == nil {
		return
	}
	// ProviderName is the launch family (BuiltinAncestor, e.g. "pi" for a
	// base="builtin:pi" provider); ProviderOverlayName is the concrete provider
	// name — identical to resolveTemplate's hint assignment.
	hints.ProviderName = resolvedProviderLaunchFamily(resolved)
	hints.ProviderOverlayName = strings.TrimSpace(resolved.Name)
	agentCfg, ok := resolveWorkerConfigAgent(cfg, template)
	if !ok {
		// No agent config to resolve install-hooks/rig overlay scope against
		// (e.g. a synthetic session). Still stage city pack overlays.
		hints.PackOverlayDirs = effectiveOverlayDirs(cfg.PackOverlayDirs, cfg.RigOverlayDirs, "")
		return
	}
	hints.ProjectHooksForbidden = agentCfg.ForbidsProjectHooks()
	if !hints.ProjectHooksForbidden {
		hints.InstallAgentHooks = config.ResolveInstallHooks(&agentCfg, &cfg.Workspace)
	}
	rigName := sessionSetupContextForAgent(cityPath, cfg.EffectiveCityName(), firstNonEmptyGCString(agentCfg.QualifiedName(), template), &agentCfg, cfg.Rigs).Rig
	hints.PackOverlayDirs = effectiveOverlayDirs(cfg.PackOverlayDirs, cfg.RigOverlayDirs, rigName)
}

func validateWorkerProjectHookIsolationProvider(cfg *config.City, template, transport string, sp runtime.Provider) error {
	if cfg == nil {
		return nil
	}
	agentCfg, ok := resolveWorkerConfigAgent(cfg, template)
	if !ok || !agentCfg.ForbidsProjectHooks() {
		return nil
	}
	provider, ok := sp.(runtime.ProjectHookIsolationCapabilityProvider)
	if ok && provider.SupportsProjectHookIsolation(strings.TrimSpace(transport)) {
		return nil
	}
	transport = strings.TrimSpace(transport)
	if transport == "" {
		transport = "default"
	}
	return fmt.Errorf(
		"agent %q: active session provider cannot attest project hook isolation for %q transport",
		agentCfg.QualifiedName(), transport,
	)
}

// resolveWorkerResumeWorkDir returns the stored legacy cwd for ordinary
// sessions. A project-hook-isolated session is different: its current config
// is authoritative, so resume re-resolves the concrete per-instance path,
// verifies any persisted cwd names the same canonical directory, and returns
// only the freshly attested spelling. This runs while worker handle creation is
// still read-only, before provider or session lifecycle mutation.
func resolveWorkerResumeWorkDir(cityPath string, cfg *config.City, info session.Info) (string, error) {
	stored := strings.TrimSpace(info.WorkDir)
	agentCfg, ok := resolveWorkerConfigAgent(cfg, info.Template)
	if !ok || !agentCfg.ForbidsProjectHooks() {
		if stored != "" {
			return stored, nil
		}
		return cityPath, nil
	}
	// resolveWorkerConfigAgent returns a synthesized pool member when the
	// persisted template is worker-N. Workdir identity recovery must still use
	// the configured pool template: against the synthesized member, its exact
	// agent_name looks like the template itself and the legacy aliasless fallback
	// incorrectly substitutes the provider session name (s-<id>).
	workDirAgent := agentCfg
	if strings.TrimSpace(agentCfg.PoolName) != "" {
		if base := findAgentByTemplate(cfg, agentCfg.PoolName); base != nil {
			workDirAgent = base.Clone()
		}
	}
	qualifiedName := sessionBeadQualifiedNameInfo(cityPath, &workDirAgent, cfg.Rigs, info)
	// This preauthorization resolver must remain filesystem-pure. Runtime
	// staging materializes the path only after SessionHandle.Start reloads the
	// persisted trigger and the Manager revalidates that witness under its
	// mutation lock.
	attested, err := workdirutil.ResolveWorkDirPathStrict(
		cityPath,
		cfg.EffectiveCityName(),
		qualifiedName,
		workDirAgent,
		cfg.Rigs,
		cityPath,
	)
	if err != nil {
		return "", fmt.Errorf("attesting resume work_dir for agent %q: %w", agentCfg.QualifiedName(), err)
	}
	if err := workdirutil.ValidateAncestorWorktreesNotStale(attested); err != nil {
		return "", fmt.Errorf("attesting resume work_dir for agent %q: %w", agentCfg.QualifiedName(), err)
	}
	if stored != "" && !samePath(stored, attested) {
		return "", fmt.Errorf(
			"agent %q: persisted work_dir %q differs from current attested work_dir %q",
			agentCfg.QualifiedName(), stored, attested,
		)
	}
	return attested, nil
}

func resolvedRuntimeMCPServersWithConfig(
	cityPath string,
	cfg *config.City,
	alias, template, provider, workDir string,
	transport string,
	metadata map[string]string,
) ([]runtime.MCPServerConfig, error) {
	if cfg == nil || strings.TrimSpace(workDir) == "" || strings.TrimSpace(transport) != "acp" {
		return nil, nil
	}
	identity := strings.TrimSpace(metadata[session.MCPIdentityMetadataKey])
	if identity == "" {
		identity = strings.TrimSpace(metadata["agent_name"])
	}
	if identity == "" {
		identity = strings.TrimSpace(alias)
	}
	if identity == "" {
		identity = strings.TrimSpace(template)
	}
	if identity == "" {
		identity = strings.TrimSpace(provider)
	}
	if agentCfg := findAgentByTemplate(cfg, template); agentCfg != nil {
		catalog, err := materialize.EffectiveMCPForSession(cfg, cityPath, agentCfg, identity, workDir)
		if err != nil {
			return nil, fmt.Errorf("loading effective MCP: %w", err)
		}
		return materialize.RuntimeMCPServers(catalog.Servers), nil
	}
	synthetic := &config.Agent{Provider: provider}
	catalog, err := materialize.EffectiveMCPForSession(cfg, cityPath, synthetic, identity, workDir)
	if err != nil {
		return nil, fmt.Errorf("loading effective MCP: %w", err)
	}
	return materialize.RuntimeMCPServers(catalog.Servers), nil
}

func resumeRuntimeMCPServersWithConfig(
	cityPath string,
	cfg *config.City,
	info session.Info,
	resolved *config.ResolvedProvider,
	transport string,
	metadata map[string]string,
) ([]runtime.MCPServerConfig, error) {
	if cfg == nil || resolved == nil {
		return nil, nil
	}
	workDir := strings.TrimSpace(info.WorkDir)
	if workDir == "" {
		workDir = cityPath
	}
	resumeMeta := make(map[string]string)
	for key, value := range metadata {
		resumeMeta[key] = value
	}
	if agentName := strings.TrimSpace(info.AgentName); agentName != "" {
		resumeMeta["agent_name"] = agentName
	}
	mcpServers, err := resolvedRuntimeMCPServersWithConfig(
		cityPath,
		cfg,
		info.Alias,
		info.Template,
		firstNonEmptyGCString(info.Provider, resolved.Name, info.Template),
		workDir,
		transport,
		resumeMeta,
	)
	if err == nil {
		return mcpServers, nil
	}
	runtimeSnapshot, loadErr := session.LoadRuntimeMCPServersSnapshot(cityPath, info.ID)
	if loadErr != nil {
		return nil, loadErr
	}
	if len(runtimeSnapshot) > 0 {
		return runtimeSnapshot, nil
	}
	stored, decodeErr := session.DecodeMCPServersSnapshot(resumeMeta[session.MCPServersSnapshotMetadataKey])
	if decodeErr != nil {
		return nil, fmt.Errorf("decoding stored MCP snapshot: %w", decodeErr)
	}
	return session.SanitizeStoredMCPSnapshotForResume(stored), nil
}

func newWorkerSessionHandleForResolvedRuntimeWithConfig(
	cityPath string,
	store beads.Store,
	sp runtime.Provider,
	cfg *config.City,
	alias, explicitName, template, title, command, provider, workDir, transport string,
	resolved *config.ResolvedProvider,
	metadata map[string]string,
) (worker.Handle, error) {
	if err := validateWorkerProjectHookIsolationProvider(cfg, template, transport, sp); err != nil {
		return nil, err
	}
	factory, err := workerFactoryWithConfig(cityPath, store, sp, cfg)
	if err != nil {
		return nil, err
	}
	mcpServers, err := resolvedRuntimeMCPServersWithConfig(cityPath, cfg, alias, template, provider, workDir, transport, metadata)
	if err != nil {
		return nil, err
	}
	sessionCfg, err := resolvedWorkerSessionConfigWithConfig(
		cityPath,
		command,
		provider,
		workDir,
		alias,
		explicitName,
		template,
		title,
		transport,
		resolved,
		metadata,
		mcpServers,
	)
	if err != nil {
		return nil, err
	}
	// Stage provider-overlay hooks on the CLI create path the same way the
	// reconciler create path does; resolvedWorkerSessionConfigWithConfig builds
	// runtime.Config directly and never routes through resolveTemplate
	// (gc-6bw8o).
	applyWorkerOverlayHints(&sessionCfg.Runtime.Hints, cfg, cityPath, template, resolved)
	return factory.SessionForResolvedRuntime(sessionCfg)
}

func resolvedWorkerSessionConfigWithConfig(
	cityPath string,
	command string,
	provider string,
	workDir string,
	alias string,
	explicitName string,
	template string,
	title string,
	transport string,
	resolved *config.ResolvedProvider,
	metadata map[string]string,
	mcpServers []runtime.MCPServerConfig,
) (worker.ResolvedSessionConfig, error) {
	if resolved == nil {
		return worker.ResolvedSessionConfig{}, fmt.Errorf("resolved provider is required")
	}
	if transport == "acp" {
		var err error
		metadata, err = session.WithStoredMCPMetadata(
			metadata,
			firstNonEmptyGCString(metadata[session.MCPIdentityMetadataKey], metadata["agent_name"]),
			mcpServers,
		)
		if err != nil {
			return worker.ResolvedSessionConfig{}, err
		}
	}
	command = strings.TrimSpace(command)
	if command == "" {
		if transport == "acp" {
			command = strings.TrimSpace(resolved.ACPCommandString())
		} else {
			command = strings.TrimSpace(resolved.CommandString())
		}
	}
	providerName := strings.TrimSpace(resolved.Name)
	if providerName == "" {
		providerName = strings.TrimSpace(provider)
	}
	if command == "" {
		command = providerName
	}
	// Seed the city-anchored identity vars on top of the provider env
	// for the CLI create-mode path. Without this, `gc session` /
	// `gc session start` style direct creates land with SessionEnv
	// lacking GC_CITY / GC_CITY_PATH / GC_CITY_RUNTIME_DIR, and the
	// spawned shell cannot locate its city. Rig-scoped env remains a
	// create-time contract owned by template_resolve.go. Matches the resume-path
	// reseed at resolvedWorkerRuntimeWithConfigAndMetadata and the
	// API-side seeding in internal/api/session_resolved_config.go.
	// Regression for upstream gastownhall/gascity#101 (re-opened).
	//
	// The controller-only overlay goes last for the same reason it does in
	// template_resolve.go: resolved.Env is config-authored, so a provider spec
	// naming one of those keys would otherwise overwrite the empty value the
	// passthrough pinned. This resolver never routes through ScrubTokenEnv.
	sessionEnv := mergeEnv(providerProcessPassthroughEnv(), resolved.Env, processenv.ControllerOnlyEnvOverlay())
	if strings.TrimSpace(cityPath) != "" {
		sessionEnv = mergeEnv(sessionEnv, cityIdentityAnchorsForCity(cityPath))
	}
	return worker.NormalizeResolvedSessionConfig(worker.ResolvedSessionConfig{
		Alias:        alias,
		ExplicitName: explicitName,
		Template:     template,
		Title:        title,
		Transport:    transport,
		Metadata:     metadata,
		Runtime: worker.ResolvedRuntime{
			Command:    command,
			WorkDir:    workDir,
			Provider:   providerName,
			SessionEnv: sessionEnv,
			Resume: session.ProviderResume{
				ResumeFlag:    resolved.ResumeFlag,
				ResumeStyle:   resolved.ResumeStyle,
				ResumeCommand: resolved.ResumeCommand,
				SessionIDFlag: resolved.SessionIDFlag,
			},
			Hints: func() runtime.Config {
				hints := workerSessionCreateHints(resolved)
				hints.Env = sessionEnv
				hints.MCPServers = mcpServers
				return hints
			}(),
		},
	})
}

func workerHandleForSessionWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, id string) (worker.Handle, error) {
	return workerHandleForSessionWithStaleKeyDetectionWaiter(cityPath, store, sp, cfg, id, nil)
}

func workerHandleForSessionWithStaleKeyDetectionWaiter(
	cityPath string,
	store beads.Store,
	sp runtime.Provider,
	cfg *config.City,
	id string,
	waiter session.StaleKeyDetectionWaiter,
) (worker.Handle, error) {
	factory, err := workerFactoryWithStaleKeyDetectionWaiter(cityPath, store, canonicalSessionWorkStore(store, cfg), sp, cfg, waiter)
	if err != nil {
		return nil, err
	}
	return factory.SessionByID(id)
}

func workerHandleForSessionTargetWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string) (worker.Handle, error) {
	return workerHandleForSessionTargetWithRuntimeHintsWithConfig(cityPath, store, sp, cfg, target, nil)
}

func workerHandleForSessionTargetWithRuntimeHintsWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string, processNames []string) (worker.Handle, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, session.ErrSessionNotFound
	}
	factory, err := workerFactoryWithConfig(cityPath, store, sp, cfg)
	if err != nil {
		return nil, err
	}
	if store != nil {
		if info, pr, err := session.ResolveSessionRecordByExactID(store, target); err == nil {
			return factory.SessionByRecord(info, pr)
		}
		if id, err := session.ResolveSessionID(store, target); err == nil {
			return factory.SessionByID(id)
		}
		if sp != nil {
			if sessionID, metaErr := sp.GetMeta(target, "GC_SESSION_ID"); metaErr == nil && strings.TrimSpace(sessionID) != "" {
				return factory.SessionByID(strings.TrimSpace(sessionID))
			}
		}
	}
	if sp == nil {
		return nil, session.ErrSessionNotFound
	}
	providerName := target
	if liveProvider, err := sp.GetMeta(target, "GC_PROVIDER"); err == nil && strings.TrimSpace(liveProvider) != "" {
		providerName = strings.TrimSpace(liveProvider)
	}
	return factory.RuntimeHandle(target, providerName, "", processNames)
}

func runtimeWorkerHandleWithConfig(
	cityPath string,
	store beads.Store,
	sp runtime.Provider,
	cfg *config.City,
	sessionName string,
	providerName string,
	transport string,
	processNames []string,
) (worker.Handle, error) {
	factory, err := workerFactoryWithConfig(cityPath, store, sp, cfg)
	if err != nil {
		return nil, err
	}
	return factory.RuntimeHandle(sessionName, providerName, transport, processNames)
}

func workerKillSessionTargetWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string) error {
	handle, err := workerHandleForSessionTargetWithConfig(cityPath, store, sp, cfg, target)
	if err != nil {
		return err
	}
	return handle.Kill(context.Background())
}

func workerStopSessionTargetWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string) error {
	handle, err := workerHandleForSessionTargetWithConfig(cityPath, store, sp, cfg, target)
	if err != nil {
		return err
	}
	return handle.Stop(context.Background())
}

func workerInterruptSessionTargetWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string) error {
	handle, err := workerHandleForSessionTargetWithConfig(cityPath, store, sp, cfg, target)
	if err != nil {
		return err
	}
	return handle.Interrupt(context.Background(), worker.InterruptRequest{})
}

func workerObserveSessionTargetWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string) (worker.LiveObservation, error) {
	return workerObserveSessionTargetWithRuntimeHintsWithConfig(cityPath, store, sp, cfg, target, nil)
}

func workerObserveSessionTargetWithRuntimeHintsWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string, processNames []string) (worker.LiveObservation, error) {
	handle, err := workerHandleForSessionTargetWithRuntimeHintsWithConfig(cityPath, store, sp, cfg, target, processNames)
	if err != nil {
		return worker.LiveObservation{}, err
	}
	return worker.ObserveHandle(context.Background(), handle)
}

func workerObserveReconcilerSessionWithWitness(
	cityPath string,
	store beads.Store,
	sp runtime.Provider,
	cfg *config.City,
	info session.Info,
	processNames []string,
	boundaries ...reconcilerMutationBoundary,
) (worker.LiveObservation, error) {
	boundary := selectedReconcilerMutationBoundary(info, cfg, boundaries...)
	if err := boundary.validateCurrent(info); err != nil {
		return worker.LiveObservation{}, err
	}
	factory, err := workerFactoryWithConfig(cityPath, store, sp, cfg)
	if err != nil {
		return worker.LiveObservation{}, err
	}
	if boundary.strict || boundary.policyErr != nil {
		decision, err := boundary.automaticRuntimeDecision()
		if err != nil {
			return worker.LiveObservation{}, err
		}
		return factory.ObserveSessionWithDecisionForReconciler(
			context.Background(), info.ID, processNames, decision, false,
		)
	}
	return factory.ObserveSessionForReconciler(context.Background(), info.ID, processNames, boundary.expected)
}

func workerObserveReconcilerTargetWithWitness(
	cityPath string,
	store beads.Store,
	sp runtime.Provider,
	cfg *config.City,
	target string,
	processNames []string,
) (worker.LiveObservation, error) {
	if store == nil {
		return workerObserveSessionTargetWithRuntimeHintsWithConfig(cityPath, store, sp, cfg, target, processNames)
	}
	id, err := session.ResolveSessionID(store, target)
	if err != nil {
		if errors.Is(err, session.ErrSessionNotFound) {
			return workerObserveSessionTargetWithRuntimeHintsWithConfig(cityPath, store, sp, cfg, target, processNames)
		}
		return worker.LiveObservation{}, err
	}
	info, persisted, err := sessionFrontDoor(store).GetPersistedResponse(id)
	if err != nil {
		return worker.LiveObservation{}, err
	}
	return workerObserveReconcilerSessionWithWitness(
		cityPath, store, sp, cfg, info, processNames,
		captureReconcilerMutationBoundary(info, cfg, persisted.Revision),
	)
}

func workerPeekReconcilerSessionWithWitness(
	cityPath string,
	store beads.Store,
	sp runtime.Provider,
	cfg *config.City,
	info session.Info,
	lines int,
	boundaries ...reconcilerMutationBoundary,
) (string, error) {
	boundary := selectedReconcilerMutationBoundary(info, cfg, boundaries...)
	if err := boundary.validateCurrent(info); err != nil {
		return "", err
	}
	factory, err := workerFactoryWithConfig(cityPath, store, sp, cfg)
	if err != nil {
		return "", err
	}
	if boundary.strict || boundary.policyErr != nil {
		decision, err := boundary.automaticRuntimeDecision()
		if err != nil {
			return "", err
		}
		return factory.PeekSessionWithDecisionForReconciler(context.Background(), info.ID, lines, decision)
	}
	return factory.PeekSessionForReconciler(context.Background(), info.ID, lines, boundary.expected)
}

func workerSessionTargetRunningWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string) (bool, error) {
	obs, err := workerObserveSessionTargetWithConfig(cityPath, store, sp, cfg, target)
	if err != nil {
		return false, err
	}
	return obs.Running, nil
}

func workerSessionTargetAliveWithConfig(store beads.Store, sp runtime.Provider, cfg *config.City, target string, processNames []string) (bool, error) {
	obs, err := workerObserveSessionTargetWithRuntimeHintsWithConfig("", store, sp, cfg, target, processNames)
	if err != nil {
		return false, err
	}
	return obs.Alive, nil
}

func workerSessionTargetAttachedWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string) (bool, error) {
	obs, err := workerObserveSessionTargetWithConfig(cityPath, store, sp, cfg, target)
	if err != nil {
		return false, err
	}
	return obs.Attached, nil
}

func workerSessionTargetLastActivityWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string) (time.Time, error) {
	obs, err := workerObserveSessionTargetWithConfig(cityPath, store, sp, cfg, target)
	if err != nil {
		return time.Time{}, err
	}
	if obs.LastActivity == nil {
		return time.Time{}, nil
	}
	return *obs.LastActivity, nil
}

func workerSessionTargetPeekWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string, lines int, processNames []string) (string, error) {
	handle, err := workerHandleForSessionTargetWithRuntimeHintsWithConfig(cityPath, store, sp, cfg, target, processNames)
	if err != nil {
		return "", err
	}
	return handle.Peek(context.Background(), lines)
}

func workerSessionTargetPendingWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string) (*worker.PendingInteraction, error) {
	handle, err := workerHandleForSessionTargetWithConfig(cityPath, store, sp, cfg, target)
	if err != nil {
		return nil, err
	}
	return handle.Pending(context.Background())
}

func workerRespondSessionTargetWithConfig(cityPath string, store beads.Store, sp runtime.Provider, cfg *config.City, target string, response worker.InteractionResponse) error {
	handle, err := workerHandleForSessionTargetWithConfig(cityPath, store, sp, cfg, target)
	if err != nil {
		return err
	}
	return handle.Respond(context.Background(), response)
}

func resolvedWorkerRuntimeWithConfig(cityPath string, cfg *config.City, info session.Info, sessionKind string) (*worker.ResolvedRuntime, error) {
	return resolvedWorkerRuntimeWithConfigAndMetadata(cityPath, cfg, info, sessionKind, nil)
}

func resolvedWorkerRuntimeWithConfigAndMetadata(cityPath string, cfg *config.City, info session.Info, sessionKind string, metadata map[string]string, providers ...runtime.Provider) (*worker.ResolvedRuntime, error) {
	if cfg == nil {
		return nil, nil
	}
	identity, err := agentutil.ResolvePersistedSessionAgent(cfg, info)
	if err != nil {
		return nil, fmt.Errorf("resolving persisted session identity: %w", err)
	}
	if identity.Resolved {
		// Every downstream provider/workdir/hint decision consumes the same
		// conflict-checked configured base. Concrete pool/adhoc identity remains
		// available through AgentName/canonical metadata on info.
		info.Template = identity.Agent.QualifiedName()
	}
	resolved, configuredTransport := resolveWorkerRuntimeProviderWithConfigAndMetadata(cfg, info, sessionKind, metadata)
	if resolved == nil {
		return nil, nil
	}
	transport := resolvedWorkerRuntimeTransport(info, resolved, configuredTransport, metadata)
	if transport == "" && startedConfigHashProvesWorkerACPTransport(cityPath, cfg, info, sessionKind, resolved, metadata, configuredTransport) {
		transport = "acp"
	}
	if transport == "" && legacyWorkerACPTransportAmbiguous(resolved, configuredTransport, info.Command, metadata) {
		return nil, fmt.Errorf("legacy session transport is ambiguous: recreate the stopped session or resume it while ACP metadata can still be persisted")
	}
	if len(providers) > 0 {
		if err := validateWorkerProjectHookIsolationProvider(cfg, info.Template, transport, providers[0]); err != nil {
			return nil, err
		}
	}

	command := resolvedWorkerRuntimeCommandForTransport(cityPath, resolved, transport, info.Command, info.Provider, metadata)

	workDir, err := resolveWorkerResumeWorkDir(cityPath, cfg, info)
	if err != nil {
		return nil, err
	}
	mcpServers, err := resumeRuntimeMCPServersWithConfig(cityPath, cfg, info, resolved, transport, metadata)
	if err != nil {
		return nil, err
	}
	resumeCommand := firstNonEmptyGCString(resolved.ResumeCommand, info.ResumeCommand)
	if overrides, err := session.ParseTemplateOverrides(metadata); err == nil && strings.TrimSpace(resumeCommand) != "" {
		resumeProvider := *resolved
		resumeProvider.ResumeCommand = resumeCommand
		if command, err := config.BuildProviderResumeCommand(&resumeProvider, overrides); err == nil && strings.TrimSpace(command) != "" {
			resumeCommand = command
		}
	}
	// Reseed the city-anchored identity vars (GC_CITY, GC_CITY_PATH,
	// GC_CITY_RUNTIME_DIR) on top of the provider env. Without this,
	// session restart paths drop the city anchor and the spawned shell
	// cannot locate its city. Rig-scoped env remains a create-time
	// contract owned by template_resolve.go.
	// Regression for upstream gastownhall/gascity#101 (re-opened).
	//
	// Identity-only (no GC_CONTROL_DISPATCHER_TRACE_DEFAULT): the
	// dispatcher trace path is per-dispatcher-qualified and must not be
	// overwritten with the city-uniform default here. template_resolve.go
	// owns the qualified override for the CLI create path.
	sessionEnv := mergeEnv(providerProcessPassthroughEnv(), resolved.Env, cityIdentityAnchorsForCity(cityPath), processenv.ControllerOnlyEnvOverlay())
	// Resolve session_live so resumed sessions get re-themed (status bar,
	// keybindings) the same way reconciler-started sessions do. Without this,
	// `gc session attach` recreates the tmux runtime with an empty
	// Hints.SessionLive, doStartSession's runSessionLive early-returns, and
	// the session_live theme/keybinding hooks never run. The setup context is
	// built via the reconciler's own sessionSetupContextForAgent() so
	// {{.Rig}}/{{.RigRoot}}/{{.AgentBase}} expand correctly. See ga-vtkhi.
	qualifiedName := firstNonEmptyGCString(info.AgentName, info.Template)
	var sessionLive []string
	if agentCfg, ok := resolveWorkerConfigAgent(cfg, info.Template); ok && len(agentCfg.SessionLive) > 0 {
		setupCtx := sessionSetupContextForAgent(cityPath, cfg.EffectiveCityName(), qualifiedName, &agentCfg, cfg.Rigs)
		setupCtx.Session = info.SessionName
		setupCtx.WorkDir = workDir
		setupCtx.ConfigDir = cityPath
		if agentCfg.SourceDir != "" {
			setupCtx.ConfigDir = agentCfg.SourceDir
		}
		sessionLive = expandSessionSetup(agentCfg.SessionLive, setupCtx)
	}
	// Project the resolved hint subset through the single StartupHints →
	// runtime.Config mapping (gc-0tna7), then layer the caller-owned
	// WorkDir/Env/MCPServers. SessionLive is resolved above (ga-vtkhi) so
	// resumed sessions re-theme; closing the remaining create-time field gap
	// is the internal/worker.Factory population follow-up.
	runtimeHints := agent.StartupHints{
		Lifecycle:              runtime.Lifecycle(resolved.Lifecycle),
		ReadyPromptPrefix:      resolved.ReadyPromptPrefix,
		ReadyDelayMs:           resolved.ReadyDelayMs,
		ProcessNames:           resolved.ProcessNames,
		EmitsPermissionWarning: resolved.EmitsPermissionWarning,
		AcceptStartupDialogs:   resolved.AcceptStartupDialogs,
		SessionLive:            sessionLive,
	}.ToRuntimeConfig()
	runtimeHints.WorkDir = workDir
	runtimeHints.Env = sessionEnv
	runtimeHints.MCPServers = mcpServers
	// Stage provider-overlay hooks on resume the same way the reconciler create
	// path does; this resume resolver builds runtime.Config directly and never
	// routes through resolveTemplate (gc-6bw8o).
	applyWorkerOverlayHints(&runtimeHints, cfg, cityPath, info.Template, resolved)
	return &worker.ResolvedRuntime{
		Command:    command,
		WorkDir:    workDir,
		Provider:   resolvedWorkerRuntimeProviderLabel(resolved, transport, info),
		SessionEnv: sessionEnv,
		Hints:      runtimeHints,
		Resume: session.ProviderResume{
			ResumeFlag:    firstNonEmptyGCString(resolved.ResumeFlag, info.ResumeFlag),
			ResumeStyle:   firstNonEmptyGCString(resolved.ResumeStyle, info.ResumeStyle),
			ResumeCommand: resumeCommand,
			SessionIDFlag: resolved.SessionIDFlag,
		},
	}, nil
}

func resolvedWorkerRuntimeProviderLabel(resolved *config.ResolvedProvider, transport string, info session.Info) string {
	if strings.TrimSpace(configuredWorkerRuntimeCommand(resolved, transport)) != "" {
		return firstNonEmptyGCString(resolved.Name, info.Provider)
	}
	return firstNonEmptyGCString(info.Provider, resolved.Name)
}

func resolvedWorkerRuntimeCommandForTransport(cityPath string, resolved *config.ResolvedProvider, transport, storedCommand, fallbackProvider string, metadata map[string]string) string {
	command := strings.TrimSpace(storedCommand)
	configuredCommand := configuredWorkerRuntimeCommand(resolved, transport)
	if configuredCommand == "" {
		return firstNonEmptyGCString(command, fallbackProvider, resolved.Name)
	}
	desiredCommand := configuredCommand
	if optionOverrides, err := session.ParseTemplateOverrides(metadata); err == nil {
		if launchCommand, err := config.BuildProviderLaunchCommand(cityPath, resolved, optionOverrides, transport); err == nil {
			desiredCommand = firstNonEmptyGCString(launchCommand.Command, configuredCommand, resolved.Name)
			if shouldPreserveStoredRuntimeCommandForTransport(command, desiredCommand, transport, optionOverrides) {
				desiredCommand = command
			}
		}
	}
	if !shouldPreserveStoredRuntimeCommand(command, desiredCommand) {
		command = desiredCommand
	}
	return firstNonEmptyGCString(command, fallbackProvider, resolved.Name)
}

func configuredWorkerRuntimeCommand(resolved *config.ResolvedProvider, transport string) string {
	if resolved == nil {
		return ""
	}
	if transport == "acp" && (strings.TrimSpace(resolved.ACPCommand) != "" || resolved.ACPArgs != nil) {
		return strings.TrimSpace(resolved.ACPCommandString())
	}
	if strings.TrimSpace(resolved.Command) != "" {
		return strings.TrimSpace(resolved.CommandString())
	}
	return ""
}

func shouldPreserveStoredRuntimeCommand(storedCommand, resolvedCommand string) bool {
	storedCommand = strings.TrimSpace(storedCommand)
	if storedCommand == "" {
		return false
	}
	resolvedCommand = strings.TrimSpace(resolvedCommand)
	if resolvedCommand == "" {
		return true
	}
	// A bare stored command (just the provider binary) lacks schema
	// defaults like --dangerously-skip-permissions and the --settings
	// path. Rebuild from the current config instead of preserving it.
	// See #799: pool-agent sessions resumed through the control-
	// dispatcher path wedged on interactive permission prompts because
	// the bare stored command was preserved without re-injecting flags.
	if storedCommand == resolvedCommand {
		return false
	}
	return strings.HasPrefix(storedCommand, resolvedCommand+" ")
}

func shouldPreserveStoredRuntimeCommandForTransport(storedCommand, resolvedCommand, _ string, optionOverrides map[string]string) bool {
	if shouldPreserveStoredRuntimeCommand(storedCommand, resolvedCommand) {
		return true
	}
	if len(optionOverrides) == 0 && storedCommandHasSettingsArg(storedCommand) && sameRuntimeCommandExecutable(storedCommand, resolvedCommand) {
		return true
	}
	return false
}

func sameRuntimeCommandExecutable(storedCommand, resolvedCommand string) bool {
	storedFields := strings.Fields(strings.TrimSpace(storedCommand))
	resolvedFields := strings.Fields(strings.TrimSpace(resolvedCommand))
	if len(storedFields) == 0 || len(resolvedFields) == 0 {
		return false
	}
	return storedFields[0] == resolvedFields[0]
}

func storedCommandHasSettingsArg(command string) bool {
	return strings.Contains(" "+strings.TrimSpace(command)+" ", " --settings ")
}

func storedWorkerSessionProvesACPTransport(resolved *config.ResolvedProvider, configuredTransport, storedCommand string, metadata map[string]string) bool {
	if metadata != nil {
		if strings.TrimSpace(metadata[session.MCPIdentityMetadataKey]) != "" ||
			strings.TrimSpace(metadata[session.MCPServersSnapshotMetadataKey]) != "" {
			return true
		}
		if strings.TrimSpace(configuredTransport) == "acp" && legacyWorkerResumeMetadataProvesACPTransport(metadata) {
			return true
		}
	}
	if resolved == nil {
		return false
	}
	acpCommand := strings.TrimSpace(resolved.ACPCommandString())
	defaultCommand := strings.TrimSpace(resolved.CommandString())
	if acpCommand == "" || acpCommand == defaultCommand {
		return false
	}
	return shouldPreserveStoredRuntimeCommand(storedCommand, acpCommand)
}

func legacyWorkerResumeMetadataProvesACPTransport(metadata map[string]string) bool {
	if metadata == nil {
		return false
	}
	return strings.TrimSpace(metadata["resume_command"]) != "" ||
		strings.TrimSpace(metadata["resume_flag"]) != "" ||
		strings.TrimSpace(metadata["session_key"]) != ""
}

func legacyWorkerACPTransportAmbiguous(resolved *config.ResolvedProvider, configuredTransport, storedCommand string, metadata map[string]string) bool {
	if strings.TrimSpace(configuredTransport) != "acp" || resolved == nil {
		return false
	}
	if storedWorkerSessionProvesACPTransport(resolved, configuredTransport, storedCommand, metadata) {
		return false
	}
	acpCommand := strings.TrimSpace(resolved.ACPCommandString())
	defaultCommand := strings.TrimSpace(resolved.CommandString())
	if acpCommand == "" || acpCommand != defaultCommand {
		return false
	}
	storedCommand = strings.TrimSpace(storedCommand)
	return storedCommand == "" || sameRuntimeCommandExecutable(storedCommand, defaultCommand)
}

func startedConfigHashProvesWorkerACPTransport(
	cityPath string,
	cfg *config.City,
	info session.Info,
	_ string,
	resolved *config.ResolvedProvider,
	metadata map[string]string,
	configuredTransport string,
) bool {
	if cfg == nil || resolved == nil || metadata == nil || strings.TrimSpace(configuredTransport) != "acp" {
		return false
	}
	startedHash := strings.TrimSpace(metadata["started_config_hash"])
	if startedHash == "" {
		return false
	}
	acpCommand := resolvedWorkerRuntimeCommandForTransport(cityPath, resolved, "acp", info.Command, info.Provider, metadata)
	defaultCommand := resolvedWorkerRuntimeCommandForTransport(cityPath, resolved, "", info.Command, info.Provider, metadata)
	mcpServers, err := resolvedRuntimeMCPServersWithConfig(
		cityPath,
		cfg,
		info.Alias,
		info.Template,
		firstNonEmptyGCString(info.Provider, resolved.Name, info.Template),
		firstNonEmptyGCString(info.WorkDir, cityPath),
		"acp",
		metadata,
	)
	if err != nil {
		return false
	}
	acpHash := runtime.CoreFingerprint(runtime.Config{
		Command:    acpCommand,
		Lifecycle:  runtime.Lifecycle(resolved.Lifecycle),
		Env:        resolved.Env,
		MCPServers: mcpServers,
	})
	defaultHash := runtime.CoreFingerprint(runtime.Config{
		Command:   defaultCommand,
		Lifecycle: runtime.Lifecycle(resolved.Lifecycle),
		Env:       resolved.Env,
	})
	if acpHash == defaultHash {
		return false
	}
	return startedHash == acpHash
}

func resolvedWorkerRuntimeTransport(info session.Info, resolved *config.ResolvedProvider, configuredTransport string, metadata map[string]string) string {
	if transport := strings.TrimSpace(info.Transport); transport != "" {
		return transport
	}
	if strings.TrimSpace(info.Provider) == "acp" {
		return "acp"
	}
	if storedWorkerSessionProvesACPTransport(resolved, configuredTransport, info.Command, metadata) {
		return "acp"
	}
	if strings.TrimSpace(configuredTransport) == config.SessionTransportTmux {
		return config.SessionTransportTmux
	}
	if strings.TrimSpace(info.Command) == "" {
		return strings.TrimSpace(configuredTransport)
	}
	return ""
}

func resolveWorkerRuntimeProviderWithConfig(cfg *config.City, info session.Info, sessionKind string) (*config.ResolvedProvider, string) {
	return resolveWorkerRuntimeProviderWithConfigAndMetadata(cfg, info, sessionKind, nil)
}

func resolveWorkerRuntimeProviderWithConfigAndMetadata(cfg *config.City, info session.Info, sessionKind string, metadata map[string]string) (*config.ResolvedProvider, string) {
	if cfg == nil {
		return nil, ""
	}
	found, foundAgent := resolveWorkerConfigAgent(cfg, info.Template)
	if session.UseAgentTemplateForProviderResolution(sessionKind, metadata, info.Provider, found.Provider, foundAgent) {
		if foundAgent {
			if resolved, err := config.ResolveProvider(&found, &cfg.Workspace, cfg.Providers, exec.LookPath); err == nil {
				return resolved, config.ResolveSessionCreateTransport(found.Session, resolved)
			}
		}
	}
	for _, providerName := range []string{info.Provider, info.Template} {
		providerName = strings.TrimSpace(providerName)
		if providerName == "" {
			continue
		}
		resolved, err := config.ResolveProvider(&config.Agent{Provider: providerName}, &cfg.Workspace, cfg.Providers, exec.LookPath)
		if err == nil {
			return resolved, strings.TrimSpace(resolved.ProviderSessionCreateTransport())
		}
	}
	return nil, ""
}

func workerDeliveryIntentForSubmitIntent(intent session.SubmitIntent) worker.DeliveryIntent {
	switch intent {
	case session.SubmitIntentFollowUp:
		return worker.DeliveryIntentFollowUp
	case session.SubmitIntentInterruptNow:
		return worker.DeliveryIntentInterruptNow
	default:
		return worker.DeliveryIntentDefault
	}
}

func workerNudgeDeliveryForMode(mode nudgeDeliveryMode) (worker.NudgeDelivery, bool) {
	switch mode {
	case nudgeDeliveryImmediate:
		return worker.NudgeDeliveryImmediate, true
	case nudgeDeliveryWaitIdle:
		return worker.NudgeDeliveryWaitIdle, true
	default:
		return "", false
	}
}

func firstNonEmptyGCString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
