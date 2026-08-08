package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/suspensionstate"
)

const (
	masontopiaCandidateFixtureDir = "testdata/masontopia-repair-canary"
	masontopiaCandidateSHA256     = "0fedd760179a2a5f1a11a4dcadb16275b7bb6cf42e415c0df92a86bf5b4a2b3b"
	masontopiaEvidenceRoot        = "/private/var/tmp/masontopia-repair-canary.PI5cSQ"
	masontopiaLiveCityRoot        = "/Users/masonjames/Projects/masontopia"
)

func TestMasontopiaRepairCandidateInitialState(t *testing.T) {
	raw := mustReadMasontopiaCandidateFixture(t)

	t.Run("preserved byte provenance and policy incompatibility", func(t *testing.T) {
		got := fmt.Sprintf("%x", sha256.Sum256(raw))
		if got != masontopiaCandidateSHA256 {
			t.Fatalf("candidate fixture SHA-256 = %s, want preserved %s", got, masontopiaCandidateSHA256)
		}
		if strings.Contains(string(raw), "project_hooks") {
			t.Fatal("preserved candidate unexpectedly declares project_hooks; provenance fixture must remain byte-exact")
		}
		if got := strings.Count(string(raw), "hooks_installed = true"); got != 11 {
			t.Fatalf("candidate hooks_installed=true declarations = %d, want 11", got)
		}
		assertPreservedMasontopiaCandidateRejectedBySchema2(t, raw)
	})

	fx := materializeMasontopiaCandidateFixture(t, raw)
	wantTemplates := masontopiaCandidateTemplates()
	wantSuspended := append([]string(nil), wantTemplates...)
	wantSuspended = removeString(wantSuspended, "mayor")

	t.Run("exact frozen-pack inventory", func(t *testing.T) {
		got := make([]string, 0, len(fx.cfg.Agents))
		for i := range fx.cfg.Agents {
			got = append(got, fx.cfg.Agents[i].QualifiedName())
		}
		sort.Strings(got)
		if strings.Join(got, "\n") != strings.Join(wantTemplates, "\n") {
			t.Fatalf("resolved templates:\n%s\nwant exact 35:\n%s", strings.Join(got, "\n"), strings.Join(wantTemplates, "\n"))
		}
		if len(got) != 35 {
			t.Fatalf("resolved template count = %d, want 35", len(got))
		}
		instagramTV := masontopiaCandidateRig(fx.cfg, "instagramtv")
		if instagramTV == nil {
			t.Fatal("loaded InstagramTV rig is missing")
		}
		if !instagramTV.SuspendedOnStart {
			t.Fatal("loaded InstagramTV rig SuspendedOnStart = false, want true")
		}

		eligible := make([]string, 0, 1)
		for i := range fx.cfg.Agents {
			a := &fx.cfg.Agents[i]
			if !a.Suspended {
				eligible = append(eligible, a.QualifiedName())
			}
		}
		if strings.Join(eligible, "\n") != "mayor" {
			t.Fatalf("individually unsuspended templates = %v, want [mayor]", eligible)
		}
		hookPolicyConflicts := make([]string, 0, 11)
		for i := range fx.cfg.Agents {
			a := fx.cfg.Agents[i]
			if a.HooksInstalled == nil || !*a.HooksInstalled {
				continue
			}
			if got := a.EffectiveProjectHooksPolicy(); got != config.ProjectHooksInherit {
				t.Errorf("hooks-installed template %q policy = %q, want inherited", a.QualifiedName(), got)
			}
			probe := a.Clone()
			probe.ProjectHooks = config.ProjectHooksForbid
			if err := config.ValidateAgents([]config.Agent{probe}); err == nil || !strings.Contains(err.Error(), "hooks_installed") {
				t.Errorf("template %q project_hooks=forbid validation = %v, want hooks_installed conflict", a.QualifiedName(), err)
			}
			hookPolicyConflicts = append(hookPolicyConflicts, a.QualifiedName())
		}
		if len(hookPolicyConflicts) != 11 {
			t.Fatalf("resolved hooks-installed/project-hooks conflicts = %v, want 11", hookPolicyConflicts)
		}
		for _, name := range wantSuspended {
			a := masontopiaCandidateAgent(fx.cfg, name)
			if a == nil {
				t.Fatalf("suspended template %q missing", name)
			}
			if !a.Suspended {
				t.Errorf("template %q is not individually suspended", name)
			}
		}

		if len(fx.cfg.NamedSessions) != 1 {
			t.Fatalf("named sessions = %d, want exactly one", len(fx.cfg.NamedSessions))
		}
		named := fx.cfg.NamedSessions[0]
		if got := named.QualifiedName(); got != "mayor" {
			t.Fatalf("named session identity = %q, want mayor", got)
		}
		if named.Mode != "always" {
			t.Fatalf("mayor named-session mode = %q, want always", named.Mode)
		}
	})

	t.Run("baseline admits Mayor only with zero pool demand", func(t *testing.T) {
		assertMasontopiaCandidatePass(t, fx, false, wantSuspended)
	})

	t.Run("individual suspension defeats routed work and runtime rig resume", func(t *testing.T) {
		assertMasontopiaCandidatePass(t, fx, true, wantSuspended)
	})

	t.Run("hold index partial fails closed before any worker replacement", func(t *testing.T) {
		assertMasontopiaCandidateHoldIndexPartial(t, fx)
	})
}

type masontopiaCandidateFixture struct {
	cityRoot string
	cfg      *config.City
}

func materializeMasontopiaCandidateFixture(t *testing.T, raw []byte) masontopiaCandidateFixture {
	t.Helper()

	sandbox := t.TempDir()
	cityRoot := filepath.Join(sandbox, "city")
	bootstrapRoot := filepath.Join(sandbox, "bootstrap")
	t.Setenv("GC_HOME", filepath.Join(sandbox, "gc-home"))

	copyMasontopiaFixtureTree(t, filepath.Join(masontopiaCandidateFixtureDir, "city"), cityRoot)
	for _, dir := range []string{
		bootstrapRoot,
		filepath.Join(bootstrapRoot, "mayor-cwd"),
		filepath.Join(bootstrapRoot, "worker-cwd"),
		filepath.Join(bootstrapRoot, "reviewer-cwd"),
		filepath.Join(cityRoot, "rigs", "instagramtv"),
		filepath.Join(cityRoot, ".gc", "scripts"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create fixture directory %s: %v", dir, err)
		}
	}
	for _, name := range []string{"worker-launch", "mayor-launch", "reviewer-launch"} {
		mustWriteMasontopiaFixtureFile(t, filepath.Join(bootstrapRoot, name), "#!/bin/sh\nexit 0\n", 0o755)
	}
	for _, name := range []string{"worker-prompt.template.md", "mayor-prompt.template.md", "reviewer-prompt.template.md"} {
		mustWriteMasontopiaFixtureFile(t, filepath.Join(bootstrapRoot, name), "fixture prompt\n", 0o644)
	}
	mustWriteMasontopiaFixtureFile(t, filepath.Join(cityRoot, ".gc", "scripts", "verified-gc"), "#!/bin/sh\nexit 1\n", 0o755)

	rebased := strings.ReplaceAll(string(raw), masontopiaEvidenceRoot, bootstrapRoot)
	rebased = strings.ReplaceAll(rebased, masontopiaLiveCityRoot, cityRoot)
	rebased = strings.Replace(rebased,
		"source = \"https://github.com/gastownhall/gascity-packs/tree/main/gascity/roles\"\nversion = \"sha:3b3b89f2011e06d84459aa7bea1552382f13930a\"",
		"source = \"./fixture-packs/gascity/roles\"", 1)
	for _, forbidden := range []string{masontopiaEvidenceRoot, masontopiaLiveCityRoot, "https://github.com/"} {
		if strings.Contains(rebased, forbidden) {
			t.Fatalf("rebased candidate still references external root %q", forbidden)
		}
	}
	mustWriteMasontopiaFixtureFile(t, filepath.Join(cityRoot, "city.toml"), rebased, 0o644)

	cfg, err := loadCityConfigWithoutBuiltinPackRefreshFS(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml"), io.Discard)
	if err != nil {
		t.Fatalf("load hermetic candidate: %v", err)
	}
	return masontopiaCandidateFixture{cityRoot: cityRoot, cfg: cfg}
}

func assertMasontopiaCandidatePass(t *testing.T, fx masontopiaCandidateFixture, seedRoutedWork bool, suspended []string) {
	t.Helper()
	instagramTV := masontopiaCandidateRig(fx.cfg, "instagramtv")
	if instagramTV == nil {
		t.Fatal("loaded InstagramTV rig is missing")
	}
	if !instagramTV.SuspendedOnStart {
		t.Fatal("loaded InstagramTV rig SuspendedOnStart = false, want true")
	}

	cityStore := beads.NewMemStore()
	rigStore := beads.NewMemStore()
	rigStores := map[string]beads.Store{"instagramtv": rigStore}

	// The runtime override deliberately defeats suspended_on_start. Every rig
	// template must still remain stopped because the candidate suspends each
	// template itself.
	resumed := false
	if err := suspensionstate.SetRigSuspended(fsys.OSFS{}, fx.cityRoot, instagramTV.Name, &resumed); err != nil {
		t.Fatalf("record hermetic runtime rig resume: %v", err)
	}
	st, err := suspensionstate.Load(fsys.OSFS{}, fx.cityRoot)
	if err != nil {
		t.Fatalf("load hermetic suspension state: %v", err)
	}
	if suspensionstate.EffectiveRigSuspended(st, instagramTV.Name, instagramTV.SuspendedOnStart) {
		t.Fatal("runtime rig-resume control did not override suspended_on_start")
	}

	if seedRoutedWork {
		for _, template := range suspended {
			store := beads.Store(cityStore)
			if strings.HasPrefix(template, "instagramtv/") {
				store = rigStore
			}
			if _, err := store.Create(beads.Bead{
				Title:    "synthetic routed demand for " + template,
				Type:     "task",
				Status:   "open",
				Metadata: map[string]string{"gc.routed_to": template},
			}); err != nil {
				t.Fatalf("seed routed work for %s: %v", template, err)
			}
		}
	}

	sp := runtime.NewFake()
	clk := &clock.Fake{Time: time.Date(2026, 8, 7, 23, 45, 0, 0, time.UTC)}
	result := buildDesiredStateWithSessionBeads(
		fx.cfg.EffectiveCityName(), fx.cityRoot, clk.Now().UTC(), fx.cfg, sp,
		cityStore, rigStores, &sessionBeadSnapshot{}, nil, io.Discard,
	)

	if result.StoreQueryPartial || result.SessionQueryPartial {
		t.Fatalf("candidate build partial: store=%v session=%v", result.StoreQueryPartial, result.SessionQueryPartial)
	}
	for family, partials := range map[string]map[string]bool{
		"scale":          result.ScaleCheckPartialTemplates,
		"pool":           result.PoolScaleCheckPartialTemplates,
		"pool-retention": result.PoolPartialRetentionTemplates,
		"named":          result.NamedScaleCheckPartialTemplates,
	} {
		for name, partial := range partials {
			if partial {
				t.Errorf("unexpected %s partial for %q", family, name)
			}
		}
	}
	if !seedRoutedWork {
		for _, template := range suspended {
			if got := result.ScaleCheckCounts[template]; got != 0 {
				t.Errorf("ScaleCheckCounts[%q] = %d, want 0", template, got)
			}
		}
	}
	if len(result.ReadyUnassignedRoutedWorkBeads) != 0 {
		t.Fatalf("ready routed demand leaked through suspension: %+v", result.ReadyUnassignedRoutedWorkBeads)
	}

	if len(result.State) != 1 || len(result.BaseState) != 1 {
		t.Fatalf("desired/base state sizes = %d/%d, want Mayor only", len(result.State), len(result.BaseState))
	}
	for _, desired := range result.State {
		if desired.ConfiguredNamedIdentity != "mayor" {
			t.Fatalf("desired session = %+v, want named Mayor only", desired)
		}
	}

	poolStates := ComputePoolDesiredStates(fx.cfg, result.AssignedWorkBeads, nil, result.ScaleCheckCounts)
	if got := PoolDesiredCounts(poolStates); len(got) != 0 {
		t.Fatalf("pool desired counts = %v, want zero before named-session demand", got)
	}

	cfgNames := configuredSessionNames(fx.cfg, fx.cfg.EffectiveCityName(), cityStore)
	syncSessionBeads(fx.cityRoot, cityStore, result.State, sp, cfgNames, fx.cfg, clk, io.Discard, true)
	sessions, err := loadSessionBeads(cityStore)
	if err != nil {
		t.Fatalf("load candidate session beads: %v", err)
	}
	poolDesired := PoolDesiredCounts(ComputePoolDesiredStates(
		fx.cfg, result.AssignedWorkBeads, sessionInfosFromBeads(sessions), result.ScaleCheckCounts,
	))
	if poolDesired == nil {
		poolDesired = map[string]int{}
	}
	if len(poolDesired) != 0 {
		t.Fatalf("pool desired counts after session sync = %v, want zero", poolDesired)
	}
	mergeNamedSessionDemand(poolDesired, result.NamedSessionDemand, fx.cfg)

	woken := reconcileSessionBeadsAtPath(
		context.Background(), fx.cityRoot, sessions, result.State, cfgNames,
		fx.cfg, sp, cityStore, nil, result.AssignedWorkBeads, rigStores,
		nil, newDrainTracker(), poolDesired, result.StoreQueryPartial, nil, fx.cfg.EffectiveCityName(),
		nil, clk, events.Discard, 0, 0, io.Discard, io.Discard,
	)
	if woken != 1 {
		t.Fatalf("reconciler woken = %d, want Mayor only", woken)
	}
	starts := 0
	for _, call := range sp.SnapshotCalls() {
		if call.Method != "Start" {
			continue
		}
		starts++
		if got := call.Config.Env["GC_ALIAS"]; got != "mayor" {
			t.Errorf("runtime Start(%q) GC_ALIAS = %q, want mayor", call.Name, got)
		}
	}
	if starts != 1 {
		t.Fatalf("runtime Start calls = %d, want exactly one Mayor start", starts)
	}
}

func assertMasontopiaCandidateHoldIndexPartial(t *testing.T, fx masontopiaCandidateFixture) {
	t.Helper()

	const workerTemplate = "repaircanary-20260807"
	cfg := *fx.cfg
	cfg.Agents = make([]config.Agent, len(fx.cfg.Agents))
	for i := range fx.cfg.Agents {
		cfg.Agents[i] = fx.cfg.Agents[i].Clone()
		if cfg.Agents[i].QualifiedName() == workerTemplate {
			cfg.Agents[i].Suspended = false
		}
	}

	cityStore := &labelBlindReadyStore{
		MemStore:             beads.NewMemStore(),
		readyErr:             fmt.Errorf("synthetic canonical-hold filtered-ready outage"),
		readyPartial:         true,
		readyErrFilteredOnly: true,
		dropReadyRows:        true,
	}
	rigStore := beads.NewMemStore()
	rigStores := map[string]beads.Store{"instagramtv": rigStore}
	if _, err := cityStore.Create(beads.Bead{
		ID:       "synthetic-candidate-work",
		Title:    "synthetic routed candidate demand",
		Type:     "task",
		Status:   "open",
		Metadata: map[string]string{"gc.routed_to": workerTemplate},
	}); err != nil {
		t.Fatalf("seed synthetic candidate demand: %v", err)
	}

	sp := runtime.NewFake()
	clk := &clock.Fake{Time: time.Date(2026, 8, 7, 23, 46, 0, 0, time.UTC)}
	result := buildDesiredStateWithSessionBeads(
		cfg.EffectiveCityName(), fx.cityRoot, clk.Now().UTC(), &cfg, sp,
		cityStore, rigStores,
		&sessionBeadSnapshot{}, nil, io.Discard,
	)

	if result.StoreQueryPartial || result.SessionQueryPartial {
		t.Fatalf("candidate global partial flags: store=%v session=%v, want scoped pool partial only", result.StoreQueryPartial, result.SessionQueryPartial)
	}
	if !result.PoolScaleCheckPartialTemplates[workerTemplate] {
		t.Fatalf("pool partial templates = %v, want %q", result.PoolScaleCheckPartialTemplates, workerTemplate)
	}
	var filtered []beads.ReadyQuery
	for _, query := range cityStore.readyQuerySnapshot() {
		if len(query.ExcludeLabels) > 0 {
			filtered = append(filtered, query)
		}
	}
	if len(filtered) != 1 || filtered[0].TierMode != beads.TierBoth ||
		strings.Join(filtered[0].ExcludeLabels, "\x00") != strings.Join(beadmeta.DispatchHoldLabels, "\x00") {
		t.Fatalf("canonical-hold filtered Ready queries = %+v, want one TierBoth query excluding %v", filtered, beadmeta.DispatchHoldLabels)
	}
	if queries := cityStore.labelQuerySnapshot(); len(queries) != 0 {
		t.Fatalf("canonical-hold label queries = %+v, want none outside the atomic Ready snapshot", queries)
	}
	if got := result.ScaleCheckCounts[workerTemplate]; got != 0 {
		t.Fatalf("ScaleCheckCounts[%q] = %d, want zero on uncertain hold index", workerTemplate, got)
	}
	if len(result.ReadyUnassignedRoutedWorkBeads) != 0 {
		t.Fatalf("partial hold index leaked ready routed demand: %+v", result.ReadyUnassignedRoutedWorkBeads)
	}
	if len(result.State) != 1 {
		t.Fatalf("partial desired state = %+v, want Mayor only", result.State)
	}
	for _, desired := range result.State {
		if desired.ConfiguredNamedIdentity != "mayor" {
			t.Fatalf("partial desired state = %+v, want Mayor only", result.State)
		}
	}

	poolStates := ComputePoolDesiredStates(&cfg, result.AssignedWorkBeads, nil, result.ScaleCheckCounts)
	if got := PoolDesiredCounts(poolStates); len(got) != 0 {
		t.Fatalf("partial pool desired counts = %v, want zero", got)
	}

	cfgNames := configuredSessionNames(&cfg, cfg.EffectiveCityName(), cityStore)
	syncSessionBeads(fx.cityRoot, cityStore, result.State, sp, cfgNames, &cfg, clk, io.Discard, true)
	sessions, err := loadSessionBeads(cityStore)
	if err != nil {
		t.Fatalf("load partial candidate session beads: %v", err)
	}
	sessionInfos := sessionInfosFromBeads(sessions)
	if len(sessionInfos) != 1 || sessionInfos[0].ConfiguredNamedIdentity != "mayor" {
		t.Fatalf("partial session rows = %+v, want Mayor only", sessionInfos)
	}

	poolDesired := PoolDesiredCounts(ComputePoolDesiredStates(
		&cfg, result.AssignedWorkBeads, sessionInfos, result.ScaleCheckCounts,
	))
	if poolDesired == nil {
		poolDesired = map[string]int{}
	}
	if len(poolDesired) != 0 {
		t.Fatalf("partial pool desired counts after sync = %v, want zero", poolDesired)
	}
	mergeNamedSessionDemand(poolDesired, result.NamedSessionDemand, &cfg)

	woken := reconcileSessionBeadsAtPath(
		context.Background(), fx.cityRoot, sessions, result.State, cfgNames,
		&cfg, sp, cityStore, nil, result.AssignedWorkBeads,
		rigStores,
		nil, newDrainTracker(), poolDesired, result.StoreQueryPartial, nil, cfg.EffectiveCityName(),
		nil, clk, events.Discard, 0, 0, io.Discard, io.Discard,
	)
	if woken != 1 {
		t.Fatalf("partial reconciler woken = %d, want Mayor only", woken)
	}
	starts := 0
	for _, call := range sp.SnapshotCalls() {
		if call.Method != "Start" {
			continue
		}
		starts++
		if got := call.Config.Env["GC_ALIAS"]; got != "mayor" {
			t.Errorf("partial runtime Start(%q) GC_ALIAS = %q, want mayor", call.Name, got)
		}
	}
	if starts != 1 {
		t.Fatalf("partial runtime Start calls = %d, want exactly one Mayor start", starts)
	}
}

func assertPreservedMasontopiaCandidateRejectedBySchema2(t *testing.T, raw []byte) {
	t.Helper()
	root := t.TempDir()
	mustWriteMasontopiaFixtureFile(t, filepath.Join(root, "pack.toml"), "[pack]\nname = \"masontopia\"\nschema = 2\n", 0o644)
	mustWriteMasontopiaFixtureFile(t, filepath.Join(root, "city.toml"), string(raw), 0o644)
	_, err := loadCityConfigWithoutBuiltinPackRefreshFS(fsys.OSFS{}, filepath.Join(root, "city.toml"), io.Discard)
	if err == nil {
		t.Fatal("byte-exact candidate unexpectedly loaded beside its schema-2 city pack")
	}
	if !strings.Contains(err.Error(), "unsupported PackV1 [[agent]] tables") {
		t.Fatalf("schema-2 candidate rejection = %v, want PackV1 [[agent]] incompatibility", err)
	}
}

func masontopiaCandidateAgent(cfg *config.City, qualifiedName string) *config.Agent {
	for i := range cfg.Agents {
		if cfg.Agents[i].QualifiedName() == qualifiedName {
			return &cfg.Agents[i]
		}
	}
	return nil
}

func masontopiaCandidateRig(cfg *config.City, name string) *config.Rig {
	for i := range cfg.Rigs {
		if cfg.Rigs[i].Name == name {
			return &cfg.Rigs[i]
		}
	}
	return nil
}

func mustReadMasontopiaCandidateFixture(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(masontopiaCandidateFixtureDir, "candidate-city.toml"))
	if err != nil {
		t.Fatalf("read candidate fixture: %v", err)
	}
	return data
}

func copyMasontopiaFixtureTree(t *testing.T, src, dst string) {
	t.Helper()
	if err := filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	}); err != nil {
		t.Fatalf("copy frozen fixture tree: %v", err)
	}
}

func mustWriteMasontopiaFixtureFile(t *testing.T, path, contents string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create parent for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(contents), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func removeString(values []string, target string) []string {
	for i, value := range values {
		if value == target {
			return append(values[:i], values[i+1:]...)
		}
	}
	return values
}

func masontopiaCandidateTemplates() []string {
	result := []string{
		"bd.dog",
		"codex",
		"codex-repaircanary-20260807",
		"codex-repairmayor-20260807",
		"codex-repairreview-20260807",
		"core.control-dispatcher",
		"mayor",
		"repaircanary-20260807",
		"reviewcanary-20260807",
		"instagramtv/codex",
		"instagramtv/codex-repaircanary-20260807",
		"instagramtv/codex-repairmayor-20260807",
		"instagramtv/codex-repairreview-20260807",
		"instagramtv/core.control-dispatcher",
		"instagramtv/gc.design-author",
		"instagramtv/gc.design-implementation-reviewer",
		"instagramtv/gc.design-test-risk-reviewer",
		"instagramtv/gc.gap-analyst",
		"instagramtv/gc.implementation-reviewer",
		"instagramtv/gc.implementation-worker",
		"instagramtv/gc.issue-triager",
		"instagramtv/gc.publisher",
		"instagramtv/gc.requirements-planner",
		"instagramtv/gc.review-synthesizer",
		"instagramtv/gc.run-operator",
		"instagramtv/gc.task-decomposer",
		"instagramtv/superpowers.brainstorming",
		"instagramtv/superpowers.code-quality-reviewer",
		"instagramtv/superpowers.code-reviewer",
		"instagramtv/superpowers.finisher",
		"instagramtv/superpowers.implementer",
		"instagramtv/superpowers.plan-reviewer",
		"instagramtv/superpowers.review-fixer",
		"instagramtv/superpowers.spec-reviewer",
		"instagramtv/superpowers.writing-plans",
	}
	sort.Strings(result)
	return result
}
