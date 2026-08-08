package runtime

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/hookartifact"
	"github.com/gastownhall/gascity/internal/overlay"
	"github.com/gastownhall/gascity/internal/processenv"
	"github.com/gastownhall/gascity/internal/shellquote"
)

// HashHookSettingsContent returns a content hash for a probed hook/settings
// file that is stable across JSON serialization differences. For reconciler-owned
// mergeable settings files (overlay.IsMergeablePath — .gemini/settings.json,
// .codex/hooks.json, etc.) it hashes the canonical JSON form, so a compact
// document and its pretty-printed equivalent fingerprint identically.
//
// This keeps the CopyFiles fingerprint deterministic even though these files
// are rewritten into canonical form out of band by the reconciler — runtime
// overlay staging (StageProviderOverlayDir → MergeSettingsJSON) or hooks.Install.
// Without canonicalization the pre-fingerprint probe could hash a raw
// non-canonical document on one tick and its canonical rewrite on the next,
// producing spurious core-fingerprint drift. Non-mergeable paths, unreadable
// files, and non-JSON content fall back to raw content hashing (HashPathContent).
func HashHookSettingsContent(path, relPath string) string {
	if overlay.IsMergeablePath(relPath) {
		if data, err := os.ReadFile(path); err == nil {
			if canon, cErr := overlay.CanonicalJSON(data); cErr == nil {
				sum := sha256.Sum256(canon)
				return fmt.Sprintf("%x", sum)
			}
		}
	}
	return HashPathContent(path)
}

// StageWorkDir applies a legacy overlay directory and CopyFiles staging before
// a provider starts the session process.
func StageWorkDir(workDir, overlayDir string, copyFiles []CopyEntry) error {
	if overlayDir != "" && workDir != "" {
		if err := stageDirStrict(overlayDir, workDir); err != nil {
			return fmt.Errorf("overlay %q -> %q: %w", overlayDir, workDir, err)
		}
	}
	return stageCopyFiles(workDir, copyFiles)
}

// StageSessionWorkDir applies provider-aware pack overlays, the agent overlay,
// and CopyFiles staging before a provider starts the session process.
func StageSessionWorkDir(cfg Config) error {
	return PrepareSessionWorkDirWithWarnings(cfg, os.Stderr)
}

// StageSessionWorkDirWithWarnings applies provider-aware pack overlays, the
// agent overlay, and CopyFiles staging before a provider starts the session
// process. Nonfatal overlay preservation warnings are written to warnings.
func StageSessionWorkDirWithWarnings(cfg Config, warnings io.Writer) error {
	return PrepareSessionWorkDirWithWarnings(cfg, warnings)
}

// PrepareSessionWorkDir applies all local staging and then performs the final
// project-hook preflight required before a provider launches in the real cwd.
func PrepareSessionWorkDir(cfg Config) error {
	return PrepareSessionWorkDirWithWarnings(cfg, os.Stderr)
}

// PrepareSessionWorkDirWithWarnings is PrepareSessionWorkDir with an explicit
// sink for nonfatal overlay preservation warnings. Under a forbidden-hook
// policy it checks the destination before staging (so symlink contamination is
// never followed), omits every registered hook artifact from all writers, and
// checks again after the last staging writer.
func PrepareSessionWorkDirWithWarnings(cfg Config, warnings io.Writer) error {
	if err := PreflightSessionWorkDir(cfg); err != nil {
		return err
	}
	skipProjectHooks := cfg.ProjectHooksForbidden
	if cfg.WorkDir != "" {
		overlayProviders := EffectiveOverlayProviderNames(cfg)
		for _, od := range cfg.PackOverlayDirs {
			if err := stageProviderOverlayDirWithProjectHookPolicy(od, cfg.WorkDir, overlayProviders, skipProjectHooks, warnings); err != nil {
				return fmt.Errorf("pack overlay %q -> %q: %w", od, cfg.WorkDir, err)
			}
		}
		if cfg.OverlayDir != "" {
			if err := stageProviderOverlayDirWithProjectHookPolicy(cfg.OverlayDir, cfg.WorkDir, overlayProviders, skipProjectHooks, warnings); err != nil {
				return fmt.Errorf("overlay %q -> %q: %w", cfg.OverlayDir, cfg.WorkDir, err)
			}
		}
	}
	if err := stageCopyFilesWithProjectHookPolicy(cfg.WorkDir, cfg.CopyFiles, skipProjectHooks); err != nil {
		return err
	}
	return PreflightSessionWorkDir(cfg)
}

// EffectiveOverlayProviderNames returns the provider overlay slots to stage for
// cfg, resolving the concrete-vs-family primary against cfg's overlay sources.
// The concrete cfg.ProviderOverlayName is honored only when a
// per-provider/<concrete>/ directory exists in one of cfg's overlay source dirs
// (PackOverlayDirs or OverlayDir); otherwise it is dropped so the slot list
// falls back to the launch family cfg.ProviderName. This keeps a provider that
// ships its own overlay (e.g. Kiro) on its concrete overlay, while letting a
// custom provider with no concrete overlay dir (e.g. base="builtin:pi"
// "pi-vllm", which has no per-provider/pi-vllm/) fall back to the family overlay
// (per-provider/pi/) where its lifecycle hooks live (gc-6bw8o).
//
// The pure OverlayProviderNames is retained for fingerprinting, which must stay
// filesystem-independent.
func EffectiveOverlayProviderNames(cfg Config) []string {
	overlayName := strings.TrimSpace(cfg.ProviderOverlayName)
	if overlayName != "" && !overlayProviderDirExists(cfg, overlayName) {
		overlayName = ""
	}
	return OverlayProviderNamesFromParts(cfg.ProviderName, overlayName, cfg.InstallAgentHooks)
}

// overlayProviderDirExists reports whether any of cfg's overlay source dirs
// contains a per-provider/<providerName>/ overlay directory.
func overlayProviderDirExists(cfg Config, providerName string) bool {
	for _, od := range cfg.PackOverlayDirs {
		if overlay.HasProviderDir(od, providerName) {
			return true
		}
	}
	return cfg.OverlayDir != "" && overlay.HasProviderDir(cfg.OverlayDir, providerName)
}

func stageCopyFiles(workDir string, copyFiles []CopyEntry) error {
	return stageCopyFilesWithProjectHookPolicy(workDir, copyFiles, false)
}

func stageCopyFilesWithProjectHookPolicy(workDir string, copyFiles []CopyEntry, forbidProjectHooks bool) error {
	for _, cf := range copyFiles {
		dst := workDir
		if cf.RelDst != "" {
			dst = filepath.Join(workDir, cf.RelDst)
		}
		effectiveDst, err := effectiveStageDestination(cf.Src, dst)
		if err != nil {
			return fmt.Errorf("resolving copy destination %q -> %q: %w", cf.Src, dst, err)
		}
		if sameFile(cf.Src, effectiveDst) {
			continue
		}
		if forbidProjectHooks {
			if _, err := stagePathOmittingProjectHooks(workDir, cf.Src, dst, effectiveDst); err != nil {
				return fmt.Errorf("copy file %q -> %q: %w", cf.Src, dst, err)
			}
			continue
		}
		if err := StagePath(cf.Src, dst); err != nil {
			return fmt.Errorf("copy file %q -> %q: %w", cf.Src, dst, err)
		}
	}

	return nil
}

// stagePathOmittingProjectHooks stages src when it is safe and reports true
// both for a completed copy and a missing source. A false result means the
// single-file destination was a registered hook artifact and was omitted.
func stagePathOmittingProjectHooks(workDir, src, dst, effectiveDst string) (bool, error) {
	info, err := os.Stat(src)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		rel, err := relativeStagePath(workDir, effectiveDst)
		if err != nil {
			return false, err
		}
		if hookartifact.Is(rel) {
			return false, nil
		}
		return true, StagePath(src, dst)
	}

	baseRel, err := relativeStagePath(workDir, dst)
	if err != nil {
		return false, err
	}
	skip := func(relPath string, _ bool) bool {
		// Match StagePath's ordinary directory-copy contract: a source tree's
		// top-level .gc runtime mirror is never staged into a session workdir.
		// The forbid path uses CopyDirWithSkip for strict error handling, so it
		// must preserve that unconditional omission explicitly before applying
		// its additional destination hook-artifact filter.
		cleanSourceRel := filepath.Clean(relPath)
		if cleanSourceRel == ".gc" || strings.HasPrefix(cleanSourceRel, ".gc"+string(filepath.Separator)) {
			return true
		}
		destinationRel := relPath
		if baseRel != "." {
			destinationRel = filepath.Join(baseRel, relPath)
		}
		return hookartifact.Is(destinationRel)
	}
	return true, overlay.CopyDirWithSkip(src, dst, skip, io.Discard)
}

func relativeStagePath(workDir, destination string) (string, error) {
	if strings.TrimSpace(workDir) == "" {
		return "", errors.New("project-hook isolation requires a non-empty workdir")
	}
	rel, err := filepath.Rel(workDir, destination)
	if err != nil {
		return "", err
	}
	clean := filepath.Clean(rel)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || filepath.IsAbs(clean) {
		return "", fmt.Errorf("copy destination %q escapes workdir %q", destination, workDir)
	}
	return clean, nil
}

// StageProviderOverlayDir copies a provider-aware overlay directory into a
// work directory and writes nonfatal preservation warnings to warnings. This is
// the runtime task-worktree staging path: it stages every overlay file
// (including reconciler-owned mergeable hook files) because staging is the sole
// writer for live task sessions — hooks.Install never runs against these dirs.
func StageProviderOverlayDir(srcDir, dstDir string, providers []string, warnings io.Writer) error {
	return stageProviderOverlayDir(srcDir, dstDir, providers, nil, warnings)
}

func stageProviderOverlayDirWithProjectHookPolicy(srcDir, dstDir string, providers []string, forbid bool, warnings io.Writer) error {
	if !forbid {
		return StageProviderOverlayDir(srcDir, dstDir, providers, warnings)
	}
	skip := func(relPath string, _ bool) bool {
		return hookartifact.Is(relPath)
	}
	return stageProviderOverlayDir(srcDir, dstDir, providers, skip, warnings)
}

// StageProviderOverlayDirSkippingMergeable copies a provider-aware overlay
// directory into a work directory like StageProviderOverlayDir, but skips
// reconciler-owned mergeable settings/hook files (overlay.IsMergeablePath —
// .codex/hooks.json, .claude/settings.json, etc.).
//
// It is used only by the build_desired_state home-dir staging path,
// which stages overlays and then immediately runs hooks.Install on the SAME
// directory. Skipping the mergeable files here makes hooks.Install the sole
// writer ON THE RECONCILE TICK, so the two writers can no longer disagree on
// hook-entry matchers and leave a permanent codex-hooks-drift hybrid.
//
// Not a global invariant: for a persistent (non-task) agent the home dir is
// also the session workDir, and session-start staging reaches these same paths
// through the non-skipping StageProviderOverlayDir (tmux.stageStartFiles,
// StageSessionWorkDir). A hybrid can therefore reappear at session start and is
// converged by the next tick — permanent drift becomes transient.
func StageProviderOverlayDirSkippingMergeable(srcDir, dstDir string, providers []string, warnings io.Writer) error {
	skip := func(relPath string, isDir bool) bool {
		return !isDir && overlay.IsMergeablePath(relPath)
	}
	return stageProviderOverlayDir(srcDir, dstDir, providers, skip, warnings)
}

// stageProviderOverlayDir stages srcDir into dstDir for the given provider
// slots, omitting any entry for which skip returns true (nil skips nothing).
//
// skip is spelled as an unnamed func type rather than overlay.SkipFunc — to
// which it stays assignable — because every declaration in package runtime must
// type-check with module-local imports stubbed out: the provider-double
// boundary guard (internal/testutil/providerledger) checks this package
// hermetically and requires module-local references to stay inside function
// bodies.
func stageProviderOverlayDir(srcDir, dstDir string, providers []string, skip func(relPath string, isDir bool) bool, warnings io.Writer) error {
	var stderr bytes.Buffer
	if err := overlay.CopyDirForProvidersWithSkip(srcDir, dstDir, providers, skip, &stderr); err != nil {
		return err
	}
	nonfatal, fatal := splitOverlayWarnings(stderr.String())
	if nonfatal != "" && warnings != nil {
		fmt.Fprintln(warnings, nonfatal) //nolint:errcheck // best-effort warning emission
	}
	if fatal != "" {
		return fmt.Errorf("%s", fatal)
	}
	return nil
}

func splitOverlayWarnings(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	var nonfatal []string
	var fatal []string
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if overlay.IsPreserveExistingWarning(line) {
			nonfatal = append(nonfatal, line)
			continue
		}
		fatal = append(fatal, line)
	}
	return strings.Join(nonfatal, "\n"), strings.Join(fatal, "\n")
}

// PreflightSessionWorkDir fails closed when a forbidden-hook session's real
// cwd, any cwd ancestor, or a symlink component can expose a registered project
// hook artifact. It never removes or rewrites contamination.
func PreflightSessionWorkDir(cfg Config) error {
	if !cfg.ProjectHooksForbidden {
		return nil
	}
	providerName := strings.TrimSpace(cfg.ProviderName)
	if !hookartifact.HasCompleteDiscoveryInventory(providerName) {
		return fmt.Errorf("project hook preflight: provider %q has no complete project discovery inventory", providerName)
	}
	if err := ValidateEnvironmentKeys(cfg.Env); err != nil {
		return fmt.Errorf("project hook preflight: launch environment: %w", err)
	}
	if err := validateProjectHookIsolatedLaunchCommand(providerName, cfg.Command); err != nil {
		return fmt.Errorf("project hook preflight: %w", err)
	}
	if cfg.PromptFlag != "" {
		return fmt.Errorf("project hook preflight: launch command: prompt flag %q is not allowed", cfg.PromptFlag)
	}
	if cfg.PromptSuffix != "" {
		promptArgv := shellquote.Split(cfg.PromptSuffix)
		if len(promptArgv) != 1 || shellquote.Quote(promptArgv[0]) != cfg.PromptSuffix {
			return errors.New("project hook preflight: launch command: prompt suffix must be exactly one canonical quoted argument")
		}
	}
	launchPath, pathExplicit := cfg.Env["PATH"]
	if !pathExplicit || strings.TrimSpace(launchPath) == "" {
		return errors.New("project hook preflight: launch command: explicit controller PATH is required")
	}
	trustedGCBin := ""
	if configuredGCBin := strings.TrimSpace(cfg.Env["GC_BIN"]); configuredGCBin != "" {
		executable, err := os.Executable()
		if err != nil {
			return fmt.Errorf("project hook preflight: launch command: resolving controller executable: %w", err)
		}
		if configuredGCBin != executable {
			return fmt.Errorf("project hook preflight: launch command: GC_BIN %q does not match controller executable %q", configuredGCBin, executable)
		}
		trustedGCBin = executable
	}
	trustedEnv := map[string]string{"PATH": os.Getenv("PATH")}
	processenv.PrependGCBinDirToPATH(trustedEnv, trustedGCBin)
	if launchPath != trustedEnv["PATH"] {
		return errors.New("project hook preflight: launch command: config PATH override is not allowed")
	}
	for _, key := range projectHookIsolationWithheldEnvKeys {
		value, explicit := cfg.Env[key]
		if !explicit || value != "" {
			return fmt.Errorf("project hook preflight: launch environment: %s must be explicitly withheld", key)
		}
	}
	workDir := strings.TrimSpace(cfg.WorkDir)
	if workDir == "" {
		return errors.New("project hook preflight: workdir is empty")
	}
	abs, err := filepath.Abs(filepath.Clean(workDir))
	if err != nil {
		return fmt.Errorf("project hook preflight: resolving workdir %q: %w", workDir, err)
	}
	canonical, err := canonicalizeStagePath(abs)
	if err != nil {
		return fmt.Errorf("project hook preflight: canonicalizing workdir %q: %w", workDir, err)
	}
	if canonical != abs {
		return fmt.Errorf("project hook preflight: workdir %q resolves through a symlink to %q", abs, canonical)
	}
	repositoryRoot, err := enclosingRepositoryRoot(abs)
	if err != nil {
		return err
	}
	if repositoryRoot != "" {
		return fmt.Errorf("project hook preflight: workdir %q is inside repository %q", abs, repositoryRoot)
	}
	if err := preflightProjectHookIsolatedConfigRoots(cfg.Env, abs); err != nil {
		return fmt.Errorf("project hook preflight: %w", err)
	}

	for base := abs; ; base = filepath.Dir(base) {
		isAncestor := base != abs
		for _, artifact := range hookartifact.All() {
			if err := preflightArtifactPath(base, artifact.RelPath, isAncestor); err != nil {
				return err
			}
		}
		parent := filepath.Dir(base)
		if parent == base {
			break
		}
	}
	return nil
}

var projectHookIsolationWithheldEnvKeys = []string{
	"BASH_ENV",
	"DYLD_FRAMEWORK_PATH",
	"DYLD_INSERT_LIBRARIES",
	"DYLD_LIBRARY_PATH",
	"ENV",
	"GCONV_PATH",
	"LD_AUDIT",
	"LD_LIBRARY_PATH",
	"LD_PRELOAD",
	"NODE_OPTIONS",
	"ZDOTDIR",
}

// ValidateEnvironmentKeys rejects names that cannot be represented as POSIX
// environment variables. Runtime providers must apply this before composing
// names into shell commands or passing them to option-parsing process argv.
// Validation is deterministic and never includes environment values in errors.
func ValidateEnvironmentKeys(env map[string]string) error {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !isPOSIXEnvironmentKey(key) {
			return fmt.Errorf("environment key %q must match [A-Za-z_][A-Za-z0-9_]*", key)
		}
	}
	return nil
}

func isPOSIXEnvironmentKey(key string) bool {
	if key == "" || !isASCIILetterOrUnderscore(key[0]) {
		return false
	}
	for i := 1; i < len(key); i++ {
		if !isASCIILetterOrUnderscore(key[i]) && (key[i] < '0' || key[i] > '9') {
			return false
		}
	}
	return true
}

func isASCIILetterOrUnderscore(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

// IsProjectHookIsolationWithheldEnv reports whether key can inject code into
// the shell, dynamic loader, or Codex runtime before the attested command
// begins. Tmux uses this contract to preserve the runtime preflight's explicit
// withholding across warm-box respawns.
func IsProjectHookIsolationWithheldEnv(key string) bool {
	for _, candidate := range projectHookIsolationWithheldEnvKeys {
		if key == candidate {
			return true
		}
	}
	return false
}

// ProjectHookIsolationWithheldEnvKeys returns the environment keys that a
// project-hook-isolated launch must pin absent. The returned slice is a copy so
// callers can safely use it to construct runtime environments.
func ProjectHookIsolationWithheldEnvKeys() []string {
	return append([]string(nil), projectHookIsolationWithheldEnvKeys...)
}

// FinalizeProjectHookIsolatedConfig applies the authoritative launch envelope
// for a project-hook-forbidden Codex session after all configurable layers have
// merged. It rejects executable and environment injection, adds the exact
// hooks feature defense, derives a per-workdir sibling config home, and then
// runs the same preflight that the concrete provider repeats immediately
// before staging and launch. Configs using the inherited policy are returned
// unchanged.
func FinalizeProjectHookIsolatedConfig(cfg Config) (Config, error) {
	if !cfg.ProjectHooksForbidden {
		return cfg, nil
	}
	providerName := strings.TrimSpace(cfg.ProviderName)
	if providerName != "codex" {
		return Config{}, fmt.Errorf("finalizing project hook isolation: provider %q has no attested command grammar", providerName)
	}
	command, err := finalizeProjectHookIsolatedCodexCommand(cfg.Command)
	if err != nil {
		return Config{}, fmt.Errorf("finalizing project hook isolation: %w", err)
	}

	workDir := cfg.WorkDir
	if strings.TrimSpace(workDir) != workDir || !filepath.IsAbs(workDir) || filepath.Clean(workDir) != workDir {
		return Config{}, fmt.Errorf("finalizing project hook isolation: workdir %q must be a canonical absolute path", workDir)
	}
	if strings.TrimSpace(filepath.Base(workDir)) == "" || filepath.Dir(workDir) == workDir {
		return Config{}, fmt.Errorf("finalizing project hook isolation: workdir %q has no per-instance path component", workDir)
	}

	env := make(map[string]string, len(cfg.Env)+len(projectHookIsolationWithheldEnvKeys)+4)
	for key, value := range cfg.Env {
		env[key] = value
	}
	for _, key := range projectHookIsolationWithheldEnvKeys {
		if value := env[key]; value != "" {
			return Config{}, fmt.Errorf("finalizing project hook isolation: launch environment %s override is not allowed", key)
		}
		env[key] = ""
	}

	controllerExecutable, err := os.Executable()
	if err != nil {
		return Config{}, fmt.Errorf("finalizing project hook isolation: resolving controller executable: %w", err)
	}
	if configured := strings.TrimSpace(env["GC_BIN"]); configured != "" && configured != controllerExecutable {
		return Config{}, fmt.Errorf("finalizing project hook isolation: GC_BIN %q does not match controller executable %q", configured, controllerExecutable)
	}
	trustedPathEnv := map[string]string{"PATH": os.Getenv("PATH")}
	processenv.PrependGCBinDirToPATH(trustedPathEnv, controllerExecutable)
	trustedPath := trustedPathEnv["PATH"]
	if configured, ok := env["PATH"]; ok && configured != "" && configured != os.Getenv("PATH") && configured != trustedPath {
		return Config{}, fmt.Errorf("finalizing project hook isolation: PATH override is not allowed")
	}
	env["GC_BIN"] = controllerExecutable
	env["PATH"] = trustedPath

	homeDir := filepath.Join(filepath.Dir(workDir), "."+filepath.Base(workDir)+"-home")
	env["HOME"] = homeDir
	env["CODEX_HOME"] = filepath.Join(homeDir, ".codex")

	finalized := cfg
	finalized.Command = command
	finalized.Env = env
	if err := PreflightSessionWorkDir(finalized); err != nil {
		return Config{}, fmt.Errorf("finalizing project hook isolation: %w", err)
	}
	return finalized, nil
}

// MaterializeProjectHookIsolatedConfigRoots creates the finalized HOME and
// CODEX_HOME as private directories immediately before launch. It first runs
// the pure preflight, refuses symlinks, files, or permissive preexisting roots,
// creates only missing path components with mode 0700, and then re-attests the
// complete config and filesystem state. Configs using the inherited policy are
// unchanged.
func MaterializeProjectHookIsolatedConfigRoots(cfg Config) error {
	if !cfg.ProjectHooksForbidden {
		return nil
	}
	if err := PreflightSessionWorkDir(cfg); err != nil {
		return fmt.Errorf("materializing project hook isolation: %w", err)
	}
	for _, root := range []struct {
		label string
		path  string
	}{
		{label: "HOME", path: cfg.Env["HOME"]},
		{label: "CODEX_HOME", path: cfg.Env["CODEX_HOME"]},
	} {
		if err := ensurePrivateConfigRootDir(root.label, root.path); err != nil {
			return fmt.Errorf("materializing project hook isolation: %w", err)
		}
	}
	if err := PreflightSessionWorkDir(cfg); err != nil {
		return fmt.Errorf("materializing project hook isolation: re-attesting roots: %w", err)
	}
	for _, root := range []struct {
		label string
		path  string
	}{
		{label: "HOME", path: cfg.Env["HOME"]},
		{label: "CODEX_HOME", path: cfg.Env["CODEX_HOME"]},
	} {
		if err := attestPrivateConfigRootDir(root.label, root.path); err != nil {
			return fmt.Errorf("materializing project hook isolation: re-attesting roots: %w", err)
		}
	}
	return nil
}

func ensurePrivateConfigRootDir(label, path string) error {
	info, err := os.Lstat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("creating %s %q: %w", label, path, err)
		}
	case err != nil:
		return fmt.Errorf("inspecting %s %q: %w", label, path, err)
	case info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm() != 0o700:
		return fmt.Errorf("%s %q must be a non-symlink directory with mode 0700", label, path)
	}
	return attestPrivateConfigRootDir(label, path)
}

func attestPrivateConfigRootDir(label, path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspecting %s %q: %w", label, path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return fmt.Errorf("%s %q must be a non-symlink directory with mode 0700", label, path)
	}
	canonical, err := canonicalizeStagePath(path)
	if err != nil {
		return fmt.Errorf("canonicalizing %s %q: %w", label, path, err)
	}
	if canonical != path {
		return fmt.Errorf("%s %q resolves through a symlink to %q", label, path, canonical)
	}
	return nil
}

// validateProjectHookIsolatedLaunchCommand accepts only the deliberately
// narrow command grammar that the runtime can attest will launch Codex
// directly. The shell still receives a command string at the tmux boundary,
// so accepting wrappers, quoting, expansion, or unknown flags here would let a
// configured command redirect cwd or config discovery after filesystem
// preflight completed.
func validateProjectHookIsolatedLaunchCommand(providerName, command string) error {
	if providerName != "codex" {
		return fmt.Errorf("launch command: provider %q has no attested command grammar", providerName)
	}
	finalized, err := finalizeProjectHookIsolatedCodexCommand(command)
	if err != nil {
		return err
	}
	if finalized != command {
		return errors.New("launch command: exact --disable hooks defense is required")
	}
	return nil
}

func finalizeProjectHookIsolatedCodexCommand(command string) (string, error) {
	args, err := splitCanonicalCodexCommand(command)
	if err != nil {
		return "", fmt.Errorf("launch command: %w", err)
	}
	if args[0] != "codex" {
		return "", fmt.Errorf("launch command: argv[0] is %q, want exactly %q", args[0], "codex")
	}

	var (
		disabledHooks bool
		resume        bool
		sessionKey    string
	)
	for i := 1; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "resume":
			if resume {
				return "", errors.New("launch command: duplicate resume subcommand")
			}
			if sessionKey != "" {
				return "", errors.New("launch command: resume subcommand follows a positional argument")
			}
			resume = true
		case "--disable":
			value, next, err := codexOptionValue(args, i, arg)
			if err != nil {
				return "", fmt.Errorf("launch command: %w", err)
			}
			if value != "hooks" {
				return "", fmt.Errorf("launch command: %s may only disable %q, got %q", arg, "hooks", value)
			}
			if disabledHooks {
				return "", errors.New("launch command: duplicate --disable hooks defense")
			}
			disabledHooks = true
			i = next
		case "--model", "-m":
			value, next, err := codexOptionValue(args, i, arg)
			if err != nil {
				return "", fmt.Errorf("launch command: %w", err)
			}
			if !isCanonicalCodexIdentifier(value) {
				return "", fmt.Errorf("launch command: %s model %q is not a canonical identifier", arg, value)
			}
			i = next
		case "--ask-for-approval":
			value, next, err := codexOptionValue(args, i, arg)
			if err != nil {
				return "", fmt.Errorf("launch command: %w", err)
			}
			if !oneOf(value, "untrusted", "on-request", "on-failure", "never") {
				return "", fmt.Errorf("launch command: unsupported approval policy %q", value)
			}
			i = next
		case "--sandbox":
			value, next, err := codexOptionValue(args, i, arg)
			if err != nil {
				return "", fmt.Errorf("launch command: %w", err)
			}
			if !oneOf(value, "read-only", "network-off") {
				return "", fmt.Errorf("launch command: unsupported sandbox %q", value)
			}
			i = next
		case "-c", "--config":
			value, next, err := codexOptionValue(args, i, arg)
			if err != nil {
				return "", fmt.Errorf("launch command: %w", err)
			}
			if !oneOf(value,
				"model_reasoning_effort=low",
				"model_reasoning_effort=medium",
				"model_reasoning_effort=high",
				"model_reasoning_effort=xhigh",
				"model_reasoning_effort=ultra",
			) {
				return "", fmt.Errorf("launch command: %s only accepts an exact model_reasoning_effort value, got %q", arg, value)
			}
			i = next
		case "--full-auto", "--dangerously-bypass-approvals-and-sandbox":
			// These are the two argument-free permission shapes declared by
			// the built-in Codex provider schema.
		default:
			if strings.HasPrefix(arg, "-") {
				return "", fmt.Errorf("launch command: unsupported flag %q", arg)
			}
			if !resume {
				return "", fmt.Errorf("launch command: interactive form has unexpected positional argument %q", arg)
			}
			if sessionKey != "" {
				return "", fmt.Errorf("launch command: resume form has multiple session keys %q and %q", sessionKey, arg)
			}
			if !isCanonicalCodexIdentifier(arg) {
				return "", fmt.Errorf("launch command: resume session key %q is not a canonical identifier", arg)
			}
			sessionKey = arg
		}
	}
	if resume && sessionKey == "" {
		return "", errors.New("launch command: resume form requires exactly one session key")
	}
	if !disabledHooks {
		args = append([]string{"codex", "--disable", "hooks"}, args[1:]...)
	}
	return strings.Join(args, " "), nil
}

func splitCanonicalCodexCommand(command string) ([]string, error) {
	if command == "" {
		return nil, errors.New("command is empty")
	}
	if strings.TrimSpace(command) != command || strings.ContainsAny(command, "\t\r\n") {
		return nil, errors.New("command must use canonical single-space argv separation")
	}
	args := strings.Split(command, " ")
	for _, arg := range args {
		if arg == "" {
			return nil, errors.New("command must use canonical single-space argv separation")
		}
		for _, r := range arg {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("-._=:/@+", r) {
				continue
			}
			return nil, fmt.Errorf("command token %q contains shell syntax", arg)
		}
	}
	return args, nil
}

func codexOptionValue(args []string, optionIndex int, option string) (string, int, error) {
	next := optionIndex + 1
	if next >= len(args) {
		return "", optionIndex, fmt.Errorf("option %s requires a value", option)
	}
	return args[next], next, nil
}

func isCanonicalCodexIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		if i > 0 && strings.ContainsRune("-._", r) {
			continue
		}
		return false
	}
	return true
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

// preflightProjectHookIsolatedConfigRoots pins Codex's user-config discovery
// to a sibling of the real cwd. Sharing, nesting, or redirecting these roots
// would let an otherwise-clean cwd inherit a config.toml or hooks.json after
// the project-artifact scan.
func preflightProjectHookIsolatedConfigRoots(env map[string]string, workDir string) error {
	home, ok := env["HOME"]
	if !ok || strings.TrimSpace(home) == "" {
		return errors.New("codex config root isolation requires an explicit HOME")
	}
	codexHome, ok := env["CODEX_HOME"]
	if !ok || strings.TrimSpace(codexHome) == "" {
		return errors.New("codex config root isolation requires an explicit CODEX_HOME")
	}

	canonicalHome, err := canonicalConfigRoot("HOME", home)
	if err != nil {
		return err
	}
	canonicalCodexHome, err := canonicalConfigRoot("CODEX_HOME", codexHome)
	if err != nil {
		return err
	}
	if filepath.Dir(canonicalHome) != filepath.Dir(workDir) || canonicalHome == workDir {
		return fmt.Errorf("codex config root HOME %q and workdir %q must be disjoint siblings", canonicalHome, workDir)
	}
	wantCodexHome := filepath.Join(canonicalHome, ".codex")
	if canonicalCodexHome != wantCodexHome {
		return fmt.Errorf("codex config root CODEX_HOME %q must equal HOME/.codex %q", canonicalCodexHome, wantCodexHome)
	}
	for _, configRoot := range []struct{ label, path string }{
		{label: "HOME", path: canonicalHome},
		{label: "CODEX_HOME", path: canonicalCodexHome},
	} {
		if repositoryRoot, err := enclosingRepositoryRoot(configRoot.path); err != nil {
			return fmt.Errorf("codex config root: %w", err)
		} else if repositoryRoot != "" {
			return fmt.Errorf("codex config root %s %q is inside repository %q", configRoot.label, configRoot.path, repositoryRoot)
		}
	}
	for _, name := range []string{"hooks.json", "config.toml"} {
		path := filepath.Join(canonicalCodexHome, name)
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("codex config root exposes forbidden file %q", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("codex config root inspecting %q: %w", path, err)
		}
	}
	for _, configRoot := range []struct{ label, path string }{
		{label: "HOME", path: canonicalHome},
		{label: "CODEX_HOME", path: canonicalCodexHome},
	} {
		info, err := os.Lstat(configRoot.path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("codex config root inspecting %s %q: %w", configRoot.label, configRoot.path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm() != 0o700 {
			return fmt.Errorf("codex config root %s %q must be a non-symlink directory with mode 0700", configRoot.label, configRoot.path)
		}
	}
	return nil
}

func canonicalConfigRoot(label, root string) (string, error) {
	if strings.TrimSpace(root) != root || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return "", fmt.Errorf("codex config root %s %q must be a canonical absolute path", label, root)
	}
	canonical, err := canonicalizeStagePath(root)
	if err != nil {
		return "", fmt.Errorf("codex config root canonicalizing %s %q: %w", label, root, err)
	}
	if canonical != root {
		return "", fmt.Errorf("codex config root %s %q resolves through a symlink to %q", label, root, canonical)
	}
	return canonical, nil
}

func enclosingRepositoryRoot(path string) (string, error) {
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		marker := filepath.Join(current, ".git")
		if _, err := os.Lstat(marker); err == nil {
			return current, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("project hook preflight: checking repository marker %q: %w", marker, err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", nil
		}
	}
}

func preflightArtifactPath(base, relPath string, isAncestor bool) error {
	current := base
	parts := strings.Split(filepath.Clean(relPath), string(filepath.Separator))
	for i, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("project hook preflight: inspecting %q: %w", current, err)
		}
		location := "cwd"
		if isAncestor {
			location = "cwd ancestor"
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("project hook preflight: %s path %q contains symlink component %q", location, relPath, current)
		}
		if i == len(parts)-1 {
			return fmt.Errorf("project hook preflight: %s exposes project hook artifact %q", location, current)
		}
		if !info.IsDir() {
			return nil
		}
	}
	return nil
}

func canonicalizeStagePath(path string) (string, error) {
	current := path
	var suffix []string
	for {
		_, err := os.Lstat(current)
		if err == nil {
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return "", err
			}
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}

func stageDirStrict(srcDir, dstDir string) error {
	var stderr bytes.Buffer
	if err := overlay.CopyDir(srcDir, dstDir, &stderr); err != nil {
		return err
	}
	if stderr.Len() > 0 {
		return fmt.Errorf("%s", strings.TrimSpace(stderr.String()))
	}
	return nil
}

// StageDir copies a directory overlay while preserving CopyDir's historical
// best-effort behavior for per-path warnings.
func StageDir(srcDir, dstDir string) error {
	return overlay.CopyDir(srcDir, dstDir, &bytes.Buffer{})
}

// StagePath copies a file or directory and returns any per-file warnings as an
// error so callers can fail fast instead of ignoring partial staging.
func StagePath(src, dst string) error {
	var stderr bytes.Buffer
	if err := overlay.CopyFileOrDir(src, dst, &stderr); err != nil {
		return err
	}
	if stderr.Len() > 0 {
		return fmt.Errorf("%s", strings.TrimSpace(stderr.String()))
	}
	return nil
}

func effectiveStageDestination(src, dst string) (string, error) {
	info, err := os.Stat(src)
	if os.IsNotExist(err) {
		return dst, nil
	}
	if err != nil {
		return "", err
	}
	if info.IsDir() {
		return dst, nil
	}
	if dstInfo, err := os.Stat(dst); err == nil && dstInfo.IsDir() {
		return filepath.Join(dst, filepath.Base(src)), nil
	} else if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	return dst, nil
}

func sameFile(src, dst string) bool {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return false
	}
	dstInfo, err := os.Stat(dst)
	if err != nil {
		return false
	}
	return os.SameFile(srcInfo, dstInfo)
}
