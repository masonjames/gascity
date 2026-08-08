package tmux

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/shellquote"
)

func TestProjectHooksForbidPreStartContaminationBlocksCreate(t *testing.T) {
	workDir := canonicalTmuxTestDir(t)
	hookPath := filepath.Join(workDir, ".codex", "hooks.json")
	ops := &fakeStartOps{
		runSetupCommandHook: func(string) {
			writeProjectHookTestFile(t, hookPath)
		},
	}

	cfg := forbiddenCodexTmuxTestConfig(workDir)
	cfg.PreStart = []string{"inject contamination"}
	err := doStartSession(context.Background(), ops, "worker-7", cfg, DefaultConfig().SetupTimeout)
	if err == nil || !strings.Contains(err.Error(), "project hook") {
		t.Fatalf("doStartSession error = %v, want project-hook preflight failure", err)
	}
	if got := countStartCalls(ops, "createSession"); got != 0 {
		t.Fatalf("createSession calls = %d, want 0; calls=%v", got, ops.callMethods())
	}
	if _, statErr := os.Stat(hookPath); statErr != nil {
		t.Fatalf("pre_start contamination was not preserved: %v", statErr)
	}
}

func TestProjectHooksForbidPreStartContaminationBlocksRespawn(t *testing.T) {
	workDir := canonicalTmuxTestDir(t)
	hookPath := filepath.Join(workDir, ".codex", "hooks.json")
	ops := &fakeStartOps{
		hasSessionResult: true,
		runSetupCommandHook: func(string) {
			writeProjectHookTestFile(t, hookPath)
		},
	}

	cfg := forbiddenCodexTmuxTestConfig(workDir)
	cfg.PreStart = []string{"inject contamination"}
	err := doRelaunchSession(context.Background(), ops, "worker-7", cfg, DefaultConfig().SetupTimeout)
	if err == nil || !strings.Contains(err.Error(), "project hook") {
		t.Fatalf("doRelaunchSession error = %v, want project-hook preflight failure", err)
	}
	if got := countStartCalls(ops, "respawnAgent"); got != 0 {
		t.Fatalf("respawnAgent calls = %d, want 0; calls=%v", got, ops.callMethods())
	}
}

func TestProjectHooksForbidRunLiveBlocksBeforeShellAction(t *testing.T) {
	tests := []struct {
		name      string
		configure func(t *testing.T, cfg *runtime.Config)
	}{
		{
			name: "unsafe launch envelope",
			configure: func(_ *testing.T, cfg *runtime.Config) {
				cfg.Command = "sh -c codex"
			},
		},
		{
			name: "contaminated real cwd",
			configure: func(t *testing.T, cfg *runtime.Config) {
				writeProjectHookTestFile(t, filepath.Join(cfg.WorkDir, ".codex", "hooks.json"))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			workDir := canonicalTmuxTestDir(t)
			marker := filepath.Join(t.TempDir(), "session-live-ran")
			cfg := forbiddenCodexTmuxTestConfig(workDir)
			cfg.SessionLive = []string{"touch " + shellquote.Quote(marker)}
			tc.configure(t, &cfg)

			provider := NewProviderWithConfig(DefaultConfig())
			err := provider.RunLive("worker-7", cfg)
			if err == nil || !strings.Contains(err.Error(), "project hook") {
				t.Errorf("RunLive error = %v, want project-hook isolation failure", err)
			}
			if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
				t.Errorf("session_live shell action ran before isolation failure; marker stat error = %v", statErr)
			}
		})
	}
}

func TestProjectHooksForbidRunLiveFinalizationIsIdempotentWithoutShellAction(t *testing.T) {
	workDir := canonicalTmuxTestDir(t)
	cfg := forbiddenCodexTmuxTestConfig(workDir)
	provider := NewProviderWithConfig(DefaultConfig())

	for call := 1; call <= 2; call++ {
		if err := provider.RunLive("worker-7", cfg); err != nil {
			t.Fatalf("RunLive call %d: %v", call, err)
		}
	}

	homeDir := filepath.Join(filepath.Dir(workDir), "."+filepath.Base(workDir)+"-home")
	for _, root := range []string{homeDir, filepath.Join(homeDir, ".codex")} {
		info, err := os.Lstat(root)
		if err != nil {
			t.Fatalf("materialized root %q: %v", root, err)
		}
		if !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("materialized root %q mode = %v, want private directory 0700", root, info.Mode())
		}
	}
}

func TestProjectHooksForbidPreStartConfigRootPermissionDriftBlocksCreateAndRespawn(t *testing.T) {
	for _, lifecycle := range []string{"start", "relaunch"} {
		t.Run(lifecycle, func(t *testing.T) {
			workDir := filepath.Join(canonicalTmuxTestDir(t), "instance", "work")
			cfg := forbiddenCodexTmuxTestConfig(workDir)
			ops := &fakeStartOps{
				hasSessionResult: lifecycle == "relaunch",
				runSetupCommandHook: func(string) {
					if err := os.MkdirAll(cfg.Env["HOME"], 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.Chmod(cfg.Env["HOME"], 0o755); err != nil {
						t.Fatal(err)
					}
				},
			}
			cfg.PreStart = []string{"drift config root mode"}

			var err error
			if lifecycle == "start" {
				err = doStartSession(t.Context(), ops, "worker-7", cfg, DefaultConfig().SetupTimeout)
			} else {
				err = doRelaunchSession(t.Context(), ops, "worker-7", cfg, DefaultConfig().SetupTimeout)
			}
			if err == nil || !strings.Contains(err.Error(), "0700") {
				t.Fatalf("%s error = %v, want config-root permission rejection", lifecycle, err)
			}
			for _, method := range []string{"createSession", "respawnAgent"} {
				if got := countStartCalls(ops, method); got != 0 {
					t.Fatalf("%s %s calls = %d, want zero; calls=%v", lifecycle, method, got, ops.callMethods())
				}
			}
		})
	}
}

func TestProjectHooksForbidPreflightRunsBeforeEveryCreate(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		workDir := canonicalTmuxTestDir(t)
		hookPath := filepath.Join(workDir, ".codex", "hooks.json")
		hookErr := error(nil)
		ops := &fakeStartOps{
			createErrs: []error{ErrNoServer},
			createSessionHook: func(call int) {
				if call == 0 {
					hookErr = writeProjectHookFile(hookPath)
				}
			},
		}

		err := ensureFreshSession(ops, "worker-7", forbiddenCodexTmuxTestConfig(workDir))
		if hookErr != nil {
			t.Fatalf("inject retry contamination: %v", hookErr)
		}
		if err == nil || !strings.Contains(err.Error(), "project hook") {
			t.Fatalf("ensureFreshSession error = %v, want project-hook preflight failure", err)
		}
		if got := countStartCalls(ops, "createSession"); got != 1 {
			t.Fatalf("createSession calls = %d, want 1; calls=%v", got, ops.callMethods())
		}
	})

	t.Run("recreate", func(t *testing.T) {
		workDir := canonicalTmuxTestDir(t)
		hookPath := filepath.Join(workDir, ".codex", "hooks.json")
		hookErr := error(nil)
		running := false
		ops := &fakeStartOps{
			createErrs:             []error{ErrSessionExists},
			isSessionRunningResult: &running,
			killSessionHook: func() {
				hookErr = writeProjectHookFile(hookPath)
			},
		}

		err := ensureFreshSession(ops, "worker-7", forbiddenCodexTmuxTestConfig(workDir))
		if hookErr != nil {
			t.Fatalf("inject recreate contamination: %v", hookErr)
		}
		if err == nil || !strings.Contains(err.Error(), "project hook") {
			t.Fatalf("ensureFreshSession error = %v, want project-hook preflight failure", err)
		}
		if got := countStartCalls(ops, "createSession"); got != 1 {
			t.Fatalf("createSession calls = %d, want 1; calls=%v", got, ops.callMethods())
		}
	})
}

func TestProjectHooksForbidDiscoverySurfacesBlockBeforeProviderStart(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		relPath  string
	}{
		{
			name:     "antigravity arbitrary workspace plugin",
			provider: "antigravity",
			relPath:  filepath.Join(".agents", "plugins", "unrelated", "hooks.json"),
		},
		{
			name:     "claude local settings",
			provider: "claude",
			relPath:  filepath.Join(".claude", "settings.local.json"),
		},
		{
			name:     "claude project mcp config",
			provider: "claude",
			relPath:  ".mcp.json",
		},
		{
			name:     "codex project config with inline hooks",
			provider: "codex",
			relPath:  filepath.Join(".codex", "config.toml"),
		},
		{
			name:     "copilot arbitrary repository hook",
			provider: "copilot",
			relPath:  filepath.Join(".github", "hooks", "unrelated.json"),
		},
		{
			name:     "copilot repository settings",
			provider: "copilot",
			relPath:  filepath.Join(".github", "copilot", "settings.json"),
		},
		{
			name:     "copilot local repository settings",
			provider: "copilot",
			relPath:  filepath.Join(".github", "copilot", "settings.local.json"),
		},
		{
			name:     "copilot cross-tool claude settings",
			provider: "copilot",
			relPath:  filepath.Join(".claude", "settings.json"),
		},
		{
			name:     "copilot cross-tool claude local settings",
			provider: "copilot",
			relPath:  filepath.Join(".claude", "settings.local.json"),
		},
		{
			name:     "kiro arbitrary nested agent config",
			provider: "kiro",
			relPath:  filepath.Join(".kiro", "agents", "nested", "unrelated.md"),
		},
		{
			name:     "kiro arbitrary nested IDE hook",
			provider: "kiro",
			relPath:  filepath.Join(".kiro", "hooks", "nested", "unrelated.json"),
		},
		{
			name:     "opencode arbitrary plugin",
			provider: "opencode",
			relPath:  filepath.Join(".opencode", "plugins", "unrelated.ts"),
		},
		{
			name:     "oh my pi arbitrary nested hook",
			provider: "omp",
			relPath:  filepath.Join(".omp", "hooks", "pre", "unrelated.ts"),
		},
		{
			name:     "pi arbitrary extension",
			provider: "pi",
			relPath:  filepath.Join(".pi", "extensions", "unrelated.ts"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.provider+" "+tc.name, func(t *testing.T) {
			for _, location := range []string{"cwd", "ancestor"} {
				t.Run(location, func(t *testing.T) {
					root := canonicalTmuxTestDir(t)
					ancestor := filepath.Join(root, "bootstrap")
					workDir := ancestor
					if location == "ancestor" {
						workDir = filepath.Join(ancestor, "instances", "worker-7")
					}
					if err := os.MkdirAll(workDir, 0o755); err != nil {
						t.Fatal(err)
					}
					writeProjectHookTestFile(t, filepath.Join(ancestor, tc.relPath))
					ops := &fakeStartOps{}

					err := doStartSession(context.Background(), ops, "worker-7", forbiddenCodexTmuxTestConfig(workDir), DefaultConfig().SetupTimeout)
					if err == nil || !strings.Contains(err.Error(), "project hook preflight") {
						t.Fatalf("doStartSession error = %v, want project-hook preflight failure", err)
					}
					if got := countStartCalls(ops, "createSession"); got != 0 {
						t.Fatalf("createSession calls = %d, want zero provider starts; calls=%v", got, ops.callMethods())
					}
				})
			}
		})
	}
}

func TestProjectHooksForbidUnknownProviderInventoryBlocksBeforeProviderStart(t *testing.T) {
	for _, provider := range []string{"", "antigravity", "claude", "copilot", "cursor", "gemini", "kimi", "kiro", "mimocode", "omp", "opencode", "pi", "groq", "cerebras", "custom-uninventoried-provider"} {
		t.Run(provider, func(t *testing.T) {
			workDir := canonicalTmuxTestDir(t)
			ops := &fakeStartOps{}

			cfg := forbiddenCodexTmuxTestConfig(workDir)
			cfg.ProviderName = provider
			cfg.PreStart = []string{"must not run"}
			err := doStartSession(context.Background(), ops, "worker-7", cfg, DefaultConfig().SetupTimeout)
			if err == nil || !strings.Contains(err.Error(), "no complete project discovery inventory") {
				t.Fatalf("doStartSession error = %v, want incomplete provider-inventory failure", err)
			}
			if got := countStartCalls(ops, "createSession"); got != 0 {
				t.Fatalf("createSession calls = %d, want zero provider starts; calls=%v", got, ops.callMethods())
			}
			if got := countStartCalls(ops, "runSetupCommand"); got != 0 {
				t.Fatalf("runSetupCommand calls = %d, want zero pre-start side effects; calls=%v", got, ops.callMethods())
			}
		})
	}
}

func TestProjectHooksForbidLinkedWorktreeRootCheckoutBlocksBeforeProviderStart(t *testing.T) {
	for _, rootArtifact := range []string{
		filepath.Join(".codex", "hooks.json"),
		filepath.Join(".codex", "config.toml"),
	} {
		t.Run(rootArtifact, func(t *testing.T) {
			root := canonicalTmuxTestDir(t)
			rootCheckout := filepath.Join(root, "root-checkout")
			linkedWorktree := filepath.Join(root, "linked-worktree")
			workDir := filepath.Join(linkedWorktree, "sessions", "worker-7")
			gitDir := filepath.Join(rootCheckout, ".git", "worktrees", "linked-worktree")
			if err := os.MkdirAll(gitDir, 0o755); err != nil {
				t.Fatal(err)
			}
			writeProjectHookTestFile(t, filepath.Join(rootCheckout, rootArtifact))
			writeProjectHookTestFile(t, filepath.Join(linkedWorktree, ".git"))
			if err := os.WriteFile(filepath.Join(linkedWorktree, ".git"), []byte("gitdir: "+gitDir+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			ops := &fakeStartOps{}

			cfg := forbiddenCodexTmuxTestConfig(workDir)
			cfg.PreStart = []string{"must not run"}
			err := doStartSession(context.Background(), ops, "worker-7", cfg, DefaultConfig().SetupTimeout)
			if err == nil || !strings.Contains(err.Error(), "repository") {
				t.Fatalf("doStartSession error = %v, want repository-contained cwd rejection", err)
			}
			if got := countStartCalls(ops, "createSession"); got != 0 {
				t.Fatalf("createSession calls = %d, want zero provider starts; calls=%v", got, ops.callMethods())
			}
			if got := countStartCalls(ops, "runSetupCommand"); got != 0 {
				t.Fatalf("runSetupCommand calls = %d, want zero pre-start side effects; calls=%v", got, ops.callMethods())
			}
		})
	}
}

func TestProjectHooksForbidInvalidLaunchCommandBlocksPreStartCreateAndRelaunch(t *testing.T) {
	for _, lifecycle := range []string{"start", "relaunch"} {
		t.Run(lifecycle, func(t *testing.T) {
			workDir := filepath.Join(canonicalTmuxTestDir(t), "instance", "work")
			ops := &fakeStartOps{hasSessionResult: lifecycle == "relaunch"}
			cfg := forbiddenCodexTmuxTestConfig(workDir)
			cfg.Command = "sh -c 'exec codex --disable hooks'"
			cfg.PreStart = []string{"must not run"}

			var err error
			if lifecycle == "start" {
				err = doStartSession(context.Background(), ops, "worker-7", cfg, DefaultConfig().SetupTimeout)
			} else {
				err = doRelaunchSession(context.Background(), ops, "worker-7", cfg, DefaultConfig().SetupTimeout)
			}
			if err == nil || !strings.Contains(err.Error(), "launch command") {
				t.Fatalf("%s error = %v, want launch-command preflight failure", lifecycle, err)
			}
			for _, method := range []string{"runSetupCommand", "createSession", "respawnAgent"} {
				if got := countStartCalls(ops, method); got != 0 {
					t.Fatalf("%s calls = %d, want zero side effects; calls=%v", method, got, ops.callMethods())
				}
			}
		})
	}
}

func TestProviderFinalizationFailureBlocksTmuxCreateAndRelaunch(t *testing.T) {
	for _, lifecycle := range []string{"start", "relaunch"} {
		t.Run(lifecycle, func(t *testing.T) {
			root := canonicalTmuxTestDir(t)
			workDir := filepath.Join(root, "instance", "work")
			copySource := filepath.Join(root, "copy-source")
			writeProjectHookTestFile(t, filepath.Join(copySource, "staging-marker"))

			executor := &fakeExecutor{}
			provider := NewProviderWithConfig(Config{SocketName: "project-hook-finalization"})
			provider.tm.exec = executor
			cfg := runtime.Config{
				WorkDir:               workDir,
				Command:               "codex resume session-123",
				ProviderName:          "codex",
				Env:                   map[string]string{"BASH_ENV": filepath.Join(root, "inject.sh")},
				CopyFiles:             []runtime.CopyEntry{{Src: copySource}},
				PreStart:              []string{"must not run"},
				ProjectHooksForbidden: true,
			}

			var err error
			if lifecycle == "start" {
				err = provider.Start(t.Context(), "worker-7", cfg)
			} else {
				err = provider.Relaunch(t.Context(), "worker-7", cfg)
			}
			if err == nil || !strings.Contains(err.Error(), "finalizing project hook isolation") {
				t.Fatalf("%s error = %v, want launch finalization rejection", lifecycle, err)
			}
			if len(executor.calls) != 0 {
				t.Fatalf("%s reached tmux executor before rejection: %v", lifecycle, executor.calls)
			}
			if _, statErr := os.Lstat(filepath.Join(workDir, "staging-marker")); !os.IsNotExist(statErr) {
				t.Fatalf("%s staged files before rejection: %v", lifecycle, statErr)
			}
		})
	}
}

func TestProviderMalformedEnvironmentKeyBlocksMaterializeStagePreStartCreateAndRelaunch(t *testing.T) {
	for _, lifecycle := range []string{"start", "relaunch"} {
		t.Run(lifecycle, func(t *testing.T) {
			root := canonicalTmuxTestDir(t)
			workDir := filepath.Join(root, "instance", "work")
			copySource := filepath.Join(root, "copy-source")
			writeProjectHookTestFile(t, filepath.Join(copySource, "staging-marker"))

			executor := &fakeExecutor{}
			provider := NewProviderWithConfig(Config{SocketName: "project-hook-invalid-env-key"})
			provider.tm.exec = executor
			cfg := runtime.Config{
				WorkDir:               workDir,
				Command:               "codex --model gpt-5.6-sol",
				ProviderName:          "codex",
				Env:                   map[string]string{"-r": ""},
				CopyFiles:             []runtime.CopyEntry{{Src: copySource}},
				PreStart:              []string{"must not run"},
				ProjectHooksForbidden: true,
			}

			var err error
			if lifecycle == "start" {
				err = provider.Start(t.Context(), "worker-7", cfg)
			} else {
				err = provider.Relaunch(t.Context(), "worker-7", cfg)
			}
			if err == nil || !strings.Contains(err.Error(), "environment key") {
				t.Fatalf("%s error = %v, want malformed environment-key rejection", lifecycle, err)
			}
			wantHome := filepath.Join(filepath.Dir(workDir), "."+filepath.Base(workDir)+"-home")
			if _, statErr := os.Lstat(wantHome); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("isolated HOME materialized before malformed-key rejection: %v", statErr)
			}
			if _, statErr := os.Lstat(filepath.Join(workDir, "staging-marker")); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("staging occurred before malformed-key rejection: %v", statErr)
			}
			if len(executor.calls) != 0 {
				t.Fatalf("tmux/pre_start/create calls = %v, want zero", executor.calls)
			}
		})
	}
}

func TestProviderInsecureConfigRootsBlockTmuxCreateAndRelaunch(t *testing.T) {
	for _, lifecycle := range []string{"start", "relaunch"} {
		t.Run(lifecycle, func(t *testing.T) {
			root := canonicalTmuxTestDir(t)
			workDir := filepath.Join(root, "instance", "work")
			if err := os.MkdirAll(workDir, 0o755); err != nil {
				t.Fatal(err)
			}
			homeDir := filepath.Join(filepath.Dir(workDir), "."+filepath.Base(workDir)+"-home")
			if err := os.MkdirAll(filepath.Join(homeDir, ".codex"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(homeDir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(filepath.Join(homeDir, ".codex"), 0o755); err != nil {
				t.Fatal(err)
			}

			executor := &fakeExecutor{}
			provider := NewProviderWithConfig(Config{SocketName: "project-hook-insecure-roots"})
			provider.tm.exec = executor
			cfg := runtime.Config{
				WorkDir:               workDir,
				Command:               "codex resume session-123",
				ProviderName:          "codex",
				ProjectHooksForbidden: true,
			}

			var err error
			if lifecycle == "start" {
				err = provider.Start(t.Context(), "worker-7", cfg)
			} else {
				err = provider.Relaunch(t.Context(), "worker-7", cfg)
			}
			if err == nil || !strings.Contains(err.Error(), "0700") {
				t.Fatalf("%s error = %v, want insecure config-root rejection", lifecycle, err)
			}
			if len(executor.calls) != 0 {
				t.Fatalf("%s reached tmux executor before config-root rejection: %v", lifecycle, executor.calls)
			}
		})
	}
}

func TestProjectHooksForbidCanonicalPromptSuffixReachesShortAndLongCreatePaths(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prompt string
	}{
		{name: "short", prompt: "short prompt with ' apostrophe"},
		{name: "long", prompt: strings.Repeat("long prompt ", 200)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workDir := filepath.Join(canonicalTmuxTestDir(t), "instance", "work")
			if err := os.MkdirAll(workDir, 0o755); err != nil {
				t.Fatal(err)
			}
			ops := &fakeStartOps{}
			cfg := forbiddenCodexTmuxTestConfig(workDir)
			cfg.PromptSuffix = shellquote.Quote(tc.prompt)

			if err := ensureFreshSession(ops, "worker-7", cfg); err != nil {
				t.Fatalf("ensureFreshSession: %v", err)
			}
			if got := countStartCalls(ops, "createSession"); got != 1 {
				t.Fatalf("createSession calls = %d, want 1; calls=%v", got, ops.callMethods())
			}
			command := ops.calls[0].command
			if tc.name == "short" && !strings.Contains(command, cfg.PromptSuffix) {
				t.Fatalf("short create command %q does not carry canonical prompt suffix", command)
			}
			if tc.name == "long" && !strings.Contains(command, "sh -c") {
				t.Fatalf("long create command %q did not use runtime-owned file expansion", command)
			}
		})
	}
}

func countStartCalls(ops *fakeStartOps, method string) int {
	count := 0
	for _, call := range ops.calls {
		if call.method == method {
			count++
		}
	}
	return count
}

func writeProjectHookTestFile(t *testing.T, name string) {
	t.Helper()
	if err := writeProjectHookFile(name); err != nil {
		t.Fatal(err)
	}
}

func writeProjectHookFile(name string) error {
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return err
	}
	return os.WriteFile(name, []byte(`{"hook":true}`), 0o644)
}

func canonicalTmuxTestDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func forbiddenCodexTmuxTestConfig(workDir string) runtime.Config {
	homeDir := filepath.Join(filepath.Dir(workDir), "home")
	cfg := runtime.Config{
		WorkDir:      workDir,
		Command:      "codex --disable hooks",
		ProviderName: "codex",
		Env: map[string]string{
			"HOME":       homeDir,
			"CODEX_HOME": filepath.Join(homeDir, ".codex"),
			"PATH":       os.Getenv("PATH"),
		},
		ProjectHooksForbidden: true,
	}
	for _, key := range runtime.ProjectHookIsolationWithheldEnvKeys() {
		cfg.Env[key] = ""
	}
	return cfg
}
