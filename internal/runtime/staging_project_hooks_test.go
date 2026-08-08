package runtime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/processenv"
	"github.com/gastownhall/gascity/internal/shellquote"
)

func TestPrepareSessionWorkDirForbidStagesThenPreflightsBeforeStart(t *testing.T) {
	t.Run("stages harmless content and omits every hook writer", func(t *testing.T) {
		root := canonicalRuntimeTestDir(t)
		workDir := filepath.Join(root, "external", "worker-7")
		packOverlay := filepath.Join(root, "pack")
		agentOverlay := filepath.Join(root, "agent")
		copySource := filepath.Join(root, "copy")

		writeTestFile(t, filepath.Join(packOverlay, "README.md"), "pack harmless")
		writeTestFile(t, filepath.Join(packOverlay, ".claude", "settings.json"), `{"hook":"pack-universal"}`)
		writeTestFile(t, filepath.Join(packOverlay, ".claude", "README.md"), "universal harmless")
		writeTestFile(t, filepath.Join(packOverlay, "per-provider", "codex", ".codex", "hooks.json"), `{"hook":"pack"}`)
		writeTestFile(t, filepath.Join(packOverlay, "per-provider", "codex", ".codex", "README.md"), "codex harmless")
		writeTestFile(t, filepath.Join(agentOverlay, "agent.txt"), "agent harmless")
		writeTestFile(t, filepath.Join(agentOverlay, ".agents", "hooks.json"), `{"hook":"agent"}`)
		writeTestFile(t, filepath.Join(copySource, "copy.txt"), "copy harmless")
		writeTestFile(t, filepath.Join(copySource, ".gemini", "settings.json"), `{"hook":"copy"}`)
		writeTestFile(t, filepath.Join(copySource, ".gc", "runtime-only.txt"), "runtime mirror")

		cfg := forbiddenCodexTestConfig(workDir)
		cfg.PackOverlayDirs = []string{packOverlay}
		cfg.OverlayDir = agentOverlay
		cfg.CopyFiles = []CopyEntry{{Src: copySource}}
		for pass := 1; pass <= 2; pass++ {
			if err := PrepareSessionWorkDirWithWarnings(cfg, nil); err != nil {
				t.Fatalf("PrepareSessionWorkDirWithWarnings pass %d: %v", pass, err)
			}
		}

		for rel, want := range map[string]string{
			"README.md":                           "pack harmless",
			filepath.Join(".claude", "README.md"): "universal harmless",
			filepath.Join(".codex", "README.md"):  "codex harmless",
			"agent.txt":                           "agent harmless",
			"copy.txt":                            "copy harmless",
		} {
			got, err := os.ReadFile(filepath.Join(workDir, rel))
			if err != nil {
				t.Fatalf("read harmless staged file %q: %v", rel, err)
			}
			if string(got) != want {
				t.Errorf("staged %q = %q, want %q", rel, got, want)
			}
		}
		for _, rel := range []string{
			filepath.Join(".claude", "settings.json"),
			filepath.Join(".codex", "hooks.json"),
			filepath.Join(".agents", "hooks.json"),
			filepath.Join(".gemini", "settings.json"),
			filepath.Join(".gc", "runtime-only.txt"),
		} {
			if _, err := os.Lstat(filepath.Join(workDir, rel)); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("forbidden artifact %q exists after staging: %v", rel, err)
			}
		}
	})

	t.Run("preserves exact contamination and fails closed", func(t *testing.T) {
		workDir := filepath.Join(canonicalRuntimeTestDir(t), "worker")
		contaminated := filepath.Join(workDir, ".codex", "hooks.json")
		writeTestFile(t, contaminated, "preserve me")

		err := PrepareSessionWorkDir(forbiddenCodexTestConfig(workDir))
		if err == nil || !strings.Contains(err.Error(), ".codex") {
			t.Fatalf("PrepareSessionWorkDir error = %v, want hook contamination", err)
		}
		got, readErr := os.ReadFile(contaminated)
		if readErr != nil || string(got) != "preserve me" {
			t.Fatalf("contamination was mutated: data=%q err=%v", got, readErr)
		}
	})

	t.Run("rejects symlink component without following it", func(t *testing.T) {
		root := canonicalRuntimeTestDir(t)
		workDir := filepath.Join(root, "worker")
		outside := filepath.Join(root, "outside")
		if err := os.MkdirAll(outside, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(workDir, ".codex")); err != nil {
			t.Fatal(err)
		}

		err := PrepareSessionWorkDir(forbiddenCodexTestConfig(workDir))
		if err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("PrepareSessionWorkDir error = %v, want symlink contamination", err)
		}
		if _, statErr := os.Lstat(filepath.Join(workDir, ".codex")); statErr != nil {
			t.Fatalf("symlink contamination removed: %v", statErr)
		}
	})

	t.Run("rejects hook artifact in cwd ancestor", func(t *testing.T) {
		root := canonicalRuntimeTestDir(t)
		ancestor := filepath.Join(root, "bootstrap")
		workDir := filepath.Join(ancestor, "instances", "worker-7")
		writeTestFile(t, filepath.Join(ancestor, ".codex", "hooks.json"), "ancestor hook")
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			t.Fatal(err)
		}

		err := PrepareSessionWorkDir(forbiddenCodexTestConfig(workDir))
		if err == nil || !strings.Contains(err.Error(), "ancestor") {
			t.Fatalf("PrepareSessionWorkDir error = %v, want ancestor contamination", err)
		}
	})
}

func TestPrepareSessionWorkDirForbidOmitsCompleteDiscoverySurfaces(t *testing.T) {
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
			name:     "opencode arbitrary nested plugin",
			provider: "opencode",
			relPath:  filepath.Join(".opencode", "plugins", "unrelated", "index.ts"),
		},
		{
			name:     "oh my pi arbitrary nested hook",
			provider: "omp",
			relPath:  filepath.Join(".omp", "hooks", "pre", "unrelated.ts"),
		},
		{
			name:     "pi arbitrary nested extension",
			provider: "pi",
			relPath:  filepath.Join(".pi", "extensions", "unrelated", "index.ts"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.provider+" "+tc.name, func(t *testing.T) {
			for _, writer := range []string{"overlay", "copy_files"} {
				t.Run(writer, func(t *testing.T) {
					root := canonicalRuntimeTestDir(t)
					workDir := filepath.Join(root, "external", "worker-7")
					source := filepath.Join(root, writer)
					writeTestFile(t, filepath.Join(source, tc.relPath), "must never stage")
					writeTestFile(t, filepath.Join(source, "harmless.txt"), "safe")

					cfg := forbiddenCodexTestConfig(workDir)
					if writer == "overlay" {
						cfg.OverlayDir = source
					} else {
						cfg.CopyFiles = []CopyEntry{{Src: source}}
					}

					if err := PrepareSessionWorkDirWithWarnings(cfg, nil); err != nil {
						t.Fatalf("PrepareSessionWorkDirWithWarnings: %v", err)
					}
					if _, err := os.Lstat(filepath.Join(workDir, tc.relPath)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("discovery surface %q exists after %s staging: %v", tc.relPath, writer, err)
					}
					got, err := os.ReadFile(filepath.Join(workDir, "harmless.txt"))
					if err != nil || string(got) != "safe" {
						t.Fatalf("harmless sibling after %s staging = %q, err=%v", writer, got, err)
					}
				})
			}
		})
	}
}

func TestPrepareSessionWorkDirForbidIncompleteProviderFailsBeforeStaging(t *testing.T) {
	for _, provider := range []string{"", "antigravity", "claude", "copilot", "cursor", "gemini", "kimi", "kiro", "mimocode", "omp", "opencode", "pi", "groq", "cerebras", "custom-uninventoried-provider"} {
		t.Run(provider, func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			workDir := filepath.Join(root, "external", "worker-7")
			overlaySource := filepath.Join(root, "overlay")
			copySource := filepath.Join(root, "copy")
			writeTestFile(t, filepath.Join(overlaySource, "overlay-marker"), "must not stage")
			writeTestFile(t, filepath.Join(copySource, "copy-marker"), "must not stage")

			cfg := forbiddenCodexTestConfig(workDir)
			cfg.ProviderName = provider
			cfg.OverlayDir = overlaySource
			cfg.CopyFiles = []CopyEntry{{Src: copySource}}
			err := PrepareSessionWorkDirWithWarnings(cfg, nil)
			if err == nil || !strings.Contains(err.Error(), "no complete project discovery inventory") {
				t.Fatalf("PrepareSessionWorkDirWithWarnings error = %v, want incomplete provider inventory", err)
			}
			for _, rel := range []string{"overlay-marker", "copy-marker"} {
				if _, statErr := os.Lstat(filepath.Join(workDir, rel)); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("staging side effect %q exists before provider inventory rejection: %v", rel, statErr)
				}
			}
		})
	}
}

func TestPrepareSessionWorkDirForbidRejectsUnattestedCodexCommandBeforeStaging(t *testing.T) {
	tests := []struct {
		name    string
		command string
	}{
		{name: "empty command", command: ""},
		{name: "wrong executable", command: "codex-raw --disable hooks"},
		{name: "wrapper command", command: "sh -c 'exec codex'"},
		{name: "environment wrapper", command: "env codex --disable hooks"},
		{name: "shell operator", command: "codex --disable hooks; true"},
		{name: "shell expansion", command: "codex --disable hooks $EXTRA"},
		{name: "quoted token", command: "codex --disable 'hooks'"},
		{name: "noncanonical whitespace", command: "codex  --disable hooks"},
		{name: "missing hook defense", command: "codex --model gpt-5.6-sol"},
		{name: "short cwd override", command: "codex -C /contaminated/project"},
		{name: "long cwd override", command: "codex --cd=/contaminated/project"},
		{name: "inline hook config", command: "codex -c 'hooks={SessionStart=[]}'"},
		{name: "long arbitrary config", command: "codex --disable hooks --config hooks.enabled=true"},
		{name: "profile", command: "codex --disable hooks --profile unsafe"},
		{name: "short profile", command: "codex --disable hooks -p unsafe"},
		{name: "enable feature", command: "codex --disable hooks --enable hooks"},
		{name: "add dir subcommand", command: "codex --disable hooks add-dir /project"},
		{name: "add dir flag", command: "codex --disable hooks --add-dir /project"},
		{name: "unknown flag", command: "codex --disable hooks --future-unsafe"},
		{name: "wrong disabled feature", command: "codex --disable shell_snapshot"},
		{name: "duplicate hook defense", command: "codex --disable hooks --disable hooks"},
		{name: "resume missing key", command: "codex --disable hooks resume"},
		{name: "resume duplicate key", command: "codex --disable hooks resume first second"},
		{name: "interactive positional", command: "codex --disable hooks unexpected"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			workDir := filepath.Join(root, "external", "worker-7")
			overlaySource := filepath.Join(root, "overlay")
			writeTestFile(t, filepath.Join(overlaySource, "staging-marker"), "must not stage")

			cfg := forbiddenCodexTestConfig(workDir)
			cfg.Command = tc.command
			cfg.OverlayDir = overlaySource
			err := PrepareSessionWorkDir(cfg)
			if err == nil || !strings.Contains(err.Error(), "launch command") {
				t.Fatalf("PrepareSessionWorkDir error = %v, want unattested launch command rejection", err)
			}
			if _, statErr := os.Lstat(filepath.Join(workDir, "staging-marker")); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("staging-marker exists before command rejection: %v", statErr)
			}
		})
	}
}

func TestPrepareSessionWorkDirForbidRejectsPostPreflightLaunchInjectionBeforeStaging(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "prompt flag cwd override", mutate: func(cfg *Config) { cfg.PromptFlag = "--cd" }},
		{name: "whitespace prompt flag", mutate: func(cfg *Config) { cfg.PromptFlag = " " }},
		{name: "unquoted prompt", mutate: func(cfg *Config) { cfg.PromptSuffix = "do work" }},
		{name: "multiple prompt arguments", mutate: func(cfg *Config) { cfg.PromptSuffix = "'do work' '--cd=/project'" }},
		{name: "prompt shell operator", mutate: func(cfg *Config) { cfg.PromptSuffix = "'do work'; touch /tmp/marker" }},
		{name: "unclosed prompt quote", mutate: func(cfg *Config) { cfg.PromptSuffix = "'do work" }},
		{name: "missing controller PATH", mutate: func(cfg *Config) { delete(cfg.Env, "PATH") }},
		{name: "provider PATH override", mutate: func(cfg *Config) { cfg.Env["PATH"] = filepath.Join(filepath.Dir(cfg.WorkDir), "wrapper-bin") }},
		{name: "GC_BIN PATH authority override", mutate: func(cfg *Config) {
			cfg.Env["GC_BIN"] = filepath.Join(filepath.Dir(cfg.WorkDir), "wrapper-bin", "gc")
			cfg.Env["PATH"] = filepath.Dir(cfg.Env["GC_BIN"]) + string(os.PathListSeparator) + os.Getenv("PATH")
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			workDir := filepath.Join(root, "instance", "work")
			overlaySource := filepath.Join(root, "overlay")
			writeTestFile(t, filepath.Join(overlaySource, "staging-marker"), "must not stage")
			cfg := forbiddenCodexTestConfig(workDir)
			cfg.OverlayDir = overlaySource
			tc.mutate(&cfg)

			err := PrepareSessionWorkDir(cfg)
			if err == nil || !strings.Contains(err.Error(), "launch command") {
				t.Fatalf("PrepareSessionWorkDir error = %v, want post-preflight launch injection rejection", err)
			}
			if _, statErr := os.Lstat(filepath.Join(workDir, "staging-marker")); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("staging-marker exists before launch rejection: %v", statErr)
			}
		})
	}
}

func TestPrepareSessionWorkDirForbidRejectsLaunchEnvironmentInjectionBeforeStaging(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config, string)
	}{
		{name: "missing BASH_ENV pin", mutate: func(cfg *Config, _ string) { delete(cfg.Env, "BASH_ENV") }},
		{name: "BASH_ENV project script", mutate: func(cfg *Config, root string) { cfg.Env["BASH_ENV"] = filepath.Join(root, "project", "bootstrap.sh") }},
		{name: "ZDOTDIR project root", mutate: func(cfg *Config, root string) { cfg.Env["ZDOTDIR"] = filepath.Join(root, "project") }},
		{name: "dynamic loader injection", mutate: func(cfg *Config, root string) { cfg.Env["DYLD_INSERT_LIBRARIES"] = filepath.Join(root, "inject.dylib") }},
		{name: "Node preload", mutate: func(cfg *Config, root string) {
			cfg.Env["NODE_OPTIONS"] = "--require=" + filepath.Join(root, "inject.js")
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			workDir := filepath.Join(root, "instance", "work")
			overlaySource := filepath.Join(root, "overlay")
			writeTestFile(t, filepath.Join(overlaySource, "staging-marker"), "must not stage")
			cfg := forbiddenCodexTestConfig(workDir)
			cfg.OverlayDir = overlaySource
			tc.mutate(&cfg, root)

			err := PrepareSessionWorkDir(cfg)
			if err == nil || !strings.Contains(err.Error(), "launch environment") {
				t.Fatalf("PrepareSessionWorkDir error = %v, want launch-environment injection rejection", err)
			}
			if _, statErr := os.Lstat(filepath.Join(workDir, "staging-marker")); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("staging-marker exists before launch-environment rejection: %v", statErr)
			}
		})
	}
}

func TestPrepareSessionWorkDirForbidRejectsMalformedEnvironmentKeysBeforeStaging(t *testing.T) {
	for _, key := range []string{"", "-r", "9START", "BAD-NAME", "BAD=NAME", "BAD NAME", "BAD;touch-marker", "ÉNV"} {
		t.Run(fmt.Sprintf("key-%q", key), func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			workDir := filepath.Join(root, "instance", "work")
			overlaySource := filepath.Join(root, "overlay")
			writeTestFile(t, filepath.Join(overlaySource, "staging-marker"), "must not stage")
			cfg := forbiddenCodexTestConfig(workDir)
			cfg.OverlayDir = overlaySource
			cfg.Env[key] = ""

			err := PrepareSessionWorkDir(cfg)
			if err == nil || !strings.Contains(err.Error(), "environment key") {
				t.Fatalf("PrepareSessionWorkDir error = %v, want malformed environment-key rejection", err)
			}
			if _, statErr := os.Lstat(filepath.Join(workDir, "staging-marker")); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("staging-marker exists before environment-key rejection: %v", statErr)
			}
		})
	}
}

func TestValidateEnvironmentKeysAcceptsPOSIXNames(t *testing.T) {
	env := map[string]string{
		"A":        "",
		"A1":       "value",
		"_":        "",
		"_lower_9": "value",
	}
	if err := ValidateEnvironmentKeys(env); err != nil {
		t.Fatalf("ValidateEnvironmentKeys: %v", err)
	}
}

func TestPrepareSessionWorkDirForbidAcceptsAuthoritativeGCBinPATHPrefix(t *testing.T) {
	root := canonicalRuntimeTestDir(t)
	workDir := filepath.Join(root, "instance", "work")
	gcBin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := forbiddenCodexTestConfig(workDir)
	cfg.Env["GC_BIN"] = gcBin
	trustedEnv := map[string]string{"PATH": os.Getenv("PATH")}
	processenv.PrependGCBinDirToPATH(trustedEnv, gcBin)
	cfg.Env["PATH"] = trustedEnv["PATH"]

	if err := PrepareSessionWorkDir(cfg); err != nil {
		t.Fatalf("PrepareSessionWorkDir with authoritative GC_BIN path prefix: %v", err)
	}
}

func TestPrepareSessionWorkDirForbidAcceptsCanonicalPromptSuffix(t *testing.T) {
	for _, prompt := range []string{
		"short prompt with ' apostrophe",
		strings.Repeat("long prompt ", 200),
	} {
		t.Run(fmt.Sprintf("length-%d", len(prompt)), func(t *testing.T) {
			workDir := filepath.Join(canonicalRuntimeTestDir(t), "instance", "work")
			cfg := forbiddenCodexTestConfig(workDir)
			cfg.PromptSuffix = shellquote.Quote(prompt)
			if err := PrepareSessionWorkDir(cfg); err != nil {
				t.Fatalf("PrepareSessionWorkDir with canonical prompt suffix: %v", err)
			}
		})
	}
}

func TestFinalizeProjectHookIsolatedConfigBuildsAuthoritativeInteractiveAndResumeLaunches(t *testing.T) {
	commands := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "interactive",
			in:   "codex --dangerously-bypass-approvals-and-sandbox --model gpt-5.6-sol -c model_reasoning_effort=ultra",
			want: "codex --disable hooks --dangerously-bypass-approvals-and-sandbox --model gpt-5.6-sol -c model_reasoning_effort=ultra",
		},
		{
			name: "resume",
			in:   "codex resume session-123 --ask-for-approval never --sandbox read-only",
			want: "codex --disable hooks resume session-123 --ask-for-approval never --sandbox read-only",
		},
		{
			name: "already defended",
			in:   "codex --disable hooks --model o3",
			want: "codex --disable hooks --model o3",
		},
	}

	for _, tc := range commands {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			workDir := filepath.Join(root, "instance-7", "work")
			originalEnv := map[string]string{"HOME": "/ambient/home", "CODEX_HOME": "/ambient/codex"}
			cfg := Config{
				WorkDir:               workDir,
				Command:               tc.in,
				ProviderName:          "codex",
				Env:                   originalEnv,
				ProjectHooksForbidden: true,
			}

			got, err := FinalizeProjectHookIsolatedConfig(cfg)
			if err != nil {
				t.Fatalf("FinalizeProjectHookIsolatedConfig: %v", err)
			}
			if got.Command != tc.want {
				t.Fatalf("Command = %q, want %q", got.Command, tc.want)
			}
			wantHome := filepath.Join(filepath.Dir(workDir), "."+filepath.Base(workDir)+"-home")
			if got.Env["HOME"] != wantHome || got.Env["CODEX_HOME"] != filepath.Join(wantHome, ".codex") {
				t.Fatalf("isolated roots HOME=%q CODEX_HOME=%q, want %q and HOME/.codex", got.Env["HOME"], got.Env["CODEX_HOME"], wantHome)
			}
			executable, execErr := os.Executable()
			if execErr != nil {
				t.Fatal(execErr)
			}
			if got.Env["GC_BIN"] != executable || got.Env["PATH"] == "" {
				t.Fatalf("controller env GC_BIN=%q PATH=%q", got.Env["GC_BIN"], got.Env["PATH"])
			}
			for _, key := range ProjectHookIsolationWithheldEnvKeys() {
				value, ok := got.Env[key]
				if !ok || value != "" {
					t.Fatalf("withheld env %s = %q present=%v, want explicit empty", key, value, ok)
				}
			}
			if originalEnv["HOME"] != "/ambient/home" || len(originalEnv) != 2 {
				t.Fatalf("Finalize mutated caller Env: %v", originalEnv)
			}
			second, err := FinalizeProjectHookIsolatedConfig(got)
			if err != nil || second.Command != got.Command || second.Env["HOME"] != got.Env["HOME"] {
				t.Fatalf("second finalization = command %q HOME %q err=%v, want idempotent", second.Command, second.Env["HOME"], err)
			}
		})
	}
}

func TestFinalizeProjectHookIsolatedConfigRejectsUnsafeAuthoredOverrides(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config, string)
	}{
		{name: "wrapper command", mutate: func(cfg *Config, _ string) { cfg.Command = "sh -c 'exec codex'" }},
		{name: "arbitrary config", mutate: func(cfg *Config, _ string) { cfg.Command = "codex -c hooks.enabled=true" }},
		{name: "PATH override", mutate: func(cfg *Config, root string) { cfg.Env["PATH"] = filepath.Join(root, "bin") }},
		{name: "GC_BIN override", mutate: func(cfg *Config, root string) { cfg.Env["GC_BIN"] = filepath.Join(root, "gc") }},
		{name: "shell injection env", mutate: func(cfg *Config, root string) { cfg.Env["BASH_ENV"] = filepath.Join(root, "inject.sh") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			cfg := Config{
				WorkDir:               filepath.Join(root, "instance-7", "work"),
				Command:               "codex --model gpt-5.6-sol",
				ProviderName:          "codex",
				Env:                   map[string]string{},
				ProjectHooksForbidden: true,
			}
			tc.mutate(&cfg, root)
			if _, err := FinalizeProjectHookIsolatedConfig(cfg); err == nil {
				t.Fatal("FinalizeProjectHookIsolatedConfig succeeded, want unsafe override rejection")
			}
		})
	}
}

func TestFinalizeProjectHookIsolatedConfigLeavesInheritedPolicyUnchanged(t *testing.T) {
	env := map[string]string{"HOME": "/ambient/home"}
	cfg := Config{Command: "custom-provider --anything", Env: env}
	got, err := FinalizeProjectHookIsolatedConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Command != cfg.Command || got.Env["HOME"] != env["HOME"] {
		t.Fatalf("inherited config changed: got=%+v want=%+v", got, cfg)
	}
}

func TestMaterializeProjectHookIsolatedConfigRootsCreatesSecureFreshDirectories(t *testing.T) {
	root := canonicalRuntimeTestDir(t)
	workDir := filepath.Join(root, "instance-7", "work")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatal(err)
	}
	finalized, err := FinalizeProjectHookIsolatedConfig(Config{
		WorkDir:               workDir,
		Command:               "codex --model gpt-5.6-sol",
		ProviderName:          "codex",
		ProjectHooksForbidden: true,
	})
	if err != nil {
		t.Fatalf("FinalizeProjectHookIsolatedConfig: %v", err)
	}

	if err := MaterializeProjectHookIsolatedConfigRoots(finalized); err != nil {
		t.Fatalf("MaterializeProjectHookIsolatedConfigRoots: %v", err)
	}
	for _, key := range []string{"HOME", "CODEX_HOME"} {
		info, err := os.Lstat(finalized.Env[key])
		if err != nil {
			t.Fatalf("lstat %s: %v", key, err)
		}
		if !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("%s mode = %v, want secure directory 0700", key, info.Mode())
		}
	}
	if err := MaterializeProjectHookIsolatedConfigRoots(finalized); err != nil {
		t.Fatalf("idempotent MaterializeProjectHookIsolatedConfigRoots: %v", err)
	}
}

func TestMaterializeProjectHookIsolatedConfigRootsRejectsUnsafePreexistingRoots(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, Config, string)
	}{
		{
			name: "symlink HOME",
			mutate: func(t *testing.T, cfg Config, root string) {
				t.Helper()
				external := filepath.Join(root, "external-home")
				if err := os.MkdirAll(external, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(external, cfg.Env["HOME"]); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "file HOME",
			mutate: func(t *testing.T, cfg Config, _ string) {
				t.Helper()
				writeTestFile(t, cfg.Env["HOME"], "not a directory")
			},
		},
		{
			name: "insecure HOME mode",
			mutate: func(t *testing.T, cfg Config, _ string) {
				t.Helper()
				if err := os.MkdirAll(cfg.Env["HOME"], 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(cfg.Env["HOME"], 0o755); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "file CODEX_HOME",
			mutate: func(t *testing.T, cfg Config, _ string) {
				t.Helper()
				if err := os.MkdirAll(cfg.Env["HOME"], 0o700); err != nil {
					t.Fatal(err)
				}
				writeTestFile(t, cfg.Env["CODEX_HOME"], "not a directory")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			workDir := filepath.Join(root, "instance-7", "work")
			if err := os.MkdirAll(workDir, 0o755); err != nil {
				t.Fatal(err)
			}
			finalized, err := FinalizeProjectHookIsolatedConfig(Config{
				WorkDir:               workDir,
				Command:               "codex --model gpt-5.6-sol",
				ProviderName:          "codex",
				ProjectHooksForbidden: true,
			})
			if err != nil {
				t.Fatalf("FinalizeProjectHookIsolatedConfig: %v", err)
			}
			tc.mutate(t, finalized, root)

			if err := MaterializeProjectHookIsolatedConfigRoots(finalized); err == nil {
				t.Fatal("MaterializeProjectHookIsolatedConfigRoots succeeded, want unsafe root rejection")
			}
		})
	}
}

func TestPrepareSessionWorkDirForbidAcceptsCanonicalCodexCommandGrammars(t *testing.T) {
	commands := []string{
		"codex --disable hooks",
		"codex --disable hooks --dangerously-bypass-approvals-and-sandbox --model gpt-5.6-sol -c model_reasoning_effort=xhigh",
		"codex --ask-for-approval untrusted --sandbox read-only --disable hooks -m o3 --config model_reasoning_effort=medium",
		"codex --full-auto --sandbox network-off --disable hooks -c model_reasoning_effort=ultra",
		"codex --disable hooks resume session-123",
		"codex --model gpt-5.6-terra --disable hooks resume -c model_reasoning_effort=high session-123",
		"codex resume --ask-for-approval never session-123 --disable hooks --sandbox read-only",
	}

	for _, command := range commands {
		t.Run(command, func(t *testing.T) {
			workDir := filepath.Join(canonicalRuntimeTestDir(t), "instance", "work")
			cfg := forbiddenCodexTestConfig(workDir)
			cfg.Command = command
			if err := PrepareSessionWorkDir(cfg); err != nil {
				t.Fatalf("PrepareSessionWorkDir(%q): %v", command, err)
			}
		})
	}
}

func TestPrepareSessionWorkDirForbidRejectsUnisolatedCodexConfigRootsBeforeStaging(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config, string)
	}{
		{name: "missing HOME", mutate: func(cfg *Config, _ string) { delete(cfg.Env, "HOME") }},
		{name: "missing CODEX_HOME", mutate: func(cfg *Config, _ string) { delete(cfg.Env, "CODEX_HOME") }},
		{name: "HOME equals workdir", mutate: func(cfg *Config, _ string) {
			cfg.Env["HOME"] = cfg.WorkDir
			cfg.Env["CODEX_HOME"] = filepath.Join(cfg.WorkDir, ".codex")
		}},
		{name: "HOME nested under workdir", mutate: func(cfg *Config, _ string) {
			cfg.Env["HOME"] = filepath.Join(cfg.WorkDir, "home")
			cfg.Env["CODEX_HOME"] = filepath.Join(cfg.Env["HOME"], ".codex")
		}},
		{name: "HOME unrelated", mutate: func(cfg *Config, root string) {
			cfg.Env["HOME"] = filepath.Join(root, "other", "home")
			cfg.Env["CODEX_HOME"] = filepath.Join(cfg.Env["HOME"], ".codex")
		}},
		{name: "CODEX_HOME redirected", mutate: func(cfg *Config, root string) { cfg.Env["CODEX_HOME"] = filepath.Join(root, "redirected-codex") }},
		{name: "noncanonical HOME", mutate: func(cfg *Config, _ string) {
			cfg.Env["HOME"] = filepath.Join(cfg.Env["HOME"], "..")
			cfg.Env["CODEX_HOME"] = filepath.Join(cfg.Env["HOME"], ".codex")
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			workDir := filepath.Join(root, "instance", "work")
			overlaySource := filepath.Join(root, "overlay")
			writeTestFile(t, filepath.Join(overlaySource, "staging-marker"), "must not stage")
			cfg := forbiddenCodexTestConfig(workDir)
			cfg.OverlayDir = overlaySource
			tc.mutate(&cfg, root)

			err := PrepareSessionWorkDir(cfg)
			if err == nil || !strings.Contains(err.Error(), "config root") {
				t.Fatalf("PrepareSessionWorkDir error = %v, want isolated config root rejection", err)
			}
			if _, statErr := os.Lstat(filepath.Join(workDir, "staging-marker")); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("staging-marker exists before config-root rejection: %v", statErr)
			}
		})
	}

	for _, name := range []string{"config.toml", "hooks.json"} {
		t.Run("CODEX_HOME "+name+" contamination is preserved", func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			workDir := filepath.Join(root, "instance", "work")
			cfg := forbiddenCodexTestConfig(workDir)
			configPath := filepath.Join(cfg.Env["CODEX_HOME"], name)
			writeTestFile(t, configPath, "preserve me")

			err := PrepareSessionWorkDir(cfg)
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("PrepareSessionWorkDir error = %v, want CODEX_HOME contamination rejection", err)
			}
			got, readErr := os.ReadFile(configPath)
			if readErr != nil || string(got) != "preserve me" {
				t.Fatalf("CODEX_HOME contamination was mutated: data=%q err=%v", got, readErr)
			}
		})
	}

	t.Run("symlinked HOME is rejected", func(t *testing.T) {
		root := canonicalRuntimeTestDir(t)
		instance := filepath.Join(root, "instance")
		workDir := filepath.Join(instance, "work")
		realHome := filepath.Join(root, "real-home")
		if err := os.MkdirAll(instance, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(realHome, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(realHome, filepath.Join(instance, "home")); err != nil {
			t.Fatal(err)
		}
		cfg := forbiddenCodexTestConfig(workDir)

		err := PrepareSessionWorkDir(cfg)
		if err == nil || !strings.Contains(err.Error(), "config root") || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("PrepareSessionWorkDir error = %v, want symlinked config root rejection", err)
		}
	})

	for _, rootEnv := range []string{"HOME", "CODEX_HOME"} {
		t.Run("repository "+rootEnv+" is rejected", func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			workDir := filepath.Join(root, "instance", "work")
			cfg := forbiddenCodexTestConfig(workDir)
			if err := os.MkdirAll(filepath.Join(cfg.Env[rootEnv], ".git"), 0o755); err != nil {
				t.Fatal(err)
			}

			err := PrepareSessionWorkDir(cfg)
			if err == nil || !strings.Contains(err.Error(), "config root") || !strings.Contains(err.Error(), "repository") {
				t.Fatalf("PrepareSessionWorkDir error = %v, want repository config root rejection", err)
			}
		})
	}
}

func TestPrepareSessionWorkDirForbidRejectsLinkedWorktreeBeforeStaging(t *testing.T) {
	for _, rootArtifact := range []string{
		filepath.Join(".codex", "hooks.json"),
		filepath.Join(".codex", "config.toml"),
	} {
		t.Run(rootArtifact, func(t *testing.T) {
			root := canonicalRuntimeTestDir(t)
			rootCheckout := filepath.Join(root, "root-checkout")
			linkedWorktree := filepath.Join(root, "linked-worktree")
			workDir := filepath.Join(linkedWorktree, "sessions", "worker-7")
			copySource := filepath.Join(root, "copy")
			gitDir := filepath.Join(rootCheckout, ".git", "worktrees", "linked-worktree")

			if err := os.MkdirAll(gitDir, 0o755); err != nil {
				t.Fatal(err)
			}
			writeTestFile(t, filepath.Join(rootCheckout, rootArtifact), "root checkout hook declaration")
			writeTestFile(t, filepath.Join(linkedWorktree, ".git"), "gitdir: "+gitDir+"\n")
			writeTestFile(t, filepath.Join(copySource, "copy-marker"), "must not stage")

			cfg := forbiddenCodexTestConfig(workDir)
			cfg.CopyFiles = []CopyEntry{{Src: copySource}}
			err := PrepareSessionWorkDir(cfg)
			if err == nil || !strings.Contains(err.Error(), "repository") {
				t.Fatalf("PrepareSessionWorkDir error = %v, want repository-contained cwd rejection", err)
			}
			if _, statErr := os.Lstat(filepath.Join(workDir, "copy-marker")); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("copy-marker exists before repository rejection: %v", statErr)
			}
		})
	}
}

func writeTestFile(t *testing.T, name, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func canonicalRuntimeTestDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func forbiddenCodexTestConfig(workDir string) Config {
	instanceDir := filepath.Dir(workDir)
	homeDir := filepath.Join(instanceDir, "home")
	cfg := Config{
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
	for _, key := range projectHookIsolationWithheldEnvKeys {
		cfg.Env[key] = ""
	}
	return cfg
}
