package session

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
)

func TestManagerFinalizesProjectHookForbiddenConfigBeforeProviderStart(t *testing.T) {
	workDir := projectHookLaunchTestWorkDir(t)
	provider := &configRootObservingFake{Fake: runtime.NewFake()}
	manager := NewManagerWithOptions(beads.NewMemStore(), provider)

	info, err := manager.CreateSession(t.Context(), CreateOptions{
		Template: "worker",
		Title:    "isolated worker",
		Command:  "codex --model gpt-5.6-sol -c model_reasoning_effort=ultra",
		WorkDir:  workDir,
		Provider: "codex",
		Hints: runtime.Config{
			ProviderName:          "codex",
			ProjectHooksForbidden: true,
		},
		ExtraMeta: map[string]string{"session_origin": "manual"},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if !provider.observedSecureRoots {
		t.Fatal("provider Start did not observe materialized secure HOME and CODEX_HOME")
	}

	started := provider.LastStartConfig(info.SessionName)
	if started == nil {
		t.Fatalf("provider Start calls = %#v, want one", provider.SnapshotCalls())
	}
	if got, want := started.Command, "codex --disable hooks --model gpt-5.6-sol -c model_reasoning_effort=ultra"; got != want {
		t.Fatalf("start Command = %q, want %q", got, want)
	}
	wantHome := filepath.Join(filepath.Dir(workDir), "."+filepath.Base(workDir)+"-home")
	if got := started.Env["HOME"]; got != wantHome {
		t.Fatalf("start HOME = %q, want %q", got, wantHome)
	}
	if got, want := started.Env["CODEX_HOME"], filepath.Join(wantHome, ".codex"); got != want {
		t.Fatalf("start CODEX_HOME = %q, want %q", got, want)
	}
	for _, key := range runtime.ProjectHookIsolationWithheldEnvKeys() {
		if value, ok := started.Env[key]; !ok || value != "" {
			t.Fatalf("start env %s = %q present=%v, want explicit empty", key, value, ok)
		}
	}
}

func TestManagerRejectsInsecurePreexistingProjectHookConfigRootsBeforeProviderStart(t *testing.T) {
	workDir := projectHookLaunchTestWorkDir(t)
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

	provider := runtime.NewFake()
	manager := NewManagerWithOptions(beads.NewMemStore(), provider)
	_, err := manager.CreateSession(t.Context(), CreateOptions{
		Template: "worker",
		Title:    "isolated worker",
		Command:  "codex --model gpt-5.6-sol",
		WorkDir:  workDir,
		Provider: "codex",
		Hints: runtime.Config{
			ProviderName:          "codex",
			ProjectHooksForbidden: true,
		},
		ExtraMeta: map[string]string{"session_origin": "manual"},
	})
	if err == nil {
		t.Fatal("CreateSession succeeded, want insecure config-root rejection")
	}
	for _, call := range provider.SnapshotCalls() {
		if call.Method == "Start" {
			t.Fatalf("provider Start occurred before insecure root rejection: %#v", call)
		}
	}
}

func TestManagerRejectsUnsafeProjectHookForbiddenConfigBeforeProviderStart(t *testing.T) {
	tests := []struct {
		name    string
		command string
		env     map[string]string
	}{
		{
			name:    "command wrapper",
			command: "sh -c 'exec codex'",
		},
		{
			name:    "shell injection environment",
			command: "codex --model gpt-5.6-sol",
			env:     map[string]string{"BASH_ENV": "/project/inject.sh"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := runtime.NewFake()
			manager := NewManagerWithOptions(beads.NewMemStore(), provider)
			_, err := manager.CreateSession(context.Background(), CreateOptions{
				Template: "worker",
				Title:    "isolated worker",
				Command:  tc.command,
				WorkDir:  projectHookLaunchTestWorkDir(t),
				Provider: "codex",
				Hints: runtime.Config{
					ProviderName:          "codex",
					ProjectHooksForbidden: true,
					Env:                   tc.env,
				},
				ExtraMeta: map[string]string{"session_origin": "manual"},
			})
			if err == nil {
				t.Fatal("CreateSession succeeded, want launch finalization rejection")
			}
			if !strings.Contains(err.Error(), "finalizing") {
				t.Fatalf("CreateSession error = %v, want finalization context", err)
			}
			for _, call := range provider.SnapshotCalls() {
				if call.Method == "Start" {
					t.Fatalf("provider Start occurred before rejection: %#v", call)
				}
			}
		})
	}
}

func TestManagerFinalizesProjectHookForbiddenResumeAndRejectsUnsafeResumeBeforeProviderStart(t *testing.T) {
	t.Run("finalized resume", func(t *testing.T) {
		provider := runtime.NewFake()
		manager := NewManagerWithOptions(beads.NewMemStore(), provider)
		info, err := manager.CreateSession(t.Context(), CreateOptions{
			Template:  "worker",
			Title:     "isolated worker",
			Command:   "codex --model gpt-5.6-sol",
			WorkDir:   projectHookLaunchTestWorkDir(t),
			Provider:  "codex",
			ExtraMeta: map[string]string{"session_origin": "ephemeral"},
			BeadOnly:  true,
		})
		if err != nil {
			t.Fatalf("CreateSession bead only: %v", err)
		}

		err = manager.Start(t.Context(), info.ID, "codex resume session-123 --model gpt-5.6-sol -c model_reasoning_effort=ultra", runtime.Config{
			ProviderName:          "codex",
			ProjectHooksForbidden: true,
		})
		if err != nil {
			t.Fatalf("Start resume: %v", err)
		}
		started := provider.LastStartConfig(info.SessionName)
		if started == nil {
			t.Fatalf("provider Start calls = %#v, want one", provider.SnapshotCalls())
		}
		if got, want := started.Command, "codex --disable hooks resume session-123 --model gpt-5.6-sol -c model_reasoning_effort=ultra"; got != want {
			t.Fatalf("resume Command = %q, want %q", got, want)
		}
	})

	t.Run("unsafe resume", func(t *testing.T) {
		provider := runtime.NewFake()
		manager := NewManagerWithOptions(beads.NewMemStore(), provider)
		info, err := manager.CreateSession(t.Context(), CreateOptions{
			Template:  "worker",
			Title:     "isolated worker",
			Command:   "codex --model gpt-5.6-sol",
			WorkDir:   projectHookLaunchTestWorkDir(t),
			Provider:  "codex",
			ExtraMeta: map[string]string{"session_origin": "ephemeral"},
			BeadOnly:  true,
		})
		if err != nil {
			t.Fatalf("CreateSession bead only: %v", err)
		}

		err = manager.Start(t.Context(), info.ID, "codex resume session-123", runtime.Config{
			ProviderName:          "codex",
			Env:                   map[string]string{"ZDOTDIR": "/project"},
			ProjectHooksForbidden: true,
		})
		if err == nil {
			t.Fatal("Start resume succeeded, want launch finalization rejection")
		}
		if got := provider.CountCalls("Start", info.SessionName); got != 0 {
			t.Fatalf("provider Start calls = %d, want zero", got)
		}
	})
}

func projectHookLaunchTestWorkDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	workDir := filepath.Join(root, "instance-7", "work")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatal(err)
	}
	return workDir
}

type configRootObservingFake struct {
	*runtime.Fake
	observedSecureRoots bool
}

func (p *configRootObservingFake) Start(ctx context.Context, name string, cfg runtime.Config) error {
	for _, key := range []string{"HOME", "CODEX_HOME"} {
		info, err := os.Lstat(cfg.Env[key])
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode().Perm() != 0o700 {
			return fmt.Errorf("%s is not a secure 0700 directory", key)
		}
	}
	p.observedSecureRoots = true
	return p.Fake.Start(ctx, name, cfg)
}
