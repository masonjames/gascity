//go:build integration

package tmux

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/shellquote"
)

var projectHooksIntegrationSocketSequence atomic.Uint64

func TestProjectHooksForbidUsesRealExternalCWDWithoutPreClaimHook(t *testing.T) {
	if !hasTmux() {
		t.Skip("tmux not installed")
	}

	t.Run("ancestor hook blocks before process start", func(t *testing.T) {
		root := canonicalProjectHooksIntegrationDir(t)
		cityDir := filepath.Join(root, "city")
		workDir := filepath.Join(cityDir, "bootstrap", "worker-1")
		if err := os.MkdirAll(filepath.Join(cityDir, ".codex"), 0o755); err != nil {
			t.Fatalf("create ancestor hook directory: %v", err)
		}
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			t.Fatalf("create workdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(cityDir, ".codex", "hooks.json"), []byte(`{"hooks":[]}`), 0o600); err != nil {
			t.Fatalf("write ancestor hook sentinel: %v", err)
		}

		observationPath := filepath.Join(root, "blocked-observation")
		claimMarkerPath := filepath.Join(root, "blocked-claim")
		helperPath := writeProjectHooksIntegrationHelper(t, root)
		provider, socketName := newProjectHooksIntegrationProvider(t)
		const sessionName = "worker-blocked"

		env := projectHooksIntegrationEnv(
			cityDir,
			"bead-blocked",
			"rig:instagramtv",
			observationPath,
			claimMarkerPath,
			socketName,
			"project-hooks-blocked",
		)
		isolateProjectHooksIntegrationCodex(t, env, workDir, helperPath)
		err := provider.Start(context.Background(), sessionName, runtime.Config{
			WorkDir:               workDir,
			Command:               "codex --disable hooks",
			ProviderName:          "codex",
			ProjectHooksForbidden: true,
			Env:                   env,
		})
		if err == nil || !strings.Contains(err.Error(), "cwd ancestor exposes project hook artifact") {
			t.Fatalf("Start error = %v, want ancestor project-hook preflight failure", err)
		}
		assertProjectHooksIntegrationPathAbsent(t, observationPath)
		assertProjectHooksIntegrationPathAbsent(t, claimMarkerPath)
		has, hasErr := provider.Tmux().HasSession(sessionName)
		if hasErr != nil {
			t.Fatalf("HasSession on isolated socket: %v", hasErr)
		}
		if has {
			t.Fatalf("session %q exists: preflight did not block before process creation", sessionName)
		}
	})

	t.Run("isolated cwd records provenance before exact claim", func(t *testing.T) {
		root := canonicalProjectHooksIntegrationDir(t)
		cityDir := filepath.Join(root, "city")
		workDir := filepath.Join(root, "bootstrap", "worker-1")
		if err := os.MkdirAll(cityDir, 0o755); err != nil {
			t.Fatalf("create city dir: %v", err)
		}
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			t.Fatalf("create isolated workdir: %v", err)
		}

		observationPath := filepath.Join(root, "clean-observation")
		claimMarkerPath := filepath.Join(root, "clean-claim")
		helperPath := writeProjectHooksIntegrationHelper(t, root)
		provider, socketName := newProjectHooksIntegrationProvider(t)
		const (
			sessionName     = "worker-clean"
			triggerID       = "bead-exact-claim"
			triggerStoreRef = "rig:instagramtv"
			channel         = "project-hooks-clean"
		)
		if err := os.MkdirAll(filepath.Join(cityDir, ".codex"), 0o755); err != nil {
			t.Fatalf("create city hook directory: %v", err)
		}
		if err := os.WriteFile(filepath.Join(cityDir, ".codex", "hooks.json"), []byte(`{"hooks":[]}`), 0o600); err != nil {
			t.Fatalf("write city hook sentinel: %v", err)
		}

		env := projectHooksIntegrationEnv(
			workDir,
			triggerID,
			triggerStoreRef,
			observationPath,
			claimMarkerPath,
			socketName,
			channel,
		)
		isolateProjectHooksIntegrationCodex(t, env, workDir, helperPath)
		if err := provider.Start(context.Background(), sessionName, runtime.Config{
			WorkDir:               workDir,
			Command:               "codex --disable hooks",
			ProviderName:          "codex",
			ProjectHooksForbidden: true,
			Env:                   env,
		}); err != nil {
			t.Fatalf("Start isolated helper: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := provider.Tmux().runCtx(waitCtx, "wait-for", channel); err != nil {
			t.Fatalf("wait for helper completion on isolated socket %q: %v", socketName, err)
		}

		observation := readProjectHooksIntegrationObservation(t, observationPath)
		if got := observation["cwd"]; got != workDir {
			t.Fatalf("real OS cwd = %q, want %q", got, workDir)
		}
		if got := observation["gc_dir"]; got != workDir {
			t.Fatalf("GC_DIR = %q, want external bootstrap %q", got, workDir)
		}
		if got := observation["trigger_work_bead_id"]; got != triggerID {
			t.Fatalf("GC_TRIGGER_WORK_BEAD_ID = %q, want %q", got, triggerID)
		}
		if got := observation["trigger_work_bead_store_ref"]; got != triggerStoreRef {
			t.Fatalf("GC_TRIGGER_WORK_BEAD_STORE_REF = %q, want %q", got, triggerStoreRef)
		}
		if got := observation["ancestor_hook_sentinel_count"]; got != "0" {
			t.Fatalf("ancestor hook sentinel count = %q, want 0", got)
		}
		marker, err := os.ReadFile(claimMarkerPath)
		if err != nil {
			t.Fatalf("read exact claim marker: %v", err)
		}
		if got, want := strings.TrimSpace(string(marker)), triggerID+"\n"+triggerStoreRef; got != want {
			t.Fatalf("exact claim marker = %q, want trigger bead/store fence %q", got, want)
		}
	})

	t.Run("server-global BASH_ENV is neutralized before parsing shell", func(t *testing.T) {
		if _, err := os.Stat("/bin/bash"); err != nil {
			t.Skip("/bin/bash is required for the shell-injection boundary proof")
		}
		root := canonicalProjectHooksIntegrationDir(t)
		workDir := filepath.Join(root, "bootstrap", "worker-env")
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			t.Fatal(err)
		}
		poisonMarker := filepath.Join(root, "server-global-bash-env-ran")
		poisonScript := filepath.Join(root, "poison-bash-env.sh")
		if err := os.WriteFile(poisonScript, []byte("printf poisoned > "+shellquote.Quote(poisonMarker)+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}

		observationPath := filepath.Join(root, "env-observation")
		claimMarkerPath := filepath.Join(root, "env-claim")
		helperPath := writeProjectHooksIntegrationHelper(t, root)
		provider, socketName := newProjectHooksIntegrationProvider(t)
		if _, err := provider.Tmux().run("new-session", "-d", "-s", "env-poison-holder", "sleep 30"); err != nil {
			t.Fatalf("start isolated tmux holder: %v", err)
		}
		if _, err := provider.Tmux().run("set-option", "-g", "default-shell", "/bin/bash"); err != nil {
			t.Fatalf("set isolated server shell: %v", err)
		}
		if _, err := provider.Tmux().run("set-environment", "-g", "BASH_ENV", poisonScript); err != nil {
			t.Fatalf("poison isolated server BASH_ENV: %v", err)
		}

		const (
			sessionName = "worker-env-neutralized"
			channel     = "project-hooks-env-neutralized"
		)
		env := projectHooksIntegrationEnv(
			workDir,
			"bead-env-neutralized",
			"rig:instagramtv",
			observationPath,
			claimMarkerPath,
			socketName,
			channel,
		)
		isolateProjectHooksIntegrationCodex(t, env, workDir, helperPath)
		if err := provider.Start(t.Context(), sessionName, runtime.Config{
			WorkDir:               workDir,
			Command:               "codex --disable hooks",
			ProviderName:          "codex",
			ProjectHooksForbidden: true,
			Env:                   env,
		}); err != nil {
			t.Fatalf("Start isolated helper: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		if _, err := provider.Tmux().runCtx(waitCtx, "wait-for", channel); err != nil {
			t.Fatalf("wait for isolated helper: %v", err)
		}
		if _, err := os.Lstat(poisonMarker); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("server-global BASH_ENV ran before launch withholding: %v", err)
		}
		if _, err := os.Stat(observationPath); err != nil {
			t.Fatalf("isolated helper did not reach its observation boundary: %v", err)
		}
	})
}

func canonicalProjectHooksIntegrationDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize integration temp dir: %v", err)
	}
	return dir
}

func newProjectHooksIntegrationProvider(t *testing.T) (*Provider, string) {
	t.Helper()
	socketName := fmt.Sprintf(
		"gc-ph-%d-%d",
		os.Getpid(),
		projectHooksIntegrationSocketSequence.Add(1),
	)
	cfg := DefaultConfig()
	cfg.SocketName = socketName
	provider := NewProviderWithConfig(cfg)
	t.Cleanup(func() {
		if err := provider.TeardownServer(); err != nil {
			t.Errorf("teardown isolated tmux socket %q: %v", socketName, err)
		}
	})
	return provider, socketName
}

func projectHooksIntegrationEnv(cityDir, triggerID, triggerStoreRef, observationPath, claimMarkerPath, socketName, channel string) map[string]string {
	return map[string]string{
		"GC_DIR":                         cityDir,
		"GC_TRIGGER_WORK_BEAD_ID":        triggerID,
		"GC_TRIGGER_WORK_BEAD_STORE_REF": triggerStoreRef,
		"GC_TEST_OBSERVATION_PATH":       observationPath,
		"GC_TEST_EXACT_CLAIM_MARKER":     claimMarkerPath,
		"GC_TEST_TMUX_SOCKET":            socketName,
		"GC_TEST_TMUX_WAIT_CHANNEL":      channel,
		"GC_TEST_EXPECTED_TRIGGER_BEAD":  triggerID,
		"GC_TEST_EXPECTED_TRIGGER_STORE": triggerStoreRef,
	}
}

func isolateProjectHooksIntegrationCodex(t *testing.T, env map[string]string, workDir, helperPath string) {
	t.Helper()
	homeDir := filepath.Join(filepath.Dir(workDir), "home")
	env["HOME"] = homeDir
	env["CODEX_HOME"] = filepath.Join(homeDir, ".codex")
	t.Setenv("PATH", filepath.Dir(helperPath)+string(os.PathListSeparator)+os.Getenv("PATH"))
	env["PATH"] = os.Getenv("PATH")
	for _, key := range runtime.ProjectHookIsolationWithheldEnvKeys() {
		env[key] = ""
	}
}

func writeProjectHooksIntegrationHelper(t *testing.T, root string) string {
	t.Helper()
	path := filepath.Join(root, "bin", "codex")
	const script = `#!/bin/sh
set -eu

cwd=$(pwd -P)
probe=$cwd
ancestor_hooks=0
while [ "$probe" != "/" ]; do
	probe=${probe%/*}
	if [ -z "$probe" ]; then
		probe=/
	fi
	if [ -e "$probe/.codex/hooks.json" ]; then
		ancestor_hooks=$((ancestor_hooks + 1))
	fi
done

observation_tmp="${GC_TEST_OBSERVATION_PATH}.tmp"
{
	printf 'cwd=%s\n' "$cwd"
	printf 'gc_dir=%s\n' "$GC_DIR"
	printf 'trigger_work_bead_id=%s\n' "$GC_TRIGGER_WORK_BEAD_ID"
	printf 'trigger_work_bead_store_ref=%s\n' "$GC_TRIGGER_WORK_BEAD_STORE_REF"
	printf 'ancestor_hook_sentinel_count=%s\n' "$ancestor_hooks"
} >"$observation_tmp"
mv -f "$observation_tmp" "$GC_TEST_OBSERVATION_PATH"

if [ "$GC_TRIGGER_WORK_BEAD_ID" != "$GC_TEST_EXPECTED_TRIGGER_BEAD" ]; then
	exit 71
fi
if [ "$GC_TRIGGER_WORK_BEAD_STORE_REF" != "$GC_TEST_EXPECTED_TRIGGER_STORE" ]; then
	exit 72
fi
claim_tmp="${GC_TEST_EXACT_CLAIM_MARKER}.tmp"
printf '%s\n%s\n' "$GC_TRIGGER_WORK_BEAD_ID" "$GC_TRIGGER_WORK_BEAD_STORE_REF" >"$claim_tmp"
mv -f "$claim_tmp" "$GC_TEST_EXACT_CLAIM_MARKER"

exec tmux -L "$GC_TEST_TMUX_SOCKET" wait-for -S "$GC_TEST_TMUX_WAIT_CHANNEL"
`
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create integration helper directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write integration helper: %v", err)
	}
	return path
}

func readProjectHooksIntegrationObservation(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read helper observation: %v", err)
	}
	values := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("malformed helper observation line %q", line)
		}
		if _, exists := values[key]; exists {
			t.Fatalf("duplicate helper observation key %q", key)
		}
		values[key] = value
	}
	wantKeys := []string{"cwd", "gc_dir", "trigger_work_bead_id", "trigger_work_bead_store_ref", "ancestor_hook_sentinel_count"}
	for _, key := range wantKeys {
		if _, ok := values[key]; !ok {
			t.Fatalf("helper observation missing %q: %v", key, values)
		}
	}
	if len(values) != len(wantKeys) {
		t.Fatalf("helper observation has unexpected fields: %v", values)
	}
	if _, err := strconv.Atoi(values["ancestor_hook_sentinel_count"]); err != nil {
		t.Fatalf("ancestor hook sentinel count is not an integer: %q", values["ancestor_hook_sentinel_count"])
	}
	return values
}

func assertProjectHooksIntegrationPathAbsent(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("path %q exists after blocked launch (err=%v)", path, err)
	}
}
