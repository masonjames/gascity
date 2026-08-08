package workdir

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

func TestResolveForbiddenProjectHooksRequiresExternalPerInstanceWorkDir(t *testing.T) {
	root := t.TempDir()
	cityPath := filepath.Join(root, "city")
	rigPath := filepath.Join(root, "repository")
	providerRoot := filepath.Join(root, "provider-projects")
	externalRoot := filepath.Join(root, "isolated-sessions")
	for _, path := range []string{cityPath, rigPath, providerRoot, externalRoot} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", path, err)
		}
	}
	rigs := []config.Rig{{Name: "demo", Path: rigPath}}

	t.Run("forbidden policy fails closed without an attested discovery root", func(t *testing.T) {
		configuredWorkDir := filepath.Join(externalRoot, "{{.Agent}}")
		agent := config.Agent{
			Name:         "worker",
			Dir:          "demo",
			ProjectHooks: config.ProjectHooksForbid,
			WorkDir:      configuredWorkDir,
		}
		wantConfigured := filepath.Join(externalRoot, "demo", "worker-7")
		if got, err := ResolveConfiguredWorkDirPath(cityPath, "test-city", "demo/worker-7", agent, rigs); err != nil {
			t.Fatalf("ResolveConfiguredWorkDirPath() error = %v, want nil", err)
		} else if got != wantConfigured {
			t.Fatalf("ResolveConfiguredWorkDirPath() = %q, want %q", got, wantConfigured)
		}
		if _, err := os.Stat(wantConfigured); !os.IsNotExist(err) {
			t.Fatalf("read-only resolver created or attested %q: Stat error = %v, want not-exist", wantConfigured, err)
		}
		_, err := ResolveWorkDirPathStrict(cityPath, "test-city", "demo/worker-7", agent, rigs)
		if err == nil || !strings.Contains(err.Error(), "attested provider discovery root") {
			t.Fatalf("ResolveWorkDirPathStrict() error = %v, want missing attested provider discovery root", err)
		}
		if got := ResolveWorkDirPath(cityPath, "test-city", "demo/worker-7", agent, rigs); got != "" {
			t.Fatalf("ResolveWorkDirPath() = %q, want empty fail-closed result for forbidden policy", got)
		}
	})

	t.Run("accepts canonical external Agent template", func(t *testing.T) {
		aliasRoot := filepath.Join(root, "isolated-alias")
		if err := os.Symlink(externalRoot, aliasRoot); err != nil {
			t.Skipf("symlink setup unavailable: %v", err)
		}
		agent := config.Agent{
			Name:         "worker",
			Dir:          "demo",
			ProjectHooks: config.ProjectHooksForbid,
			WorkDir:      filepath.Join(aliasRoot, "{{.Agent}}"),
		}
		got, err := ResolveWorkDirPathStrict(cityPath, "test-city", "demo/worker-7", agent, rigs, providerRoot)
		if err != nil {
			t.Fatalf("ResolveWorkDirPathStrict() error = %v, want nil", err)
		}
		canonicalExternalRoot, err := filepath.EvalSymlinks(externalRoot)
		if err != nil {
			t.Fatalf("EvalSymlinks(%q): %v", externalRoot, err)
		}
		want := filepath.Join(canonicalExternalRoot, "demo", "worker-7")
		if got != want {
			t.Fatalf("ResolveWorkDirPathStrict() = %q, want canonical %q", got, want)
		}
	})

	t.Run("accepts fixed external cwd for a single-session agent", func(t *testing.T) {
		maxActive := 1
		fixed := filepath.Join(externalRoot, "single-worker")
		agent := config.Agent{
			Name:              "worker",
			ProjectHooks:      config.ProjectHooksForbid,
			MaxActiveSessions: &maxActive,
			WorkDir:           fixed,
		}
		got, err := ResolveWorkDirPathStrict(cityPath, "test-city", "worker", agent, rigs, providerRoot)
		if err != nil {
			t.Fatalf("ResolveWorkDirPathStrict() error = %v, want nil", err)
		}
		want, err := canonicalizeMissingLeafPath(fixed)
		if err != nil {
			t.Fatalf("canonicalizeMissingLeafPath(%q): %v", fixed, err)
		}
		if got != want {
			t.Fatalf("ResolveWorkDirPathStrict() = %q, want %q", got, want)
		}
	})

	tests := []struct {
		name       string
		workDir    string
		prepare    func(t *testing.T) string
		wantErr    string
		extraRoots []string
	}{
		{
			name:    "rejects empty fallback",
			workDir: "",
			wantErr: "explicit work_dir",
		},
		{
			name:    "rejects city containment",
			workDir: filepath.Join(cityPath, "sessions", "{{.Agent}}"),
			wantErr: "city root",
		},
		{
			name:    "rejects rig containment",
			workDir: filepath.Join(rigPath, ".sessions", "{{.Agent}}"),
			wantErr: "rig root",
		},
		{
			name:       "rejects provider discovery containment",
			workDir:    filepath.Join(providerRoot, "{{.Agent}}"),
			wantErr:    "provider discovery root",
			extraRoots: []string{providerRoot},
		},
		{
			name:    "rejects shared multi-slot path",
			workDir: filepath.Join(externalRoot, "shared"),
			wantErr: "does not vary per instance",
		},
		{
			name: "rejects repository containment",
			prepare: func(t *testing.T) string {
				repo := filepath.Join(root, "unregistered-repository")
				if err := os.MkdirAll(filepath.Join(repo, ".git"), 0o755); err != nil {
					t.Fatal(err)
				}
				return filepath.Join(repo, "sessions", "{{.Agent}}")
			},
			wantErr: "repository",
		},
		{
			name: "rejects missing leaf behind symlink escape",
			prepare: func(t *testing.T) string {
				alias := filepath.Join(externalRoot, "city-escape")
				if err := os.Symlink(cityPath, alias); err != nil {
					t.Skipf("symlink setup unavailable: %v", err)
				}
				return filepath.Join(alias, "not-created", "{{.Agent}}")
			},
			wantErr: "city root",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			workDir := tc.workDir
			if tc.prepare != nil {
				workDir = tc.prepare(t)
			}
			agent := config.Agent{
				Name:         "worker",
				Dir:          "demo",
				ProjectHooks: config.ProjectHooksForbid,
				WorkDir:      workDir,
			}
			roots := tc.extraRoots
			if len(roots) == 0 {
				roots = []string{providerRoot}
			}
			_, err := ResolveWorkDirPathStrict(cityPath, "test-city", "demo/worker-7", agent, rigs, roots...)
			if err == nil {
				t.Fatalf("ResolveWorkDirPathStrict() error = nil, want %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want %q", err, tc.wantErr)
			}
		})
	}

	t.Run("rejects different slot names that canonicalize to one path", func(t *testing.T) {
		shared := filepath.Join(root, "one-real-directory")
		if err := os.MkdirAll(shared, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, slot := range []string{"worker-1", "worker-2"} {
			if err := os.Symlink(shared, filepath.Join(externalRoot, slot)); err != nil {
				t.Skipf("symlink setup unavailable: %v", err)
			}
		}
		agent := config.Agent{
			Name:         "worker",
			ProjectHooks: config.ProjectHooksForbid,
			WorkDir:      filepath.Join(externalRoot, "{{.Agent}}"),
		}
		_, err := ResolveWorkDirPathStrict(cityPath, "test-city", "worker-1", agent, nil, providerRoot)
		if err == nil || !strings.Contains(err.Error(), "does not vary per instance") {
			t.Fatalf("ResolveWorkDirPathStrict() error = %v, want canonical multi-slot collision", err)
		}
	})

	t.Run("rejects conditional collisions outside sampled slots", func(t *testing.T) {
		maxActive := 4
		agent := config.Agent{
			Name:              "worker",
			ProjectHooks:      config.ProjectHooksForbid,
			MaxActiveSessions: &maxActive,
			WorkDir: filepath.Join(
				externalRoot,
				`{{if eq .AgentBase "worker-1"}}worker-1{{else if eq .AgentBase "worker-2"}}worker-2{{else}}shared{{end}}`,
			),
		}
		_, err := ResolveWorkDirPathStrict(cityPath, "test-city", "worker-3", agent, nil, providerRoot)
		if err == nil || !strings.Contains(err.Error(), "terminal identity component") {
			t.Fatalf("ResolveWorkDirPathStrict() error = %v, want structural per-instance identity failure", err)
		}
	})
}
