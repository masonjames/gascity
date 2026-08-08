package hooks

import (
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestProjectHookArtifactsAreCanonicalAndComplete(t *testing.T) {
	want := map[string][]string{
		"antigravity": {filepath.Join(".agents", "hooks.json")},
		"claude": {
			filepath.Join(".claude", "settings.json"),
			filepath.Join(".gc", "settings.json"),
			filepath.Join("hooks", "claude.json"),
		},
		"codex":    {filepath.Join(".codex", "hooks.json")},
		"copilot":  {filepath.Join(".github", "hooks", "gascity.json")},
		"cursor":   {filepath.Join(".cursor", "hooks.json")},
		"gemini":   {filepath.Join(".gemini", "settings.json")},
		"kimi":     {filepath.Join(".kimi", "config.toml"), filepath.Join(".kimi", "hooks", "gascity-session-start.py")},
		"kiro":     {filepath.Join(".kiro", "agents", "gascity.json")},
		"mimocode": {filepath.Join(".mimocode", "plugin", "gascity.js")},
		"omp":      {filepath.Join(".omp", "hooks", "gc-hook.ts")},
		"opencode": {filepath.Join(".opencode", "plugins", "gascity.js")},
		"pi":       {filepath.Join(".pi", "extensions", "gc-hooks.js")},
	}

	for provider, paths := range want {
		got := ProjectHookArtifactPaths(provider)
		sort.Strings(got)
		sort.Strings(paths)
		if !reflect.DeepEqual(got, paths) {
			t.Errorf("ProjectHookArtifactPaths(%q) = %v, want %v", provider, got, paths)
		}
		for _, rel := range paths {
			if !IsProjectHookArtifact(rel) {
				t.Errorf("IsProjectHookArtifact(%q) = false", rel)
			}
			if !IsProjectHookArtifactForProvider(provider, rel) {
				t.Errorf("IsProjectHookArtifactForProvider(%q, %q) = false", provider, rel)
			}
		}
	}

	// Providers whose hook transport is OpenCode still resolve to the same
	// concrete project artifact without duplicating the canonical registry.
	for _, provider := range []string{"groq", "cerebras"} {
		got := ProjectHookArtifactPaths(provider)
		wantPaths := []string{filepath.Join(".opencode", "plugins", "gascity.js")}
		if !reflect.DeepEqual(got, wantPaths) {
			t.Errorf("ProjectHookArtifactPaths(%q) = %v, want %v", provider, got, wantPaths)
		}
	}

	for _, harmless := range []string{
		"AGENTS.md",
		filepath.Join(".github", "copilot-instructions.md"),
		filepath.Join("docs", "hooks.json"),
		filepath.Join(".codex", "README.md"),
	} {
		if IsProjectHookArtifact(harmless) {
			t.Errorf("harmless path %q classified as a project hook artifact", harmless)
		}
	}

	for _, unsafe := range []string{"", ".", "../.codex/hooks.json", "/tmp/.codex/hooks.json"} {
		if IsProjectHookArtifact(unsafe) {
			t.Errorf("unsafe/non-relative path %q classified as a project hook artifact", unsafe)
		}
	}
}

func TestProjectHookDiscoverySurfacesMatchExactFilesAndExecutableSubtrees(t *testing.T) {
	tests := []struct {
		provider string
		relPath  string
		want     bool
	}{
		{provider: "antigravity", relPath: filepath.Join(".agents", "plugins", "unrelated", "hooks.json"), want: true},
		{provider: "antigravity", relPath: filepath.Join(".agents", "plugin", "unrelated", "hooks.json"), want: false},
		{provider: "claude", relPath: filepath.Join(".claude", "settings.local.json"), want: true},
		{provider: "claude", relPath: filepath.Join(".claude", "settings.local.json.bak"), want: false},
		{provider: "claude", relPath: ".mcp.json", want: true},
		{provider: "claude", relPath: ".mcp.json.bak", want: false},
		{provider: "codex", relPath: filepath.Join(".codex", "config.toml"), want: true},
		{provider: "copilot", relPath: filepath.Join(".github", "hooks", "unrelated.json"), want: true},
		{provider: "copilot", relPath: filepath.Join(".github", "copilot", "settings.json"), want: true},
		{provider: "copilot", relPath: filepath.Join(".github", "copilot", "settings.local.json"), want: true},
		{provider: "copilot", relPath: filepath.Join(".claude", "settings.json"), want: true},
		{provider: "copilot", relPath: filepath.Join(".claude", "settings.local.json"), want: true},
		{provider: "copilot", relPath: filepath.Join(".github", "hooks-old", "unrelated.json"), want: false},
		{provider: "kiro", relPath: filepath.Join(".kiro", "agents", "nested", "unrelated.md"), want: true},
		{provider: "kiro", relPath: filepath.Join(".kiro", "hooks", "nested", "unrelated.json"), want: true},
		{provider: "kiro", relPath: filepath.Join(".kiro", "hook", "unrelated.json"), want: false},
		{provider: "opencode", relPath: filepath.Join(".opencode", "plugins"), want: true},
		{provider: "opencode", relPath: filepath.Join(".opencode", "plugins", "unrelated.ts"), want: true},
		{provider: "opencode", relPath: filepath.Join(".opencode", "plugins", "pkg", "index.js"), want: true},
		{provider: "opencode", relPath: filepath.Join(".opencode", "plugins-old", "unrelated.ts"), want: false},
		{provider: "opencode", relPath: "opencode.json", want: true},
		{provider: "opencode", relPath: filepath.Join(".opencode", "opencode.jsonc"), want: true},
		{provider: "omp", relPath: filepath.Join(".omp", "hooks", "pre", "unrelated.ts"), want: true},
		{provider: "omp", relPath: filepath.Join(".omp", "hook", "unrelated.ts"), want: false},
		{provider: "pi", relPath: filepath.Join(".pi", "extensions"), want: true},
		{provider: "pi", relPath: filepath.Join(".pi", "extensions", "unrelated.ts"), want: true},
		{provider: "pi", relPath: filepath.Join(".pi", "extensions", "pkg", "index.ts"), want: true},
		{provider: "pi", relPath: filepath.Join(".pi", "extension", "unrelated.ts"), want: false},
		{provider: "pi", relPath: filepath.Join(".pi", "settings.json"), want: true},
	}

	for _, tc := range tests {
		if got := IsProjectHookArtifactForProvider(tc.provider, tc.relPath); got != tc.want {
			t.Errorf("IsProjectHookArtifactForProvider(%q, %q) = %t, want %t", tc.provider, tc.relPath, got, tc.want)
		}
		if got := IsProjectHookArtifact(tc.relPath); got != tc.want {
			t.Errorf("IsProjectHookArtifact(%q) = %t, want %t", tc.relPath, got, tc.want)
		}
	}
}

func TestProjectHookDiscoveryInventoryFailsClosedForUnknownProvider(t *testing.T) {
	for _, provider := range []string{"codex"} {
		if !HasCompleteProjectHookDiscoveryInventory(provider) {
			t.Errorf("HasCompleteProjectHookDiscoveryInventory(%q) = false", provider)
		}
	}
	for _, provider := range []string{"", "antigravity", "claude", "copilot", "cursor", "gemini", "kimi", "kiro", "mimocode", "omp", "opencode", "pi", "groq", "cerebras", "custom-provider"} {
		if HasCompleteProjectHookDiscoveryInventory(provider) {
			t.Errorf("HasCompleteProjectHookDiscoveryInventory(%q) = true", provider)
		}
	}
}
