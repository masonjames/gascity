// Package hookartifact owns the provider-neutral inventory of project paths
// that can execute Gas City hooks. It is dependency-free so both the hook
// installer and low-level runtime staging can consume the same data.
package hookartifact

import (
	"path/filepath"
	"strings"
)

// Artifact identifies one provider-controlled project path whose presence can
// cause a provider to execute Gas City hooks from a session cwd.
type Artifact struct {
	Provider string
	RelPath  string
	// Match distinguishes one exact artifact from an executable discovery
	// directory whose entrypoint names belong to the provider or user.
	Match MatchKind
	// Managed marks exact artifacts Gas City installs or projects. Discovery-only
	// exact files and directory roots are fences, never installation inputs.
	Managed bool
}

// MatchKind defines how an artifact path fences provider discovery.
type MatchKind uint8

const (
	// MatchExactPath matches only RelPath itself.
	MatchExactPath MatchKind = iota
	// MatchDirectorySubtree matches RelPath and every path beneath it. It is
	// used for provider directories whose executable entrypoint filenames are
	// user-selected rather than owned by Gas City.
	MatchDirectorySubtree
)

var artifacts = []Artifact{
	managedExact("antigravity", filepath.Join(".agents", "hooks.json")),
	discoverySubtree("antigravity", filepath.Join(".agents", "plugins")),
	managedExact("claude", filepath.Join(".claude", "settings.json")),
	discoveryExact("claude", filepath.Join(".claude", "settings.local.json")),
	discoveryExact("claude", ".mcp.json"),
	managedExact("claude", filepath.Join(".gc", "settings.json")),
	managedExact("claude", filepath.Join("hooks", "claude.json")),
	managedExact("codex", filepath.Join(".codex", "hooks.json")),
	discoveryExact("codex", filepath.Join(".codex", "config.toml")),
	managedExact("copilot", filepath.Join(".github", "hooks", "gascity.json")),
	discoverySubtree("copilot", filepath.Join(".github", "hooks")),
	discoveryExact("copilot", filepath.Join(".github", "copilot", "settings.json")),
	discoveryExact("copilot", filepath.Join(".github", "copilot", "settings.local.json")),
	discoveryExact("copilot", filepath.Join(".claude", "settings.json")),
	discoveryExact("copilot", filepath.Join(".claude", "settings.local.json")),
	managedExact("cursor", filepath.Join(".cursor", "hooks.json")),
	managedExact("gemini", filepath.Join(".gemini", "settings.json")),
	managedExact("kimi", filepath.Join(".kimi", "config.toml")),
	managedExact("kimi", filepath.Join(".kimi", "hooks", "gascity-session-start.py")),
	managedExact("kiro", filepath.Join(".kiro", "agents", "gascity.json")),
	discoverySubtree("kiro", filepath.Join(".kiro", "agents")),
	discoverySubtree("kiro", filepath.Join(".kiro", "hooks")),
	managedExact("mimocode", filepath.Join(".mimocode", "plugin", "gascity.js")),
	managedExact("omp", filepath.Join(".omp", "hooks", "gc-hook.ts")),
	discoverySubtree("omp", filepath.Join(".omp", "hooks")),
	managedExact("opencode", filepath.Join(".opencode", "plugins", "gascity.js")),
	discoverySubtree("opencode", filepath.Join(".opencode", "plugins")),
	discoveryExact("opencode", "opencode.json"),
	discoveryExact("opencode", "opencode.jsonc"),
	discoveryExact("opencode", filepath.Join(".opencode", "opencode.json")),
	discoveryExact("opencode", filepath.Join(".opencode", "opencode.jsonc")),
	managedExact("pi", filepath.Join(".pi", "extensions", "gc-hooks.js")),
	discoverySubtree("pi", filepath.Join(".pi", "extensions")),
	discoveryExact("pi", filepath.Join(".pi", "settings.json")),
}

// completeProviders is deliberately independent of artifacts. A new provider
// must opt into forbid only after its complete default project discovery
// surface has been inventoried; having one Gas City-managed filename is not
// sufficient evidence.
var completeProviders = map[string]struct{}{
	"codex": {},
}

func managedExact(provider, relPath string) Artifact {
	return Artifact{Provider: provider, RelPath: relPath, Match: MatchExactPath, Managed: true}
}

func discoveryExact(provider, relPath string) Artifact {
	return Artifact{Provider: provider, RelPath: relPath, Match: MatchExactPath}
}

func discoverySubtree(provider, relPath string) Artifact {
	return Artifact{Provider: provider, RelPath: relPath, Match: MatchDirectorySubtree}
}

// All returns a copy of the canonical artifact inventory.
func All() []Artifact {
	out := make([]Artifact, len(artifacts))
	copy(out, artifacts)
	return out
}

// Paths returns only canonical Gas City-managed exact artifact paths for
// provider. Discovery-only files and directory-subtree roots are deliberately
// excluded so callers never stage an arbitrary provider plugin directory.
// Providers implemented through OpenCode share OpenCode's managed artifact.
func Paths(provider string) []string {
	provider = family(provider)
	var out []string
	for _, artifact := range artifacts {
		if artifact.Provider == provider && artifact.Managed {
			out = append(out, artifact.RelPath)
		}
	}
	return out
}

// HasCompleteDiscoveryInventory reports whether the provider's default
// project-scoped executable discovery surfaces are fully represented. Unknown
// providers fail closed instead of inheriting an incomplete filename guess.
func HasCompleteDiscoveryInventory(provider string) bool {
	_, ok := completeProviders[family(provider)]
	return ok
}

// Is reports whether relPath is any registered project hook artifact.
func Is(relPath string) bool {
	clean, ok := canonical(relPath)
	if !ok {
		return false
	}
	for _, artifact := range artifacts {
		if artifact.matches(clean) {
			return true
		}
	}
	return false
}

// IsForProvider reports whether relPath is registered for provider.
func IsForProvider(provider, relPath string) bool {
	clean, ok := canonical(relPath)
	if !ok {
		return false
	}
	provider = family(provider)
	for _, artifact := range artifacts {
		if artifact.Provider == provider && artifact.matches(clean) {
			return true
		}
	}
	return false
}

func (a Artifact) matches(clean string) bool {
	if clean == a.RelPath {
		return true
	}
	return a.Match == MatchDirectorySubtree && strings.HasPrefix(clean, a.RelPath+string(filepath.Separator))
}

func canonical(relPath string) (string, bool) {
	relPath = strings.TrimSpace(relPath)
	if relPath == "" || filepath.IsAbs(relPath) {
		return "", false
	}
	clean := filepath.Clean(filepath.FromSlash(relPath))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", false
	}
	return clean, true
}

func family(provider string) string {
	switch strings.TrimSpace(provider) {
	case "groq", "cerebras":
		return "opencode"
	default:
		return strings.TrimSpace(provider)
	}
}
