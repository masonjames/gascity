package hooks

import "github.com/gastownhall/gascity/internal/hookartifact"

// ProjectHookArtifact identifies one provider-controlled project path whose
// presence can cause a provider to execute Gas City hooks from a session cwd.
// RelPath is canonical and relative to the project root.
type ProjectHookArtifact = hookartifact.Artifact

// ProjectHookArtifacts returns a copy of the canonical artifact inventory.
func ProjectHookArtifacts() []ProjectHookArtifact {
	return hookartifact.All()
}

// ProjectHookArtifactPaths returns only canonical Gas City-managed exact
// artifact paths for provider. Providers implemented through OpenCode share
// OpenCode's managed artifact. Discovery-only surfaces remain filter inputs and
// are never returned as staging sources.
func ProjectHookArtifactPaths(provider string) []string {
	return hookartifact.Paths(provider)
}

// IsProjectHookArtifact reports whether relPath is any registered project hook
// artifact. Absolute and parent-traversing paths are never accepted.
func IsProjectHookArtifact(relPath string) bool {
	return hookartifact.Is(relPath)
}

// IsProjectHookArtifactForProvider reports whether relPath is registered for
// provider, including providers whose hook transport resolves to OpenCode.
func IsProjectHookArtifactForProvider(provider, relPath string) bool {
	return hookartifact.IsForProvider(provider, relPath)
}

// HasCompleteProjectHookDiscoveryInventory reports whether forbid can rely on
// a complete default project-discovery inventory for provider.
func HasCompleteProjectHookDiscoveryInventory(provider string) bool {
	return hookartifact.HasCompleteDiscoveryInventory(provider)
}
