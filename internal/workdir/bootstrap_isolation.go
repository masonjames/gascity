package workdir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"text/template/parse"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/pathutil"
)

type forbiddenRoot struct {
	path string
	kind string
}

// validateForbiddenProjectHooksWorkDir applies the filesystem half of the
// project-hook isolation contract after ResolveWorkDirPathStrict has required
// a locally attested provider discovery root for a forbid-policy agent. Runtime
// providers still receive a lower-layer boolean or provider-native policy so
// they do not import config.
func validateForbiddenProjectHooksWorkDir(
	cityPath string,
	cityName string,
	qualifiedName string,
	a config.Agent,
	rigs []config.Rig,
	resolvedPath string,
	providerDiscoveryRoots []string,
) (string, error) {
	if strings.TrimSpace(a.WorkDir) == "" {
		return "", fmt.Errorf("agent %q: project hook isolation requires an explicit work_dir; inherited city or rig fallback is unsafe", a.QualifiedName())
	}

	roots := make([]forbiddenRoot, 0, 2+len(rigs)+len(providerDiscoveryRoots))
	roots = append(roots, forbiddenRoot{path: cityPath, kind: "city root"})
	for _, rig := range rigs {
		roots = append(roots, forbiddenRoot{path: rig.Path, kind: "rig root"})
	}
	if strings.TrimSpace(a.Dir) != "" {
		roots = append(roots, forbiddenRoot{path: ResolveDirPath(cityPath, a.Dir), kind: "repository root"})
	}
	for _, root := range providerDiscoveryRoots {
		roots = append(roots, forbiddenRoot{path: root, kind: "provider discovery root"})
	}

	canonicalResolved, err := validateForbiddenWorkDirCandidate(resolvedPath, roots)
	if err != nil {
		return "", fmt.Errorf("agent %q: %w", a.QualifiedName(), err)
	}
	if a.SupportsMultipleSessions() {
		identityField, err := forbiddenWorkDirTerminalIdentityField(a.WorkDir)
		if err != nil {
			return "", fmt.Errorf("agent %q: validating terminal identity component: %w", a.QualifiedName(), err)
		}
		if identityField == "" {
			return "", fmt.Errorf("agent %q: work_dir does not vary per instance: multi-session project hook isolation requires a direct {{.Agent}} or {{.AgentBase}} terminal identity component", a.QualifiedName())
		}
		ctx := PathContextForQualifiedName(cityPath, cityName, qualifiedName, a, rigs)
		expectedSuffix := ctx.Agent
		if identityField == "AgentBase" {
			expectedSuffix = ctx.AgentBase
		}
		if !pathEndsWithIdentity(canonicalResolved, expectedSuffix) {
			return "", fmt.Errorf(
				"agent %q: work_dir does not vary per instance: canonical path %q does not preserve terminal identity component %q",
				a.QualifiedName(), canonicalResolved, expectedSuffix,
			)
		}
	}
	if !a.SupportsMultipleSessions() {
		return canonicalResolved, nil
	}

	firstName := a.QualifiedInstanceName(a.Name + "-1")
	secondName := a.QualifiedInstanceName(a.Name + "-2")
	firstPath, err := resolveWorkDirPathStrict(cityPath, cityName, firstName, a, rigs)
	if err != nil {
		return "", fmt.Errorf("agent %q: resolving work_dir for instance %q: %w", a.QualifiedName(), firstName, err)
	}
	secondPath, err := resolveWorkDirPathStrict(cityPath, cityName, secondName, a, rigs)
	if err != nil {
		return "", fmt.Errorf("agent %q: resolving work_dir for instance %q: %w", a.QualifiedName(), secondName, err)
	}
	canonicalFirst, err := validateForbiddenWorkDirCandidate(firstPath, roots)
	if err != nil {
		return "", fmt.Errorf("agent %q: work_dir for instance %q: %w", a.QualifiedName(), firstName, err)
	}
	canonicalSecond, err := validateForbiddenWorkDirCandidate(secondPath, roots)
	if err != nil {
		return "", fmt.Errorf("agent %q: work_dir for instance %q: %w", a.QualifiedName(), secondName, err)
	}
	if strings.TrimSpace(qualifiedName) != "" && qualifiedName != firstName && pathutil.SamePath(canonicalResolved, canonicalFirst) {
		return "", sharedInstanceWorkDirError(a, qualifiedName, firstName, canonicalResolved)
	}
	if strings.TrimSpace(qualifiedName) != "" && qualifiedName != secondName && pathutil.SamePath(canonicalResolved, canonicalSecond) {
		return "", sharedInstanceWorkDirError(a, qualifiedName, secondName, canonicalResolved)
	}
	if pathutil.SamePath(canonicalFirst, canonicalSecond) {
		return "", sharedInstanceWorkDirError(a, firstName, secondName, canonicalFirst)
	}

	return canonicalResolved, nil
}

func forbiddenWorkDirTerminalIdentityField(spec string) (string, error) {
	tmpl, err := template.New("workdir-isolation").Option("missingkey=error").Parse(spec)
	if err != nil {
		return "", err
	}
	if tmpl.Tree == nil || tmpl.Root == nil || len(tmpl.Root.Nodes) == 0 {
		return "", nil
	}
	action, ok := tmpl.Root.Nodes[len(tmpl.Root.Nodes)-1].(*parse.ActionNode)
	if !ok || action.Pipe == nil || len(action.Pipe.Decl) != 0 || len(action.Pipe.Cmds) != 1 {
		return "", nil
	}
	command := action.Pipe.Cmds[0]
	if command == nil || len(command.Args) != 1 {
		return "", nil
	}
	field, ok := command.Args[0].(*parse.FieldNode)
	if !ok || len(field.Ident) != 1 {
		return "", nil
	}
	switch field.Ident[0] {
	case "Agent", "AgentBase":
		return field.Ident[0], nil
	default:
		return "", nil
	}
}

func pathEndsWithIdentity(path, identity string) bool {
	path = filepath.Clean(path)
	identity = filepath.Clean(filepath.FromSlash(strings.TrimSpace(identity)))
	if identity == "." || filepath.IsAbs(identity) {
		return false
	}
	return path == identity || strings.HasSuffix(path, string(filepath.Separator)+identity)
}

func sharedInstanceWorkDirError(a config.Agent, firstName, secondName, path string) error {
	return fmt.Errorf(
		"agent %q: work_dir does not vary per instance: %q and %q both canonicalize to %q",
		a.QualifiedName(), firstName, secondName, path,
	)
}

func validateForbiddenWorkDirCandidate(candidate string, roots []forbiddenRoot) (string, error) {
	canonicalCandidate, err := canonicalizeMissingLeafPath(candidate)
	if err != nil {
		return "", fmt.Errorf("canonicalizing work_dir %q: %w", candidate, err)
	}
	for _, root := range roots {
		if strings.TrimSpace(root.path) == "" {
			continue
		}
		canonicalRoot, err := canonicalizeMissingLeafPath(root.path)
		if err != nil {
			return "", fmt.Errorf("canonicalizing %s %q: %w", root.kind, root.path, err)
		}
		if pathWithinCanonical(canonicalRoot, canonicalCandidate) {
			return "", fmt.Errorf("work_dir %q is inside %s %q", canonicalCandidate, root.kind, canonicalRoot)
		}
	}
	if repositoryRoot, err := enclosingRepositoryRoot(canonicalCandidate); err != nil {
		return "", err
	} else if repositoryRoot != "" {
		return "", fmt.Errorf("work_dir %q is inside repository %q", canonicalCandidate, repositoryRoot)
	}
	return canonicalCandidate, nil
}

// canonicalizeMissingLeafPath resolves symlinks through the deepest existing
// ancestor and then restores the not-yet-created suffix. This is stricter than
// calling filepath.EvalSymlinks on the final path: a missing session leaf must
// not hide an existing ancestor symlink that redirects into a forbidden root.
func canonicalizeMissingLeafPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("path is empty")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}

	current := abs
	var suffix []string
	for {
		_, statErr := os.Lstat(current)
		if statErr == nil {
			resolved, evalErr := filepath.EvalSymlinks(current)
			if evalErr != nil {
				return "", evalErr
			}
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(statErr, os.ErrNotExist) {
			return "", statErr
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", statErr
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}

func pathWithinCanonical(root, candidate string) bool {
	if root == "" || candidate == "" {
		return false
	}
	rel, err := filepath.Rel(root, candidate)
	if err != nil {
		return false
	}
	return rel == "." || !pathutil.IsOutsideDir(rel)
}

func enclosingRepositoryRoot(path string) (string, error) {
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		marker := filepath.Join(current, ".git")
		if _, err := os.Lstat(marker); err == nil {
			return current, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("checking repository marker %q: %w", marker, err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", nil
		}
	}
}
