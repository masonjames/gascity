package beads

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// ErrGuardedAssignmentClaimUnsupported reports that a store cannot atomically
// bind an exact work bead to an explicit actor while checking all assignment
// preconditions in the same transaction.
var ErrGuardedAssignmentClaimUnsupported = errors.New("guarded assignment claim unsupported")

// AssignmentClaimCoLocatedWitness is an exact predicate over a second bead
// that must live in the same store as the work bead. Providers check it in the
// same lock or transaction as the work decision. ExpectedMetadata requires
// key presence plus byte-exact values. AbsentOrEmptyMetadata accepts only a
// missing key or a byte-empty value, allowing callers to fence optional raw
// metadata without teaching the beads layer what that metadata means.
type AssignmentClaimCoLocatedWitness struct {
	ID                    string
	ExpectedStatus        string
	ExpectedType          string
	ExpectedRevision      int64
	RequiredLabels        []string
	ExpectedMetadata      map[string]string
	AbsentOrEmptyMetadata []string
	// ExactLabels and ExactMetadata, when non-nil, require set/map equality
	// rather than merely checking the required subset above. Nil preserves the
	// legacy subset predicate; an explicitly empty value requires no labels or
	// metadata. These fields are role-neutral and let callers bind an atomic
	// cross-bead decision to one complete captured authority row.
	ExactLabels   []string
	ExactMetadata map[string]string
}

// AssignmentClaimRequest is the complete input to one exact guarded
// assignment. ExpectedMetadata fences routing and other caller-observed
// metadata. ForbiddenLabels excludes an open/unassigned row from a new claim;
// it is deliberately transparent to an exact same-owner idempotent post-state
// so a hold added after assignment does not revoke Tier-1/Tier-2 ownership.
// RequireIdempotent makes the operation a read-only atomic revalidation: only
// the exact already-assigned post-state may succeed, and an otherwise eligible
// open bead is returned as a predicate mismatch without being assigned.
// AssignmentMetadata is written in the same atomic mutation as the
// assignee/status transition, so assignment evidence can never lag a
// successful assignment. CoLocatedWitness, when non-nil, requires the exact
// second bead to exist in the same store and match inside the same atomic
// operation; an absent or drifted witness is a predicate mismatch.
//
// A claim is deliberately narrower than a general conditional update:
// ExpectedStatus must be "open" and ExpectedAssignee must be empty. The only
// other successful starting state is the idempotent post-state
// (in_progress, Actor), and that path requires every AssignmentMetadata entry
// to already match exactly.
type AssignmentClaimRequest struct {
	ID                 string
	Actor              string
	ExpectedStatus     string
	ExpectedAssignee   string
	ExpectedMetadata   map[string]string
	ForbiddenLabels    []string
	AssignmentMetadata map[string]string
	CoLocatedWitness   *AssignmentClaimCoLocatedWitness
	RequireIdempotent  bool
}

// GuardedAssignmentClaimer is an optional Store capability for atomically
// assigning one exact bead. A successful call returns the authoritative
// post-write bead and ok=true. A predicate mismatch returns the zero bead,
// false, nil. A missing exact ID returns ErrNotFound. Invalid input,
// unsupported storage, and backend failures return a non-nil error.
type GuardedAssignmentClaimer interface {
	ClaimAssignment(ctx context.Context, req AssignmentClaimRequest) (Bead, bool, error)
}

// GuardedAssignmentClaimerHandleProvider lets wrappers expose a guarded claim
// handle only when their backing store really supports the capability. It
// avoids interface promotion falsely advertising support through a Store-only
// embedded field.
type GuardedAssignmentClaimerHandleProvider interface {
	GuardedAssignmentClaimerHandle() (GuardedAssignmentClaimer, bool)
}

// GuardedAssignmentClaimerFor resolves store's optional guarded-assignment
// capability. Handle providers are consulted first so a wrapper that also has
// a forwarding method can veto capability when its backing is incapable.
func GuardedAssignmentClaimerFor(store Store) (GuardedAssignmentClaimer, bool) {
	if store == nil {
		return nil, false
	}
	if provider, ok := store.(GuardedAssignmentClaimerHandleProvider); ok {
		return provider.GuardedAssignmentClaimerHandle()
	}
	claimer, ok := store.(GuardedAssignmentClaimer)
	return claimer, ok
}

func normalizeAssignmentClaimRequest(req AssignmentClaimRequest) (AssignmentClaimRequest, error) {
	req.ID = strings.TrimSpace(req.ID)
	if req.ID == "" {
		return AssignmentClaimRequest{}, errors.New("guarded assignment claim: empty bead ID")
	}
	req.Actor = strings.TrimSpace(req.Actor)
	if req.Actor == "" {
		return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: empty actor", req.ID)
	}
	req.ExpectedStatus = strings.TrimSpace(req.ExpectedStatus)
	if req.ExpectedStatus != "open" {
		return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: expected status must be %q, got %q", req.ID, "open", req.ExpectedStatus)
	}
	if strings.TrimSpace(req.ExpectedAssignee) != "" {
		return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: expected assignee must be empty", req.ID)
	}
	if len(req.ExpectedMetadata) == 0 {
		return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: expected metadata is required", req.ID)
	}
	if len(req.ForbiddenLabels) == 0 {
		return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: forbidden labels are required", req.ID)
	}
	if len(req.AssignmentMetadata) == 0 {
		return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: assignment metadata is required", req.ID)
	}

	expected := make(map[string]string, len(req.ExpectedMetadata))
	for key, value := range req.ExpectedMetadata {
		key = strings.TrimSpace(key)
		if key == "" {
			return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: expected metadata has an empty key", req.ID)
		}
		if _, exists := expected[key]; exists {
			return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: expected metadata has duplicate normalized key %q", req.ID, key)
		}
		expected[key] = value
	}
	assignment := make(map[string]string, len(req.AssignmentMetadata))
	for key, value := range req.AssignmentMetadata {
		key = strings.TrimSpace(key)
		if key == "" {
			return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: assignment metadata has an empty key", req.ID)
		}
		if _, exists := assignment[key]; exists {
			return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: assignment metadata has duplicate normalized key %q", req.ID, key)
		}
		if expectedValue, ok := expected[key]; ok && expectedValue != value {
			return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: metadata key %q has conflicting expected and assignment values", req.ID, key)
		}
		assignment[key] = value
	}
	forbidden := make([]string, 0, len(req.ForbiddenLabels))
	seen := make(map[string]struct{}, len(req.ForbiddenLabels))
	for _, label := range req.ForbiddenLabels {
		label = strings.TrimSpace(label)
		if label == "" {
			return AssignmentClaimRequest{}, fmt.Errorf("guarded assignment claim on %q: forbidden labels contain an empty value", req.ID)
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		forbidden = append(forbidden, label)
	}
	req.ExpectedAssignee = ""
	req.ExpectedMetadata = expected
	req.AssignmentMetadata = assignment
	req.ForbiddenLabels = forbidden
	normalizedWitness, err := normalizeAssignmentClaimCoLocatedWitness(req.ID, req.CoLocatedWitness)
	if err != nil {
		return AssignmentClaimRequest{}, err
	}
	req.CoLocatedWitness = normalizedWitness
	return req, nil
}

func normalizeAssignmentClaimCoLocatedWitness(workID string, witness *AssignmentClaimCoLocatedWitness) (*AssignmentClaimCoLocatedWitness, error) {
	if witness == nil {
		return nil, nil
	}
	normalized := &AssignmentClaimCoLocatedWitness{
		ID:               strings.TrimSpace(witness.ID),
		ExpectedStatus:   strings.TrimSpace(witness.ExpectedStatus),
		ExpectedType:     strings.TrimSpace(witness.ExpectedType),
		ExpectedRevision: witness.ExpectedRevision,
	}
	if normalized.ID == "" {
		return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness has an empty bead ID", workID)
	}
	if normalized.ID == workID {
		return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness must name a different bead", workID)
	}
	if normalized.ExpectedStatus == "" {
		return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness has an empty expected status", workID)
	}
	if normalized.ExpectedType == "" {
		return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness has an empty expected type", workID)
	}
	if normalized.ExpectedRevision < 0 {
		return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness has a negative expected revision", workID)
	}

	if len(witness.RequiredLabels) == 0 {
		return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness required labels are missing", workID)
	}
	seenLabels := make(map[string]struct{}, len(witness.RequiredLabels))
	for _, label := range witness.RequiredLabels {
		label = strings.TrimSpace(label)
		if label == "" {
			return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness required labels contain an empty value", workID)
		}
		if _, seen := seenLabels[label]; seen {
			continue
		}
		seenLabels[label] = struct{}{}
		normalized.RequiredLabels = append(normalized.RequiredLabels, label)
	}

	if len(witness.ExpectedMetadata) == 0 {
		return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness expected metadata is missing", workID)
	}
	normalized.ExpectedMetadata = make(map[string]string, len(witness.ExpectedMetadata))
	for key, value := range witness.ExpectedMetadata {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness expected metadata has an empty key", workID)
		}
		if _, exists := normalized.ExpectedMetadata[key]; exists {
			return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness expected metadata has duplicate normalized key %q", workID, key)
		}
		normalized.ExpectedMetadata[key] = value
	}

	seenAbsentOrEmpty := make(map[string]struct{}, len(witness.AbsentOrEmptyMetadata))
	for _, key := range witness.AbsentOrEmptyMetadata {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness absent-or-empty metadata has an empty key", workID)
		}
		if _, conflicts := normalized.ExpectedMetadata[key]; conflicts {
			return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness metadata key %q has conflicting presence predicates", workID, key)
		}
		if _, duplicate := seenAbsentOrEmpty[key]; duplicate {
			continue
		}
		seenAbsentOrEmpty[key] = struct{}{}
		normalized.AbsentOrEmptyMetadata = append(normalized.AbsentOrEmptyMetadata, key)
	}
	if witness.ExactLabels != nil {
		normalized.ExactLabels = make([]string, 0, len(witness.ExactLabels))
		seenExactLabels := make(map[string]struct{}, len(witness.ExactLabels))
		for _, label := range witness.ExactLabels {
			label = strings.TrimSpace(label)
			if label == "" {
				return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness exact labels contain an empty value", workID)
			}
			if _, seen := seenExactLabels[label]; seen {
				continue
			}
			seenExactLabels[label] = struct{}{}
			normalized.ExactLabels = append(normalized.ExactLabels, label)
		}
		slices.Sort(normalized.ExactLabels)
		for _, required := range normalized.RequiredLabels {
			if !slices.Contains(normalized.ExactLabels, required) {
				return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness exact labels omit required label %q", workID, required)
			}
		}
	}
	if witness.ExactMetadata != nil {
		normalized.ExactMetadata = make(map[string]string, len(witness.ExactMetadata))
		for key, value := range witness.ExactMetadata {
			key = strings.TrimSpace(key)
			if key == "" {
				return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness exact metadata has an empty key", workID)
			}
			if _, exists := normalized.ExactMetadata[key]; exists {
				return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness exact metadata has duplicate normalized key %q", workID, key)
			}
			normalized.ExactMetadata[key] = value
		}
		for key, expected := range normalized.ExpectedMetadata {
			if actual, present := normalized.ExactMetadata[key]; !present || actual != expected {
				return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness exact metadata does not satisfy expected key %q", workID, key)
			}
		}
		for _, key := range normalized.AbsentOrEmptyMetadata {
			if actual, present := normalized.ExactMetadata[key]; present && actual != "" {
				return nil, fmt.Errorf("guarded assignment claim on %q: co-located witness exact metadata violates absent-or-empty key %q", workID, key)
			}
		}
	}
	return normalized, nil
}

func assignmentClaimCoLocatedWitnessMatches(current Bead, req AssignmentClaimRequest) bool {
	witness := req.CoLocatedWitness
	if witness == nil {
		return true
	}
	if current.ID != witness.ID || current.Status != witness.ExpectedStatus || current.Type != witness.ExpectedType ||
		(witness.ExpectedRevision > 0 && current.Revision != witness.ExpectedRevision) {
		return false
	}
	for _, label := range witness.RequiredLabels {
		if !slices.Contains(current.Labels, label) {
			return false
		}
	}
	for key, expected := range witness.ExpectedMetadata {
		actual, present := current.Metadata[key]
		if !present || actual != expected {
			return false
		}
	}
	for _, key := range witness.AbsentOrEmptyMetadata {
		if actual, present := current.Metadata[key]; present && actual != "" {
			return false
		}
	}
	if witness.ExactLabels != nil {
		actual := append([]string(nil), current.Labels...)
		for i := range actual {
			actual[i] = strings.TrimSpace(actual[i])
		}
		slices.Sort(actual)
		actual = slices.Compact(actual)
		if !slices.Equal(actual, witness.ExactLabels) {
			return false
		}
	}
	if witness.ExactMetadata != nil && !maps.Equal(current.Metadata, witness.ExactMetadata) {
		return false
	}
	return true
}

type assignmentClaimDecision uint8

const (
	assignmentClaimMismatch assignmentClaimDecision = iota
	assignmentClaimApply
	assignmentClaimIdempotent
)

func decideAssignmentClaim(current Bead, req AssignmentClaimRequest) assignmentClaimDecision {
	if current.ID != req.ID {
		return assignmentClaimMismatch
	}
	for key, expected := range req.ExpectedMetadata {
		actual, present := current.Metadata[key]
		if !present || actual != expected {
			return assignmentClaimMismatch
		}
	}
	if current.Status == "in_progress" && current.Assignee == req.Actor {
		for key, expected := range req.AssignmentMetadata {
			actual, present := current.Metadata[key]
			if !present || actual != expected {
				return assignmentClaimMismatch
			}
		}
		return assignmentClaimIdempotent
	}
	if req.RequireIdempotent {
		return assignmentClaimMismatch
	}
	for _, forbidden := range req.ForbiddenLabels {
		if slices.Contains(current.Labels, forbidden) {
			return assignmentClaimMismatch
		}
	}
	if current.Status != req.ExpectedStatus || current.Assignee != req.ExpectedAssignee {
		return assignmentClaimMismatch
	}
	return assignmentClaimApply
}

func assignmentClaimUpdate(req AssignmentClaimRequest) UpdateOpts {
	status := "in_progress"
	actor := req.Actor
	return UpdateOpts{
		Status:   &status,
		Assignee: &actor,
		Metadata: maps.Clone(req.AssignmentMetadata),
	}
}

// ClaimAssignment implements GuardedAssignmentClaimer for the in-memory store.
func (m *MemStore) ClaimAssignment(_ context.Context, req AssignmentClaimRequest) (Bead, bool, error) {
	normalized, err := normalizeAssignmentClaimRequest(req)
	if err != nil {
		return Bead{}, false, err
	}
	return m.claimAssignmentNormalized(normalized)
}

func (m *MemStore) claimAssignmentNormalized(req AssignmentClaimRequest) (Bead, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := m.indexOfLocked(req.ID)
	if i < 0 {
		return Bead{}, false, fmt.Errorf("guarded assignment claim on %q: %w", req.ID, ErrNotFound)
	}
	if req.CoLocatedWitness != nil {
		witnessIndex := m.indexOfLocked(req.CoLocatedWitness.ID)
		if witnessIndex < 0 || !assignmentClaimCoLocatedWitnessMatches(m.beads[witnessIndex], req) {
			return Bead{}, false, nil
		}
	}
	switch decideAssignmentClaim(m.beads[i], req) {
	case assignmentClaimMismatch:
		return Bead{}, false, nil
	case assignmentClaimIdempotent:
		return cloneBead(m.beads[i]), true, nil
	case assignmentClaimApply:
		m.applyUpdateLocked(i, assignmentClaimUpdate(req))
		return cloneBead(m.beads[i]), true, nil
	default:
		return Bead{}, false, errors.New("guarded assignment claim: invalid decision")
	}
}

var _ GuardedAssignmentClaimer = (*MemStore)(nil)
