package beads

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// ErrAssignmentReleaseUnsupported reports that a store cannot atomically
// verify and release one exact assignment together with its co-located absence
// predicate.
var ErrAssignmentReleaseUnsupported = errors.New("assignment release unsupported")

// CoLocatedClassPredicate describes one generic row-class alternative. Its
// non-empty type and labels are AND predicates. CoLocatedMatchPredicate ORs
// these alternatives so callers can recognize rows carrying any one of several
// independent class markers.
type CoLocatedClassPredicate struct {
	ExpectedType   string
	RequiredLabels []string
}

// CoLocatedMatchPredicate describes a generic row whose presence vetoes an
// assignment release. A row must match ExpectedStatus and at least one
// ClassAnyOf alternative. MatchValue is then compared, with OR semantics,
// against the row ID when MatchID is true, each exact MetadataKeys value, and
// each trimmed token in DelimitedMetadataKeys (metadata key -> delimiter). The
// beads layer assigns no domain meaning to any field or metadata key.
type CoLocatedMatchPredicate struct {
	ExpectedStatus        string
	ClassAnyOf            []CoLocatedClassPredicate
	MatchValue            string
	MatchID               bool
	MetadataKeys          []string
	DelimitedMetadataKeys map[string]string
}

// AssignmentReleaseRequest is the complete input to one exact guarded
// assignment release. The work row must still be the named in-progress
// assignment at ExpectedRevision, its complete metadata map must equal
// ExpectedMetadata byte-for-byte, and none of ForbiddenLabels may be present.
// The release and ReleaseMetadata merge happen in one mutation.
// AbsentCoLocatedMatch is required and makes the same transaction snapshot
// prove no matching row.
type AssignmentReleaseRequest struct {
	ID                   string
	ExpectedStatus       string
	ExpectedAssignee     string
	ExpectedRevision     *int64
	ExpectedMetadata     map[string]string
	ForbiddenLabels      []string
	ReleaseMetadata      map[string]string
	AbsentCoLocatedMatch *CoLocatedMatchPredicate
}

// AssignmentReleaser is an optional Store capability for atomically releasing
// one exact assignment. A successful call returns the authoritative released
// row and ok=true. Any predicate mismatch returns the zero bead, false, nil.
// A missing exact ID returns ErrNotFound. Unsupported stores and backend
// failures return a non-nil error without a fallback mutation.
type AssignmentReleaser interface {
	ReleaseAssignment(ctx context.Context, req AssignmentReleaseRequest) (Bead, bool, error)
}

// AssignmentReleaserHandleProvider lets wrappers expose the capability only
// when their backing store honestly implements the complete atomic operation.
type AssignmentReleaserHandleProvider interface {
	AssignmentReleaserHandle() (AssignmentReleaser, bool)
}

// AssignmentReleaserFor resolves a store's optional atomic assignment-release
// capability. Handle providers are consulted first so transparent wrappers can
// veto false capability promotion.
func AssignmentReleaserFor(store Store) (AssignmentReleaser, bool) {
	if store == nil {
		return nil, false
	}
	if provider, ok := store.(AssignmentReleaserHandleProvider); ok {
		return provider.AssignmentReleaserHandle()
	}
	releaser, ok := store.(AssignmentReleaser)
	return releaser, ok
}

func normalizeAssignmentReleaseRequest(req AssignmentReleaseRequest) (AssignmentReleaseRequest, error) {
	req.ID = strings.TrimSpace(req.ID)
	if req.ID == "" {
		return AssignmentReleaseRequest{}, errors.New("assignment release: empty bead ID")
	}
	req.ExpectedStatus = strings.TrimSpace(req.ExpectedStatus)
	if req.ExpectedStatus != "in_progress" {
		return AssignmentReleaseRequest{}, fmt.Errorf("assignment release on %q: expected status must be %q, got %q", req.ID, "in_progress", req.ExpectedStatus)
	}
	req.ExpectedAssignee = strings.TrimSpace(req.ExpectedAssignee)
	if req.ExpectedAssignee == "" {
		return AssignmentReleaseRequest{}, fmt.Errorf("assignment release on %q: expected assignee is empty", req.ID)
	}
	if req.ExpectedRevision == nil {
		return AssignmentReleaseRequest{}, fmt.Errorf("assignment release on %q: expected revision is required", req.ID)
	}
	revision := *req.ExpectedRevision
	req.ExpectedRevision = &revision

	if len(req.ExpectedMetadata) == 0 {
		return AssignmentReleaseRequest{}, fmt.Errorf("assignment release on %q: expected metadata is required", req.ID)
	}
	var err error
	req.ExpectedMetadata, err = normalizeAssignmentReleaseMetadata(req.ID, "expected", req.ExpectedMetadata)
	if err != nil {
		return AssignmentReleaseRequest{}, err
	}
	req.ReleaseMetadata, err = normalizeAssignmentReleaseMetadata(req.ID, "release", req.ReleaseMetadata)
	if err != nil {
		return AssignmentReleaseRequest{}, err
	}
	if len(req.ForbiddenLabels) == 0 {
		return AssignmentReleaseRequest{}, fmt.Errorf("assignment release on %q: forbidden labels are required", req.ID)
	}
	seenLabels := make(map[string]struct{}, len(req.ForbiddenLabels))
	forbidden := make([]string, 0, len(req.ForbiddenLabels))
	for _, label := range req.ForbiddenLabels {
		label = strings.TrimSpace(label)
		if label == "" {
			return AssignmentReleaseRequest{}, fmt.Errorf("assignment release on %q: forbidden labels contain an empty value", req.ID)
		}
		if _, duplicate := seenLabels[label]; duplicate {
			continue
		}
		seenLabels[label] = struct{}{}
		forbidden = append(forbidden, label)
	}
	req.ForbiddenLabels = forbidden

	req.AbsentCoLocatedMatch, err = normalizeCoLocatedMatchPredicate(req.ID, req.ExpectedAssignee, req.AbsentCoLocatedMatch)
	if err != nil {
		return AssignmentReleaseRequest{}, err
	}
	return req, nil
}

func normalizeAssignmentReleaseMetadata(id, kind string, metadata map[string]string) (map[string]string, error) {
	normalized := make(map[string]string, len(metadata))
	for key, value := range metadata {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("assignment release on %q: %s metadata has an empty key", id, kind)
		}
		if _, duplicate := normalized[key]; duplicate {
			return nil, fmt.Errorf("assignment release on %q: %s metadata has duplicate normalized key %q", id, kind, key)
		}
		normalized[key] = value
	}
	return normalized, nil
}

func normalizeCoLocatedMatchPredicate(workID, expectedAssignee string, predicate *CoLocatedMatchPredicate) (*CoLocatedMatchPredicate, error) {
	if predicate == nil {
		return nil, fmt.Errorf("assignment release on %q: co-located absence predicate is required", workID)
	}
	normalized := &CoLocatedMatchPredicate{
		ExpectedStatus: strings.TrimSpace(predicate.ExpectedStatus),
		MatchValue:     strings.TrimSpace(predicate.MatchValue),
		MatchID:        predicate.MatchID,
	}
	if normalized.ExpectedStatus == "" {
		return nil, fmt.Errorf("assignment release on %q: co-located match status is required", workID)
	}
	if normalized.MatchValue == "" || normalized.MatchValue != expectedAssignee {
		return nil, fmt.Errorf("assignment release on %q: co-located match value must equal expected assignee", workID)
	}
	if len(predicate.ClassAnyOf) == 0 {
		return nil, fmt.Errorf("assignment release on %q: co-located class alternatives are required", workID)
	}
	for i, alternative := range predicate.ClassAnyOf {
		class := CoLocatedClassPredicate{ExpectedType: strings.TrimSpace(alternative.ExpectedType)}
		seenLabels := make(map[string]struct{}, len(alternative.RequiredLabels))
		for _, label := range alternative.RequiredLabels {
			label = strings.TrimSpace(label)
			if label == "" {
				return nil, fmt.Errorf("assignment release on %q: co-located class alternative %d contains an empty label", workID, i)
			}
			if _, duplicate := seenLabels[label]; duplicate {
				continue
			}
			seenLabels[label] = struct{}{}
			class.RequiredLabels = append(class.RequiredLabels, label)
		}
		if class.ExpectedType == "" && len(class.RequiredLabels) == 0 {
			return nil, fmt.Errorf("assignment release on %q: co-located class alternative %d has no predicate", workID, i)
		}
		normalized.ClassAnyOf = append(normalized.ClassAnyOf, class)
	}

	seen := make(map[string]struct{}, len(predicate.MetadataKeys)+len(predicate.DelimitedMetadataKeys))
	for _, key := range predicate.MetadataKeys {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("assignment release on %q: co-located metadata keys contain an empty value", workID)
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		normalized.MetadataKeys = append(normalized.MetadataKeys, key)
	}
	normalized.DelimitedMetadataKeys = make(map[string]string, len(predicate.DelimitedMetadataKeys))
	for key, delimiter := range predicate.DelimitedMetadataKeys {
		key = strings.TrimSpace(key)
		if key == "" || delimiter == "" {
			return nil, fmt.Errorf("assignment release on %q: co-located delimited metadata key and delimiter are required", workID)
		}
		if _, conflict := seen[key]; conflict {
			return nil, fmt.Errorf("assignment release on %q: co-located metadata key %q has conflicting match modes", workID, key)
		}
		seen[key] = struct{}{}
		normalized.DelimitedMetadataKeys[key] = delimiter
	}
	if !normalized.MatchID && len(normalized.MetadataKeys) == 0 && len(normalized.DelimitedMetadataKeys) == 0 {
		return nil, fmt.Errorf("assignment release on %q: co-located match has no value source", workID)
	}
	return normalized, nil
}

func assignmentReleaseWorkMatches(current Bead, req AssignmentReleaseRequest) bool {
	if current.ID != req.ID || current.Status != req.ExpectedStatus || current.Assignee != req.ExpectedAssignee || current.Revision != *req.ExpectedRevision {
		return false
	}
	if len(current.Metadata) != len(req.ExpectedMetadata) {
		return false
	}
	for key, expected := range req.ExpectedMetadata {
		actual, present := current.Metadata[key]
		if !present || actual != expected {
			return false
		}
	}
	for _, forbidden := range req.ForbiddenLabels {
		if slices.Contains(current.Labels, forbidden) {
			return false
		}
	}
	return true
}

func coLocatedMatchPredicateMatches(current Bead, predicate *CoLocatedMatchPredicate) bool {
	if predicate == nil || current.Status != predicate.ExpectedStatus {
		return false
	}
	classMatches := false
	for _, alternative := range predicate.ClassAnyOf {
		if alternative.ExpectedType != "" && current.Type != alternative.ExpectedType {
			continue
		}
		labelsMatch := true
		for _, required := range alternative.RequiredLabels {
			if !slices.Contains(current.Labels, required) {
				labelsMatch = false
				break
			}
		}
		if labelsMatch {
			classMatches = true
			break
		}
	}
	if !classMatches {
		return false
	}
	if predicate.MatchID && strings.TrimSpace(current.ID) == predicate.MatchValue {
		return true
	}
	for _, key := range predicate.MetadataKeys {
		if strings.TrimSpace(current.Metadata[key]) == predicate.MatchValue {
			return true
		}
	}
	for key, delimiter := range predicate.DelimitedMetadataKeys {
		for _, value := range strings.Split(current.Metadata[key], delimiter) {
			if strings.TrimSpace(value) == predicate.MatchValue {
				return true
			}
		}
	}
	return false
}

func assignmentReleaseBlocked(rows []Bead, predicate *CoLocatedMatchPredicate) bool {
	for _, row := range rows {
		if coLocatedMatchPredicateMatches(row, predicate) {
			return true
		}
	}
	return false
}

func assignmentReleaseUpdate(req AssignmentReleaseRequest) UpdateOpts {
	status, assignee := "open", ""
	return UpdateOpts{Status: &status, Assignee: &assignee, Metadata: maps.Clone(req.ReleaseMetadata)}
}

func assignmentReleasePostStateMatches(current Bead, req AssignmentReleaseRequest) bool {
	if current.ID != req.ID || current.Status != "open" || current.Assignee != "" || current.Revision == *req.ExpectedRevision {
		return false
	}
	expectedMetadata := maps.Clone(req.ExpectedMetadata)
	for key, value := range req.ReleaseMetadata {
		expectedMetadata[key] = value
	}
	if len(current.Metadata) != len(expectedMetadata) {
		return false
	}
	for key, expected := range expectedMetadata {
		actual, present := current.Metadata[key]
		if !present || actual != expected {
			return false
		}
	}
	return true
}

// ReleaseAssignment implements AssignmentReleaser for the in-memory store.
func (m *MemStore) ReleaseAssignment(_ context.Context, req AssignmentReleaseRequest) (Bead, bool, error) {
	normalized, err := normalizeAssignmentReleaseRequest(req)
	if err != nil {
		return Bead{}, false, err
	}
	return m.releaseAssignmentNormalized(normalized)
}

func (m *MemStore) releaseAssignmentNormalized(req AssignmentReleaseRequest) (Bead, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := m.indexOfLocked(req.ID)
	if i < 0 {
		return Bead{}, false, fmt.Errorf("assignment release on %q: %w", req.ID, ErrNotFound)
	}
	if !assignmentReleaseWorkMatches(m.beads[i], req) || assignmentReleaseBlocked(m.beads, req.AbsentCoLocatedMatch) {
		return Bead{}, false, nil
	}
	m.applyUpdateLocked(i, assignmentReleaseUpdate(req))
	released := cloneBead(m.beads[i])
	if !assignmentReleasePostStateMatches(released, req) {
		return Bead{}, false, fmt.Errorf("assignment release on %q: in-memory readback did not match released post-state", req.ID)
	}
	return released, true, nil
}

var _ AssignmentReleaser = (*MemStore)(nil)
