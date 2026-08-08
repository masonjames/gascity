package session

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
)

// This file is the confined session-class assignee-identity vocabulary: the
// forms under which a work bead may be assigned to a session. It is shared by
// the reconciler orphan-release loops (which enumerate every form a live
// session answers to) and the API assignee list filter and assign stamper
// (which enumerate the same set and pick the durable stamp form). Confining it
// here keeps the session-bead metadata keys (session_name / alias /
// configured_named_identity / alias_history) out of cmd/gc and internal/api, so
// those callers speak session identities via session.Info instead of cracking
// beads.Bead.Metadata directly.
//
// All reads use the RAW Info mirrors (SessionNameMetadata, not SessionName)
// because Info.SessionName falls back to sessionNameFor(ID); admitting that
// derived runtime name into the assignee set would match work the session was
// never assigned.

// AssigneeIdentities returns every identifier under which a work bead could be
// assigned to this session: the session bead ID, session_name,
// configured_named_identity, current alias, and any prior aliases preserved in
// alias_history — each trimmed, empty values skipped, in that order. Pool
// polecat aliases (e.g. "nux") are first-class assignment identities, so
// leaving them out of orphan-detection resets in-progress work under a live
// owner — see the SkipsLiveSessionAssignedByAlias regression tests.
func AssigneeIdentities(i Info) []string {
	identities := make([]string, 0, 5)
	if id := strings.TrimSpace(i.ID); id != "" {
		identities = append(identities, id)
	}
	if sn := strings.TrimSpace(i.SessionNameMetadata); sn != "" {
		identities = append(identities, sn)
	}
	if ni := strings.TrimSpace(i.ConfiguredNamedIdentity); ni != "" {
		identities = append(identities, ni)
	}
	if al := strings.TrimSpace(i.Alias); al != "" {
		identities = append(identities, al)
	}
	for _, prior := range i.AliasHistory {
		if prior = strings.TrimSpace(prior); prior != "" {
			identities = append(identities, prior)
		}
	}
	return identities
}

// AssigneeIdentifier returns the durable agent-facing ownership identity of a
// session: its current public alias, configured named identity, or runtime
// session name, falling back to the bead ID when no name metadata is present.
// This is the same alias-first identity RuntimeEnvWithSessionContext exposes
// through GC_ALIAS and BEADS_ACTOR; GC_AGENT mirrors it only for compatibility.
// Keeping API assignment normalization on this rule prevents one session from
// owning work under a different exact string than it presents to bd.
func AssigneeIdentifier(i Info) string {
	for _, v := range []string{i.Alias, i.ConfiguredNamedIdentity, i.SessionNameMetadata} {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return i.ID
}

// GuardedAssignmentCoLocatedWitness projects the exact session row predicates
// that must remain current when a controller atomically assigns work in the
// same bead store. Keeping this projection in session confines session schema
// vocabulary here; the lower beads layer only evaluates generic predicates.
func GuardedAssignmentCoLocatedWitness(i Info) (*beads.AssignmentClaimCoLocatedWitness, error) {
	id := strings.TrimSpace(i.ID)
	if id == "" {
		return nil, errors.New("guarded assignment session witness: empty session ID")
	}
	if i.Closed {
		return nil, fmt.Errorf("guarded assignment session witness %q: session is closed", id)
	}
	if i.Type != BeadType {
		return nil, fmt.Errorf("guarded assignment session witness %q: type %q, want %q", id, i.Type, BeadType)
	}
	if !slices.Contains(i.Labels, LabelSession) {
		return nil, fmt.Errorf("guarded assignment session witness %q: missing label %q", id, LabelSession)
	}
	for _, required := range []struct {
		key   string
		value string
	}{
		{key: "template", value: i.Template},
		{key: "session_name", value: i.SessionNameMetadata},
		{key: "instance_token", value: i.InstanceToken},
		{key: "state", value: i.MetadataState},
	} {
		if strings.TrimSpace(required.value) == "" {
			return nil, fmt.Errorf("guarded assignment session witness %q: empty %s metadata", id, required.key)
		}
	}

	witness := &beads.AssignmentClaimCoLocatedWitness{
		ID:             id,
		ExpectedStatus: "open",
		ExpectedType:   BeadType,
		RequiredLabels: []string{LabelSession},
		ExpectedMetadata: map[string]string{
			"template":       i.Template,
			"session_name":   i.SessionNameMetadata,
			"instance_token": i.InstanceToken,
			"state":          i.MetadataState,
		},
	}
	for _, identity := range []struct {
		key   string
		value string
	}{
		{key: "alias", value: i.Alias},
		{key: NamedSessionIdentityMetadata, value: i.ConfiguredNamedIdentity},
	} {
		if identity.value == "" {
			witness.AbsentOrEmptyMetadata = append(witness.AbsentOrEmptyMetadata, identity.key)
			continue
		}
		witness.ExpectedMetadata[identity.key] = identity.value
	}
	return witness, nil
}

// GuardedAssignmentCoLocatedExactWitness extends the ordinary co-located
// session predicate with the revision and complete metadata/label snapshot
// from the same persisted fetch. It is used when assigning WORK must be atomic
// with one exact SESSION authority row, rather than with only the legacy
// lifecycle/actor subset.
func GuardedAssignmentCoLocatedExactWitness(i Info, persisted PersistedResponse) (*beads.AssignmentClaimCoLocatedWitness, error) {
	witness, err := GuardedAssignmentCoLocatedWitness(i)
	if err != nil {
		return nil, err
	}
	if persisted.Revision <= 0 {
		return nil, fmt.Errorf("guarded assignment session witness %q: missing positive revision", i.ID)
	}
	witness.ExpectedRevision = persisted.Revision
	witness.ExactLabels = append([]string{}, i.Labels...)
	witness.ExactMetadata = maps.Clone(persisted.Metadata)
	if witness.ExactMetadata == nil {
		witness.ExactMetadata = map[string]string{}
	}
	return witness, nil
}

// AssignmentReleaseCoLocatedMatch returns the generic co-located absence
// predicate that recognizes every durable assignee identity of an open session
// row. Session schema vocabulary remains confined to this package; the beads
// layer evaluates only the generic status/type/label/value-source shape.
func AssignmentReleaseCoLocatedMatch(assignee string) (*beads.CoLocatedMatchPredicate, error) {
	assignee = strings.TrimSpace(assignee)
	if assignee == "" {
		return nil, errors.New("assignment release session match: empty assignee")
	}
	return &beads.CoLocatedMatchPredicate{
		ExpectedStatus: "open",
		ClassAnyOf: []beads.CoLocatedClassPredicate{
			{ExpectedType: BeadType},
			{RequiredLabels: []string{LabelSession}},
		},
		MatchValue: assignee,
		MatchID:    true,
		MetadataKeys: []string{
			"session_name",
			NamedSessionIdentityMetadata,
			"alias",
		},
		DelimitedMetadataKeys: map[string]string{
			aliasHistoryMetadataKey: ",",
		},
	}, nil
}
