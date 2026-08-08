package session

import "github.com/gastownhall/gascity/internal/beads"

// PersistedResponse is the persisted half of a session's API response: the
// bead-stored facts (status and metadata) that the response builder needs but
// that are not part of the scalar session.Info projection. It exists so the
// response path can speak domain types end to end — session.Info for the scalar
// fields, PersistedResponse for the status/metadata-derived fields — without a
// *beads.Bead crossing into the API layer.
//
// Bead serialization is confined here: PersistedResponseFromBead is the only
// place the API response path learns these facts come from a bead. Metadata is
// the full persisted metadata map; callers decode specific keys through the
// existing session codecs (ParseTemplateOverrides, SubmissionCapabilitiesForMetadata,
// LifecycleDisplayReasonWithLiveness, the NamedSessionMetadataKey lookup), never
// by re-reading a bead.
type PersistedResponse struct {
	// Revision is the store's optimistic-concurrency token for the exact bead
	// fetch that produced Info and this response. Strict ownership boundaries
	// carry it into conditional SESSION updates so an external writer cannot be
	// overwritten between validation and commit.
	Revision int64
	// Status is the persisted bead status ("open"/"closed"), used to derive the
	// lifecycle reason and to gate the metadata-derived fields on closed beads.
	Status string
	// Metadata is the full persisted session metadata map.
	Metadata map[string]string
}

// PersistedResponseFromBead projects a persisted session bead onto the
// PersistedResponse fields the API response builder consumes. It is pure and
// backend-invariant: it reads only stored bead fields, so a bead round-trips to
// the same PersistedResponse regardless of which backend stored it.
func PersistedResponseFromBead(b beads.Bead) PersistedResponse {
	return PersistedResponse{
		Revision: b.Revision,
		Status:   b.Status,
		Metadata: b.Metadata,
	}
}
