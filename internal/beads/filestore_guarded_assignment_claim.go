package beads

import "context"

// ClaimAssignment applies an exact guarded assignment while holding the
// cross-process file lock. A failed flush restores the pre-claim in-memory
// snapshot, leaving both memory and disk unchanged.
func (fs *FileStore) ClaimAssignment(_ context.Context, req AssignmentClaimRequest) (Bead, bool, error) {
	normalized, err := normalizeAssignmentClaimRequest(req)
	if err != nil {
		return Bead{}, false, err
	}
	fs.fmu.Lock()
	defer fs.fmu.Unlock()
	if err := fs.locker.Lock(); err != nil {
		return Bead{}, false, err
	}
	defer fs.locker.Unlock() //nolint:errcheck // best-effort unlock
	if err := fs.reloadFromDisk(); err != nil {
		return Bead{}, false, err
	}
	snap := fs.snapshotLocked()
	claimed, ok, err := fs.claimAssignmentNormalized(normalized)
	if err != nil || !ok {
		return claimed, ok, err
	}
	// An idempotent retry returns the exact stored snapshot and need not rewrite
	// the file. Detect it from the unchanged snapshot revision.
	beforeRevision := int64(-1)
	for _, bead := range snap.beads {
		if bead.ID == normalized.ID {
			beforeRevision = bead.Revision
			break
		}
	}
	if claimed.Revision == beforeRevision {
		return claimed, true, nil
	}
	if err := fs.save(); err != nil {
		fs.restoreFrom(snap.seq, snap.beads, snap.deps)
		return Bead{}, false, err
	}
	return claimed, true, nil
}

var _ GuardedAssignmentClaimer = (*FileStore)(nil)
