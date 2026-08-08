package beads

import "context"

// ReleaseAssignment checks the complete release predicate and persists the
// release while holding the cross-process file lock. A failed flush restores
// the exact pre-operation in-memory snapshot.
func (fs *FileStore) ReleaseAssignment(_ context.Context, req AssignmentReleaseRequest) (Bead, bool, error) {
	normalized, err := normalizeAssignmentReleaseRequest(req)
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
	released, won, err := fs.releaseAssignmentNormalized(normalized)
	if err != nil || !won {
		return released, won, err
	}
	if err := fs.save(); err != nil {
		fs.restoreFrom(snap.seq, snap.beads, snap.deps)
		return Bead{}, false, err
	}
	return released, true, nil
}

var _ AssignmentReleaser = (*FileStore)(nil)
