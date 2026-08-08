package beads

import "context"

// CreateAssignmentClaim checks, creates, and assigns while holding the
// cross-process file lock. A failed flush restores the exact pre-operation
// memory snapshot, so neither row can survive alone.
func (fs *FileStore) CreateAssignmentClaim(_ context.Context, req CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error) {
	normalized, err := normalizeCreateAssignmentClaimRequest(req)
	if err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	fs.fmu.Lock()
	defer fs.fmu.Unlock()
	if err := fs.locker.Lock(); err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	defer fs.locker.Unlock() //nolint:errcheck // best-effort unlock
	if err := fs.reloadFromDisk(); err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	snap := fs.snapshotLocked()
	result, won, err := fs.createAssignmentClaimNormalized(normalized)
	if err != nil {
		fs.restoreFrom(snap.seq, snap.beads, snap.deps)
		return CreateAssignmentClaimResult{}, false, err
	}
	if !won {
		return result, won, err
	}
	if err := fs.save(); err != nil {
		fs.restoreFrom(snap.seq, snap.beads, snap.deps)
		return CreateAssignmentClaimResult{}, false, err
	}
	return result, true, nil
}

var _ CreateAssignmentClaimer = (*FileStore)(nil)
