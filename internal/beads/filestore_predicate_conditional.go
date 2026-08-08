package beads

var _ PredicateConditionalWriter = (*FileStore)(nil)

// UpdateIfPredicate reloads under the cross-process file lock, evaluates the
// predicate and applies opts against that exact snapshot, then persists it.
func (fs *FileStore) UpdateIfPredicate(id string, predicate BeadPredicate, opts UpdateOpts) (Bead, error) {
	if err := validatePredicateConditionalUpdate(id, predicate, opts); err != nil {
		return Bead{}, err
	}
	opts = clonePredicateConditionalUpdate(opts)
	fs.fmu.Lock()
	defer fs.fmu.Unlock()
	if fs.DisableConditionalWrites {
		return Bead{}, ErrConditionalWriteUnsupported
	}
	if err := fs.locker.Lock(); err != nil {
		return Bead{}, err
	}
	defer fs.locker.Unlock() //nolint:errcheck // best-effort unlock
	if err := fs.reloadFromDisk(); err != nil {
		return Bead{}, err
	}
	snap := fs.snapshotLocked()
	post, err := fs.MemStore.UpdateIfPredicate(id, predicate, opts)
	if err != nil {
		return Bead{}, err
	}
	if err := fs.save(); err != nil {
		return fs.reconcilePredicateConditionalSave(id, post, snap, err)
	}
	return cloneBead(post), nil
}

// CloseIfPredicate reloads under the cross-process file lock, evaluates the
// predicate, applies finalPatch, closes, and persists one atomic file image.
func (fs *FileStore) CloseIfPredicate(id string, predicate BeadPredicate, finalPatch UpdateOpts) (Bead, error) {
	if err := validatePredicateConditionalClose(id, predicate, finalPatch); err != nil {
		return Bead{}, err
	}
	finalPatch = clonePredicateConditionalUpdate(finalPatch)
	fs.fmu.Lock()
	defer fs.fmu.Unlock()
	if fs.DisableConditionalWrites {
		return Bead{}, ErrConditionalWriteUnsupported
	}
	if err := fs.locker.Lock(); err != nil {
		return Bead{}, err
	}
	defer fs.locker.Unlock() //nolint:errcheck // best-effort unlock
	if err := fs.reloadFromDisk(); err != nil {
		return Bead{}, err
	}
	snap := fs.snapshotLocked()
	post, err := fs.MemStore.CloseIfPredicate(id, predicate, finalPatch)
	if err != nil {
		return Bead{}, err
	}
	if err := fs.save(); err != nil {
		return fs.reconcilePredicateConditionalSave(id, post, snap, err)
	}
	return cloneBead(post), nil
}

func (fs *FileStore) reconcilePredicateConditionalSave(id string, expected Bead, snap memSnapshot, writeErr error) (Bead, error) {
	freshness, freshnessErr := fs.currentFreshness()
	if freshnessErr == nil && freshness.exists {
		if err := fs.reloadFromDisk(); err != nil {
			fs.restoreFrom(snap.seq, snap.beads, snap.deps)
			return Bead{}, writeErr
		}
		current, getErr := fs.MemStore.Get(id)
		if getErr == nil && predicateConditionalPoststateEqual(expected, current) {
			return cloneBead(current), nil
		}
	}
	fs.restoreFrom(snap.seq, snap.beads, snap.deps)
	return Bead{}, writeErr
}
