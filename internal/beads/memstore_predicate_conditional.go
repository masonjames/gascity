package beads

import "fmt"

var _ PredicateConditionalWriter = (*MemStore)(nil)

// UpdateIfPredicate validates and updates one bead while holding MemStore's
// mutation lock, returning the exact stored poststate.
func (m *MemStore) UpdateIfPredicate(id string, predicate BeadPredicate, opts UpdateOpts) (Bead, error) {
	if err := validatePredicateConditionalUpdate(id, predicate, opts); err != nil {
		return Bead{}, err
	}
	opts = clonePredicateConditionalUpdate(opts)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.DisableConditionalWrites {
		return Bead{}, ErrConditionalWriteUnsupported
	}
	i := m.indexOfLocked(id)
	if i < 0 {
		return Bead{}, fmt.Errorf("updating bead %q: %w", id, ErrNotFound)
	}
	if err := predicate(cloneBead(m.beads[i])); err != nil {
		return Bead{}, err
	}
	m.applyUpdateLocked(i, opts)
	return cloneBead(m.beads[i]), nil
}

// CloseIfPredicate validates, applies finalPatch, and closes one bead while
// holding MemStore's mutation lock. The patch and close consume one revision.
func (m *MemStore) CloseIfPredicate(id string, predicate BeadPredicate, finalPatch UpdateOpts) (Bead, error) {
	if err := validatePredicateConditionalClose(id, predicate, finalPatch); err != nil {
		return Bead{}, err
	}
	finalPatch = clonePredicateConditionalUpdate(finalPatch)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.DisableConditionalWrites {
		return Bead{}, ErrConditionalWriteUnsupported
	}
	i := m.indexOfLocked(id)
	if i < 0 {
		return Bead{}, fmt.Errorf("closing bead %q: %w", id, ErrNotFound)
	}
	if err := predicate(cloneBead(m.beads[i])); err != nil {
		return Bead{}, err
	}
	if m.beads[i].Status == "closed" && isEmptyUpdateOpts(finalPatch) {
		return cloneBead(m.beads[i]), nil
	}
	status := "closed"
	finalPatch.Status = &status
	m.applyUpdateLocked(i, finalPatch)
	return cloneBead(m.beads[i]), nil
}
