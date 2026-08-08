package beads

var (
	_ PredicateConditionalWriter               = (*CachingStore)(nil)
	_ PredicateConditionalWriterHandleProvider = (*CachingStore)(nil)
)

// PredicateConditionalWriterHandle exposes the cache forwarding handle only
// when the resolved backing supports the capability.
func (c *CachingStore) PredicateConditionalWriterHandle() (PredicateConditionalWriter, bool) {
	if _, ok := PredicateConditionalWriterFor(c.conditionalBacking()); !ok {
		return nil, false
	}
	return c, true
}

// UpdateIfPredicate forwards the atomic operation and evicts the cached row.
// Even though the backend returns its exact poststate, a later external write
// may already have won before this method returns, so the poststate is used for
// notification only and is never installed as a clean cache entry.
func (c *CachingStore) UpdateIfPredicate(id string, predicate BeadPredicate, opts UpdateOpts) (Bead, error) {
	writer, ok := PredicateConditionalWriterFor(c.conditionalBacking())
	if !ok {
		return Bead{}, ErrConditionalWriteUnsupported
	}
	post, err := writer.UpdateIfPredicate(id, predicate, opts)
	c.evictForConditionalWrite(id)
	if err != nil {
		return Bead{}, err
	}
	c.notifyChange("bead.updated", post)
	return cloneBead(post), nil
}

// CloseIfPredicate forwards the atomic patch-and-close, evicts the cached row,
// and emits the exact closed poststate returned by the backing transaction.
func (c *CachingStore) CloseIfPredicate(id string, predicate BeadPredicate, finalPatch UpdateOpts) (Bead, error) {
	writer, ok := PredicateConditionalWriterFor(c.conditionalBacking())
	if !ok {
		return Bead{}, ErrConditionalWriteUnsupported
	}
	post, err := writer.CloseIfPredicate(id, predicate, finalPatch)
	c.evictForConditionalWrite(id)
	if err != nil {
		return Bead{}, err
	}
	c.notifyChange("bead.closed", post)
	return cloneBead(post), nil
}
