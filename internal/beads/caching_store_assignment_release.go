package beads

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// AssignmentReleaserHandle exposes this cache as the release handle only when
// its backing implements the complete atomic operation.
func (c *CachingStore) AssignmentReleaserHandle() (AssignmentReleaser, bool) {
	if c == nil {
		return nil, false
	}
	if _, ok := AssignmentReleaserFor(c.backing); !ok {
		return nil, false
	}
	return c, true
}

// ReleaseAssignment delegates the indivisible predicate and mutation to the
// authoritative backing. Predicate losers evict the advisory work projection;
// ambiguous failures leave a dirty fence; and an event observed while the
// backing call is in flight wins over the older transaction readback.
func (c *CachingStore) ReleaseAssignment(ctx context.Context, req AssignmentReleaseRequest) (Bead, bool, error) {
	normalized, err := normalizeAssignmentReleaseRequest(req)
	if err != nil {
		return Bead{}, false, err
	}
	releaser, ok := AssignmentReleaserFor(c.backing)
	if !ok {
		return Bead{}, false, ErrAssignmentReleaseUnsupported
	}
	c.mu.RLock()
	startSeq := c.beadSeq[normalized.ID]
	c.mu.RUnlock()

	released, won, err := releaser.ReleaseAssignment(ctx, normalized)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Bead{}, false, err
		}
		c.mu.Lock()
		c.noteLocalMutationLocked(normalized.ID)
		c.markDirtyLocked(normalized.ID)
		c.mu.Unlock()
		return Bead{}, false, err
	}
	if !won {
		c.evictForConditionalWrite(normalized.ID)
		return Bead{}, false, nil
	}
	if released.ID != normalized.ID {
		c.evictForConditionalWrite(normalized.ID)
		return Bead{}, false, fmt.Errorf("assignment release on %q returned %q: %w", normalized.ID, released.ID, ErrIDCollision)
	}
	if !assignmentReleasePostStateMatches(released, normalized) {
		c.evictForConditionalWrite(normalized.ID)
		return Bead{}, false, fmt.Errorf("assignment release on %q: backing readback did not match released post-state", normalized.ID)
	}

	now := time.Now()
	c.mu.Lock()
	if c.beadSeq[normalized.ID] != startSeq {
		c.noteLocalMutationLocked(normalized.ID)
		delete(c.beads, normalized.ID)
		delete(c.deps, normalized.ID)
		c.markDirtyLocked(normalized.ID)
		c.clearDependentReadyProjectionsLocked(normalized.ID)
		c.markFreshLocked(now)
		c.updateStatsLocked()
		c.mu.Unlock()
		return cloneBead(released), true, nil
	}
	c.noteLocalMutationLocked(normalized.ID)
	c.absorbFreshLocked(normalized.ID, released, now, absorbOpts{
		depsMode:   depsFromFields,
		seqMode:    seqKeep,
		clearDirty: true,
	})
	c.clearDependentReadyProjectionsLocked(normalized.ID)
	c.markFreshLocked(now)
	c.updateStatsLocked()
	c.mu.Unlock()
	c.notifyChange("bead.updated", released)
	return cloneBead(released), true, nil
}

var (
	_ AssignmentReleaser               = (*CachingStore)(nil)
	_ AssignmentReleaserHandleProvider = (*CachingStore)(nil)
)
