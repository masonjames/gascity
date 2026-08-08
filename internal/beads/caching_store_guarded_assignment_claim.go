package beads

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// GuardedAssignmentClaimerHandle exposes this cache as the claim handle only
// when its backing can perform the complete atomic operation.
func (c *CachingStore) GuardedAssignmentClaimerHandle() (GuardedAssignmentClaimer, bool) {
	if c == nil {
		return nil, false
	}
	if _, ok := GuardedAssignmentClaimerFor(c.backing); !ok {
		return nil, false
	}
	return c, true
}

// ClaimAssignment forwards the complete guarded claim to the backing and
// installs only the authoritative bead returned by that operation. A lost
// predicate race evicts the candidate so the next read cannot repeat against a
// stale cached snapshot, while an event observed during the backing call wins
// over the older transaction readback.
func (c *CachingStore) ClaimAssignment(ctx context.Context, req AssignmentClaimRequest) (Bead, bool, error) {
	normalized, err := normalizeAssignmentClaimRequest(req)
	if err != nil {
		return Bead{}, false, err
	}
	claimer, ok := GuardedAssignmentClaimerFor(c.backing)
	if !ok {
		return Bead{}, false, ErrGuardedAssignmentClaimUnsupported
	}
	c.mu.RLock()
	startSeq := c.beadSeq[normalized.ID]
	c.mu.RUnlock()
	claimed, won, err := claimer.ClaimAssignment(ctx, normalized)
	if err != nil {
		// ErrNotFound is a definite, contractually mutation-free outcome for
		// this exact-ID operation. Preserve the cache byte-for-byte; unlike an
		// ambiguous backend failure, there is no may-have-committed state to
		// fence from a concurrent scan.
		if errors.Is(err, ErrNotFound) {
			return Bead{}, false, err
		}
		c.mu.Lock()
		// Preserve the dirty fence across a full scan that started before this
		// ambiguous failure; otherwise its stale row can merge back as clean.
		c.noteLocalMutationLocked(normalized.ID)
		c.markDirtyLocked(normalized.ID)
		c.mu.Unlock()
		return Bead{}, false, err
	}
	if !won {
		c.evictForConditionalWrite(normalized.ID)
		return Bead{}, false, nil
	}
	if claimed.ID != normalized.ID {
		c.evictForConditionalWrite(normalized.ID)
		return Bead{}, false, fmt.Errorf("guarded assignment claim on %q returned %q: %w", normalized.ID, claimed.ID, ErrIDCollision)
	}
	if decideAssignmentClaim(claimed, normalized) != assignmentClaimIdempotent {
		c.evictForConditionalWrite(normalized.ID)
		return Bead{}, false, fmt.Errorf("guarded assignment claim on %q: backing readback did not match assigned post-state", normalized.ID)
	}
	if normalized.RequireIdempotent {
		c.mu.Lock()
		if c.beadSeq[normalized.ID] == startSeq {
			// This mode is an authoritative read-only verification. Refresh the
			// cached projection, but do not stamp a local mutation sequence,
			// publish a bead.updated event, or overwrite an observation that
			// arrived while the backing transaction was in flight.
			c.absorbFreshLocked(normalized.ID, claimed, time.Now(), absorbOpts{
				depsMode:   depsFromFields,
				seqMode:    seqKeep,
				clearDirty: true,
			})
			c.clearDependentReadyProjectionsLocked(normalized.ID)
			c.markFreshLocked(time.Now())
			c.updateStatsLocked()
		}
		c.mu.Unlock()
		return cloneBead(claimed), true, nil
	}

	c.mu.Lock()
	if c.beadSeq[normalized.ID] != startSeq {
		// An event, reconcile, or another local operation installed a later
		// observation while the backing transaction was in flight. It may be a
		// genuinely newer row or a late stale event, so neither that row nor the
		// claim readback is safe to keep clean. Preserve the sequence fence,
		// evict, and force the next read through the authoritative backing.
		c.noteLocalMutationLocked(normalized.ID)
		delete(c.beads, normalized.ID)
		delete(c.deps, normalized.ID)
		c.markDirtyLocked(normalized.ID)
		c.clearDependentReadyProjectionsLocked(normalized.ID)
		c.markFreshLocked(time.Now())
		c.updateStatsLocked()
		c.mu.Unlock()
		return cloneBead(claimed), true, nil
	}
	c.noteLocalMutationLocked(normalized.ID)
	c.absorbFreshLocked(normalized.ID, claimed, time.Now(), absorbOpts{
		depsMode:   depsFromFields,
		seqMode:    seqKeep,
		clearDirty: true,
	})
	c.clearDependentReadyProjectionsLocked(normalized.ID)
	c.markFreshLocked(time.Now())
	c.updateStatsLocked()
	c.mu.Unlock()
	c.notifyChange("bead.updated", claimed)
	return cloneBead(claimed), true, nil
}

var (
	_ GuardedAssignmentClaimer               = (*CachingStore)(nil)
	_ GuardedAssignmentClaimerHandleProvider = (*CachingStore)(nil)
)
