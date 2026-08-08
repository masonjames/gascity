package beads

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// CreateAssignmentClaimerHandle exposes the complete capability only when the
// backing store honestly supports it.
func (c *CachingStore) CreateAssignmentClaimerHandle() (CreateAssignmentClaimer, bool) {
	if c == nil {
		return nil, false
	}
	if _, ok := CreateAssignmentClaimerFor(c.backing); !ok {
		return nil, false
	}
	return c, true
}

// CreateAssignmentClaim forwards the indivisible backing operation, then
// refreshes both returned rows in the cache. A predicate loser evicts the work
// candidate so stale advisory state cannot authorize a repeated attempt.
func (c *CachingStore) CreateAssignmentClaim(ctx context.Context, req CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error) {
	normalized, err := normalizeCreateAssignmentClaimRequest(req)
	if err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	claimer, ok := CreateAssignmentClaimerFor(c.backing)
	if !ok {
		return CreateAssignmentClaimResult{}, false, ErrCreateAssignmentClaimUnsupported
	}
	c.mu.RLock()
	workStartSeq := c.beadSeq[normalized.Claim.ID]
	c.mu.RUnlock()
	result, won, err := claimer.CreateAssignmentClaim(ctx, normalized)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			c.mu.Lock()
			c.noteLocalMutationLocked(normalized.Claim.ID)
			c.markDirtyLocked(normalized.Claim.ID)
			c.mu.Unlock()
		}
		return CreateAssignmentClaimResult{}, false, err
	}
	if !won {
		c.evictForConditionalWrite(normalized.Claim.ID)
		return CreateAssignmentClaimResult{}, false, nil
	}
	if result.Created.ID == "" || result.Claimed.ID != normalized.Claim.ID {
		c.evictForConditionalWrite(normalized.Claim.ID)
		return CreateAssignmentClaimResult{}, false, fmt.Errorf("create assignment claim on %q returned created=%q claimed=%q: %w", normalized.Claim.ID, result.Created.ID, result.Claimed.ID, ErrIDCollision)
	}
	effective := createAssignmentClaimEffectiveRequest(normalized, result.Created.ID)
	if decideAssignmentClaim(result.Claimed, effective) != assignmentClaimIdempotent {
		c.evictForConditionalWrite(normalized.Claim.ID)
		return CreateAssignmentClaimResult{}, false, fmt.Errorf("create assignment claim on %q: backing readback did not match assigned post-state", normalized.Claim.ID)
	}
	now := time.Now()
	c.mu.Lock()
	workDrifted := c.beadSeq[result.Claimed.ID] != workStartSeq
	createdDrifted := c.beadSeq[result.Created.ID] != 0
	if workDrifted || createdDrifted {
		// An event or reconcile installed an observation while the atomic backing
		// call was in flight. The transaction result is authoritative for the
		// caller, but neither cache projection is safe to prefer. Evict and dirty
		// the pair so their relationship is reloaded from backing together with
		// subsequent point reads.
		for _, id := range []string{result.Created.ID, result.Claimed.ID} {
			c.noteLocalMutationLocked(id)
			delete(c.beads, id)
			delete(c.deps, id)
			c.markDirtyLocked(id)
			c.clearDependentReadyProjectionsLocked(id)
		}
		c.markFreshLocked(now)
		c.updateStatsLocked()
		c.mu.Unlock()
		return CreateAssignmentClaimResult{
			Created: cloneBead(result.Created),
			Claimed: cloneBead(result.Claimed),
		}, true, nil
	}
	for _, row := range []Bead{result.Created, result.Claimed} {
		c.noteLocalMutationLocked(row.ID)
		c.absorbFreshLocked(row.ID, row, now, absorbOpts{
			depsMode:   depsFromFields,
			seqMode:    seqKeep,
			clearDirty: true,
		})
		c.clearDependentReadyProjectionsLocked(row.ID)
	}
	c.markFreshLocked(now)
	c.updateStatsLocked()
	c.mu.Unlock()
	c.notifyChange("bead.created", result.Created)
	c.notifyChange("bead.updated", result.Claimed)
	return CreateAssignmentClaimResult{
		Created: cloneBead(result.Created),
		Claimed: cloneBead(result.Claimed),
	}, true, nil
}

var (
	_ CreateAssignmentClaimer               = (*CachingStore)(nil)
	_ CreateAssignmentClaimerHandleProvider = (*CachingStore)(nil)
)
