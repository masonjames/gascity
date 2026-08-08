package beads

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	beadslib "github.com/steveyegge/beads"
)

const (
	nativeGuardedAssignmentAttempts     = 3
	nativeGuardedAssignmentRetryBackoff = 25 * time.Millisecond
)

// ClaimAssignment atomically checks exact route, label, and co-located witness
// guards and writes the assignment plus witness through one native Dolt
// transaction. It
// intentionally does not make NativeDoltStore a ConditionalWriter: this narrow
// transaction needs no revision token and must not widen the unrelated
// revision-CAS capability.
func (s *NativeDoltStore) ClaimAssignment(ctx context.Context, req AssignmentClaimRequest) (Bead, bool, error) {
	normalized, err := normalizeAssignmentClaimRequest(req)
	if err != nil {
		return Bead{}, false, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	storage, release, err := s.acquireStorage()
	if err != nil {
		return Bead{}, false, err
	}
	defer release()
	opCtx, cancel := nativeDoltOperationContext(ctx)
	defer cancel()
	for attempt := 1; attempt <= nativeGuardedAssignmentAttempts; attempt++ {
		claimed, ok, attemptAuthorizedPostState, claimErr := s.claimAssignmentOnce(opCtx, storage, normalized)
		if claimErr == nil {
			return claimed, ok, nil
		}
		// A missing/colliding exact ID is a definite pre-mutation failure, not
		// an ambiguous commit that needs reconciliation.
		if errors.Is(claimErr, ErrNotFound) {
			return Bead{}, false, claimErr
		}

		// Both pinned native providers can surface an error after their SQL row
		// commit but before/while creating the Dolt version commit. Re-read the
		// authoritative row before reporting failure. An exact actor plus
		// assignment-metadata post-state proves this attempt succeeded across later
		// co-located-witness drift only when its callback had already passed that
		// witness and reached an authorized idempotent/update readback. Otherwise the
		// current witness remains required before accepting an idempotent row or
		// retrying an unchanged eligible row after a known-not-committed conflict.
		observed, witnessMatches, readErr := nativeAssignmentClaimReadback(opCtx, storage, normalized)
		if readErr != nil {
			if errors.Is(readErr, ErrNotFound) {
				return Bead{}, false, readErr
			}
			return Bead{}, false, errors.Join(claimErr,
				fmt.Errorf("re-reading guarded assignment claim on %q after error: %w", normalized.ID, readErr))
		}
		decision := decideAssignmentClaim(observed, normalized)
		if decision == assignmentClaimIdempotent {
			if attemptAuthorizedPostState || witnessMatches {
				return cloneBead(observed), true, nil
			}
			return Bead{}, false, nil
		}
		if !witnessMatches {
			return Bead{}, false, nil
		}
		switch decision {
		case assignmentClaimMismatch:
			return Bead{}, false, nil
		case assignmentClaimApply:
			if !isNativeDoltSerializationConflict(claimErr) || attempt == nativeGuardedAssignmentAttempts {
				return Bead{}, false, claimErr
			}
		default:
			return Bead{}, false, errors.New("native guarded assignment claim: invalid post-error decision")
		}

		delay := time.Duration(attempt) * nativeGuardedAssignmentRetryBackoff
		select {
		case <-opCtx.Done():
			return Bead{}, false, errors.Join(claimErr, opCtx.Err())
		case <-time.After(delay):
		}
	}
	return Bead{}, false, errors.New("native guarded assignment claim: retry loop exhausted")
}

func (s *NativeDoltStore) claimAssignmentOnce(ctx context.Context, storage beadslib.Storage, req AssignmentClaimRequest) (Bead, bool, bool, error) {
	var claimed Bead
	var ok bool
	var authorizedPostState bool
	commitMsg := fmt.Sprintf("gc: guarded assignment claim on bead %s", req.ID)
	err := storage.RunInTransaction(ctx, commitMsg, func(tx beadslib.Transaction) error {
		// Pinned providers may discard a noncommitted callback and invoke it
		// again. Only the latest invocation may escape RunInTransaction.
		claimed = Bead{}
		ok = false
		authorizedPostState = false

		issue, err := tx.GetIssue(ctx, req.ID)
		if err != nil {
			return nativeStoreError(req.ID, err)
		}
		if issue == nil {
			return fmt.Errorf("guarded assignment claim on %q: %w", req.ID, ErrNotFound)
		}
		if req.CoLocatedWitness != nil {
			witnessIssue, witnessErr := tx.GetIssue(ctx, req.CoLocatedWitness.ID)
			if witnessErr != nil {
				if errors.Is(nativeStoreError(req.CoLocatedWitness.ID, witnessErr), ErrNotFound) {
					return nil
				}
				return nativeStoreError(req.CoLocatedWitness.ID, witnessErr)
			}
			if witnessIssue == nil {
				return nil
			}
			witnessLabels, witnessErr := tx.GetLabels(ctx, req.CoLocatedWitness.ID)
			if witnessErr != nil {
				return nativeStoreError(req.CoLocatedWitness.ID, witnessErr)
			}
			witnessBead, witnessErr := nativeAssignmentClaimBead(witnessIssue, witnessLabels, req.CoLocatedWitness.ID)
			if witnessErr != nil {
				return witnessErr
			}
			if !assignmentClaimCoLocatedWitnessMatches(witnessBead, req) {
				return nil
			}
		}
		labels, err := tx.GetLabels(ctx, req.ID)
		if err != nil {
			return nativeStoreError(req.ID, err)
		}
		// Transaction.GetIssue projects only the issue row in the pinned beads
		// provider. Canonical holds live in the separate labels table, so they
		// must be read from the same transaction snapshot before eligibility is
		// decided.
		current, err := nativeAssignmentClaimBead(issue, labels, req.ID)
		if err != nil {
			return err
		}
		switch decideAssignmentClaim(current, req) {
		case assignmentClaimMismatch:
			return nil
		case assignmentClaimIdempotent:
			claimed = cloneBead(current)
			ok = true
			authorizedPostState = true
			return nil
		case assignmentClaimApply:
			metadata := maps.Clone(current.Metadata)
			if metadata == nil {
				metadata = make(map[string]string, len(req.AssignmentMetadata))
			}
			for key, value := range req.AssignmentMetadata {
				metadata[key] = value
			}
			raw, err := metadataRawFromMap(metadata)
			if err != nil {
				return err
			}
			if err := tx.UpdateIssue(ctx, req.ID, map[string]interface{}{
				"status":   "in_progress",
				"assignee": req.Actor,
				"metadata": raw,
			}, req.Actor); err != nil {
				return nativeStoreError(req.ID, err)
			}
			written, err := tx.GetIssue(ctx, req.ID)
			if err != nil {
				return nativeStoreError(req.ID, err)
			}
			if written == nil {
				return fmt.Errorf("guarded assignment claim on %q: transaction readback returned nil", req.ID)
			}
			writtenLabels, err := tx.GetLabels(ctx, req.ID)
			if err != nil {
				return nativeStoreError(req.ID, err)
			}
			claimed, err = nativeAssignmentClaimBead(written, writtenLabels, req.ID)
			if err != nil {
				return err
			}
			if decideAssignmentClaim(claimed, req) != assignmentClaimIdempotent {
				return fmt.Errorf("guarded assignment claim on %q: transaction readback did not match assigned post-state", req.ID)
			}
			ok = true
			authorizedPostState = true
			return nil
		default:
			return errors.New("native guarded assignment claim: invalid decision")
		}
	})
	if err != nil {
		return Bead{}, false, authorizedPostState, nativeStoreError(req.ID, err)
	}
	if !ok {
		return Bead{}, false, false, nil
	}
	return cloneBead(claimed), true, authorizedPostState, nil
}

func nativeAssignmentClaimReadback(ctx context.Context, storage beadslib.Storage, req AssignmentClaimRequest) (Bead, bool, error) {
	if req.CoLocatedWitness != nil {
		return nativeAssignmentClaimWitnessedReadback(ctx, storage, req)
	}
	issue, err := storage.GetIssue(ctx, req.ID)
	if err != nil {
		return Bead{}, false, nativeStoreError(req.ID, err)
	}
	if issue == nil {
		return Bead{}, false, fmt.Errorf("re-reading guarded assignment claim on %q: %w", req.ID, ErrNotFound)
	}
	labels, err := storage.GetLabels(ctx, req.ID)
	if err != nil {
		return Bead{}, false, nativeStoreError(req.ID, err)
	}
	bead, err := nativeAssignmentClaimBead(issue, labels, req.ID)
	return bead, true, err
}

func nativeAssignmentClaimWitnessedReadback(ctx context.Context, storage beadslib.Storage, req AssignmentClaimRequest) (Bead, bool, error) {
	var observed Bead
	var witnessMatches bool
	var readComplete bool
	commitMsg := fmt.Sprintf("gc: read back guarded assignment claim on bead %s", req.ID)
	err := storage.RunInTransaction(ctx, commitMsg, func(tx beadslib.Transaction) error {
		// RunInTransaction may retry this callback after discarding its prior
		// snapshot. Never let that abandoned snapshot authorize the final read.
		observed = Bead{}
		witnessMatches = false
		readComplete = false

		issue, err := tx.GetIssue(ctx, req.ID)
		if err != nil {
			return nativeStoreError(req.ID, err)
		}
		if issue == nil {
			return fmt.Errorf("re-reading guarded assignment claim on %q: %w", req.ID, ErrNotFound)
		}
		labels, err := tx.GetLabels(ctx, req.ID)
		if err != nil {
			return nativeStoreError(req.ID, err)
		}
		observed, err = nativeAssignmentClaimBead(issue, labels, req.ID)
		if err != nil {
			return err
		}

		witnessIssue, err := tx.GetIssue(ctx, req.CoLocatedWitness.ID)
		if err != nil {
			if errors.Is(nativeStoreError(req.CoLocatedWitness.ID, err), ErrNotFound) {
				readComplete = true
				return nil
			}
			return nativeStoreError(req.CoLocatedWitness.ID, err)
		}
		if witnessIssue == nil {
			readComplete = true
			return nil
		}
		witnessLabels, err := tx.GetLabels(ctx, req.CoLocatedWitness.ID)
		if err != nil {
			return nativeStoreError(req.CoLocatedWitness.ID, err)
		}
		witness, err := nativeAssignmentClaimBead(witnessIssue, witnessLabels, req.CoLocatedWitness.ID)
		if err != nil {
			return err
		}
		witnessMatches = assignmentClaimCoLocatedWitnessMatches(witness, req)
		readComplete = true
		return nil
	})
	// This is a read-only recovery transaction. Once the callback completed,
	// its co-located snapshot is authoritative even if the provider reports a
	// later version-staging failure while closing the transaction.
	if readComplete {
		return cloneBead(observed), witnessMatches, nil
	}
	if err != nil {
		return Bead{}, false, nativeStoreError(req.ID, err)
	}
	return Bead{}, false, errors.New("native guarded assignment claim: incomplete witnessed readback")
}

func nativeAssignmentClaimBead(issue *beadslib.Issue, labels []string, expectedID string) (Bead, error) {
	if issue == nil {
		return Bead{}, fmt.Errorf("guarded assignment claim on %q: %w", expectedID, ErrNotFound)
	}
	issue.Labels = labels
	bead, err := beadFromNativeIssue(issue)
	if err != nil {
		return Bead{}, err
	}
	// beadFromNativeIssue intentionally projects bd's extended statuses into
	// Gas City's three-status view. The guard must compare the native status
	// exactly, before that lossy projection can turn blocked/deferred into open
	// and make non-claimable work appear eligible.
	bead.Status = string(issue.Status)
	if bead.ID != expectedID {
		return Bead{}, fmt.Errorf("guarded assignment claim on %q resolved to %q: %w", expectedID, bead.ID, ErrIDCollision)
	}
	return bead, nil
}

var _ GuardedAssignmentClaimer = (*NativeDoltStore)(nil)
