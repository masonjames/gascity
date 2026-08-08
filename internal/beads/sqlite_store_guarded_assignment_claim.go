package beads

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"time"
)

// ClaimAssignment atomically checks the exact assignment snapshot and writes
// the owner, status, and assignment witness in one SQLite transaction.
func (s *SQLiteStore) ClaimAssignment(ctx context.Context, req AssignmentClaimRequest) (Bead, bool, error) {
	if err := s.ensureOpen(); err != nil {
		return Bead{}, false, err
	}
	normalized, err := normalizeAssignmentClaimRequest(req)
	if err != nil {
		return Bead{}, false, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var claimed Bead
	var ok bool
	err = retryOnBusy(func() error {
		claimed = Bead{}
		ok = false
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("sqlite guarded assignment claim: begin tx: %w", err)
		}
		defer tx.Rollback() //nolint:errcheck
		current, err := s.getTx(ctx, tx, normalized.ID)
		if err != nil {
			if errors.Is(err, ErrNotFound) || errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("guarded assignment claim on %q: %w", normalized.ID, ErrNotFound)
			}
			return err
		}
		if normalized.CoLocatedWitness != nil {
			witness, witnessErr := s.getTx(ctx, tx, normalized.CoLocatedWitness.ID)
			if witnessErr != nil {
				if errors.Is(witnessErr, ErrNotFound) || errors.Is(witnessErr, sql.ErrNoRows) {
					return tx.Commit()
				}
				return witnessErr
			}
			if !assignmentClaimCoLocatedWitnessMatches(witness, normalized) {
				return tx.Commit()
			}
		}
		switch decideAssignmentClaim(current, normalized) {
		case assignmentClaimMismatch:
			return tx.Commit()
		case assignmentClaimIdempotent:
			claimed = cloneBead(current)
			ok = true
			return tx.Commit()
		case assignmentClaimApply:
			before := current
			current.Status = "in_progress"
			current.Assignee = normalized.Actor
			current.Metadata = maps.Clone(current.Metadata)
			if current.Metadata == nil {
				current.Metadata = make(map[string]string, len(normalized.AssignmentMetadata))
			}
			for key, value := range normalized.AssignmentMetadata {
				current.Metadata[key] = value
			}
			current.UpdatedAt = time.Now()
			if err := s.upsertBeadTx(ctx, tx, current); err != nil {
				return err
			}
			if err := s.bumpClaimFenceIfOwnershipTransitionTx(ctx, tx, before, &current); err != nil {
				return err
			}
			current, err = s.getTx(ctx, tx, normalized.ID)
			if err != nil {
				return fmt.Errorf("guarded assignment claim on %q: re-reading write: %w", normalized.ID, err)
			}
			if decideAssignmentClaim(current, normalized) != assignmentClaimIdempotent {
				return fmt.Errorf("guarded assignment claim on %q: transaction readback did not match assigned post-state", normalized.ID)
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("sqlite guarded assignment claim: commit: %w", err)
			}
			claimed = cloneBead(current)
			ok = true
			return nil
		default:
			return errors.New("sqlite guarded assignment claim: invalid decision")
		}
	})
	if err != nil {
		return Bead{}, false, err
	}
	return claimed, ok, nil
}

var _ GuardedAssignmentClaimer = (*SQLiteStore)(nil)
