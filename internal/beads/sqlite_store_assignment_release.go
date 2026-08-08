package beads

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// AssignmentReleaserHandle exposes the atomic release only for a writable
// schema carrying the revision column required by the exact work predicate.
func (s *SQLiteStore) AssignmentReleaserHandle() (AssignmentReleaser, bool) {
	if s == nil || s.ensureOpen() != nil || s.readOnly || !s.hasRevisionColumn {
		return nil, false
	}
	return s, true
}

// ReleaseAssignment verifies the exact work snapshot, forbidden labels, and
// co-located absence predicate in one SQLite transaction before releasing the
// assignment and merging release metadata.
func (s *SQLiteStore) ReleaseAssignment(ctx context.Context, req AssignmentReleaseRequest) (Bead, bool, error) {
	if err := s.ensureOpen(); err != nil {
		return Bead{}, false, err
	}
	if s.readOnly || !s.hasRevisionColumn {
		return Bead{}, false, ErrAssignmentReleaseUnsupported
	}
	normalized, err := normalizeAssignmentReleaseRequest(req)
	if err != nil {
		return Bead{}, false, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var released Bead
	var won bool
	err = retryOnBusy(func() error {
		released = Bead{}
		won = false
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("sqlite assignment release: begin tx: %w", err)
		}
		defer tx.Rollback() //nolint:errcheck

		current, err := s.getTx(ctx, tx, normalized.ID)
		if err != nil {
			if errors.Is(err, ErrNotFound) || errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("assignment release on %q: %w", normalized.ID, ErrNotFound)
			}
			return err
		}
		if !assignmentReleaseWorkMatches(current, normalized) {
			return tx.Commit()
		}
		blocked, err := s.assignmentReleaseBlockedTx(ctx, tx, normalized.AbsentCoLocatedMatch)
		if err != nil {
			return err
		}
		if blocked {
			return tx.Commit()
		}

		before := current
		current = applySQLiteUpdateOpts(current, assignmentReleaseUpdate(normalized))
		current.UpdatedAt = time.Now()
		if err := s.upsertBeadTx(ctx, tx, current); err != nil {
			return err
		}
		if err := s.bumpClaimFenceIfOwnershipTransitionTx(ctx, tx, before, &current); err != nil {
			return err
		}
		current, err = s.getTx(ctx, tx, normalized.ID)
		if err != nil {
			return fmt.Errorf("assignment release on %q: re-reading write: %w", normalized.ID, err)
		}
		if !assignmentReleasePostStateMatches(current, normalized) {
			return fmt.Errorf("assignment release on %q: transaction readback did not match released post-state", normalized.ID)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("sqlite assignment release: commit: %w", err)
		}
		released = cloneBead(current)
		won = true
		return nil
	})
	if err != nil {
		return Bead{}, false, err
	}
	return released, won, nil
}

func (s *SQLiteStore) assignmentReleaseBlockedTx(ctx context.Context, tx *sql.Tx, predicate *CoLocatedMatchPredicate) (bool, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT `+s.sqliteBeadProjection()+` FROM beads b WHERE b.status=?`,
		predicate.ExpectedStatus,
	)
	if err != nil {
		return false, fmt.Errorf("sqlite assignment release: querying co-located matches: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		row, err := scanSQLiteBead(rows)
		if err != nil {
			return false, fmt.Errorf("sqlite assignment release: scanning co-located match: %w", err)
		}
		if coLocatedMatchPredicateMatches(row, predicate) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("sqlite assignment release: reading co-located matches: %w", err)
	}
	return false, nil
}

var (
	_ AssignmentReleaser               = (*SQLiteStore)(nil)
	_ AssignmentReleaserHandleProvider = (*SQLiteStore)(nil)
)
