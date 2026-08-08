package beads

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var _ PredicateConditionalWriter = (*SQLiteStore)(nil)

// UpdateIfPredicate evaluates predicate and applies opts in one SQLite write
// transaction, returning the exact committed poststate.
func (s *SQLiteStore) UpdateIfPredicate(id string, predicate BeadPredicate, opts UpdateOpts) (Bead, error) {
	if err := s.ensureOpen(); err != nil {
		return Bead{}, err
	}
	if err := validatePredicateConditionalUpdate(id, predicate, opts); err != nil {
		return Bead{}, err
	}
	opts = clonePredicateConditionalUpdate(opts)
	return s.predicateConditionalWrite(id, predicate, func(b Bead) Bead {
		next := applySQLiteUpdateOpts(b, opts)
		next.UpdatedAt = time.Now()
		return next
	})
}

// CloseIfPredicate evaluates predicate, applies finalPatch, and closes in one
// SQLite write transaction, returning the exact committed poststate.
func (s *SQLiteStore) CloseIfPredicate(id string, predicate BeadPredicate, finalPatch UpdateOpts) (Bead, error) {
	if err := s.ensureOpen(); err != nil {
		return Bead{}, err
	}
	if err := validatePredicateConditionalClose(id, predicate, finalPatch); err != nil {
		return Bead{}, err
	}
	finalPatch = clonePredicateConditionalUpdate(finalPatch)
	return s.predicateConditionalWrite(id, predicate, func(b Bead) Bead {
		next := applySQLiteUpdateOpts(b, finalPatch)
		next.Status = "closed"
		next.UpdatedAt = time.Now()
		return next
	})
}

func (s *SQLiteStore) predicateConditionalWrite(id string, predicate BeadPredicate, mutate func(Bead) Bead) (Bead, error) {
	var post Bead
	err := retryOnBusy(func() error {
		post = Bead{}
		ctx := context.Background()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("sqlite predicate-conditional write: begin tx: %w", err)
		}
		defer tx.Rollback() //nolint:errcheck
		before, err := s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := predicate(cloneBead(before)); err != nil {
			return err
		}
		next := mutate(cloneBead(before))
		if err := s.upsertBeadTx(ctx, tx, next); err != nil {
			return err
		}
		after, err := s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := s.bumpClaimFenceIfOwnershipTransitionTx(ctx, tx, before, &after); err != nil {
			return err
		}
		// A claim-fence transition is stored outside bead_json, so hydrate the
		// exact poststate again after the transition before committing.
		post, err = s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if post.ID == "" {
			return Bead{}, err
		}
		current, getErr := s.Get(id)
		if getErr == nil && predicateConditionalPoststateEqual(post, current) {
			return cloneBead(current), nil
		}
		if getErr != nil {
			return Bead{}, errors.Join(err, fmt.Errorf("reconciling sqlite predicate-conditional poststate for %q: %w", id, getErr))
		}
		return Bead{}, err
	}
	return cloneBead(post), nil
}
