package beads

import (
	"context"
	"errors"
	"fmt"

	beadslib "github.com/steveyegge/beads"
)

var _ PredicateConditionalWriter = (*NativeDoltStore)(nil)

// UpdateIfPredicate evaluates predicate over a transaction-hydrated bead and
// applies opts in that same Native Dolt transaction. Callback-local result
// state is reset on every invocation because the pinned storage may retry the
// callback after discarding a serialization-conflicted attempt.
func (s *NativeDoltStore) UpdateIfPredicate(id string, predicate BeadPredicate, opts UpdateOpts) (Bead, error) {
	if err := validatePredicateConditionalUpdate(id, predicate, opts); err != nil {
		return Bead{}, err
	}
	opts = clonePredicateConditionalUpdate(opts)
	storage, release, err := s.acquireStorage()
	if err != nil {
		return Bead{}, err
	}
	defer release()
	ctx, cancel := nativeDoltOperationContext(context.TODO())
	defer cancel()

	var post Bead
	err = storage.RunInTransaction(ctx, fmt.Sprintf("gc: predicate update bead %s", id), func(tx beadslib.Transaction) error {
		post = Bead{}
		current, err := nativePredicateConditionalBead(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := predicate(cloneBead(current)); err != nil {
			return err
		}
		if err := s.applyUpdateInTx(ctx, tx, id, opts); err != nil {
			return err
		}
		post, err = nativePredicateConditionalBead(ctx, tx, id)
		return err
	})
	if err != nil {
		return s.reconcilePredicateConditionalPoststate(id, post, nativeStoreError(id, err))
	}
	return cloneBead(post), nil
}

// CloseIfPredicate evaluates predicate, applies finalPatch, and closes the
// bead inside one Native Dolt transaction. The exact closed poststate is read
// back from the transaction before commit.
func (s *NativeDoltStore) CloseIfPredicate(id string, predicate BeadPredicate, finalPatch UpdateOpts) (Bead, error) {
	if err := validatePredicateConditionalClose(id, predicate, finalPatch); err != nil {
		return Bead{}, err
	}
	finalPatch = clonePredicateConditionalUpdate(finalPatch)
	storage, release, err := s.acquireStorage()
	if err != nil {
		return Bead{}, err
	}
	defer release()
	ctx, cancel := nativeDoltOperationContext(context.TODO())
	defer cancel()

	var post Bead
	err = storage.RunInTransaction(ctx, fmt.Sprintf("gc: predicate close bead %s", id), func(tx beadslib.Transaction) error {
		post = Bead{}
		current, err := nativePredicateConditionalBead(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := predicate(cloneBead(current)); err != nil {
			return err
		}
		if !isEmptyUpdateOpts(finalPatch) {
			if err := s.applyUpdateInTx(ctx, tx, id, finalPatch); err != nil {
				return err
			}
		}
		if err := s.applyCloseInTx(ctx, tx, id); err != nil {
			return err
		}
		post, err = nativePredicateConditionalBead(ctx, tx, id)
		return err
	})
	if err != nil {
		return s.reconcilePredicateConditionalPoststate(id, post, nativeStoreError(id, err))
	}
	return cloneBead(post), nil
}

func (s *NativeDoltStore) reconcilePredicateConditionalPoststate(id string, expected Bead, writeErr error) (Bead, error) {
	if expected.ID == "" {
		return Bead{}, writeErr
	}
	current, err := s.Get(id)
	if err != nil {
		return Bead{}, errors.Join(writeErr, fmt.Errorf("reconciling predicate-conditional poststate for %q: %w", id, err))
	}
	if !predicateConditionalPoststateEqual(expected, current) {
		return Bead{}, writeErr
	}
	return cloneBead(current), nil
}

func nativePredicateConditionalBead(ctx context.Context, tx beadslib.Transaction, id string) (Bead, error) {
	issue, err := tx.GetIssue(ctx, id)
	if err != nil {
		return Bead{}, nativeStoreError(id, err)
	}
	if issue == nil {
		return Bead{}, fmt.Errorf("predicate-conditional read on %q: %w", id, ErrNotFound)
	}
	labels, err := tx.GetLabels(ctx, id)
	if err != nil {
		return Bead{}, nativeStoreError(id, err)
	}
	dependencies, err := tx.GetDependencyRecords(ctx, id)
	if err != nil {
		return Bead{}, nativeStoreError(id, err)
	}
	clonedIssue := *issue
	clonedIssue.Labels = append([]string(nil), labels...)
	clonedIssue.Dependencies = cloneNativeDependencies(dependencies)
	bead, err := beadFromNativeIssue(&clonedIssue)
	if err != nil {
		return Bead{}, err
	}
	return cloneBead(bead), nil
}
