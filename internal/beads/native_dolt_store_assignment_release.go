package beads

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"

	beadslib "github.com/steveyegge/beads"
)

type nativeAssignmentReleaseAllTierTransactionProvider interface {
	NativeAssignmentReleaseAllTierTransactions() bool
}

// AssignmentReleaserHandle exposes native atomic release only when the pinned
// storage's transaction-scoped search is known to merge every storage tier.
// The server Dolt transaction searches only the durable issues table and can
// miss no-history rows in wisps, so it must fail closed before any mutation.
func (s *NativeDoltStore) AssignmentReleaserHandle() (AssignmentReleaser, bool) {
	storage, release, err := s.acquireStorage()
	if err != nil {
		return nil, false
	}
	supported := nativeAssignmentReleaseHasAllTierTransaction(storage)
	release()
	if !supported {
		return nil, false
	}
	return s, true
}

func nativeAssignmentReleaseHasAllTierTransaction(storage beadslib.Storage) bool {
	if provider, ok := storage.(nativeAssignmentReleaseAllTierTransactionProvider); ok {
		return provider.NativeAssignmentReleaseAllTierTransactions()
	}
	// The pinned public beads API does not expose backend identity or an
	// all-tier-transaction capability. Recognize only its concrete embedded
	// store, whose embeddedTransaction.SearchIssues delegates to the all-tier
	// issueops merge. Unknown wrappers and the server DoltStore fail closed.
	typ := reflect.TypeOf(storage)
	if typ == nil || typ.Kind() != reflect.Pointer {
		return false
	}
	elem := typ.Elem()
	return elem.PkgPath() == "github.com/steveyegge/beads/internal/storage/embeddeddolt" && elem.Name() == "EmbeddedDoltStore"
}

// ReleaseAssignment verifies the complete release predicate and applies the
// release through one native Dolt transaction. Every retry re-reads both the
// work row and the co-located absence predicate from its own transaction
// snapshot.
func (s *NativeDoltStore) ReleaseAssignment(ctx context.Context, req AssignmentReleaseRequest) (Bead, bool, error) {
	normalized, err := normalizeAssignmentReleaseRequest(req)
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
	if !nativeAssignmentReleaseHasAllTierTransaction(storage) {
		release()
		return Bead{}, false, ErrAssignmentReleaseUnsupported
	}
	defer release()
	opCtx, cancel := nativeDoltOperationContext(ctx)
	defer cancel()

	for attempt := 1; attempt <= nativeGuardedAssignmentAttempts; attempt++ {
		released, won, authorized, releaseErr := s.releaseAssignmentOnce(opCtx, storage, normalized)
		if releaseErr == nil {
			return released, won, nil
		}
		if errors.Is(releaseErr, ErrNotFound) {
			return Bead{}, false, releaseErr
		}

		// Native providers can return an error after the SQL transaction is
		// visible but before its Dolt version commit is staged. Only a callback
		// that reached an exact released readback authorizes recovery.
		if authorized {
			observed, readErr := nativeAssignmentReleaseReadback(opCtx, storage, normalized)
			if readErr == nil && assignmentReleasePostStateMatches(observed, normalized) {
				return cloneBead(observed), true, nil
			}
			if readErr != nil {
				releaseErr = errors.Join(releaseErr, fmt.Errorf("re-reading assignment release on %q after error: %w", normalized.ID, readErr))
			}
		}
		if !isNativeDoltSerializationConflict(releaseErr) || attempt == nativeGuardedAssignmentAttempts {
			return Bead{}, false, releaseErr
		}
		delay := time.Duration(attempt) * nativeGuardedAssignmentRetryBackoff
		select {
		case <-opCtx.Done():
			return Bead{}, false, errors.Join(releaseErr, opCtx.Err())
		case <-time.After(delay):
		}
	}
	return Bead{}, false, errors.New("native assignment release: retry loop exhausted")
}

func (s *NativeDoltStore) releaseAssignmentOnce(ctx context.Context, storage beadslib.Storage, req AssignmentReleaseRequest) (Bead, bool, bool, error) {
	var released Bead
	var won, authorized bool
	commitMsg := fmt.Sprintf("gc: guarded assignment release on bead %s", req.ID)
	err := storage.RunInTransaction(ctx, commitMsg, func(tx beadslib.Transaction) error {
		// Providers may discard and retry a callback. State from an abandoned
		// snapshot must never authorize the result of the final invocation.
		released = Bead{}
		won = false
		authorized = false

		issue, err := tx.GetIssue(ctx, req.ID)
		if err != nil {
			return nativeStoreError(req.ID, err)
		}
		if issue == nil {
			return fmt.Errorf("assignment release on %q: %w", req.ID, ErrNotFound)
		}
		labels, err := tx.GetLabels(ctx, req.ID)
		if err != nil {
			return nativeStoreError(req.ID, err)
		}
		current, err := nativeAssignmentClaimBead(issue, labels, req.ID)
		if err != nil {
			return err
		}
		if !assignmentReleaseWorkMatches(current, req) {
			return nil
		}
		blocked, err := nativeAssignmentReleaseBlocked(ctx, tx, req.AbsentCoLocatedMatch)
		if err != nil {
			return err
		}
		if blocked {
			return nil
		}

		metadata := maps.Clone(current.Metadata)
		if metadata == nil {
			metadata = make(map[string]string, len(req.ReleaseMetadata))
		}
		for key, value := range req.ReleaseMetadata {
			metadata[key] = value
		}
		raw, err := metadataRawFromMap(metadata)
		if err != nil {
			return err
		}
		if err := tx.UpdateIssue(ctx, req.ID, map[string]interface{}{
			"status":   "open",
			"assignee": "",
			"metadata": raw,
		}, s.actor); err != nil {
			return nativeStoreError(req.ID, err)
		}
		written, err := tx.GetIssue(ctx, req.ID)
		if err != nil {
			return nativeStoreError(req.ID, err)
		}
		if written == nil {
			return fmt.Errorf("assignment release on %q: transaction readback returned nil", req.ID)
		}
		writtenLabels, err := tx.GetLabels(ctx, req.ID)
		if err != nil {
			return nativeStoreError(req.ID, err)
		}
		released, err = nativeAssignmentClaimBead(written, writtenLabels, req.ID)
		if err != nil {
			return err
		}
		if !assignmentReleasePostStateMatches(released, req) {
			return fmt.Errorf("assignment release on %q: transaction readback did not match released post-state", req.ID)
		}
		won = true
		authorized = true
		return nil
	})
	if err != nil {
		return Bead{}, false, authorized, nativeStoreError(req.ID, err)
	}
	if !won {
		return Bead{}, false, false, nil
	}
	return cloneBead(released), true, true, nil
}

func nativeAssignmentReleaseBlocked(ctx context.Context, tx beadslib.Transaction, predicate *CoLocatedMatchPredicate) (bool, error) {
	status := beadslib.Status(predicate.ExpectedStatus)
	issues, err := tx.SearchIssues(ctx, "", beadslib.IssueFilter{Status: &status})
	if err != nil {
		return false, err
	}
	for _, issue := range issues {
		if issue == nil || string(issue.Status) != predicate.ExpectedStatus {
			continue
		}
		labels, err := tx.GetLabels(ctx, issue.ID)
		if err != nil {
			return false, nativeStoreError(issue.ID, err)
		}
		row, err := nativeAssignmentClaimBead(issue, labels, issue.ID)
		if err != nil {
			return false, err
		}
		if coLocatedMatchPredicateMatches(row, predicate) {
			return true, nil
		}
	}
	return false, nil
}

func nativeAssignmentReleaseReadback(ctx context.Context, storage beadslib.Storage, req AssignmentReleaseRequest) (Bead, error) {
	issue, err := storage.GetIssue(ctx, req.ID)
	if err != nil {
		return Bead{}, nativeStoreError(req.ID, err)
	}
	if issue == nil {
		return Bead{}, fmt.Errorf("re-reading assignment release on %q: %w", req.ID, ErrNotFound)
	}
	labels, err := storage.GetLabels(ctx, req.ID)
	if err != nil {
		return Bead{}, nativeStoreError(req.ID, err)
	}
	return nativeAssignmentClaimBead(issue, labels, req.ID)
}

var (
	_ AssignmentReleaser               = (*NativeDoltStore)(nil)
	_ AssignmentReleaserHandleProvider = (*NativeDoltStore)(nil)
)
