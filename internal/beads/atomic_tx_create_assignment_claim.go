package beads

import (
	"context"
	"errors"
	"fmt"

	beadslib "github.com/steveyegge/beads"
)

// CreateAssignmentClaim uses one rollback-capable SQLite transaction for the
// exact read, fresh create, assignment, and both authoritative readbacks.
func (s *SQLiteStore) CreateAssignmentClaim(ctx context.Context, req CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error) {
	if err := s.ensureOpen(); err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	normalized, err := normalizeCreateAssignmentClaimRequest(req)
	if err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	return createAssignmentClaimWithAtomicTx(ctx, s, normalized)
}

// CreateAssignmentClaim uses one rollback-capable native Dolt transaction for
// the exact read, fresh create, assignment, and both authoritative readbacks.
func (s *NativeDoltStore) CreateAssignmentClaim(ctx context.Context, req CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error) {
	normalized, err := normalizeCreateAssignmentClaimRequest(req)
	if err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	result, won, err := runCreateAssignmentClaimAtomicTx(ctx, s, normalized)
	if err == nil {
		return result, won, nil
	}
	if errors.Is(err, ErrNotFound) || !won || result.Created.ID == "" {
		return CreateAssignmentClaimResult{}, false, err
	}
	// Native providers commit the SQL transaction before version staging. If
	// staging fails, RunInTransaction returns an error even though both rows are
	// already visible. Reconcile only the witness ID produced by an authorized
	// callback attempt; never infer an unrelated pre-existing post-state.
	authoritative, matched, readErr := s.readCreateAssignmentClaimPostState(ctx, normalized, result.Created.ID)
	if readErr != nil {
		return CreateAssignmentClaimResult{}, false, errors.Join(err, readErr)
	}
	if matched {
		return authoritative, true, nil
	}
	return CreateAssignmentClaimResult{}, false, err
}

func (s *NativeDoltStore) readCreateAssignmentClaimPostState(ctx context.Context, req CreateAssignmentClaimRequest, expectedCreatedID string) (CreateAssignmentClaimResult, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	storage, release, err := s.acquireStorage()
	if err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	defer release()
	opCtx, cancel := nativeDoltOperationContext(ctx)
	defer cancel()
	var result CreateAssignmentClaimResult
	var matched, readComplete bool
	err = storage.RunInTransaction(opCtx, fmt.Sprintf("gc: read back create assignment claim on bead %s", req.Claim.ID), func(tx beadslib.Transaction) error {
		// A provider retry discards the prior snapshot. Only the last completed
		// callback may authorize recovery.
		result = CreateAssignmentClaimResult{}
		matched = false
		readComplete = false
		reader := &nativeDoltTx{store: s, ctx: opCtx, tx: tx}
		claimed, err := reader.Get(req.Claim.ID)
		if err != nil {
			return err
		}
		effective := createAssignmentClaimEffectiveRequest(req, expectedCreatedID)
		if decideAssignmentClaim(claimed, effective) != assignmentClaimIdempotent {
			readComplete = true
			return nil
		}
		created, err := reader.Get(expectedCreatedID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				readComplete = true
				return nil
			}
			return err
		}
		if !createdWitnessMatchesRequest(created, req.Witness) {
			readComplete = true
			return nil
		}
		result = CreateAssignmentClaimResult{Created: cloneBead(created), Claimed: cloneBead(claimed)}
		matched = true
		readComplete = true
		return nil
	})
	// A read-only callback can complete before the provider reports the same
	// post-SQL version-staging error. Its one-transaction snapshot remains the
	// authoritative answer.
	if readComplete {
		return result, matched, nil
	}
	if err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	return CreateAssignmentClaimResult{}, false, errors.New("native create assignment claim: incomplete post-state readback")
}

var (
	_ CreateAssignmentClaimer = (*SQLiteStore)(nil)
	_ CreateAssignmentClaimer = (*NativeDoltStore)(nil)
)
