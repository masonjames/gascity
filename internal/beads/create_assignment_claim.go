package beads

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// ErrCreateAssignmentClaimUnsupported reports that a store cannot atomically
// create one fresh co-located witness and claim one exact work bead.
var ErrCreateAssignmentClaimUnsupported = errors.New("create assignment claim unsupported")

// CreateAssignmentClaimRequest describes one atomic fresh-witness assignment.
// Claim carries the exact work predicates and static assignment metadata.
// Witness must have an empty ID so the store mints a fresh row. Every key in
// CreatedWitnessIDMetadataKeys is populated on the claimed bead with the ID
// minted for Witness. The beads layer deliberately assigns no meaning to the
// witness type, labels, metadata, or dynamic metadata keys.
type CreateAssignmentClaimRequest struct {
	Claim                        AssignmentClaimRequest
	Witness                      Bead
	CreatedWitnessIDMetadataKeys []string
}

// CreateAssignmentClaimResult returns the two authoritative post-transaction
// rows. Both fields are zero when an exact predicate does not match.
type CreateAssignmentClaimResult struct {
	Created Bead
	Claimed Bead
}

// CreateAssignmentClaimer is an optional Store capability for atomically
// creating a fresh witness and claiming one exact work bead. A successful call
// returns both authoritative rows and ok=true. A predicate mismatch returns a
// zero result, false, nil and creates nothing. A missing exact work ID returns
// ErrNotFound.
type CreateAssignmentClaimer interface {
	CreateAssignmentClaim(ctx context.Context, req CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error)
}

// CreateAssignmentClaimerHandleProvider lets wrappers expose the capability
// only when their backing store honestly provides the complete atomic action.
type CreateAssignmentClaimerHandleProvider interface {
	CreateAssignmentClaimerHandle() (CreateAssignmentClaimer, bool)
}

// CreateAssignmentClaimerFor resolves a store's optional atomic
// fresh-witness assignment capability.
func CreateAssignmentClaimerFor(store Store) (CreateAssignmentClaimer, bool) {
	if store == nil {
		return nil, false
	}
	if provider, ok := store.(CreateAssignmentClaimerHandleProvider); ok {
		return provider.CreateAssignmentClaimerHandle()
	}
	claimer, ok := store.(CreateAssignmentClaimer)
	return claimer, ok
}

func normalizeCreateAssignmentClaimRequest(req CreateAssignmentClaimRequest) (CreateAssignmentClaimRequest, error) {
	claim, err := normalizeAssignmentClaimRequest(req.Claim)
	if err != nil {
		return CreateAssignmentClaimRequest{}, err
	}
	if claim.CoLocatedWitness != nil {
		return CreateAssignmentClaimRequest{}, fmt.Errorf("create assignment claim on %q: pre-existing co-located witness is not allowed", claim.ID)
	}
	if claim.RequireIdempotent {
		return CreateAssignmentClaimRequest{}, fmt.Errorf("create assignment claim on %q: idempotent-only mode is not allowed for a fresh witness", claim.ID)
	}
	witness := cloneBead(req.Witness)
	if strings.TrimSpace(witness.ID) != "" {
		return CreateAssignmentClaimRequest{}, fmt.Errorf("create assignment claim on %q: witness ID must be empty", claim.ID)
	}
	if strings.TrimSpace(witness.Title) == "" {
		return CreateAssignmentClaimRequest{}, fmt.Errorf("create assignment claim on %q: witness title is empty", claim.ID)
	}
	if strings.TrimSpace(witness.Type) == "" {
		return CreateAssignmentClaimRequest{}, fmt.Errorf("create assignment claim on %q: witness type is empty", claim.ID)
	}

	keys := make([]string, 0, len(req.CreatedWitnessIDMetadataKeys))
	seen := make(map[string]struct{}, len(req.CreatedWitnessIDMetadataKeys))
	for _, key := range req.CreatedWitnessIDMetadataKeys {
		key = strings.TrimSpace(key)
		if key == "" {
			return CreateAssignmentClaimRequest{}, fmt.Errorf("create assignment claim on %q: created-witness metadata key is empty", claim.ID)
		}
		if _, conflict := claim.AssignmentMetadata[key]; conflict {
			return CreateAssignmentClaimRequest{}, fmt.Errorf("create assignment claim on %q: created-witness metadata key %q conflicts with static assignment metadata", claim.ID, key)
		}
		if _, conflict := claim.ExpectedMetadata[key]; conflict {
			return CreateAssignmentClaimRequest{}, fmt.Errorf("create assignment claim on %q: created-witness metadata key %q conflicts with expected metadata", claim.ID, key)
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return CreateAssignmentClaimRequest{}, fmt.Errorf("create assignment claim on %q: created-witness metadata keys are required", claim.ID)
	}
	req.Claim = claim
	req.Witness = witness
	req.CreatedWitnessIDMetadataKeys = keys
	return req, nil
}

func createAssignmentClaimEffectiveRequest(req CreateAssignmentClaimRequest, createdID string) AssignmentClaimRequest {
	claim := req.Claim
	claim.AssignmentMetadata = maps.Clone(claim.AssignmentMetadata)
	if claim.AssignmentMetadata == nil {
		claim.AssignmentMetadata = make(map[string]string, len(req.CreatedWitnessIDMetadataKeys))
	}
	for _, key := range req.CreatedWitnessIDMetadataKeys {
		claim.AssignmentMetadata[key] = createdID
	}
	return claim
}

// ReadTx is the optional read surface needed by atomic multi-row predicates.
// It intentionally does not widen Tx: only backends that can read from the
// exact transaction snapshot implement it.
type ReadTx interface {
	Tx
	Get(id string) (Bead, error)
}

func runCreateAssignmentClaimAtomicTx(ctx context.Context, store Store, req CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error) {
	if !StoreSupportsAtomicTx(store) {
		return CreateAssignmentClaimResult{}, false, ErrCreateAssignmentClaimUnsupported
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	var result CreateAssignmentClaimResult
	var won bool
	// Only a witness minted by a completed callback in this invocation may make
	// a later provider retry idempotent. Without this authorization, an
	// unrelated preexisting work+witness pair with the same caller metadata
	// could be laundered as a successful fresh create.
	authorizedCreatedID := ""
	err := store.Tx("gc: create witness and claim assignment", func(tx Tx) error {
		// Native transaction providers may discard and retry this callback.
		// Never let a result from an abandoned snapshot escape.
		result = CreateAssignmentClaimResult{}
		won = false
		reader, ok := tx.(ReadTx)
		if !ok {
			return ErrCreateAssignmentClaimUnsupported
		}
		current, err := reader.Get(req.Claim.ID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return fmt.Errorf("create assignment claim on %q: %w", req.Claim.ID, ErrNotFound)
			}
			return err
		}
		if decideAssignmentClaim(current, req.Claim) != assignmentClaimApply {
			previous, matched, err := existingCreateAssignmentClaimResult(reader, current, req, authorizedCreatedID)
			if err != nil {
				return err
			}
			if matched {
				result = previous
				won = true
			}
			return nil
		}
		created, err := tx.Create(req.Witness)
		if err != nil {
			return err
		}
		if strings.TrimSpace(created.ID) == "" {
			return errors.New("create assignment claim: store returned an empty witness ID")
		}
		effective := createAssignmentClaimEffectiveRequest(req, created.ID)
		if err := tx.Update(req.Claim.ID, assignmentClaimUpdate(effective)); err != nil {
			return err
		}
		created, err = reader.Get(created.ID)
		if err != nil {
			return fmt.Errorf("create assignment claim: reading created witness %q: %w", created.ID, err)
		}
		claimed, err := reader.Get(req.Claim.ID)
		if err != nil {
			return fmt.Errorf("create assignment claim: reading claimed work %q: %w", req.Claim.ID, err)
		}
		if decideAssignmentClaim(claimed, effective) != assignmentClaimIdempotent {
			return fmt.Errorf("create assignment claim on %q: transaction readback did not match assigned post-state", req.Claim.ID)
		}
		authorizedCreatedID = created.ID
		result = CreateAssignmentClaimResult{Created: cloneBead(created), Claimed: cloneBead(claimed)}
		won = true
		return nil
	})
	return result, won, err
}

func createAssignmentClaimWithAtomicTx(ctx context.Context, store Store, req CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error) {
	result, won, err := runCreateAssignmentClaimAtomicTx(ctx, store, req)
	if err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	return result, won, nil
}

func existingCreateAssignmentClaimResult(reader ReadTx, current Bead, req CreateAssignmentClaimRequest, authorizedCreatedID string) (CreateAssignmentClaimResult, bool, error) {
	authorizedCreatedID = strings.TrimSpace(authorizedCreatedID)
	if authorizedCreatedID == "" {
		return CreateAssignmentClaimResult{}, false, nil
	}
	createdID := ""
	for _, key := range req.CreatedWitnessIDMetadataKeys {
		value := strings.TrimSpace(current.Metadata[key])
		if value == "" {
			return CreateAssignmentClaimResult{}, false, nil
		}
		if createdID == "" {
			createdID = value
			continue
		}
		if value != createdID {
			return CreateAssignmentClaimResult{}, false, nil
		}
	}
	if createdID == "" || createdID == req.Claim.ID || createdID != authorizedCreatedID {
		return CreateAssignmentClaimResult{}, false, nil
	}
	effective := createAssignmentClaimEffectiveRequest(req, createdID)
	if decideAssignmentClaim(current, effective) != assignmentClaimIdempotent {
		return CreateAssignmentClaimResult{}, false, nil
	}
	created, err := reader.Get(createdID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return CreateAssignmentClaimResult{}, false, nil
		}
		return CreateAssignmentClaimResult{}, false, err
	}
	if !createdWitnessMatchesRequest(created, req.Witness) {
		return CreateAssignmentClaimResult{}, false, nil
	}
	return CreateAssignmentClaimResult{Created: cloneBead(created), Claimed: cloneBead(current)}, true, nil
}

func createdWitnessMatchesRequest(created, requested Bead) bool {
	if strings.TrimSpace(created.ID) == "" || created.Status != "open" || created.Title != requested.Title || created.Type != requested.Type {
		return false
	}
	for _, label := range requested.Labels {
		if !slices.Contains(created.Labels, label) {
			return false
		}
	}
	for key, value := range requested.Metadata {
		actual, present := created.Metadata[key]
		if !present || actual != value {
			return false
		}
	}
	return true
}

// CreateAssignmentClaim implements the atomic fresh-witness assignment under
// the in-memory store's single state lock.
func (m *MemStore) CreateAssignmentClaim(_ context.Context, req CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error) {
	normalized, err := normalizeCreateAssignmentClaimRequest(req)
	if err != nil {
		return CreateAssignmentClaimResult{}, false, err
	}
	return m.createAssignmentClaimNormalized(normalized)
}

func (m *MemStore) createAssignmentClaimNormalized(req CreateAssignmentClaimRequest) (CreateAssignmentClaimResult, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	workIndex := m.indexOfLocked(req.Claim.ID)
	if workIndex < 0 {
		return CreateAssignmentClaimResult{}, false, fmt.Errorf("create assignment claim on %q: %w", req.Claim.ID, ErrNotFound)
	}
	if decideAssignmentClaim(m.beads[workIndex], req.Claim) != assignmentClaimApply {
		return CreateAssignmentClaimResult{}, false, nil
	}
	seq, beforeBeads, beforeDeps := m.snapshot()
	rollback := func() {
		m.seq = seq
		m.beads = beforeBeads
		m.deps = beforeDeps
	}
	created := m.createLocked(req.Witness)
	effective := createAssignmentClaimEffectiveRequest(req, created.ID)
	m.applyUpdateLocked(workIndex, assignmentClaimUpdate(effective))
	claimed := cloneBead(m.beads[workIndex])
	if decideAssignmentClaim(claimed, effective) != assignmentClaimIdempotent {
		rollback()
		return CreateAssignmentClaimResult{}, false, fmt.Errorf("create assignment claim on %q: in-memory readback did not match assigned post-state", req.Claim.ID)
	}
	return CreateAssignmentClaimResult{Created: created, Claimed: claimed}, true, nil
}

var _ CreateAssignmentClaimer = (*MemStore)(nil)
