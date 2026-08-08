package beads

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
)

// ErrPredicateConditionalWriteInvalid reports a nil predicate, an empty
// update, or another request shape that cannot perform a conditional mutation.
var ErrPredicateConditionalWriteInvalid = errors.New("invalid predicate-conditional write")

// BeadPredicate is a pure caller-supplied validation over one fully hydrated
// current bead. Implementations may invoke it more than once when their
// transaction engine retries a discarded attempt. It must not perform I/O,
// mutate external state, or retain/mutate the Bead passed to it.
type BeadPredicate func(Bead) error

// PredicateConditionalWriter is an optional narrow store capability for
// atomically validating caller-owned state that is not completely fenced by a
// store's revision contract. The predicate and mutation execute in the same
// transaction or store critical section. Current includes row fields, labels,
// and metadata. A predicate error aborts with no write and is returned
// unchanged. A success returns the authoritative poststate produced by that
// exact mutation.
//
// This capability is deliberately separate from ConditionalWriter. A backend
// may implement it without claiming that its revision advances on every
// user-visible mutation.
type PredicateConditionalWriter interface {
	// UpdateIfPredicate validates the current bead and atomically applies opts.
	UpdateIfPredicate(id string, predicate BeadPredicate, opts UpdateOpts) (Bead, error)
	// CloseIfPredicate validates the current bead, atomically applies finalPatch,
	// and closes it. finalPatch.Status must be nil; close owns the final status.
	CloseIfPredicate(id string, predicate BeadPredicate, finalPatch UpdateOpts) (Bead, error)
}

// PredicateConditionalWriterHandleProvider exposes a predicate writer through
// a runtime wrapper without claiming that every possible backing supports it.
type PredicateConditionalWriterHandleProvider interface {
	PredicateConditionalWriterHandle() (PredicateConditionalWriter, bool)
}

// PredicateConditionalWriterFor returns the narrow transactional-predicate
// capability when store can provide it. Wrapper-declared conditional-write
// targets are followed so typed class and policy projections preserve the
// backing capability. This is a capability lookup, not the rollout-policy seam.
func PredicateConditionalWriterFor(store Store) (PredicateConditionalWriter, bool) {
	if store == nil {
		return nil, false
	}
	store = followConditionalWritesResolveTarget(store)
	if provider, ok := store.(PredicateConditionalWriterHandleProvider); ok {
		return provider.PredicateConditionalWriterHandle()
	}
	writer, ok := store.(PredicateConditionalWriter)
	return writer, ok
}

func validatePredicateConditionalUpdate(id string, predicate BeadPredicate, opts UpdateOpts) error {
	if predicate == nil {
		return fmt.Errorf("%w: update %q has nil predicate", ErrPredicateConditionalWriteInvalid, id)
	}
	if isEmptyUpdateOpts(opts) {
		return fmt.Errorf("%w: update %q: %w", ErrPredicateConditionalWriteInvalid, id, ErrEmptyConditionalUpdate)
	}
	return nil
}

func validatePredicateConditionalClose(id string, predicate BeadPredicate, finalPatch UpdateOpts) error {
	if predicate == nil {
		return fmt.Errorf("%w: close %q has nil predicate", ErrPredicateConditionalWriteInvalid, id)
	}
	if finalPatch.Status != nil {
		return fmt.Errorf("%w: close %q final patch cannot set status", ErrPredicateConditionalWriteInvalid, id)
	}
	return nil
}

func clonePredicateConditionalUpdate(opts UpdateOpts) UpdateOpts {
	opts.Title = clonePredicateStringPtr(opts.Title)
	opts.Status = clonePredicateStringPtr(opts.Status)
	opts.Type = clonePredicateStringPtr(opts.Type)
	opts.Priority = cloneIntPtr(opts.Priority)
	opts.Description = clonePredicateStringPtr(opts.Description)
	opts.ParentID = clonePredicateStringPtr(opts.ParentID)
	opts.Assignee = clonePredicateStringPtr(opts.Assignee)
	opts.Labels = append([]string(nil), opts.Labels...)
	opts.RemoveLabels = append([]string(nil), opts.RemoveLabels...)
	if opts.Metadata != nil {
		metadata := make(map[string]string, len(opts.Metadata))
		for key, value := range opts.Metadata {
			metadata[key] = value
		}
		opts.Metadata = metadata
	}
	return opts
}

func clonePredicateStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

// PredicateSnapshotEqual reports semantic equality of every hydrated Bead
// field, including Revision and ClaimFence. It ignores representation-only
// differences introduced by durable round trips: monotonic clock readings,
// time locations for equal instants, nil versus empty collections, and label
// or dependency enumeration order.
func PredicateSnapshotEqual(expected, current Bead) bool {
	return reflect.DeepEqual(canonicalPredicateSnapshot(expected), canonicalPredicateSnapshot(current))
}

func predicateConditionalPoststateEqual(expected, current Bead) bool {
	return PredicateSnapshotEqual(expected, current)
}

func canonicalPredicateSnapshot(value Bead) Bead {
	value = cloneBead(value)
	value.CreatedAt = value.CreatedAt.UTC()
	value.UpdatedAt = value.UpdatedAt.UTC()
	if value.DeferUntil != nil {
		deferred := value.DeferUntil.UTC()
		value.DeferUntil = &deferred
	}
	if len(value.Labels) == 0 {
		value.Labels = nil
	} else {
		slices.Sort(value.Labels)
	}
	if len(value.Needs) == 0 {
		value.Needs = nil
	} else {
		slices.Sort(value.Needs)
	}
	if len(value.Dependencies) == 0 {
		value.Dependencies = nil
	} else {
		sort.Slice(value.Dependencies, func(i, j int) bool {
			left, right := value.Dependencies[i], value.Dependencies[j]
			if left.IssueID != right.IssueID {
				return left.IssueID < right.IssueID
			}
			if left.DependsOnID != right.DependsOnID {
				return left.DependsOnID < right.DependsOnID
			}
			return left.Type < right.Type
		})
	}
	if len(value.Metadata) == 0 {
		value.Metadata = nil
	}
	return value
}
