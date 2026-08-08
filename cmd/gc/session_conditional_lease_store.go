package main

import (
	"fmt"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/session"
)

// conditionalLeaseStore confines legacy session front-door writes inside one
// strict conditional-mutation lease. Reads delegate to the original store;
// every session-row write is revision-CAS through the lease. Unsupported
// multi-row operations fail closed instead of escaping to the embedded store.
type conditionalLeaseStore struct {
	beads.Store
	lease *session.ConditionalMutationLease
}

func (s *conditionalLeaseStore) Create(beads.Bead) (beads.Bead, error) {
	return beads.Bead{}, s.unsupported("create")
}

func (s *conditionalLeaseStore) Update(id string, opts beads.UpdateOpts) error {
	if err := s.requireSession(id); err != nil {
		return err
	}
	return s.lease.Patch(opts)
}

func (s *conditionalLeaseStore) Close(id string) error {
	closed := "closed"
	return s.Update(id, beads.UpdateOpts{Status: &closed})
}

func (s *conditionalLeaseStore) Reopen(id string) error {
	open := "open"
	return s.Update(id, beads.UpdateOpts{Status: &open})
}

func (s *conditionalLeaseStore) CloseAll([]string, map[string]string) (int, error) {
	return 0, s.unsupported("close-all")
}

func (s *conditionalLeaseStore) SetMetadata(id, key, value string) error {
	if err := s.requireSession(id); err != nil {
		return err
	}
	return s.lease.SetMetadata(key, value)
}

func (s *conditionalLeaseStore) SetMetadataBatch(id string, metadata map[string]string) error {
	if err := s.requireSession(id); err != nil {
		return err
	}
	if len(metadata) == 0 {
		return nil
	}
	return s.lease.Patch(beads.UpdateOpts{Metadata: metadata})
}

func (s *conditionalLeaseStore) SetLocalString(string, string, string) error {
	return s.unsupported("set-local-string")
}

func (s *conditionalLeaseStore) Tx(_ string, fn func(beads.Tx) error) error {
	if fn == nil {
		return s.unsupported("nil transaction")
	}
	tx := &conditionalLeaseTx{sessionID: s.lease.SessionID()}
	if err := fn(tx); err != nil {
		return err
	}
	if !tx.changed {
		return nil
	}
	return s.lease.Patch(tx.opts)
}

func (s *conditionalLeaseStore) Delete(string) error {
	return s.unsupported("delete")
}

func (s *conditionalLeaseStore) DepAdd(string, string, string) error {
	return s.unsupported("dependency-add")
}

func (s *conditionalLeaseStore) DepRemove(string, string) error {
	return s.unsupported("dependency-remove")
}

func (s *conditionalLeaseStore) requireSession(id string) error {
	if s == nil || s.lease == nil || id != s.lease.SessionID() {
		return fmt.Errorf("%w: lease session %q cannot mutate %q", session.ErrConditionalMutationInvalid, s.leaseSessionID(), id)
	}
	return nil
}

func (s *conditionalLeaseStore) leaseSessionID() string {
	if s == nil || s.lease == nil {
		return ""
	}
	return s.lease.SessionID()
}

func (s *conditionalLeaseStore) unsupported(operation string) error {
	return fmt.Errorf("%w: strict lease store does not support %s", session.ErrConditionalMutationInvalid, operation)
}

type conditionalLeaseTx struct {
	sessionID string
	opts      beads.UpdateOpts
	changed   bool
}

func (tx *conditionalLeaseTx) Create(beads.Bead) (beads.Bead, error) {
	return beads.Bead{}, fmt.Errorf("%w: strict lease transaction cannot create", session.ErrConditionalMutationInvalid)
}

func (tx *conditionalLeaseTx) Update(id string, opts beads.UpdateOpts) error {
	if id != tx.sessionID {
		return fmt.Errorf("%w: strict lease transaction for %q cannot update %q", session.ErrConditionalMutationInvalid, tx.sessionID, id)
	}
	mergeConditionalLeaseUpdate(&tx.opts, opts)
	tx.changed = tx.changed || conditionalLeaseUpdateChanged(opts)
	return nil
}

func (tx *conditionalLeaseTx) SetMetadataBatch(id string, metadata map[string]string) error {
	return tx.Update(id, beads.UpdateOpts{Metadata: metadata})
}

func (tx *conditionalLeaseTx) Close(id string) error {
	closed := "closed"
	return tx.Update(id, beads.UpdateOpts{Status: &closed})
}

func mergeConditionalLeaseUpdate(dst *beads.UpdateOpts, src beads.UpdateOpts) {
	if src.Title != nil {
		dst.Title = src.Title
	}
	if src.Status != nil {
		dst.Status = src.Status
	}
	if src.Type != nil {
		dst.Type = src.Type
	}
	if src.Priority != nil {
		dst.Priority = src.Priority
	}
	if src.Description != nil {
		dst.Description = src.Description
	}
	if src.ParentID != nil {
		dst.ParentID = src.ParentID
	}
	if src.Assignee != nil {
		dst.Assignee = src.Assignee
	}
	dst.Labels = append(dst.Labels, src.Labels...)
	dst.RemoveLabels = append(dst.RemoveLabels, src.RemoveLabels...)
	if len(src.Metadata) > 0 {
		if dst.Metadata == nil {
			dst.Metadata = make(map[string]string, len(src.Metadata))
		}
		for key, value := range src.Metadata {
			dst.Metadata[key] = value
		}
	}
}

func conditionalLeaseUpdateChanged(opts beads.UpdateOpts) bool {
	return opts.Title != nil || opts.Status != nil || opts.Type != nil || opts.Priority != nil ||
		opts.Description != nil || opts.ParentID != nil || opts.Assignee != nil ||
		len(opts.Labels) > 0 || len(opts.RemoveLabels) > 0 || len(opts.Metadata) > 0
}
