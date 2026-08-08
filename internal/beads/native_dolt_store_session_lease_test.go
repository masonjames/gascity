package beads_test

import (
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/session"
)

func TestNativeDoltStorePredicateCapabilitySupportsSessionLeaseLifecycle(t *testing.T) {
	t.Run("reserve patch and commit", func(t *testing.T) {
		store, front, id, captured := newNativeSessionLeaseFixture(t)
		err := front.WithConditionalMutation(session.ConditionalMutationRequest{
			SessionID:        id,
			ExpectedRevision: captured.Revision,
			Action:           "native-session-commit",
		}, func(lease *session.ConditionalMutationLease) error {
			if err := lease.Patch(beads.UpdateOpts{Labels: []string{"phase:reserved"}}); err != nil {
				return err
			}
			return lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"final": "true"}})
		})
		if err != nil {
			t.Fatalf("WithConditionalMutation: %v", err)
		}
		got, err := store.Get(id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !nativeSessionLeaseHasLabel(got.Labels, "phase:reserved") || got.Metadata["final"] != "true" ||
			got.Metadata[session.ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("committed row = %+v", got)
		}
	})

	t.Run("commit close returns exact inactive poststate", func(t *testing.T) {
		store, front, id, captured := newNativeSessionLeaseFixture(t)
		err := front.WithConditionalMutation(session.ConditionalMutationRequest{
			SessionID:        id,
			ExpectedRevision: captured.Revision,
			Action:           "native-session-commit-close",
		}, func(lease *session.ConditionalMutationLease) error {
			if err := lease.CommitClose(beads.UpdateOpts{Metadata: map[string]string{"closed_by": "test"}}); err != nil {
				return err
			}
			info, persisted := lease.Current()
			if !info.Closed || persisted.Metadata["closed_by"] != "test" {
				t.Fatalf("CommitClose Current = (%+v, %#v)", info, persisted.Metadata)
			}
			return lease.Release()
		})
		if err != nil {
			t.Fatalf("WithConditionalMutation: %v", err)
		}
		got, err := store.Get(id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Status != "closed" || got.Metadata["closed_by"] != "test" ||
			got.Metadata[session.ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("closed row = %+v", got)
		}
	})

	t.Run("label drift blocks final commit", func(t *testing.T) {
		store, front, id, captured := newNativeSessionLeaseFixture(t)
		err := front.WithConditionalMutation(session.ConditionalMutationRequest{
			SessionID:        id,
			ExpectedRevision: captured.Revision,
			Action:           "native-session-label-drift",
		}, func(lease *session.ConditionalMutationLease) error {
			if err := store.Update(id, beads.UpdateOpts{Labels: []string{"foreign"}}); err != nil {
				return err
			}
			return lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"final": "must-not-write"}})
		})
		if !errors.Is(err, session.ErrConditionalMutationLost) {
			t.Fatalf("WithConditionalMutation error = %v, want lost", err)
		}
		got, getErr := store.Get(id)
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if !nativeSessionLeaseHasLabel(got.Labels, "foreign") || got.Metadata["final"] != "" {
			t.Fatalf("drift result = %+v", got)
		}
	})

	t.Run("lease nonce ABA is fenced by partial row revision", func(t *testing.T) {
		store, front, id, captured := newNativeSessionLeaseFixture(t)
		err := front.WithConditionalMutation(session.ConditionalMutationRequest{
			SessionID:        id,
			ExpectedRevision: captured.Revision,
			Action:           "native-session-nonce-aba",
		}, func(lease *session.ConditionalMutationLease) error {
			held, err := store.Get(id)
			if err != nil {
				return err
			}
			owned := held.Metadata[session.ConditionalMutationLeaseMetadataKey]
			writer, ok := beads.MetadataCASWriterFor(store)
			if !ok {
				t.Fatal("NativeDoltStore missing metadata CAS")
			}
			const foreign = `{"version":1,"nonce":"foreign","action":"other","predicates":[]}`
			swapped, err := writer.CompareAndSetMetadataKey(id, session.ConditionalMutationLeaseMetadataKey, owned, foreign)
			if err != nil || !swapped {
				t.Fatalf("first nonce swap = (%v, %v)", swapped, err)
			}
			swapped, err = writer.CompareAndSetMetadataKey(id, session.ConditionalMutationLeaseMetadataKey, foreign, owned)
			if err != nil || !swapped {
				t.Fatalf("second nonce swap = (%v, %v)", swapped, err)
			}
			return lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"final": "must-not-write"}})
		})
		if !errors.Is(err, session.ErrConditionalMutationLost) {
			t.Fatalf("WithConditionalMutation error = %v, want lost after ABA", err)
		}
		got, getErr := store.Get(id)
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if got.Metadata["final"] != "" {
			t.Fatalf("stale lease committed after ABA: %#v", got.Metadata)
		}
	})
}

func TestNativeDoltStoreSessionLeaseReconcilesCommittedPredicateErrors(t *testing.T) {
	t.Run("acquire", func(t *testing.T) {
		store, arm := beads.NewNativeDoltStoreForPredicateCommitThenError()
		front, id, captured := seedNativeSessionLeaseFixture(t, store)
		arm()
		callbackCalls := 0
		err := front.WithConditionalMutation(session.ConditionalMutationRequest{
			SessionID:        id,
			ExpectedRevision: captured.Revision,
			Action:           "native-commit-error-acquire",
		}, func(lease *session.ConditionalMutationLease) error {
			callbackCalls++
			return lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"final": "true"}})
		})
		if err != nil || callbackCalls != 1 {
			t.Fatalf("WithConditionalMutation = (callbacks=%d, err=%v), want committed acquire", callbackCalls, err)
		}
		got, getErr := store.Get(id)
		if getErr != nil || got.Metadata["final"] != "true" || got.Metadata[session.ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("final row = (%+v, %v)", got, getErr)
		}
	})

	t.Run("final commit", func(t *testing.T) {
		store, arm := beads.NewNativeDoltStoreForPredicateCommitThenError()
		front, id, captured := seedNativeSessionLeaseFixture(t, store)
		err := front.WithConditionalMutation(session.ConditionalMutationRequest{
			SessionID:        id,
			ExpectedRevision: captured.Revision,
			Action:           "native-commit-error-final",
		}, func(lease *session.ConditionalMutationLease) error {
			arm()
			return lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"final": "true"}})
		})
		if err != nil {
			t.Fatalf("WithConditionalMutation: %v", err)
		}
		got, getErr := store.Get(id)
		if getErr != nil || got.Metadata["final"] != "true" || got.Metadata[session.ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("final row = (%+v, %v)", got, getErr)
		}
	})

	t.Run("final close", func(t *testing.T) {
		store, arm := beads.NewNativeDoltStoreForPredicateCommitThenError()
		front, id, captured := seedNativeSessionLeaseFixture(t, store)
		err := front.WithConditionalMutation(session.ConditionalMutationRequest{
			SessionID:        id,
			ExpectedRevision: captured.Revision,
			Action:           "native-commit-error-final-close",
		}, func(lease *session.ConditionalMutationLease) error {
			arm()
			return lease.CommitClose(beads.UpdateOpts{Metadata: map[string]string{"final": "closed"}})
		})
		if err != nil {
			t.Fatalf("WithConditionalMutation: %v", err)
		}
		got, getErr := store.Get(id)
		if getErr != nil || got.Status != "closed" || got.Metadata["final"] != "closed" ||
			got.Metadata[session.ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("final closed row = (%+v, %v)", got, getErr)
		}
	})
}

func newNativeSessionLeaseFixture(t *testing.T) (beads.Store, *session.Store, string, session.PersistedResponse) {
	t.Helper()
	store := beads.NewNativeDoltStoreForConformance()
	front, id, captured := seedNativeSessionLeaseFixture(t, store)
	return store, front, id, captured
}

func seedNativeSessionLeaseFixture(t *testing.T, store beads.Store) (*session.Store, string, session.PersistedResponse) {
	t.Helper()
	created, err := store.Create(beads.Bead{
		Title:  "native session",
		Type:   session.BeadType,
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"state": string(session.StateAsleep),
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	front := session.NewStore(beads.SessionStore{Store: store})
	_, captured, err := front.GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("GetPersistedResponse: %v", err)
	}
	return front, created.ID, captured
}

func nativeSessionLeaseHasLabel(labels []string, expected string) bool {
	for _, label := range labels {
		if label == expected {
			return true
		}
	}
	return false
}
