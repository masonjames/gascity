package session

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/fsys"
)

func TestConditionalMutationRejectsWriterBetweenCaptureAndAcquire(t *testing.T) {
	mem, _, captured := newConditionalMutationFixture(t, "session-acquire-drift")
	injected := false
	store := &injectBeforeConditionalUpdateStore{MemStore: mem}
	store.inject = func() {
		injected = true
		if err := mem.Update("session-acquire-drift", beads.UpdateOpts{Metadata: map[string]string{"foreign": "won"}}); err != nil {
			t.Fatalf("injecting external write: %v", err)
		}
	}
	front := NewStore(beads.SessionStore{Store: store})
	called := false
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-acquire-drift",
		ExpectedRevision: captured.Revision,
		Action:           "test-acquire",
	}, func(*ConditionalMutationLease) error {
		called = true
		return nil
	})
	if !injected {
		t.Fatal("external writer was not injected")
	}
	if called {
		t.Fatal("callback ran after the captured revision drifted")
	}
	if !errors.Is(err, ErrConditionalMutationLost) || !beads.IsPreconditionFailed(err) {
		t.Fatalf("WithConditionalMutation error = %v, want lease-lost precondition", err)
	}
	got, getErr := mem.Get("session-acquire-drift")
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if got.Metadata["foreign"] != "won" || got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("persisted metadata = %#v, want foreign winner and no lease", got.Metadata)
	}
}

func TestConditionalMutationExternalWriterAfterAcquireIsNeverOverwritten(t *testing.T) {
	mem, front, captured := newConditionalMutationFixture(t, "session-post-acquire-drift")
	var commitErr error
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-post-acquire-drift",
		ExpectedRevision: captured.Revision,
		Action:           "test-finalize",
	}, func(lease *ConditionalMutationLease) error {
		current := lease.Revision()
		if err := mem.UpdateIfMatch(lease.SessionID(), current, beads.UpdateOpts{
			Metadata: map[string]string{"foreign": "won"},
		}); err != nil {
			t.Fatalf("external conditional write: %v", err)
		}
		commitErr = lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"final": "ours"}})
		return commitErr
	})
	if !errors.Is(commitErr, ErrConditionalMutationLost) || !beads.IsPreconditionFailed(commitErr) {
		t.Fatalf("Commit error = %v, want lease-lost precondition", commitErr)
	}
	if !errors.Is(err, ErrConditionalMutationLost) {
		t.Fatalf("WithConditionalMutation error = %v, want lease lost", err)
	}
	got, getErr := mem.Get("session-post-acquire-drift")
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if got.Metadata["foreign"] != "won" || got.Metadata["final"] != "" {
		t.Fatalf("persisted metadata = %#v, want foreign write and no final commit", got.Metadata)
	}
	if got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("owned lease was not cleared after drift: %q", got.Metadata[ConditionalMutationLeaseMetadataKey])
	}
}

func TestConditionalMutationExternalEffectSuccessThenDriftRequiresCallerCompensation(t *testing.T) {
	mem, front, captured := newConditionalMutationFixture(t, "session-external-effect-drift")
	externalEffectStarted := false
	compensated := false
	lifecycleCommitted := false
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-external-effect-drift",
		ExpectedRevision: captured.Revision,
		Action:           "test-external-effect",
	}, func(lease *ConditionalMutationLease) error {
		externalEffectStarted = true
		if err := mem.UpdateIfMatch(lease.SessionID(), lease.Revision(), beads.UpdateOpts{
			Metadata: map[string]string{"external_after_effect": "true"},
		}); err != nil {
			t.Fatalf("injecting post-effect drift: %v", err)
		}
		if err := lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"lifecycle_commit": "true"}}); err != nil {
			compensated = true
			return err
		}
		lifecycleCommitted = true
		return nil
	})
	if !externalEffectStarted || !compensated || lifecycleCommitted {
		t.Fatalf("externalEffectStarted=%v compensated=%v lifecycleCommitted=%v", externalEffectStarted, compensated, lifecycleCommitted)
	}
	if !errors.Is(err, ErrConditionalMutationLost) {
		t.Fatalf("WithConditionalMutation error = %v, want lease lost", err)
	}
	got, getErr := mem.Get("session-external-effect-drift")
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if got.Metadata["lifecycle_commit"] != "" || got.Metadata["external_after_effect"] != "true" {
		t.Fatalf("persisted metadata = %#v, want drift but zero final commit", got.Metadata)
	}
}

func TestConditionalMutationErrorClearsOnlyOwnedNonce(t *testing.T) {
	t.Run("owned", func(t *testing.T) {
		mem, front, captured := newConditionalMutationFixture(t, "session-release-owned")
		sentinel := errors.New("external effect failed")
		err := front.WithConditionalMutation(ConditionalMutationRequest{
			SessionID:        "session-release-owned",
			ExpectedRevision: captured.Revision,
			Action:           "test-effect-error",
		}, func(*ConditionalMutationLease) error { return sentinel })
		if !errors.Is(err, sentinel) {
			t.Fatalf("WithConditionalMutation error = %v, want external-effect error", err)
		}
		got, _ := mem.Get("session-release-owned")
		if got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("owned lease remains: %q", got.Metadata[ConditionalMutationLeaseMetadataKey])
		}
	})

	t.Run("foreign", func(t *testing.T) {
		mem, front, captured := newConditionalMutationFixture(t, "session-release-foreign")
		const foreign = `{"version":1,"nonce":"foreign","action":"other","predicates":[]}`
		err := front.WithConditionalMutation(ConditionalMutationRequest{
			SessionID:        "session-release-foreign",
			ExpectedRevision: captured.Revision,
			Action:           "test-effect-error",
		}, func(lease *ConditionalMutationLease) error {
			return mem.UpdateIfMatch(lease.SessionID(), lease.Revision(), beads.UpdateOpts{
				Metadata: map[string]string{ConditionalMutationLeaseMetadataKey: foreign},
			})
		})
		if !errors.Is(err, ErrConditionalMutationLost) {
			t.Fatalf("WithConditionalMutation error = %v, want lost foreign nonce", err)
		}
		got, _ := mem.Get("session-release-foreign")
		if got.Metadata[ConditionalMutationLeaseMetadataKey] != foreign {
			t.Fatalf("foreign lease = %q, want preserved %q", got.Metadata[ConditionalMutationLeaseMetadataKey], foreign)
		}
	})
}

func TestConditionalMutationPreserveLeavesUncompensatedIntentHeld(t *testing.T) {
	mem, front, captured := newConditionalMutationFixture(t, "session-preserve-uncompensated")
	sentinel := errors.New("provider effect could not be contained")
	var encoded string
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-preserve-uncompensated",
		ExpectedRevision: captured.Revision,
		Action:           "test-uncompensated-effect",
	}, func(lease *ConditionalMutationLease) error {
		encoded = lease.encodedIntent
		if err := mem.UpdateIfMatch(lease.SessionID(), lease.Revision(), beads.UpdateOpts{
			Metadata: map[string]string{"provider_effect_may_have_started": "true"},
		}); err != nil {
			t.Fatalf("injecting post-effect drift: %v", err)
		}
		if err := lease.Preserve(); err != nil {
			t.Fatalf("Preserve: %v", err)
		}
		if err := lease.Preserve(); err != nil {
			t.Fatalf("second Preserve: %v", err)
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithConditionalMutation error = %v, want uncompensated sentinel", err)
	}
	got, getErr := mem.Get("session-preserve-uncompensated")
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if got.Metadata[ConditionalMutationLeaseMetadataKey] != encoded || encoded == "" {
		t.Fatalf("preserved lease = %q, want exact owned intent %q", got.Metadata[ConditionalMutationLeaseMetadataKey], encoded)
	}
	if err := RequireNoConditionalMutationLease(PersistedResponseFromBead(got)); !errors.Is(err, ErrConditionalMutationHeld) {
		t.Fatalf("RequireNoConditionalMutationLease error = %v, want held", err)
	}

	called := false
	err = front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-preserve-uncompensated",
		ExpectedRevision: got.Revision,
		Action:           "test-followup",
	}, func(*ConditionalMutationLease) error {
		called = true
		return nil
	})
	if called || !errors.Is(err, ErrConditionalMutationHeld) {
		t.Fatalf("followup = (called=%v, err=%v), want held refusal", called, err)
	}
}

func TestConditionalMutationFailsClosedWithoutFullConditionalWriter(t *testing.T) {
	mem, _, captured := newConditionalMutationFixture(t, "session-unsupported")
	front := NewStore(beads.SessionStore{Store: conditionalMutationStoreOnly{Store: mem}})
	called := false
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-unsupported",
		ExpectedRevision: captured.Revision,
		Action:           "test-unsupported",
	}, func(*ConditionalMutationLease) error {
		called = true
		return nil
	})
	if called {
		t.Fatal("callback ran without a full ConditionalWriter")
	}
	if !errors.Is(err, ErrConditionalMutationUnsupported) {
		t.Fatalf("WithConditionalMutation error = %v, want unsupported", err)
	}
}

func TestConditionalMutationPredicatesOwnWitnessTruthTable(t *testing.T) {
	tests := []struct {
		name          string
		triggerID     string
		triggerRef    string
		wantCallback  bool
		wantPredicate bool
	}{
		{name: "zero pair accepted by caller", wantCallback: true, wantPredicate: true},
		{name: "partial pair rejected by caller", triggerID: "work-1", wantPredicate: true},
		{name: "complete pair accepted by caller", triggerID: "work-1", triggerRef: "rig-a", wantCallback: true, wantPredicate: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem, front, captured := newConditionalMutationFixture(t, "session-predicate")
			if tc.triggerID != "" || tc.triggerRef != "" {
				if err := mem.Update("session-predicate", beads.UpdateOpts{Metadata: map[string]string{
					beadmeta.TriggerBeadIDMetadataKey:       tc.triggerID,
					beadmeta.TriggerBeadStoreRefMetadataKey: tc.triggerRef,
				}}); err != nil {
					t.Fatalf("seeding trigger: %v", err)
				}
				_, captured, _ = front.GetPersistedResponse("session-predicate")
			}
			predicateCalls := 0
			callbackCalls := 0
			err := front.WithConditionalMutation(ConditionalMutationRequest{
				SessionID:        "session-predicate",
				ExpectedRevision: captured.Revision,
				Action:           "test-truth-table",
				Predicates: []ConditionalMutationPredicate{{
					Identity: "caller-owned-trigger-truth-table",
					Validate: func(info Info, _ PersistedResponse) error {
						predicateCalls++
						id, ref := info.TriggerBeadID, info.TriggerBeadStoreRef
						if (id == "") != (ref == "") {
							return errors.New("caller rejects partial pair")
						}
						return nil
					},
				}},
			}, func(lease *ConditionalMutationLease) error {
				callbackCalls++
				return lease.Commit(beads.UpdateOpts{})
			})
			if tc.wantCallback && err != nil {
				t.Fatalf("WithConditionalMutation: %v", err)
			}
			if !tc.wantCallback && err == nil {
				t.Fatal("WithConditionalMutation succeeded for caller-rejected predicate")
			}
			if (callbackCalls > 0) != tc.wantCallback {
				t.Fatalf("callback calls = %d, wantCallback=%v", callbackCalls, tc.wantCallback)
			}
			if tc.wantPredicate && predicateCalls == 0 {
				t.Fatal("caller predicate was not evaluated")
			}
		})
	}
}

func TestConditionalMutationMethodsAdvanceRevisionAndRejectStaleToken(t *testing.T) {
	_, front, captured := newConditionalMutationFixture(t, "session-methods")
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-methods",
		ExpectedRevision: captured.Revision,
		Action:           "test-methods",
	}, func(lease *ConditionalMutationLease) error {
		if lease.CapturedRevision() != captured.Revision || lease.Action() != "test-methods" || lease.Nonce() == "" {
			t.Fatalf("lease identity = captured:%d action:%q nonce:%q", lease.CapturedRevision(), lease.Action(), lease.Nonce())
		}
		acquiredRevision := lease.Revision()
		stale := cloneConditionalMutationLeaseForTest(lease)
		if err := lease.SetMetadata("phase", "one"); err != nil {
			return err
		}
		if lease.Revision() == acquiredRevision {
			t.Fatal("SetMetadata did not advance revision")
		}
		if err := stale.Patch(beads.UpdateOpts{Metadata: map[string]string{"stale": "bad"}}); !errors.Is(err, ErrConditionalMutationLost) {
			t.Fatalf("stale Patch error = %v, want lease lost", err)
		}
		beforePatch := lease.Revision()
		if err := lease.Patch(beads.UpdateOpts{Metadata: map[string]string{"phase": "two"}}); err != nil {
			return err
		}
		if lease.Revision() == beforePatch {
			t.Fatal("Patch did not advance revision")
		}
		info, persisted := lease.Current()
		if info.ID != "session-methods" || persisted.Metadata["phase"] != "two" {
			t.Fatalf("Current = (%+v, %#v)", info, persisted.Metadata)
		}
		return lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"done": "true"}})
	})
	if err != nil {
		t.Fatalf("WithConditionalMutation: %v", err)
	}
}

func TestConditionalMutationCloseAdvancesRevisionAndReleasesLease(t *testing.T) {
	mem, front, captured := newConditionalMutationFixture(t, "session-close")
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-close",
		ExpectedRevision: captured.Revision,
		Action:           "test-close",
	}, func(lease *ConditionalMutationLease) error {
		before := lease.Revision()
		if err := lease.Close(); err != nil {
			return err
		}
		if lease.Revision() == before {
			t.Fatal("Close did not advance revision")
		}
		info, _ := lease.Current()
		if !info.Closed {
			t.Fatal("Current did not project the conditional close")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithConditionalMutation: %v", err)
	}
	got, getErr := mem.Get("session-close")
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if got.Status != "closed" || got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("closed row = status %q metadata %#v", got.Status, got.Metadata)
	}
}

func TestConditionalMutationCommitCloseAtomicallyClearsLease(t *testing.T) {
	mem, front, captured := newConditionalMutationFixture(t, "session-commit-close")
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-commit-close",
		ExpectedRevision: captured.Revision,
		Action:           "test-commit-close",
	}, func(lease *ConditionalMutationLease) error {
		if err := lease.CommitClose(beads.UpdateOpts{Metadata: map[string]string{"final": "true"}}); err != nil {
			return err
		}
		info, persisted := lease.Current()
		if !info.Closed || persisted.Metadata["final"] != "true" {
			t.Fatalf("Current after CommitClose = (%+v, %#v)", info, persisted.Metadata)
		}
		if err := lease.Release(); err != nil {
			t.Fatalf("Release after CommitClose: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithConditionalMutation: %v", err)
	}
	got, getErr := mem.Get("session-commit-close")
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if got.Status != "closed" || got.Metadata["final"] != "true" || got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("committed close = status %q metadata %#v", got.Status, got.Metadata)
	}
}

func TestConditionalMutationRejectsLeaseNonceABA(t *testing.T) {
	mem, front, captured := newConditionalMutationFixture(t, "session-lease-aba")
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-lease-aba",
		ExpectedRevision: captured.Revision,
		Action:           "test-lease-aba",
	}, func(lease *ConditionalMutationLease) error {
		const foreign = `{"version":1,"nonce":"foreign","action":"other","predicates":[]}`
		swapped, err := mem.CompareAndSetMetadataKey(lease.SessionID(), ConditionalMutationLeaseMetadataKey, lease.encodedIntent, foreign)
		if err != nil || !swapped {
			t.Fatalf("first nonce swap = (%v, %v)", swapped, err)
		}
		swapped, err = mem.CompareAndSetMetadataKey(lease.SessionID(), ConditionalMutationLeaseMetadataKey, foreign, lease.encodedIntent)
		if err != nil || !swapped {
			t.Fatalf("second nonce swap = (%v, %v)", swapped, err)
		}
		return lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"final": "must-not-write"}})
	})
	if !errors.Is(err, ErrConditionalMutationLost) {
		t.Fatalf("WithConditionalMutation error = %v, want lost after nonce ABA", err)
	}
	got, getErr := mem.Get("session-lease-aba")
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if got.Metadata["final"] != "" {
		t.Fatalf("stale lease committed after ABA: %#v", got.Metadata)
	}
}

func TestConditionalMutationActiveLeaseCannotBeReplaced(t *testing.T) {
	_, front, captured := newConditionalMutationFixture(t, "session-held")
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-held",
		ExpectedRevision: captured.Revision,
		Action:           "owner",
	}, func(lease *ConditionalMutationLease) error {
		_, acquireErr := front.acquireConditionalMutationLocked(ConditionalMutationRequest{
			SessionID:        "session-held",
			ExpectedRevision: lease.Revision(),
			Action:           "contender",
		})
		if !errors.Is(acquireErr, ErrConditionalMutationHeld) {
			t.Fatalf("contending acquire error = %v, want held", acquireErr)
		}
		return lease.Commit(beads.UpdateOpts{})
	})
	if err != nil {
		t.Fatalf("WithConditionalMutation: %v", err)
	}
}

func TestRequireNoConditionalMutationLease(t *testing.T) {
	if err := RequireNoConditionalMutationLease(PersistedResponse{Metadata: map[string]string{}}); err != nil {
		t.Fatalf("empty lease: %v", err)
	}
	if err := RequireNoConditionalMutationLease(PersistedResponse{Metadata: map[string]string{
		ConditionalMutationLeaseMetadataKey: "malformed-but-active",
	}}); !errors.Is(err, ErrConditionalMutationHeld) {
		t.Fatalf("active malformed lease error = %v, want held", err)
	}
}

func TestConditionalMutationSameIDDifferentPhysicalStoreCannotUseToken(t *testing.T) {
	_, frontA, capturedA := newConditionalMutationFixture(t, "same-id")
	_, frontB, _ := newConditionalMutationFixture(t, "same-id")
	err := frontA.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "same-id",
		ExpectedRevision: capturedA.Revision,
		Action:           "store-a",
	}, func(lease *ConditionalMutationLease) error {
		if err := lease.validateStore(frontB); !errors.Is(err, ErrConditionalMutationStoreMismatch) {
			t.Fatalf("validateStore(foreign) = %v, want store mismatch", err)
		}
		return lease.Commit(beads.UpdateOpts{})
	})
	if err != nil {
		t.Fatalf("WithConditionalMutation: %v", err)
	}
}

func TestConditionalMutationConcurrentAcquisitionHasOneWinnerWithoutDeadlock(t *testing.T) {
	_, front, captured := newConditionalMutationFixture(t, "session-race")
	const contenders = 24
	start := make(chan struct{})
	var callbacks atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_ = front.WithConditionalMutation(ConditionalMutationRequest{
				SessionID:        "session-race",
				ExpectedRevision: captured.Revision,
				Action:           fmt.Sprintf("contender-%d", i),
			}, func(lease *ConditionalMutationLease) error {
				callbacks.Add(1)
				return lease.Commit(beads.UpdateOpts{})
			})
		}(i)
	}
	close(start)
	wg.Wait()
	if got := callbacks.Load(); got != 1 {
		t.Fatalf("callbacks = %d, want exactly one", got)
	}
}

func TestConditionalMutationReconcilesCommitThenErrorWithoutFalseCompensation(t *testing.T) {
	tests := []struct {
		name       string
		errorOnCAS int32
	}{
		{name: "acquire committed before refresh error", errorOnCAS: 1},
		{name: "final commit committed before refresh error", errorOnCAS: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem, _, captured := newConditionalMutationFixture(t, "session-commit-then-error")
			store := &conditionalMutationCommitThenErrorStore{
				MemStore:   mem,
				errorOnCAS: tc.errorOnCAS,
			}
			front := NewStore(beads.SessionStore{Store: store})
			callbackCalls := 0
			err := front.WithConditionalMutation(ConditionalMutationRequest{
				SessionID:        "session-commit-then-error",
				ExpectedRevision: captured.Revision,
				Action:           "test-commit-then-error",
			}, func(lease *ConditionalMutationLease) error {
				callbackCalls++
				return lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"committed": "true"}})
			})
			if err != nil {
				t.Fatalf("WithConditionalMutation: %v", err)
			}
			if callbackCalls != 1 {
				t.Fatalf("callback calls = %d, want 1", callbackCalls)
			}
			got, getErr := mem.Get("session-commit-then-error")
			if getErr != nil {
				t.Fatalf("Get: %v", getErr)
			}
			if got.Metadata["committed"] != "true" || got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
				t.Fatalf("metadata = %#v, want reconciled committed result", got.Metadata)
			}
		})
	}
}

func TestConditionalMutationRevisionAcquireAmbiguityRejectsLaterForeignDrift(t *testing.T) {
	mem, _, captured := newConditionalMutationFixture(t, "session-revision-acquire-ambiguous")
	store := &conditionalMutationAmbiguousRevisionStore{
		MemStore:          mem,
		errorOnUpdateCall: 1,
		driftOnUpdateCall: 1,
		driftAfterError:   beads.UpdateOpts{Metadata: map[string]string{"foreign_after_acquire": "won"}},
	}
	front := NewStore(beads.SessionStore{Store: store})
	callbackCalls := 0
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-revision-acquire-ambiguous",
		ExpectedRevision: captured.Revision,
		Action:           "test-revision-acquire-ambiguous",
	}, func(*ConditionalMutationLease) error {
		callbackCalls++
		return nil
	})
	if !errors.Is(err, ErrConditionalMutationLost) {
		t.Fatalf("WithConditionalMutation error = %v, want lease lost", err)
	}
	if callbackCalls != 0 {
		t.Fatalf("callback calls = %d, want zero", callbackCalls)
	}
	got, getErr := mem.Get("session-revision-acquire-ambiguous")
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if got.Metadata["foreign_after_acquire"] != "won" {
		t.Fatalf("metadata = %#v, want later foreign drift preserved", got.Metadata)
	}
	if got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("metadata = %#v, own pre-effect lease was stranded", got.Metadata)
	}
}

func TestConditionalMutationRevisionAcquireSuccessRejectsPostwriteForeignDrift(t *testing.T) {
	mem, _, captured := newConditionalMutationFixture(t, "session-revision-acquire-success-drift")
	store := &conditionalMutationAmbiguousRevisionStore{
		MemStore:          mem,
		driftOnUpdateCall: 1,
		driftAfterError:   beads.UpdateOpts{Metadata: map[string]string{"foreign_after_acquire": "won"}},
	}
	front := NewStore(beads.SessionStore{Store: store})
	callbackCalls := 0
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-revision-acquire-success-drift",
		ExpectedRevision: captured.Revision,
		Action:           "test-revision-acquire-success-drift",
	}, func(*ConditionalMutationLease) error {
		callbackCalls++
		return nil
	})
	if !errors.Is(err, ErrConditionalMutationLost) {
		t.Fatalf("WithConditionalMutation error = %v, want lease lost", err)
	}
	if callbackCalls != 0 {
		t.Fatalf("callback calls = %d, want zero", callbackCalls)
	}
	got, getErr := mem.Get("session-revision-acquire-success-drift")
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if got.Metadata["foreign_after_acquire"] != "won" || got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("metadata = %#v, want foreign drift and exact own lease cleanup", got.Metadata)
	}
}

func TestConditionalMutationRevisionCommitAmbiguityRejectsLaterForeignDrift(t *testing.T) {
	mem, _, captured := newConditionalMutationFixture(t, "session-revision-commit-ambiguous")
	store := &conditionalMutationAmbiguousRevisionStore{
		MemStore:          mem,
		errorOnUpdateCall: 2,
		driftOnUpdateCall: 2,
		driftAfterError:   beads.UpdateOpts{Metadata: map[string]string{"foreign_after_commit": "won"}},
	}
	front := NewStore(beads.SessionStore{Store: store})
	var commitErr error
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-revision-commit-ambiguous",
		ExpectedRevision: captured.Revision,
		Action:           "test-revision-commit-ambiguous",
	}, func(lease *ConditionalMutationLease) error {
		before := lease.Revision()
		commitErr = lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"committed": "true"}})
		if lease.Revision() != before {
			t.Fatalf("lease adopted ambiguous drifted revision %d, want %d", lease.Revision(), before)
		}
		return commitErr
	})
	if !errors.Is(commitErr, ErrConditionalMutationLost) {
		t.Fatalf("Commit error = %v, want lease lost", commitErr)
	}
	if !errors.Is(err, ErrConditionalMutationLost) {
		t.Fatalf("WithConditionalMutation error = %v, want lease lost", err)
	}
	got, getErr := mem.Get("session-revision-commit-ambiguous")
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if got.Metadata["committed"] != "true" || got.Metadata["foreign_after_commit"] != "won" {
		t.Fatalf("metadata = %#v, want committed write plus later foreign drift", got.Metadata)
	}
}

func TestConditionalMutationRevisionPatchAndCommitSuccessRejectPostwriteForeignDrift(t *testing.T) {
	tests := []struct {
		name   string
		commit bool
	}{
		{name: "patch"},
		{name: "commit", commit: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := "session-revision-" + tc.name + "-success-drift"
			mem, _, captured := newConditionalMutationFixture(t, id)
			store := &conditionalMutationAmbiguousRevisionStore{
				MemStore:          mem,
				driftOnUpdateCall: 2,
				driftAfterError:   beads.UpdateOpts{Metadata: map[string]string{"foreign_after_write": "won"}},
			}
			front := NewStore(beads.SessionStore{Store: store})
			var mutationErr error
			err := front.WithConditionalMutation(ConditionalMutationRequest{
				SessionID:        id,
				ExpectedRevision: captured.Revision,
				Action:           "test-revision-" + tc.name + "-success-drift",
			}, func(lease *ConditionalMutationLease) error {
				before := lease.Revision()
				patch := beads.UpdateOpts{Metadata: map[string]string{"ours": "true"}}
				if tc.commit {
					mutationErr = lease.Commit(patch)
				} else {
					mutationErr = lease.Patch(patch)
				}
				if lease.Revision() != before {
					t.Fatalf("lease adopted drifted revision %d, want %d", lease.Revision(), before)
				}
				return mutationErr
			})
			if !errors.Is(mutationErr, ErrConditionalMutationLost) {
				t.Fatalf("mutation error = %v, want lease lost", mutationErr)
			}
			if !errors.Is(err, ErrConditionalMutationLost) {
				t.Fatalf("WithConditionalMutation error = %v, want lease lost", err)
			}
			got, getErr := mem.Get(id)
			if getErr != nil {
				t.Fatalf("Get: %v", getErr)
			}
			if got.Metadata["ours"] != "true" || got.Metadata["foreign_after_write"] != "won" {
				t.Fatalf("metadata = %#v, want committed write plus later foreign drift", got.Metadata)
			}
		})
	}
}

func TestConditionalMutationRevisionCloseAmbiguityReconcilesOnlyExactPoststate(t *testing.T) {
	t.Run("clean committed close", func(t *testing.T) {
		mem, _, captured := newConditionalMutationFixture(t, "session-revision-close-clean")
		store := &conditionalMutationAmbiguousRevisionStore{MemStore: mem, errorOnClose: true}
		front := NewStore(beads.SessionStore{Store: store})
		err := front.WithConditionalMutation(ConditionalMutationRequest{
			SessionID:        "session-revision-close-clean",
			ExpectedRevision: captured.Revision,
			Action:           "test-revision-close-clean",
		}, func(lease *ConditionalMutationLease) error {
			return lease.Close()
		})
		if err != nil {
			t.Fatalf("WithConditionalMutation: %v", err)
		}
		got, getErr := mem.Get("session-revision-close-clean")
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if got.Status != "closed" || got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("closed row = status %q metadata %#v", got.Status, got.Metadata)
		}
	})

	t.Run("later foreign drift", func(t *testing.T) {
		mem, _, captured := newConditionalMutationFixture(t, "session-revision-close-ambiguous")
		store := &conditionalMutationAmbiguousRevisionStore{
			MemStore:        mem,
			errorOnClose:    true,
			driftAfterClose: true,
			driftAfterError: beads.UpdateOpts{Metadata: map[string]string{"foreign_after_close": "won"}},
		}
		front := NewStore(beads.SessionStore{Store: store})
		var closeErr error
		err := front.WithConditionalMutation(ConditionalMutationRequest{
			SessionID:        "session-revision-close-ambiguous",
			ExpectedRevision: captured.Revision,
			Action:           "test-revision-close-ambiguous",
		}, func(lease *ConditionalMutationLease) error {
			beforeRevision := lease.Revision()
			beforeInfo, _ := lease.Current()
			closeErr = lease.Close()
			afterInfo, _ := lease.Current()
			if lease.Revision() != beforeRevision || beforeInfo.Closed != afterInfo.Closed {
				t.Fatalf("lease adopted ambiguous close: revision %d -> %d, closed %v -> %v",
					beforeRevision, lease.Revision(), beforeInfo.Closed, afterInfo.Closed)
			}
			return closeErr
		})
		if !errors.Is(closeErr, ErrConditionalMutationLost) {
			t.Fatalf("Close error = %v, want lease lost", closeErr)
		}
		if !errors.Is(err, ErrConditionalMutationLost) {
			t.Fatalf("WithConditionalMutation error = %v, want lease lost", err)
		}
		got, getErr := mem.Get("session-revision-close-ambiguous")
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if got.Status != "closed" || got.Metadata["foreign_after_close"] != "won" {
			t.Fatalf("closed row = status %q metadata %#v, want committed close plus later drift", got.Status, got.Metadata)
		}
		if got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("metadata = %#v, own lease was not safely released", got.Metadata)
		}
	})

	t.Run("nil error with later foreign drift", func(t *testing.T) {
		mem, _, captured := newConditionalMutationFixture(t, "session-revision-close-success-drift")
		store := &conditionalMutationAmbiguousRevisionStore{
			MemStore:        mem,
			driftAfterClose: true,
			driftAfterError: beads.UpdateOpts{Metadata: map[string]string{"foreign_after_close": "won"}},
		}
		front := NewStore(beads.SessionStore{Store: store})
		var closeErr error
		err := front.WithConditionalMutation(ConditionalMutationRequest{
			SessionID:        "session-revision-close-success-drift",
			ExpectedRevision: captured.Revision,
			Action:           "test-revision-close-success-drift",
		}, func(lease *ConditionalMutationLease) error {
			before := lease.Revision()
			closeErr = lease.Close()
			if lease.Revision() != before {
				t.Fatalf("lease adopted drifted revision %d, want %d", lease.Revision(), before)
			}
			return closeErr
		})
		if !errors.Is(closeErr, ErrConditionalMutationLost) {
			t.Fatalf("Close error = %v, want lease lost", closeErr)
		}
		if !errors.Is(err, ErrConditionalMutationLost) {
			t.Fatalf("WithConditionalMutation error = %v, want lease lost", err)
		}
		got, getErr := mem.Get("session-revision-close-success-drift")
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if got.Status != "closed" || got.Metadata["foreign_after_close"] != "won" || got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("closed row = status %q metadata %#v, want close, drift, and released lease", got.Status, got.Metadata)
		}
	})
}

func TestConditionalMutationAmbiguousPredicateAcquireClearsOwnLeaseAfterLaterDrift(t *testing.T) {
	mem, _, captured := newConditionalMutationFixture(t, "session-predicate-acquire-ambiguous")
	store := &conditionalMutationAmbiguousPredicateAcquireStore{
		Store: mem,
		mem:   mem,
	}
	front := NewStore(beads.SessionStore{Store: store})
	callbackCalls := 0
	err := front.WithConditionalMutation(ConditionalMutationRequest{
		SessionID:        "session-predicate-acquire-ambiguous",
		ExpectedRevision: captured.Revision,
		Action:           "test-predicate-acquire-ambiguous",
	}, func(*ConditionalMutationLease) error {
		callbackCalls++
		return nil
	})
	if err == nil {
		t.Fatal("WithConditionalMutation error = nil, want injected ambiguous acquire error")
	}
	if callbackCalls != 0 {
		t.Fatalf("callback calls = %d, want zero", callbackCalls)
	}
	got, getErr := mem.Get("session-predicate-acquire-ambiguous")
	if getErr != nil {
		t.Fatalf("Get: %v", getErr)
	}
	if got.Metadata["foreign_after_acquire"] != "won" {
		t.Fatalf("metadata = %#v, want later foreign drift preserved", got.Metadata)
	}
	if got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
		t.Fatalf("metadata = %#v, own pre-effect lease was stranded", got.Metadata)
	}
}

func TestConditionalMutationFileStoreCrossHandleFencesExternalWriter(t *testing.T) {
	t.Run("between capture and acquire", func(t *testing.T) {
		first, second, front, id, captured := newConditionalMutationFileStoreFixture(t)
		if err := second.Update(id, beads.UpdateOpts{Metadata: map[string]string{"external": "before-acquire"}}); err != nil {
			t.Fatalf("external Update: %v", err)
		}
		called := false
		err := front.WithConditionalMutation(ConditionalMutationRequest{
			SessionID:        id,
			ExpectedRevision: captured.Revision,
			Action:           "file-cross-handle-acquire",
		}, func(*ConditionalMutationLease) error {
			called = true
			return nil
		})
		if called || !errors.Is(err, ErrConditionalMutationLost) {
			t.Fatalf("called=%v err=%v, want zero callback and lost lease", called, err)
		}
		got, getErr := first.Get(id)
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if got.Metadata["external"] != "before-acquire" || got.Metadata[ConditionalMutationLeaseMetadataKey] != "" {
			t.Fatalf("metadata = %#v, want external winner and no lease", got.Metadata)
		}
	})

	t.Run("after acquire before final commit", func(t *testing.T) {
		first, second, front, id, captured := newConditionalMutationFileStoreFixture(t)
		err := front.WithConditionalMutation(ConditionalMutationRequest{
			SessionID:        id,
			ExpectedRevision: captured.Revision,
			Action:           "file-cross-handle-finalize",
		}, func(lease *ConditionalMutationLease) error {
			foreign, getErr := second.Get(id)
			if getErr != nil {
				return getErr
			}
			if err := second.UpdateIfMatch(id, foreign.Revision, beads.UpdateOpts{
				Metadata: map[string]string{"external": "after-acquire"},
			}); err != nil {
				return err
			}
			return lease.Commit(beads.UpdateOpts{Metadata: map[string]string{"final": "ours"}})
		})
		if !errors.Is(err, ErrConditionalMutationLost) {
			t.Fatalf("WithConditionalMutation error = %v, want lost lease", err)
		}
		got, getErr := first.Get(id)
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		if got.Metadata["external"] != "after-acquire" || got.Metadata["final"] != "" {
			t.Fatalf("metadata = %#v, want external winner and zero final commit", got.Metadata)
		}
	})
}

type conditionalMutationStoreOnly struct{ beads.Store }

type injectBeforeConditionalUpdateStore struct {
	*beads.MemStore
	once   sync.Once
	inject func()
}

type conditionalMutationCommitThenErrorStore struct {
	*beads.MemStore
	calls      atomic.Int32
	errorOnCAS int32
}

type conditionalMutationAmbiguousRevisionStore struct {
	*beads.MemStore
	updateCalls       atomic.Int32
	errorOnUpdateCall int32
	driftOnUpdateCall int32
	errorOnClose      bool
	driftAfterClose   bool
	driftAfterError   beads.UpdateOpts
}

type conditionalMutationAmbiguousPredicateAcquireStore struct {
	beads.Store
	mem  *beads.MemStore
	once sync.Once
}

func (s *conditionalMutationAmbiguousPredicateAcquireStore) UpdateIfPredicate(id string, predicate beads.BeadPredicate, opts beads.UpdateOpts) (beads.Bead, error) {
	writer, _ := beads.PredicateConditionalWriterFor(s.mem)
	post, err := writer.UpdateIfPredicate(id, predicate, opts)
	if err != nil {
		return beads.Bead{}, err
	}
	s.once.Do(func() {
		_ = s.mem.Update(id, beads.UpdateOpts{Metadata: map[string]string{"foreign_after_acquire": "won"}})
	})
	return post, errors.New("injected ambiguous predicate acquire error after committed write and later drift")
}

func (s *conditionalMutationAmbiguousPredicateAcquireStore) CloseIfPredicate(id string, predicate beads.BeadPredicate, finalPatch beads.UpdateOpts) (beads.Bead, error) {
	writer, _ := beads.PredicateConditionalWriterFor(s.mem)
	return writer.CloseIfPredicate(id, predicate, finalPatch)
}

func (s *conditionalMutationAmbiguousPredicateAcquireStore) CompareAndSetMetadataKey(id, key, expected, next string) (bool, error) {
	return s.mem.CompareAndSetMetadataKey(id, key, expected, next)
}

func (s *conditionalMutationCommitThenErrorStore) UpdateIfMatch(id string, expectedRevision int64, opts beads.UpdateOpts) error {
	err := s.MemStore.UpdateIfMatch(id, expectedRevision, opts)
	if err != nil {
		return err
	}
	if s.calls.Add(1) == s.errorOnCAS {
		return errors.New("injected refresh failure after committed CAS")
	}
	return nil
}

func (s *conditionalMutationAmbiguousRevisionStore) UpdateIfMatch(id string, expectedRevision int64, opts beads.UpdateOpts) error {
	err := s.MemStore.UpdateIfMatch(id, expectedRevision, opts)
	if err != nil {
		return err
	}
	call := s.updateCalls.Add(1)
	if call == s.driftOnUpdateCall && !conditionalMutationTestUpdateEmpty(s.driftAfterError) {
		if err := s.Update(id, s.driftAfterError); err != nil {
			return fmt.Errorf("injecting foreign drift after ambiguous update: %w", err)
		}
	}
	if call != s.errorOnUpdateCall {
		return nil
	}
	return errors.New("injected ambiguous revision update error after committed write")
}

func (s *conditionalMutationAmbiguousRevisionStore) CloseIfMatch(id string, expectedRevision int64) error {
	err := s.MemStore.CloseIfMatch(id, expectedRevision)
	if err != nil {
		return err
	}
	if s.driftAfterClose && !conditionalMutationTestUpdateEmpty(s.driftAfterError) {
		if err := s.Update(id, s.driftAfterError); err != nil {
			return fmt.Errorf("injecting foreign drift after ambiguous close: %w", err)
		}
	}
	if !s.errorOnClose {
		return nil
	}
	return errors.New("injected ambiguous revision close error after committed close")
}

func conditionalMutationTestUpdateEmpty(opts beads.UpdateOpts) bool {
	return opts.Title == nil && opts.Status == nil && opts.Type == nil && opts.Priority == nil &&
		opts.Description == nil && opts.ParentID == nil && opts.Assignee == nil &&
		len(opts.Labels) == 0 && len(opts.RemoveLabels) == 0 && len(opts.Metadata) == 0
}

func (s *injectBeforeConditionalUpdateStore) UpdateIfMatch(id string, expectedRevision int64, opts beads.UpdateOpts) error {
	s.once.Do(s.inject)
	return s.MemStore.UpdateIfMatch(id, expectedRevision, opts)
}

func newConditionalMutationFixture(t *testing.T, id string) (*beads.MemStore, *Store, PersistedResponse) {
	t.Helper()
	mem := beads.NewMemStoreFrom(1, []beads.Bead{{
		ID:     id,
		Title:  id,
		Type:   BeadType,
		Status: "open",
		Labels: []string{LabelSession},
		Metadata: map[string]string{
			"state": string(StateAsleep),
		},
		Revision: 1,
	}}, nil)
	front := NewStore(beads.SessionStore{Store: mem})
	_, persisted, err := front.GetPersistedResponse(id)
	if err != nil {
		t.Fatalf("GetPersistedResponse: %v", err)
	}
	return mem, front, persisted
}

func newConditionalMutationFileStoreFixture(t *testing.T) (*beads.FileStore, *beads.FileStore, *Store, string, PersistedResponse) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "beads.json")
	first, err := beads.OpenFileStore(fsys.OSFS{}, path)
	if err != nil {
		t.Fatalf("OpenFileStore(first): %v", err)
	}
	created, err := first.Create(beads.Bead{
		Title:  "cross-handle session",
		Type:   BeadType,
		Status: "open",
		Labels: []string{LabelSession},
		Metadata: map[string]string{
			"state": string(StateAsleep),
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	front := NewStore(beads.SessionStore{Store: first})
	_, captured, err := front.GetPersistedResponse(created.ID)
	if err != nil {
		t.Fatalf("GetPersistedResponse: %v", err)
	}
	second, err := beads.OpenFileStore(fsys.OSFS{}, path)
	if err != nil {
		t.Fatalf("OpenFileStore(second): %v", err)
	}
	return first, second, front, created.ID, captured
}

func cloneConditionalMutationLeaseForTest(source *ConditionalMutationLease) *ConditionalMutationLease {
	source.mu.Lock()
	defer source.mu.Unlock()
	clone := &ConditionalMutationLease{
		front:           source.front,
		writer:          source.writer,
		predicateWriter: source.predicateWriter,
		metadataWriter:  source.metadataWriter,
		storeIdentity:   source.storeIdentity,
		sessionID:       source.sessionID,
		capturedRev:     source.capturedRev,
		revision:        source.revision,
		action:          source.action,
		nonce:           source.nonce,
		encodedIntent:   source.encodedIntent,
		predicates:      append([]ConditionalMutationPredicate(nil), source.predicates...),
		current:         source.current,
		active:          source.active,
		usePredicate:    source.usePredicate,
	}
	return clone
}
