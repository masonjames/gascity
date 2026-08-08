package main

import (
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/beadstest"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/testutil"
)

func TestBindPoolSessionTriggerBeadSerializesWithLiveBoundaryAndReloadsAuthority(t *testing.T) {
	const sessionID = "session-lock-witness"
	backing := beads.NewMemStoreFrom(1, []beads.Bead{{
		ID:     sessionID,
		Type:   session.BeadType,
		Title:  "strict pool session",
		Status: "open",
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"state":                                 string(session.StateAsleep),
			beadmeta.TriggerBeadIDMetadataKey:       "work-current",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
			beadmeta.BrainParentSIDMetadataKey:      "parent-current",
		},
	}}, nil)
	recorder := beadstest.NewRecordingStore(backing)
	front := session.NewStore(beads.SessionStore{Store: recorder})
	current, err := front.Get(sessionID)
	if err != nil {
		t.Fatalf("Get(current): %v", err)
	}
	// The caller snapshot already appears to match the requested trigger. Only
	// the required under-lock reload can discover that the durable row still
	// carries work-current and therefore needs one cluster update.
	callerSnapshot := current.ApplyPatch(session.MetadataPatch{
		beadmeta.TriggerBeadIDMetadataKey:       "work-requested",
		beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		beadmeta.BrainParentSIDMetadataKey:      "parent-requested",
	})
	recorder.Reset()

	started := make(chan struct{})
	result := make(chan struct {
		info session.Info
		err  error
	}, 1)
	err = session.WithSessionMutationLock(sessionID, func() error {
		go func() {
			close(started)
			info, bindErr := bindPoolSessionTriggerBead(
				&agentBuildParams{beadStore: recorder},
				nil,
				"strict-worker-1",
				callerSnapshot,
				SessionRequest{
					WorkBeadID:     "work-requested",
					WorkStoreRef:   "city:fixture-city",
					BrainParentSID: "parent-requested",
				},
			)
			result <- struct {
				info session.Info
				err  error
			}{info: info, err: bindErr}
		}()
		<-started
		select {
		case got := <-result:
			t.Fatalf("trigger bind crossed held session mutation lock: info=%+v err=%v", got.info, got.err)
		case <-time.After(testutil.GoroutineRaceTimeout / 10):
		}
		if calls := recorder.Calls(); len(calls) != 0 {
			t.Fatalf("trigger bind mutated while live boundary lock was held: %+v", calls)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithSessionMutationLock: %v", err)
	}

	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("bindPoolSessionTriggerBead: %v", got.err)
		}
		if got.info.TriggerBeadID != "work-requested" || got.info.BrainParentSID != "parent-requested" {
			t.Fatalf("bound info = %+v, want requested trigger and parent", got.info)
		}
	case <-time.After(testutil.GoroutineRaceTimeout):
		t.Fatal("trigger bind did not resume after session mutation lock release")
	}

	calls := recorder.CallsForOp("Update")
	if len(calls) != 1 {
		t.Fatalf("Update calls = %+v, want one authoritative cluster write", calls)
	}
	after, err := front.Get(sessionID)
	if err != nil {
		t.Fatalf("Get(after): %v", err)
	}
	if after.TriggerBeadID != "work-requested" || after.TriggerBeadStoreRef != "city:fixture-city" || after.BrainParentSID != "parent-requested" {
		t.Fatalf("persisted trigger cluster = %+v, want requested authority", after)
	}
}
