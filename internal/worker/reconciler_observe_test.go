package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/beadstest"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

func TestFactoryReconcilerObservationRejectsDriftBeforeRepairRouteOrProvider(t *testing.T) {
	for _, operation := range []string{"observe", "peek"} {
		for _, drift := range []struct {
			name     string
			metadata map[string]string
		}{
			{name: "clear", metadata: map[string]string{
				beadmeta.TriggerBeadIDMetadataKey:       "",
				beadmeta.TriggerBeadStoreRefMetadataKey: "",
			}},
			{name: "repoint", metadata: map[string]string{
				beadmeta.TriggerBeadIDMetadataKey: "work-other",
			}},
			{name: "suspend", metadata: map[string]string{
				"state": string(sessionpkg.StateSuspended),
			}},
		} {
			t.Run(operation+"/"+drift.name, func(t *testing.T) {
				const id = "session-observe-strict"
				backing := beads.NewMemStoreFrom(1, []beads.Bead{{
					ID:     id,
					Type:   "",
					Title:  "repairable strict row",
					Status: "open",
					Labels: []string{sessionpkg.LabelSession},
					Metadata: map[string]string{
						"session_name":                          "s-" + id,
						"template":                              "strict-worker",
						"state":                                 string(sessionpkg.StateActive),
						"provider":                              "acp",
						"transport":                             "acp",
						beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
						beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
					},
				}}, nil)
				recorder := beadstest.NewRecordingStore(backing)
				provider := &launchAuthorizationRouteFake{Fake: runtime.NewFake()}
				factory, err := NewFactory(FactoryConfig{Store: recorder, Provider: provider})
				if err != nil {
					t.Fatalf("NewFactory: %v", err)
				}
				if err := backing.Update(id, beads.UpdateOpts{Metadata: drift.metadata}); err != nil {
					t.Fatalf("inject drift: %v", err)
				}
				recorder.Reset()
				witness := sessionpkg.LiveBoundaryWitness{
					TriggerBeadID:       "work-exact",
					TriggerBeadStoreRef: "city:fixture-city",
				}
				switch operation {
				case "observe":
					_, err = factory.ObserveSessionForReconciler(context.Background(), id, []string{"codex"}, witness)
				case "peek":
					_, err = factory.PeekSessionForReconciler(context.Background(), id, 20, witness)
				}
				if !errors.Is(err, sessionpkg.ErrLiveBoundaryWitnessMismatch) {
					t.Fatalf("%s error = %v, want witness mismatch", operation, err)
				}
				if calls := recorder.Calls(); len(calls) != 0 {
					t.Fatalf("session mutations after %s drift = %+v, want none", drift.name, calls)
				}
				if calls := provider.SnapshotCalls(); len(calls) != 0 {
					t.Fatalf("provider calls after %s drift = %+v, want none", drift.name, calls)
				}
				if provider.routes != 0 || provider.unroutes != 0 {
					t.Fatalf("ACP routing after %s drift = route %d unroute %d, want zero", drift.name, provider.routes, provider.unroutes)
				}
				current, err := backing.Get(id)
				if err != nil {
					t.Fatalf("Get final row: %v", err)
				}
				if current.Type != "" {
					t.Fatalf("repairable row type = %q, want empty after refusal", current.Type)
				}
			})
		}
	}
}

func TestFactoryReconcilerLifecycleObservationAllowsExactSuspendedRow(t *testing.T) {
	store := beads.NewMemStore()
	provider := runtime.NewFake()
	manager := sessionpkg.NewManagerWithOptions(store, provider)
	created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
		BeadOnly: true,
		Template: "strict-worker",
		Title:    "Strict Worker",
		Command:  "true",
		WorkDir:  t.TempDir(),
		Provider: "fake",
		ExtraMeta: map[string]string{
			"state":                                 string(sessionpkg.StateSuspended),
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	factory, err := NewFactory(FactoryConfig{Store: store, Provider: provider})
	if err != nil {
		t.Fatalf("NewFactory: %v", err)
	}
	witness := sessionpkg.LiveBoundaryWitness{TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"}
	if _, err := factory.ObserveSessionForReconcilerLifecycle(context.Background(), created.ID, nil, witness); err != nil {
		t.Fatalf("ObserveSessionForReconcilerLifecycle: %v", err)
	}
	if _, err := factory.ObserveSessionForReconciler(context.Background(), created.ID, nil, witness); !errors.Is(err, sessionpkg.ErrLiveBoundaryWitnessMismatch) {
		t.Fatalf("wake observation error = %v, want suspended refusal", err)
	}
}

func TestFactoryReconcilerObservationRequiresCapturedZeroToRemainZero(t *testing.T) {
	backing := beads.NewMemStore()
	recorder := beadstest.NewRecordingStore(backing)
	provider := runtime.NewFake()
	manager := sessionpkg.NewManagerWithOptions(recorder, provider)
	created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
		BeadOnly:     true,
		ExplicitName: "ordinary-worker",
		Template:     "ordinary-worker",
		Title:        "Ordinary Worker",
		Command:      "true",
		WorkDir:      t.TempDir(),
		Provider:     "fake",
		ExtraMeta: map[string]string{
			"state": string(sessionpkg.StateActive),
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	factory, err := NewFactory(FactoryConfig{Store: recorder, Provider: provider})
	if err != nil {
		t.Fatalf("NewFactory: %v", err)
	}
	providerBaseline := len(provider.SnapshotCalls())
	if _, err := factory.ObserveSessionForReconciler(context.Background(), created.ID, nil, sessionpkg.LiveBoundaryWitness{}); err != nil {
		t.Fatalf("zero->zero observation: %v", err)
	}
	if calls := provider.SnapshotCalls()[providerBaseline:]; len(calls) == 0 {
		t.Fatal("zero->zero observation made no provider call")
	}
	if err := backing.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{
		beadmeta.TriggerBeadIDMetadataKey:       "work-new",
		beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
	}}); err != nil {
		t.Fatalf("bind trigger after capture: %v", err)
	}
	recorder.Reset()
	providerBaseline = len(provider.SnapshotCalls())
	if _, err := factory.ObserveSessionForReconciler(context.Background(), created.ID, nil, sessionpkg.LiveBoundaryWitness{}); !errors.Is(err, sessionpkg.ErrLiveBoundaryWitnessMismatch) {
		t.Fatalf("zero->complete observation error = %v, want mismatch", err)
	}
	if calls := recorder.Calls(); len(calls) != 0 {
		t.Fatalf("zero->complete observation session mutations = %+v, want none", calls)
	}
	if calls := provider.SnapshotCalls()[providerBaseline:]; len(calls) != 0 {
		t.Fatalf("zero->complete observation provider calls = %+v, want none", calls)
	}
}
