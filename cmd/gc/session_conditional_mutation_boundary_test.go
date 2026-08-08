package main

import (
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

type sessionMutationStoreOnly struct{ beads.Store }

func TestStrictReconcilerMutationBoundaryRequiresCapturedRevisionCAS(t *testing.T) {
	for _, tc := range []struct {
		name       string
		wrap       func(beads.Store) beads.Store
		drift      bool
		driftInRun bool
		wantErr    error
	}{
		{name: "exact revision commits"},
		{name: "drift before acquire refuses", drift: true, wantErr: session.ErrConditionalMutationLost},
		{name: "unsupported writer refuses", wrap: func(store beads.Store) beads.Store { return sessionMutationStoreOnly{Store: store} }, wantErr: session.ErrConditionalMutationUnsupported},
		{name: "drift after acquire is not overwritten", driftInRun: true, wantErr: session.ErrConditionalMutationLost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backing := beads.NewMemStore()
			row, err := backing.Create(beads.Bead{
				Title:  "strict-worker",
				Type:   session.BeadType,
				Status: "open",
				Labels: []string{session.LabelSession},
				Metadata: map[string]string{
					"template":                              "strict-worker",
					"session_name":                          "strict-worker-1",
					"state":                                 string(session.StateActive),
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			store := beads.Store(backing)
			if tc.wrap != nil {
				store = tc.wrap(store)
			}
			captured, persisted, err := sessionFrontDoor(store).GetPersistedResponse(row.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.drift {
				if err := backing.Update(row.ID, beads.UpdateOpts{Metadata: map[string]string{"foreign": "before"}}); err != nil {
					t.Fatal(err)
				}
			}
			cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
			boundary := captureReconcilerMutationBoundary(captured, cfg, persisted.Revision).lifecycleMutation()
			actionCalls := 0
			ran, err := boundary.run(store, nil, func(current session.Info, front *session.Store) error {
				actionCalls++
				if tc.driftInRun {
					if err := backing.Update(current.ID, beads.UpdateOpts{Metadata: map[string]string{"foreign": "after"}}); err != nil {
						return err
					}
				}
				return front.ApplyPatch(current.ID, session.MetadataPatch{"ours": "committed"})
			})
			if tc.wantErr == nil {
				if err != nil || !ran || actionCalls != 1 {
					t.Fatalf("run=(ran=%t calls=%d err=%v), want true,1,nil", ran, actionCalls, err)
				}
			} else {
				if !errors.Is(err, tc.wantErr) || ran {
					t.Fatalf("run=(ran=%t calls=%d err=%v), want false,%v", ran, actionCalls, err, tc.wantErr)
				}
				if !tc.driftInRun && actionCalls != 0 {
					t.Fatalf("action calls=%d, want 0 before acquire", actionCalls)
				}
			}
			got, err := backing.Get(row.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantErr == nil && got.Metadata["ours"] != "committed" {
				t.Fatalf("metadata=%#v, want committed patch", got.Metadata)
			}
			if tc.wantErr != nil && got.Metadata["ours"] != "" {
				t.Fatalf("metadata=%#v, refused patch was committed", got.Metadata)
			}
			if got.Metadata[session.ConditionalMutationLeaseMetadataKey] != "" {
				t.Fatalf("lease residue=%q", got.Metadata[session.ConditionalMutationLeaseMetadataKey])
			}
		})
	}
}

func TestStrictReconcilerMutationBoundaryWithoutDecisionRevisionFailsClosed(t *testing.T) {
	store := beads.NewMemStore()
	row, err := store.Create(beads.Bead{
		Title:  "strict-worker",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"template":                              "strict-worker",
			"state":                                 string(session.StateSuspended),
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	captured := session.InfoFromPersistedBead(row)
	boundary := captureReconcilerMutationBoundary(captured, &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}).lifecycleMutation()
	actionCalls := 0
	ran, err := boundary.run(store, nil, func(session.Info, *session.Store) error {
		actionCalls++
		return nil
	})
	if !errors.Is(err, session.ErrConditionalMutationInvalid) || ran || actionCalls != 0 {
		t.Fatalf("run=(ran=%t calls=%d err=%v), want fail-closed missing revision", ran, actionCalls, err)
	}
}

func TestStrictReconcilerMutationBoundaryAcceptsCapturedRevisionZero(t *testing.T) {
	row := beads.Bead{
		ID:       "gc-zero",
		Title:    "strict-worker",
		Type:     session.BeadType,
		Status:   "open",
		Revision: 0,
		Labels:   []string{session.LabelSession},
		Metadata: map[string]string{
			"template":                              "strict-worker",
			"state":                                 string(session.StateSuspended),
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	}
	store := beads.NewMemStoreFrom(1, []beads.Bead{row}, nil)
	captured, persisted, err := sessionFrontDoor(store).GetPersistedResponse(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Revision != 0 {
		t.Fatalf("revision=%d, want 0", persisted.Revision)
	}
	boundary := captureReconcilerMutationBoundary(
		captured,
		&config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}},
		persisted.Revision,
	).lifecycleMutation()
	actionCalls := 0
	ran, err := boundary.run(store, nil, func(current session.Info, front *session.Store) error {
		actionCalls++
		return front.ApplyPatch(current.ID, session.MetadataPatch{"zero_revision": "accepted"})
	})
	if err != nil || !ran || actionCalls != 1 {
		t.Fatalf("run=(ran=%t calls=%d err=%v), want true,1,nil", ran, actionCalls, err)
	}
	got, err := store.Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata["zero_revision"] != "accepted" {
		t.Fatalf("metadata=%#v, want zero-revision mutation committed", got.Metadata)
	}
}
