package main

import (
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

func TestSessionSnapshotCarriesCapturedRevisionIntoMutationBoundary(t *testing.T) {
	store := beads.NewMemStore()
	row, err := store.Create(beads.Bead{
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
	updatedTitle := "strict-worker updated"
	if err := store.Update(row.ID, beads.UpdateOpts{Title: &updatedTitle}); err != nil {
		t.Fatal(err)
	}
	want, err := store.Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := loadSessionBeadSnapshot(store)
	if err != nil {
		t.Fatal(err)
	}
	rows := snapshot.OpenForReconcile()
	if len(rows) != 1 {
		t.Fatalf("rows=%d, want 1", len(rows))
	}
	if got := rows[0].Persisted.Revision; got != want.Revision {
		t.Fatalf("captured revision=%d, want %d", got, want.Revision)
	}
	boundary := captureReconcilerMutationBoundary(
		rows[0].Info,
		&config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}},
		rows[0].Persisted.Revision,
	)
	if got := boundary.capturedRevision; got != want.Revision {
		t.Fatalf("boundary revision=%d, want %d", got, want.Revision)
	}
	if !boundary.revisionCaptured {
		t.Fatal("boundary did not retain decision-revision presence")
	}
	candidate := captureStartCandidateForWake(
		rows[0].Info,
		TemplateParams{TemplateName: "strict-worker"},
		0,
		&config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}},
		"work-exact",
		rows[0].Persisted.Revision,
	)
	if got := candidate.capturedRevision; got != want.Revision {
		t.Fatalf("candidate revision=%d, want %d", got, want.Revision)
	}
	if !candidate.revisionCaptured {
		t.Fatal("candidate did not retain decision-revision presence")
	}
	tick := newReconcileTick(
		[]session.Info{rows[0].Info},
		map[string]int64{rows[0].Info.ID: rows[0].Persisted.Revision},
	)
	if got := tick.capturedRevision(rows[0].Info.ID); got != want.Revision {
		t.Fatalf("tick revision=%d, want %d", got, want.Revision)
	}
}

func TestReconcileTickAdvancesOnlyItsOwnConditionalMutationRevision(t *testing.T) {
	store := beads.NewMemStore()
	row, err := store.Create(beads.Bead{
		Title:  "strict-worker",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"template":                              "strict-worker",
			"session_name":                          "strict-worker-1",
			"state":                                 string(session.StateSuspended),
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	info, persisted, err := sessionFrontDoor(store).GetPersistedResponse(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	tick := newReconcileTick([]session.Info{info}, map[string]int64{info.ID: persisted.Revision})
	boundary := tick.mutationBoundary(info, &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}).lifecycleMutation()
	ran, err := boundary.run(store, nil, func(current session.Info, front *session.Store) error {
		return front.ApplyPatch(current.ID, session.MetadataPatch{"own_tick_write": "true"})
	})
	if err != nil || !ran {
		t.Fatalf("conditional boundary=(ran=%t err=%v), want success", ran, err)
	}
	current, err := store.Get(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := tick.capturedRevision(info.ID); got != current.Revision {
		t.Fatalf("tick revision=%d, current=%d", got, current.Revision)
	}

	stale := tick.capturedRevision(info.ID)
	if err := store.Update(info.ID, beads.UpdateOpts{Metadata: map[string]string{"foreign": "true"}}); err != nil {
		t.Fatal(err)
	}
	actions := 0
	ran, err = tick.mutationBoundary(tick.infoByID[info.ID], &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}).lifecycleMutation().run(store, nil, func(session.Info, *session.Store) error {
		actions++
		return nil
	})
	if !errors.Is(err, session.ErrConditionalMutationLost) || ran || actions != 0 {
		t.Fatalf("external drift=(ran=%t actions=%d err=%v), want CAS refusal", ran, actions, err)
	}
	if got := tick.capturedRevision(info.ID); got != stale {
		t.Fatalf("tick adopted external revision: got=%d want=%d", got, stale)
	}
}

func TestReconcileTickDoesNotInventMissingRevisionZero(t *testing.T) {
	info := session.Info{
		ID:                  "strict-missing-revision",
		Type:                session.BeadType,
		State:               session.StateSuspended,
		Template:            "strict-worker",
		TriggerBeadID:       "work-exact",
		TriggerBeadStoreRef: "city:fixture-city",
	}
	cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
	for _, tick := range []*reconcileTick{
		newReconcileTick([]session.Info{info}),
		newReconcileTick([]session.Info{info}, map[string]int64{"some-other-session": 0}),
	} {
		boundary := tick.mutationBoundary(info, cfg).lifecycleMutation()
		if boundary.revisionCaptured {
			t.Fatal("missing revision was treated as a captured zero token")
		}
		candidate := tick.startCandidateForWake(info, TemplateParams{TemplateName: "strict-worker"}, 0, cfg, "work-exact")
		if candidate.revisionCaptured {
			t.Fatal("missing revision was treated as a captured zero start-candidate token")
		}
	}
}

func TestReconcileTickStartCandidateRetainsCapturedRevisionZero(t *testing.T) {
	info := session.Info{
		ID:                  "strict-revision-zero",
		Type:                session.BeadType,
		State:               session.StateActive,
		Template:            "strict-worker",
		TriggerBeadID:       "work-exact",
		TriggerBeadStoreRef: "city:fixture-city",
	}
	cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
	tick := newReconcileTick([]session.Info{info}, map[string]int64{info.ID: 0})
	candidate := tick.startCandidateForWake(info, TemplateParams{TemplateName: "strict-worker"}, 0, cfg, "work-exact")
	if !candidate.revisionCaptured || candidate.capturedRevision != 0 {
		t.Fatalf("candidate revision=(%d,%t), want captured zero", candidate.capturedRevision, candidate.revisionCaptured)
	}
}

func TestReconcilerMutationBoundaryBuildsSameReadAutomaticRuntimeDecision(t *testing.T) {
	info := session.Info{
		ID:                  "strict-runtime-decision",
		Type:                session.BeadType,
		State:               session.StateActive,
		Template:            "strict-worker",
		InstanceToken:       "instance-exact",
		TriggerBeadID:       "work-exact",
		TriggerBeadStoreRef: "city:fixture-city",
	}
	cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
	decision, err := captureReconcilerMutationBoundary(info, cfg, 0).automaticRuntimeDecision()
	if err != nil {
		t.Fatalf("automaticRuntimeDecision: %v", err)
	}
	if decision.Captured.ID != info.ID || decision.ExpectedRevision != 0 || !decision.RevisionCaptured {
		t.Fatalf("decision carrier = %+v, want exact info and captured revision zero", decision)
	}
	if decision.Witness != (session.LiveBoundaryWitness{TriggerBeadID: "work-exact", TriggerBeadStoreRef: "city:fixture-city"}) {
		t.Fatalf("decision witness = %+v, want exact pair", decision.Witness)
	}
	if !decision.ProjectHooksForbidden {
		t.Fatal("decision lost captured forbid policy")
	}

	_, err = captureReconcilerMutationBoundary(info, cfg).automaticRuntimeDecision()
	if !errors.Is(err, session.ErrAutomaticRuntimeDecisionInvalid) {
		t.Fatalf("missing-revision decision error = %v, want ErrAutomaticRuntimeDecisionInvalid", err)
	}
}

func TestReconcilerMutationBoundaryChainsOnlyExactAutomaticRuntimeCommit(t *testing.T) {
	info := session.Info{
		ID:                  "strict-runtime-commit",
		Type:                session.BeadType,
		State:               session.StateActive,
		Template:            "strict-worker",
		InstanceToken:       "instance-exact",
		TriggerBeadID:       "work-exact",
		TriggerBeadStoreRef: "city:fixture-city",
	}
	cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
	advancedRevision := int64(-1)
	boundary := captureReconcilerMutationBoundary(info, cfg, 3).withRevisionSink(func(revision int64) {
		advancedRevision = revision
	})
	chained, err := boundary.afterAutomaticRuntimeCommit(session.AutomaticRuntimeCommit{
		Info:      info,
		Persisted: session.PersistedResponse{Revision: 5},
	})
	if err != nil {
		t.Fatalf("afterAutomaticRuntimeCommit: %v", err)
	}
	if chained.capturedRevision != 5 || !chained.revisionCaptured || advancedRevision != 5 {
		t.Fatalf("chained revision=(%d,%t) sink=%d, want 5,true,5", chained.capturedRevision, chained.revisionCaptured, advancedRevision)
	}

	drifted := info
	drifted.TriggerBeadID = "work-repointed"
	_, err = boundary.afterAutomaticRuntimeCommit(session.AutomaticRuntimeCommit{
		Info:      drifted,
		Persisted: session.PersistedResponse{Revision: 6},
	})
	if !errors.Is(err, session.ErrLiveBoundaryWitnessMismatch) {
		t.Fatalf("drifted commit error = %v, want witness mismatch", err)
	}
	if advancedRevision != 5 {
		t.Fatalf("drifted commit advanced sink to %d, want unchanged 5", advancedRevision)
	}
}
