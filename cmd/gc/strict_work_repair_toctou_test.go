package main

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

// strictRepairInterleaveStore deterministically lands an ownership mutation at
// the exact legacy read-then-write seam. A strict repair must never call one of
// these unconditional writers; entering the wrapper proves that a claim,
// repoint, or hold could land after the repair's stale read.
type strictRepairInterleaveStore struct {
	beads.Store
	once   sync.Once
	writes atomic.Int64
	inject func(beads.Store, string) error
}

func (s *strictRepairInterleaveStore) beforeWrite(id string) error {
	s.writes.Add(1)
	var err error
	s.once.Do(func() {
		if s.inject != nil {
			err = s.inject(s.Store, id)
		}
	})
	return err
}

func (s *strictRepairInterleaveStore) Update(id string, opts beads.UpdateOpts) error {
	if err := s.beforeWrite(id); err != nil {
		return err
	}
	return s.Store.Update(id, opts)
}

func (s *strictRepairInterleaveStore) SetMetadata(id, key, value string) error {
	if err := s.beforeWrite(id); err != nil {
		return err
	}
	return s.Store.SetMetadata(id, key, value)
}

func (s *strictRepairInterleaveStore) SetMetadataBatch(id string, kvs map[string]string) error {
	if err := s.beforeWrite(id); err != nil {
		return err
	}
	return s.Store.SetMetadataBatch(id, kvs)
}

func strictRepairBuildParams(cfg *config.City, store beads.Store) *agentBuildParams {
	return &agentBuildParams{
		city:                   cfg,
		beadStore:              store,
		canonicalCityWorkStore: beads.WorkStore{Store: store},
	}
}

func assertStrictRepairDidNotWrite(t *testing.T, store *strictRepairInterleaveStore, id string, beforeRevision int64) {
	t.Helper()
	if writes := store.writes.Load(); writes != 0 {
		t.Fatalf("strict legacy repair entered %d unconditional write(s), want 0", writes)
	}
	after, err := store.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != beforeRevision {
		t.Fatalf("strict work %s revision=%d, want unchanged %d", id, after.Revision, beforeRevision)
	}
}

func TestStrictSameStoreLegacyWorkRepairsDoNotEnterTOCTOUWindow(t *testing.T) {
	t.Run("observability stamp and root propagation skip repoint window", func(t *testing.T) {
		cfg := &config.City{Agents: []config.Agent{{Name: "worker", ProjectHooks: config.ProjectHooksForbid}}}
		root := beads.Bead{ID: "root-1", Type: "task", Status: "open", Metadata: map[string]string{beadmeta.KindMetadataKey: beadmeta.KindWorkflow}}
		step := beads.Bead{
			ID: "step-1", Type: "task", Status: "in_progress", Assignee: "strict-session",
			Metadata: map[string]string{beadmeta.RootBeadIDMetadataKey: root.ID},
		}
		mem := beads.NewMemStoreFrom(0, []beads.Bead{root, step}, nil)
		store := &strictRepairInterleaveStore{
			Store: mem,
			inject: func(base beads.Store, id string) error {
				lateOwner := "late-owner"
				return base.Update(id, beads.UpdateOpts{Assignee: &lateOwner})
			},
		}
		beforeStep, _ := mem.Get(step.ID)
		beforeRoot, _ := mem.Get(root.ID)
		sessions := newSessionBeadSnapshot([]beads.Bead{{
			ID: "session-1", Type: session.BeadType, Status: "open",
			Metadata: map[string]string{
				"template": "worker", "agent_name": "worker", "session_name": "strict-session",
				"state": string(session.StateActive), "work_dir": "/strict/workdir",
			},
		}})

		stampRunSessionIdentityWithStoreFence(
			strictRepairBuildParams(cfg, store),
			[]beads.Bead{step},
			[]beads.Store{store},
			sessions,
			io.Discard,
		)

		assertStrictRepairDidNotWrite(t, store, step.ID, beforeStep.Revision)
		afterRoot, err := mem.Get(root.ID)
		if err != nil {
			t.Fatal(err)
		}
		if afterRoot.Revision != beforeRoot.Revision {
			t.Fatalf("strict root revision=%d, want unchanged %d", afterRoot.Revision, beforeRoot.Revision)
		}
	})

	t.Run("assigned legacy rehome skips owner repoint window", func(t *testing.T) {
		cfg := legacyBoundRecoveryConfig()
		cfg.Agents[0].ProjectHooks = config.ProjectHooksForbid
		const legacy = "rig-A/gc.planner"
		work := workBead("assigned-1", legacy, legacy, "in_progress", 5)
		mem := beads.NewMemStoreFrom(0, []beads.Bead{work}, nil)
		store := &strictRepairInterleaveStore{
			Store: mem,
			inject: func(base beads.Store, id string) error {
				lateOwner := "late-session-owner"
				return base.Update(id, beads.UpdateOpts{Assignee: &lateOwner})
			},
		}
		before, _ := mem.Get(work.ID)

		canonicalizeLegacyBoundAssignedWorkWithStoreFence(
			strictRepairBuildParams(cfg, store), cfg,
			[]beads.Bead{work}, []beads.Store{store}, newSessionBeadSnapshot(nil), io.Discard,
		)

		assertStrictRepairDidNotWrite(t, store, work.ID, before.Revision)
	})

	t.Run("unassigned legacy route rehome skips hold window", func(t *testing.T) {
		cfg := legacyBoundRecoveryConfig()
		cfg.Agents[0].ProjectHooks = config.ProjectHooksForbid
		const legacy = "rig-A/gc.planner"
		work := workBead("unassigned-1", legacy, "", "open", 5)
		mem := beads.NewMemStoreFrom(0, []beads.Bead{work}, nil)
		store := &strictRepairInterleaveStore{
			Store: mem,
			inject: func(base beads.Store, id string) error {
				return base.Update(id, beads.UpdateOpts{Labels: []string{beadmeta.HoldExternalLabel}})
			},
		}
		before, _ := mem.Get(work.ID)

		canonicalizeLegacyBoundUnassignedRoutedWorkWithStoreFence(
			strictRepairBuildParams(cfg, store), cfg,
			[]beads.Bead{work}, []beads.Store{store}, io.Discard,
		)

		assertStrictRepairDidNotWrite(t, store, work.ID, before.Revision)
	})

	t.Run("control route repair skips hold window", func(t *testing.T) {
		maxActive := 1
		cfg := &config.City{Agents: []config.Agent{{
			Name:              config.ControlDispatcherAgentName,
			BindingName:       "core",
			Dir:               "fixture",
			StartCommand:      config.ControlDispatcherStartCommandFor("{{.Agent}}"),
			MaxActiveSessions: &maxActive,
			ProjectHooks:      config.ProjectHooksForbid,
		}}}
		work := beads.Bead{
			ID: "control-1", Type: "task", Status: "open",
			Metadata: map[string]string{
				beadmeta.KindMetadataKey:         beadmeta.KindWorkflowFinalize,
				beadmeta.RoutedToMetadataKey:     "core.control-dispatcher",
				beadmeta.RootStoreRefMetadataKey: "rig:fixture",
			},
		}
		mem := beads.NewMemStoreFrom(0, []beads.Bead{work}, nil)
		store := &strictRepairInterleaveStore{
			Store: mem,
			inject: func(base beads.Store, id string) error {
				return base.Update(id, beads.UpdateOpts{Labels: []string{beadmeta.HoldMayorLabel}})
			},
		}
		before, _ := mem.Get(work.ID)
		workSnapshot := []beads.Bead{work}

		repairControlDispatcherRoutesForStoreScopeWithStoreFence(
			t.Name(), strictRepairBuildParams(cfg, store), cfg,
			workSnapshot, []beads.Store{store}, []string{"rig:fixture"}, io.Discard,
		)

		assertStrictRepairDidNotWrite(t, store, work.ID, before.Revision)
	})

	t.Run("route recovery skips late claim window", func(t *testing.T) {
		cfg := &config.City{Agents: []config.Agent{{Name: "worker", ProjectHooks: config.ProjectHooksForbid}}}
		work := beads.Bead{
			ID: "route-1", Type: "task", Status: "open",
			Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "worker"},
		}
		mem := beads.NewMemStoreFrom(0, []beads.Bead{work}, nil)
		store := &strictRepairInterleaveStore{
			Store: mem,
			inject: func(base beads.Store, id string) error {
				status, owner := "in_progress", "late-claim-owner"
				return base.Update(id, beads.UpdateOpts{Status: &status, Assignee: &owner})
			},
		}
		before, _ := mem.Get(work.ID)

		restored, err := restoreCarriedWorkRoutesWithPredicate(store, func(route string, candidate beads.Store) bool {
			return strictWorkMutationAuthorized(
				cfg, route,
				beads.SessionStore{Store: store},
				beads.WorkStore{Store: store},
				candidate,
			)
		})
		if err != nil {
			t.Fatal(err)
		}
		if restored != 0 {
			t.Fatalf("restored=%d, want 0 for strict route recovery", restored)
		}
		assertStrictRepairDidNotWrite(t, store, work.ID, before.Revision)
	})
}

func TestInheritSameStoreLegacyWorkRepairsRemainCompatible(t *testing.T) {
	cfg := &config.City{Agents: []config.Agent{{Name: "worker", ProjectHooks: config.ProjectHooksInherit}}}
	work := beads.Bead{
		ID: "route-inherit", Type: "task", Status: "open",
		Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "worker"},
	}
	mem := beads.NewMemStoreFrom(0, []beads.Bead{work}, nil)

	restored, err := restoreCarriedWorkRoutesWithPredicate(mem, func(route string, candidate beads.Store) bool {
		return strictWorkMutationAuthorized(
			cfg, route,
			beads.SessionStore{Store: mem},
			beads.WorkStore{Store: mem},
			candidate,
		)
	})
	if err != nil {
		t.Fatal(err)
	}
	if restored != 1 {
		t.Fatalf("inherit restored=%d, want 1", restored)
	}
	got, err := mem.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata[beadmeta.RoutedToMetadataKey] != "worker" {
		t.Fatalf("inherit gc.routed_to=%q, want worker", got.Metadata[beadmeta.RoutedToMetadataKey])
	}
}
