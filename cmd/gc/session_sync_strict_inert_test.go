package main

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
)

func TestSameStoreStrictSyncLeavesExactRoutedRowAndWorkInert(t *testing.T) {
	store := beads.NewMemStore()
	work, err := store.Create(beads.Bead{
		Title:    "strict demand",
		Type:     "task",
		Status:   "open",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
	})
	if err != nil {
		t.Fatalf("create work: %v", err)
	}
	row, err := store.Create(beads.Bead{
		Title:  "worker-1",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession, "template:worker"},
		Metadata: map[string]string{
			"template":                              "worker",
			"agent_name":                            "worker",
			"session_name":                          "worker-1",
			"state":                                 string(session.StateActive),
			beadmeta.TriggerBeadIDMetadataKey:       work.ID,
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	inProgress, assignee := "in_progress", row.ID
	if err := store.Update(work.ID, beads.UpdateOpts{Status: &inProgress, Assignee: &assignee}); err != nil {
		t.Fatalf("assign work: %v", err)
	}
	beforeRow, err := store.Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := loadSessionBeadSnapshot(store)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{
		Workspace: config.Workspace{Name: "fixture-city"},
		Agents: []config.Agent{{
			Name:         "worker",
			ProjectHooks: config.ProjectHooksForbid,
		}},
	}
	var stderr bytes.Buffer
	_, updated := syncSessionBeadsWithStores(
		"",
		beads.SessionStore{Store: store},
		beads.WorkStore{Store: store},
		nil,
		map[string]TemplateParams{},
		runtime.NewFake(),
		map[string]bool{},
		cfg,
		&clock.Fake{Time: time.Unix(1_700_000_000, 0).UTC()},
		&stderr,
		false,
		snapshot,
	)
	if updated == nil {
		t.Fatalf("updated snapshot is nil; stderr=%q", stderr.String())
	}
	afterRow, err := store.Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	afterWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterRow, beforeRow) {
		t.Fatalf("strict row changed during sync:\nbefore=%+v\nafter=%+v\nstderr=%q", beforeRow, afterRow, stderr.String())
	}
	if !reflect.DeepEqual(afterWork, beforeWork) {
		t.Fatalf("strict work changed during sync:\nbefore=%+v\nafter=%+v\nstderr=%q", beforeWork, afterWork, stderr.String())
	}
}
