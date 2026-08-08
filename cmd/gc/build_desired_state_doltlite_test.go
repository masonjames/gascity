//go:build gascity_native_beads

package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
)

func TestNativeDoltliteCanonicalHoldNeverBecomesDefaultPoolDemand(t *testing.T) {
	const (
		template = "fixture/worker"
		heldID   = "gc-held-older"
		unheldID = "gc-unheld-newer"
	)
	store := newControllerDemandDoltliteFixture(t, []doltliteDemandFixture{
		{
			ID:        heldID,
			Title:     "older canonically held work",
			CreatedAt: time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC),
			Labels:    []string{beadmeta.HoldMayorLabel},
		},
		{
			ID:        unheldID,
			Title:     "newer unheld work",
			CreatedAt: time.Date(2026, 8, 7, 12, 0, 1, 0, time.UTC),
		},
	}, template)

	ready, err := store.Ready()
	if err != nil {
		t.Fatalf("native DoltLite Ready: %v", err)
	}
	if got := doltliteDemandIDs(ready); !reflect.DeepEqual(got, []string{heldID, unheldID}) {
		t.Fatalf("native DoltLite Ready IDs = %v, want deterministic label-blind order", got)
	}
	for _, row := range ready {
		if len(row.Labels) != 0 {
			t.Fatalf("native DoltLite Ready labels for %s = %v, want label-blind projection", row.ID, row.Labels)
		}
	}
	heldRows, err := beads.HandlesFor(store).Live.List(beads.ListQuery{Label: beadmeta.HoldMayorLabel})
	if err != nil {
		t.Fatalf("native DoltLite live hold-label list: %v", err)
	}
	if got := doltliteDemandIDs(heldRows); !reflect.DeepEqual(got, []string{heldID}) {
		t.Fatalf("native DoltLite live hold-label IDs = %v, want [%s]", got, heldID)
	}

	counts, demand, partial, errs := defaultScaleCheckCountsAndDemand(nil, []defaultScaleCheckTarget{{
		template: template,
		storeKey: "rig:fixture",
		store:    store,
	}}, newReadyDemandCache())
	if len(errs) != 0 || len(partial) != 0 {
		t.Fatalf("default demand errors=%v partial=%v, want complete native reads", errs, partial)
	}
	if got := counts[template]; got != 1 {
		t.Fatalf("default demand count = %d, want only the unheld row", got)
	}
	if got := demand[template].WorkBeadIDs; !reflect.DeepEqual(got, []string{unheldID}) {
		t.Fatalf("default demand IDs = %v, want [%s]", got, unheldID)
	}
	if got := demand[template].StoreRefs[unheldID]; got != "rig:fixture" {
		t.Fatalf("default demand store ref = %q, want rig:fixture", got)
	}
}

type doltliteDemandFixture struct {
	ID        string
	Title     string
	CreatedAt time.Time
	Labels    []string
}

func newControllerDemandDoltliteFixture(t *testing.T, rows []doltliteDemandFixture, route string) *beads.DoltliteReadStore {
	t.Helper()
	root := t.TempDir()
	beadsDir := filepath.Join(root, ".beads")
	dbDir := filepath.Join(beadsDir, "doltlite")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatalf("create DoltLite fixture directory: %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(beadsDir, "metadata.json"),
		[]byte(`{"backend":"doltlite","database":"doltlite","dolt_database":"hq"}`),
		0o600,
	); err != nil {
		t.Fatalf("write DoltLite fixture metadata: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dbDir, "hq.db")+"?_busy_timeout=10000")
	if err != nil {
		t.Fatalf("open DoltLite fixture writer: %v", err)
	}
	for _, statement := range controllerDemandDoltliteSchema {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			t.Fatalf("create DoltLite fixture schema: %v\nstatement: %s", err, statement)
		}
	}
	metadata := `{"gc.routed_to":"` + route + `"}`
	for _, row := range rows {
		createdAt := row.CreatedAt.Format(time.RFC3339Nano)
		if _, err := db.Exec(`INSERT INTO issues (
			id, title, status, issue_type, priority, created_at, updated_at,
			assignee, description, design, acceptance_criteria, notes, metadata,
			ephemeral, no_history
		) VALUES (?, ?, 'open', 'task', 2, ?, ?, '', '', '', '', '', ?, 0, 0)`,
			row.ID, row.Title, createdAt, createdAt, metadata,
		); err != nil {
			_ = db.Close()
			t.Fatalf("insert DoltLite fixture row %s: %v", row.ID, err)
		}
		for _, label := range row.Labels {
			if _, err := db.Exec(`INSERT INTO labels (issue_id, label) VALUES (?, ?)`, row.ID, label); err != nil {
				_ = db.Close()
				t.Fatalf("insert DoltLite fixture label %s for %s: %v", label, row.ID, err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close DoltLite fixture writer: %v", err)
	}

	backing := beads.NewBdStore(root, func(string, string, ...string) ([]byte, error) {
		t.Fatal("native DoltLite demand reads must not invoke the bd subprocess")
		return nil, nil
	})
	store, err := beads.NewDoltliteReadStore(root, backing)
	if err != nil {
		t.Fatalf("open native DoltLite read store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.CloseStore(); err != nil {
			t.Errorf("close native DoltLite read store: %v", err)
		}
	})
	return store
}

func doltliteDemandIDs(rows []beads.Bead) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	return ids
}

var controllerDemandDoltliteSchema = []string{
	`CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)`,
	`CREATE TABLE issues (
		id TEXT PRIMARY KEY, title TEXT, status TEXT, issue_type TEXT,
		priority INTEGER, created_at TEXT, updated_at TEXT, assignee TEXT,
		description TEXT, design TEXT, acceptance_criteria TEXT, notes TEXT,
		metadata TEXT, ephemeral INTEGER DEFAULT 0, no_history INTEGER DEFAULT 0
	)`,
	`CREATE TABLE wisps (
		id TEXT PRIMARY KEY, title TEXT, status TEXT, issue_type TEXT,
		priority INTEGER, created_at TEXT, updated_at TEXT, assignee TEXT,
		description TEXT, design TEXT, acceptance_criteria TEXT, notes TEXT,
		metadata TEXT, ephemeral INTEGER DEFAULT 0, no_history INTEGER DEFAULT 0
	)`,
	`CREATE TABLE labels (issue_id TEXT, label TEXT)`,
	`CREATE TABLE wisp_labels (issue_id TEXT, label TEXT)`,
	`CREATE TABLE dependencies (
		issue_id TEXT, depends_on_id TEXT, depends_on_issue_id TEXT,
		depends_on_wisp_id TEXT, depends_on_external TEXT, type TEXT
	)`,
	`CREATE TABLE wisp_dependencies (
		issue_id TEXT, depends_on_id TEXT, depends_on_issue_id TEXT,
		depends_on_wisp_id TEXT, depends_on_external TEXT, type TEXT
	)`,
	`INSERT INTO config (key, value) VALUES ('issue_prefix', 'gc')`,
}
