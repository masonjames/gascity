package main

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

type explicitCloseGetFailureStore struct {
	beads.Store
	err error
}

func (s explicitCloseGetFailureStore) Get(string) (beads.Bead, error) {
	return beads.Bead{}, s.err
}

func TestCaptureExplicitSessionCloseAuthorityFailsClosedOnReadError(t *testing.T) {
	wantErr := errors.New("injected classification read failure")
	store := explicitCloseGetFailureStore{Store: beads.NewMemStore(), err: wantErr}
	info, strict, witness, err := captureExplicitSessionCloseAuthority(
		&config.City{Workspace: config.Workspace{Name: "test-city"}},
		"/cities/test-city",
		store,
		beads.WorkStore{Store: store.Store},
		"session-strict",
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("capture error = %v, want %v", err, wantErr)
	}
	if info.ID != "" || strict || witness != (session.LiveBoundaryWitness{}) {
		t.Fatalf("capture result = (info=%+v, strict=%t, witness=%+v), want zero authority", info, strict, witness)
	}
}

// TestCmdSessionCloseReleasesAssignedWorkBeads is a regression for
// gastownhall/gascity#2625. After `gc session close`, any work bead still
// assigned to the closed session must be released (Assignee cleared, Status
// reset to open) so the pool scale-check picks up the freed demand on the
// next reconcile tick. Without it, Source-1 CachedReady stays stale, the
// pool scale-check sees scaleCount=0, and no fresh worker spawns even
// though the demand is admittable.
func TestCmdSessionCloseReleasesAssignedWorkBeads(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
start_command = "true"
max_active_sessions = 1
`)
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_DIR", t.TempDir())
	t.Setenv("GC_BEADS", "file")
	t.Setenv("GC_SESSION", "fake")

	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}

	sessionBead, err := store.Create(beads.Bead{
		Title:  "stranded worker",
		Type:   session.BeadType,
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"session_name": "worker-stranded",
			"template":     "worker",
			"state":        "active",
		},
	})
	if err != nil {
		t.Fatalf("Create(session bead): %v", err)
	}

	work, err := store.Create(beads.Bead{
		Title:    "admittable demand",
		Type:     "task",
		Assignee: sessionBead.ID,
		Metadata: map[string]string{"gc.routed_to": "worker"},
	})
	if err != nil {
		t.Fatalf("Create(work bead): %v", err)
	}
	inProgress := "in_progress"
	if err := store.Update(work.ID, beads.UpdateOpts{Status: &inProgress}); err != nil {
		t.Fatalf("mark work in_progress: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := cmdSessionClose([]string{sessionBead.ID}, &stdout, &stderr); code != 0 {
		t.Fatalf("cmdSessionClose = %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}

	reopened, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("reopen city store: %v", err)
	}

	gotSession, err := reopened.Get(sessionBead.ID)
	if err != nil {
		t.Fatalf("Get(session bead): %v", err)
	}
	if gotSession.Status != "closed" {
		t.Errorf("session bead status = %q, want closed", gotSession.Status)
	}

	gotWork, err := reopened.Get(work.ID)
	if err != nil {
		t.Fatalf("Get(work bead): %v", err)
	}
	if gotWork.Assignee != "" {
		t.Errorf("work bead Assignee = %q, want empty (released after session close)", gotWork.Assignee)
	}
	if gotWork.Status != "open" {
		t.Errorf("work bead Status = %q, want open (reset so the routed queue can re-pick it)", gotWork.Status)
	}
}

func TestCmdSessionCloseStrictSessionFailsClosedBeforeSessionOrWorkMutation(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "strict-worker"
start_command = "true"
project_hooks = "forbid"
max_active_sessions = 1
`)
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_DIR", t.TempDir())
	t.Setenv("GC_BEADS", "file")
	t.Setenv("GC_SESSION", "fake")

	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}
	work, err := store.Create(beads.Bead{
		Title:    "strict demand",
		Type:     "task",
		Metadata: map[string]string{"gc.routed_to": "strict-worker"},
	})
	if err != nil {
		t.Fatalf("Create(work): %v", err)
	}
	sessionBead, err := store.Create(beads.Bead{
		Title:  "strict worker",
		Type:   session.BeadType,
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"session_name":                          "strict-worker-1",
			"template":                              "strict-worker",
			"state":                                 "active",
			beadmeta.TriggerBeadIDMetadataKey:       work.ID,
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:test-city",
		},
	})
	if err != nil {
		t.Fatalf("Create(session): %v", err)
	}
	inProgress := "in_progress"
	assignee := sessionBead.ID
	if err := store.Update(work.ID, beads.UpdateOpts{Status: &inProgress, Assignee: &assignee}); err != nil {
		t.Fatalf("assign work: %v", err)
	}

	beforeSession, err := store.Get(sessionBead.ID)
	if err != nil {
		t.Fatalf("Get(session before): %v", err)
	}
	beforeWork, err := store.Get(work.ID)
	if err != nil {
		t.Fatalf("Get(work before): %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := cmdSessionClose([]string{sessionBead.ID}, &stdout, &stderr); code != 1 {
		t.Fatalf("cmdSessionClose = %d, want 1; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !bytes.Contains(stderr.Bytes(), []byte("exact-incarnation conditional close boundary")) {
		t.Fatalf("stderr = %q, want strict close refusal", stderr.String())
	}

	reopened, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("reopen city store: %v", err)
	}
	afterSession, err := reopened.Get(sessionBead.ID)
	if err != nil {
		t.Fatalf("Get(session after): %v", err)
	}
	afterWork, err := reopened.Get(work.ID)
	if err != nil {
		t.Fatalf("Get(work after): %v", err)
	}
	if afterSession.Status != beforeSession.Status || afterSession.Revision != beforeSession.Revision {
		t.Fatalf("strict refusal mutated session: before=%+v after=%+v", beforeSession, afterSession)
	}
	if afterWork.Status != beforeWork.Status || afterWork.Assignee != beforeWork.Assignee || afterWork.Revision != beforeWork.Revision {
		t.Fatalf("strict refusal mutated work: before=%+v after=%+v", beforeWork, afterWork)
	}
}

func TestStrictCloseCascadeUsesAtomicAssignmentReleaseWithoutLegacyFallback(t *testing.T) {
	for _, tc := range []struct {
		name         string
		hold         bool
		wantStatus   string
		wantAssignee bool
	}{
		{name: "exact_release", wantStatus: "open"},
		{name: "late_canonical_hold_refuses", hold: true, wantStatus: "in_progress", wantAssignee: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := beads.NewMemStore()
			cfg := &config.City{Agents: []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}}}
			sessionBead, err := store.Create(beads.Bead{
				Title:  "strict worker",
				Type:   session.BeadType,
				Labels: []string{session.LabelSession},
				Metadata: map[string]string{
					"session_name":                          "strict-worker-1",
					"template":                              "strict-worker",
					"state":                                 "active",
					beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
					beadmeta.TriggerBeadStoreRefMetadataKey: "city:test-city",
				},
			})
			if err != nil {
				t.Fatalf("Create(session): %v", err)
			}
			labels := []string(nil)
			if tc.hold {
				labels = []string{"hold:mayor"}
			}
			work, err := store.Create(beads.Bead{
				Title:    "strict demand",
				Type:     "task",
				Assignee: sessionBead.ID,
				Labels:   labels,
				Metadata: map[string]string{"gc.routed_to": "strict-worker"},
			})
			if err != nil {
				t.Fatalf("Create(work): %v", err)
			}
			inProgress := "in_progress"
			if err := store.Update(work.ID, beads.UpdateOpts{Status: &inProgress}); err != nil {
				t.Fatalf("mark work in_progress: %v", err)
			}

			var stderr bytes.Buffer
			if closed := closeBeadWithConfig(store, cfg, sessionBead.ID, "test-close", time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC), &stderr); !closed {
				t.Fatalf("closeBeadWithConfig = false; stderr=%s", stderr.String())
			}
			gotSession, err := store.Get(sessionBead.ID)
			if err != nil {
				t.Fatalf("Get(session): %v", err)
			}
			if gotSession.Status != "closed" {
				t.Fatalf("session status = %q, want closed", gotSession.Status)
			}
			gotWork, err := store.Get(work.ID)
			if err != nil {
				t.Fatalf("Get(work): %v", err)
			}
			if gotWork.Status != tc.wantStatus {
				t.Fatalf("work status = %q, want %q", gotWork.Status, tc.wantStatus)
			}
			if tc.wantAssignee {
				if gotWork.Assignee != sessionBead.ID {
					t.Fatalf("held work assignee = %q, want %q", gotWork.Assignee, sessionBead.ID)
				}
			} else if gotWork.Assignee != "" {
				t.Fatalf("released work assignee = %q, want empty", gotWork.Assignee)
			}
		})
	}
}

func TestExplicitSessionCloseStrictModeRequiresCanonicalSameStoreTrigger(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents:    []config.Agent{{Name: "strict-worker", ProjectHooks: config.ProjectHooksForbid}},
	}
	row := beads.Bead{
		ID:     "session-strict",
		Type:   session.BeadType,
		Status: "open",
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"template":                              "strict-worker",
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:test-city",
		},
	}
	sessionStore := beads.NewMemStore()
	strict, err := explicitSessionCloseStrictMode(cfg, "/cities/test-city", sessionStore, beads.WorkStore{Store: sessionStore}, row)
	if err != nil || !strict {
		t.Fatalf("same-store strict close mode = (strict=%t, err=%v), want true,nil", strict, err)
	}
	strict, err = explicitSessionCloseStrictMode(cfg, "/cities/test-city", sessionStore, beads.WorkStore{Store: beads.NewMemStore()}, row)
	if err == nil || strict {
		t.Fatalf("split-store strict close mode = (strict=%t, err=%v), want false,error", strict, err)
	}
	row.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey] = "city:other"
	strict, err = explicitSessionCloseStrictMode(cfg, "/cities/test-city", sessionStore, beads.WorkStore{Store: sessionStore}, row)
	if err == nil || strict {
		t.Fatalf("wrong-store trigger strict close mode = (strict=%t, err=%v), want false,error", strict, err)
	}
}
