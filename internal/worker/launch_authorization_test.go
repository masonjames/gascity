package worker

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/beadstest"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

var errTestLaunchUnauthorized = errors.New("test launch unauthorized")

type launchAuthorizationRouteFake struct {
	*runtime.Fake
	routes   int
	unroutes int
}

func (f *launchAuthorizationRouteFake) RouteACP(string) { f.routes++ }
func (f *launchAuthorizationRouteFake) Unroute(string)  { f.unroutes++ }

func TestSessionHandleLaunchAuthorizationRejectsNewWakeBeforeAnyMutation(t *testing.T) {
	operations := []struct {
		name string
		run  func(context.Context, *SessionHandle) error
	}{
		{name: "start", run: func(ctx context.Context, h *SessionHandle) error { return h.Start(ctx) }},
		{name: "start resolved", run: func(ctx context.Context, h *SessionHandle) error {
			return h.StartResolved(ctx, "codex --resume exact", runtime.Config{Command: "codex --resume exact"})
		}},
		{name: "attach", run: func(ctx context.Context, h *SessionHandle) error { return h.Attach(ctx) }},
		{name: "message", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Message(ctx, MessageRequest{Text: "work"})
			return err
		}},
		{name: "respond", run: func(ctx context.Context, h *SessionHandle) error {
			return h.Respond(ctx, InteractionResponse{Action: "approve"})
		}},
		{name: "nudge default wake", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Wake: NudgeWakeIfNeeded})
			return err
		}},
		{name: "nudge immediate wake", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Delivery: NudgeDeliveryImmediate, Wake: NudgeWakeIfNeeded})
			return err
		}},
		{name: "nudge wait idle wake", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Delivery: NudgeDeliveryWaitIdle, Wake: NudgeWakeIfNeeded})
			return err
		}},
		{name: "nudge live only", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Wake: NudgeWakeLiveOnly})
			return err
		}},
		{name: "create started", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Create(ctx, CreateModeStarted)
			return err
		}},
	}

	for _, tc := range operations {
		t.Run(tc.name, func(t *testing.T) {
			store := beads.NewMemStore()
			provider := runtime.NewFake()
			manager := sessionpkg.NewManagerWithOptions(store, provider)
			var requests []LaunchAuthorizationRequest
			handle, err := NewSessionHandle(SessionHandleConfig{
				Manager: manager,
				Session: SessionSpec{
					Template: "strict-worker",
					Command:  "codex --disable hooks",
					WorkDir:  t.TempDir(),
					Provider: "codex",
				},
				AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
					requests = append(requests, req)
					return LaunchAuthorization{}, errTestLaunchUnauthorized
				},
			})
			if err != nil {
				t.Fatalf("NewSessionHandle: %v", err)
			}

			err = tc.run(context.Background(), handle)
			if !errors.Is(err, errTestLaunchUnauthorized) {
				t.Fatalf("error = %v, want launch refusal", err)
			}
			if len(requests) != 1 {
				t.Fatalf("authorization requests = %d, want 1", len(requests))
			}
			if requests[0].Info != nil {
				t.Fatalf("new-session authorization Info = %+v, want nil", requests[0].Info)
			}
			if requests[0].Session.Template != "strict-worker" {
				t.Fatalf("authorization template = %q, want strict-worker", requests[0].Session.Template)
			}
			all, listErr := store.List(beads.ListQuery{AllowScan: true})
			if listErr != nil {
				t.Fatalf("List: %v", listErr)
			}
			if len(all) != 0 {
				t.Fatalf("store mutated before authorization: %+v", all)
			}
			if calls := provider.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("provider called before authorization: %+v", calls)
			}
		})
	}
}

func TestRequireExactSessionTriggerAuthorityRejectsRelocatedSessionStore(t *testing.T) {
	cityStore := beads.NewMemStore()
	sessionStore := beads.NewMemStore()
	req := LaunchAuthorizationRequest{
		Info: &sessionpkg.Info{
			ID:                  "session-strict",
			TriggerBeadID:       "work-exact",
			TriggerBeadStoreRef: "city:fixture-city",
		},
		SessionStore:       sessionStore,
		CanonicalCityStore: cityStore,
	}

	if _, err := RequireExactSessionTriggerAuthority(req, "city:fixture-city"); !errors.Is(err, ErrLaunchUnauthorized) {
		t.Fatalf("authorization error = %v, want ErrLaunchUnauthorized for physically relocated session store", err)
	}

	req.SessionStore = beads.SessionStore{Store: cityStore}
	if _, err := RequireExactSessionTriggerAuthority(req, "city:fixture-city"); err != nil {
		t.Fatalf("co-located session store authorization: %v", err)
	}
}

func TestSessionHandleDeferredCreatePersistsInertWithoutLaunchAuthorization(t *testing.T) {
	store := beads.NewMemStore()
	provider := runtime.NewFake()
	manager := sessionpkg.NewManagerWithOptions(store, provider)
	called := 0
	handle, err := NewSessionHandle(SessionHandleConfig{
		Manager: manager,
		Session: SessionSpec{
			Template: "strict-worker",
			Command:  "codex --disable hooks",
			WorkDir:  t.TempDir(),
			Provider: "codex",
		},
		AuthorizeLaunch: func(context.Context, LaunchAuthorizationRequest) (LaunchAuthorization, error) {
			called++
			return LaunchAuthorization{}, errTestLaunchUnauthorized
		},
	})
	if err != nil {
		t.Fatalf("NewSessionHandle: %v", err)
	}
	info, err := handle.Create(context.Background(), CreateModeDeferred)
	if err != nil {
		t.Fatalf("Create(deferred): %v", err)
	}
	if info.ID == "" {
		t.Fatal("Create(deferred) returned empty ID")
	}
	if called != 0 {
		t.Fatalf("launch authorizer called %d times for inert create, want 0", called)
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls = %+v, want none", calls)
	}
}

func TestSessionHandleExistingWakeReloadsAuthoritativeRecordImmediatelyBeforeAuthorization(t *testing.T) {
	operations := []struct {
		name string
		run  func(context.Context, *SessionHandle) error
	}{
		{name: "start", run: func(ctx context.Context, h *SessionHandle) error { return h.Start(ctx) }},
		{name: "start resolved", run: func(ctx context.Context, h *SessionHandle) error {
			return h.StartResolved(ctx, "codex --resume exact", runtime.Config{Command: "codex --resume exact"})
		}},
		{name: "attach", run: func(ctx context.Context, h *SessionHandle) error { return h.Attach(ctx) }},
		{name: "message", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Message(ctx, MessageRequest{Text: "work"})
			return err
		}},
		{name: "respond", run: func(ctx context.Context, h *SessionHandle) error {
			return h.Respond(ctx, InteractionResponse{Action: "approve"})
		}},
		{name: "nudge default", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Wake: NudgeWakeIfNeeded})
			return err
		}},
		{name: "nudge immediate", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Delivery: NudgeDeliveryImmediate, Wake: NudgeWakeIfNeeded})
			return err
		}},
		{name: "nudge wait idle", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Delivery: NudgeDeliveryWaitIdle, Wake: NudgeWakeIfNeeded})
			return err
		}},
		{name: "nudge live only", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Wake: NudgeWakeLiveOnly})
			return err
		}},
	}

	for _, tc := range operations {
		t.Run(tc.name, func(t *testing.T) {
			store := beads.NewMemStore()
			provider := runtime.NewFake()
			manager := sessionpkg.NewManagerWithOptions(store, provider)
			created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
				BeadOnly: true,
				Template: "strict-worker",
				Title:    "Strict Worker",
				Command:  "codex --disable hooks",
				WorkDir:  t.TempDir(),
				Provider: "codex",
			})
			if err != nil {
				t.Fatalf("CreateSession(deferred): %v", err)
			}
			var seen *sessionpkg.Info
			handle, err := NewSessionHandle(SessionHandleConfig{
				Manager: manager,
				Session: SessionSpec{ID: created.ID, Template: "strict-worker", Command: "codex --disable hooks", WorkDir: created.WorkDir, Provider: "codex"},
				AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
					if req.Info != nil {
						cloned := *req.Info
						seen = &cloned
					}
					return LaunchAuthorization{}, errTestLaunchUnauthorized
				},
			})
			if err != nil {
				t.Fatalf("NewSessionHandle: %v", err)
			}
			if err := store.Update(created.ID, beads.UpdateOpts{Metadata: map[string]string{
				beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
				beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
			}}); err != nil {
				t.Fatalf("Update(trigger): %v", err)
			}
			before, err := store.Get(created.ID)
			if err != nil {
				t.Fatalf("Get(before): %v", err)
			}
			provider.Calls = nil

			err = tc.run(context.Background(), handle)
			if !errors.Is(err, errTestLaunchUnauthorized) {
				t.Fatalf("error = %v, want launch refusal", err)
			}
			if seen == nil || seen.ID != created.ID || seen.TriggerBeadID != "work-exact" || seen.TriggerBeadStoreRef != "city:fixture-city" {
				t.Fatalf("authorizer saw stale session info: %+v", seen)
			}
			if calls := provider.SnapshotCalls(); len(calls) != 0 {
				t.Fatalf("provider called after refusal: %+v", calls)
			}
			after, err := store.Get(created.ID)
			if err != nil {
				t.Fatalf("Get(after): %v", err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("session mutated after refusal:\n got %+v\nwant %+v", after, before)
			}
		})
	}
}

func TestSessionHandleWitnessRejectsTriggerDriftBeforeEveryLiveSideEffect(t *testing.T) {
	operations := []struct {
		name string
		run  func(context.Context, *SessionHandle) error
	}{
		{name: "start", run: func(ctx context.Context, h *SessionHandle) error { return h.Start(ctx) }},
		{name: "start resolved", run: func(ctx context.Context, h *SessionHandle) error {
			return h.StartResolved(ctx, "codex --resume exact", runtime.Config{Command: "codex --resume exact"})
		}},
		{name: "attach", run: func(ctx context.Context, h *SessionHandle) error { return h.Attach(ctx) }},
		{name: "message", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Message(ctx, MessageRequest{Text: "work"})
			return err
		}},
		{name: "respond", run: func(ctx context.Context, h *SessionHandle) error {
			return h.Respond(ctx, InteractionResponse{Action: "approve"})
		}},
		{name: "nudge default wake", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Wake: NudgeWakeIfNeeded})
			return err
		}},
		{name: "nudge immediate wake", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Delivery: NudgeDeliveryImmediate, Wake: NudgeWakeIfNeeded})
			return err
		}},
		{name: "nudge wait idle wake", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Delivery: NudgeDeliveryWaitIdle, Wake: NudgeWakeIfNeeded})
			return err
		}},
		{name: "nudge default live only", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Wake: NudgeWakeLiveOnly})
			return err
		}},
		{name: "nudge immediate live only", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Delivery: NudgeDeliveryImmediate, Wake: NudgeWakeLiveOnly})
			return err
		}},
		{name: "nudge wait idle live only", run: func(ctx context.Context, h *SessionHandle) error {
			_, err := h.Nudge(ctx, NudgeRequest{Text: "work", Delivery: NudgeDeliveryWaitIdle, Wake: NudgeWakeLiveOnly})
			return err
		}},
	}

	for _, drift := range []struct {
		name     string
		metadata map[string]string
	}{
		{name: "clear", metadata: map[string]string{
			beadmeta.TriggerBeadIDMetadataKey:       "",
			beadmeta.TriggerBeadStoreRefMetadataKey: "",
		}},
		{name: "repoint", metadata: map[string]string{
			beadmeta.TriggerBeadIDMetadataKey:       "work-repointed",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		}},
	} {
		for _, operation := range operations {
			t.Run(drift.name+"/"+operation.name, func(t *testing.T) {
				backing := beads.NewMemStore()
				recorder := beadstest.NewRecordingStore(backing)
				provider := &launchAuthorizationRouteFake{Fake: runtime.NewFake()}
				manager := sessionpkg.NewManagerWithOptions(recorder, provider)
				created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
					BeadOnly:  true,
					Template:  "strict-worker",
					Title:     "Strict Worker",
					Command:   "codex --disable hooks",
					WorkDir:   t.TempDir(),
					Provider:  "acp",
					Transport: "acp",
					ExtraMeta: map[string]string{
						beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
						beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
					},
				})
				if err != nil {
					t.Fatalf("CreateSession(deferred): %v", err)
				}
				recorder.Reset()
				provider.routes = 0
				provider.unroutes = 0
				callbackCalls := 0
				handle, err := NewSessionHandle(SessionHandleConfig{
					Manager:            manager,
					SessionStore:       recorder,
					CanonicalCityStore: recorder,
					Session: SessionSpec{
						ID:        created.ID,
						Template:  "strict-worker",
						Command:   "codex --disable hooks",
						WorkDir:   created.WorkDir,
						Provider:  "acp",
						Transport: "acp",
					},
					AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
						callbackCalls++
						authorization, err := RequireExactSessionTriggerAuthority(req, "city:fixture-city")
						if err != nil {
							return LaunchAuthorization{}, err
						}
						// Deterministically inject the controller-side clear/repoint at
						// callback return. The Manager must reload and reject this exact
						// witness under its session mutation lock.
						if err := recorder.Update(created.ID, beads.UpdateOpts{Metadata: drift.metadata}); err != nil {
							return LaunchAuthorization{}, err
						}
						recorder.Reset()
						return authorization, nil
					},
				})
				if err != nil {
					t.Fatalf("NewSessionHandle: %v", err)
				}

				err = operation.run(context.Background(), handle)
				if !errors.Is(err, ErrLaunchUnauthorized) || !errors.Is(err, sessionpkg.ErrLiveBoundaryWitnessMismatch) {
					t.Fatalf("error = %v, want launch authorization and witness mismatch", err)
				}
				if callbackCalls != 1 {
					t.Fatalf("authorization callback calls = %d, want 1", callbackCalls)
				}
				if calls := recorder.Calls(); len(calls) != 0 {
					t.Fatalf("session mutated after drift injection: %+v", calls)
				}
				if calls := provider.SnapshotCalls(); len(calls) != 0 {
					t.Fatalf("provider called after drift injection: %+v", calls)
				}
				if provider.routes != 0 || provider.unroutes != 0 {
					t.Fatalf("ACP routing mutated after drift injection: routes=%d unroutes=%d", provider.routes, provider.unroutes)
				}
				after, err := backing.Get(created.ID)
				if err != nil {
					t.Fatalf("Get(after): %v", err)
				}
				for key, want := range drift.metadata {
					if got := after.Metadata[key]; got != want {
						t.Fatalf("drifted metadata %q = %q, want %q", key, got, want)
					}
				}
			})
		}
	}
}

func TestSessionHandleRejectedRepairableStrictRowHasZeroWritesOrRoutes(t *testing.T) {
	const id = "session-empty-type"
	backing := beads.NewMemStoreFrom(1, []beads.Bead{{
		ID:     id,
		Type:   "",
		Title:  "repairable strict row",
		Status: "open",
		Labels: []string{sessionpkg.LabelSession},
		Metadata: map[string]string{
			"session_name": "s-" + id,
			"template":     "strict-worker",
			"state":        string(sessionpkg.StateStartPending),
			"command":      "codex",
			"provider":     "acp",
			"transport":    "acp",
		},
	}}, nil)
	recorder := beadstest.NewRecordingStore(backing)
	provider := &launchAuthorizationRouteFake{Fake: runtime.NewFake()}
	manager := sessionpkg.NewManagerWithOptions(recorder, provider)
	handle, err := NewSessionHandle(SessionHandleConfig{
		Manager:            manager,
		SessionStore:       recorder,
		CanonicalCityStore: recorder,
		Session:            SessionSpec{ID: id, Template: "strict-worker", Command: "codex", Provider: "acp", Transport: "acp"},
		AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
			return RequireExactSessionTriggerAuthority(req, "city:fixture-city")
		},
	})
	if err != nil {
		t.Fatalf("NewSessionHandle: %v", err)
	}

	err = handle.Start(context.Background())
	if !errors.Is(err, ErrLaunchUnauthorized) {
		t.Fatalf("Start error = %v, want ErrLaunchUnauthorized", err)
	}
	if calls := recorder.Calls(); len(calls) != 0 {
		t.Fatalf("rejected repairable row was healed or mutated: %+v", calls)
	}
	after, err := backing.Get(id)
	if err != nil {
		t.Fatalf("Get(after): %v", err)
	}
	if after.Type != "" {
		t.Fatalf("rejected row type = %q, want unchanged empty type", after.Type)
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls before rejection: %+v", calls)
	}
	if provider.routes != 0 || provider.unroutes != 0 {
		t.Fatalf("ACP route mutations before rejection: routes=%d unroutes=%d", provider.routes, provider.unroutes)
	}
}

func TestFactorySessionByIDRejectsRepairableStrictRowBeforeHealEnrichOrRoute(t *testing.T) {
	const id = "session-factory-empty-type"
	backing := beads.NewMemStoreFrom(1, []beads.Bead{{
		ID:     id,
		Type:   "",
		Title:  "repairable strict factory row",
		Status: "open",
		Labels: []string{sessionpkg.LabelSession},
		Metadata: map[string]string{
			"session_name": "s-" + id,
			"template":     "strict-worker",
			"state":        string(sessionpkg.StateActive),
			"command":      "codex",
			"provider":     "acp",
			"transport":    "acp",
		},
	}}, nil)
	recorder := beadstest.NewRecordingStore(backing)
	provider := &launchAuthorizationRouteFake{Fake: runtime.NewFake()}
	factory, err := NewFactory(FactoryConfig{
		Store:    recorder,
		Provider: provider,
		AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
			return RequireExactSessionTriggerAuthority(req, "city:fixture-city")
		},
	})
	if err != nil {
		t.Fatalf("NewFactory: %v", err)
	}

	_, err = factory.SessionByID(id)
	if !errors.Is(err, ErrLaunchUnauthorized) {
		t.Fatalf("SessionByID error = %v, want ErrLaunchUnauthorized", err)
	}
	if calls := recorder.Calls(); len(calls) != 0 {
		t.Fatalf("SessionByID healed or mutated rejected row: %+v", calls)
	}
	after, err := backing.Get(id)
	if err != nil {
		t.Fatalf("Get(after): %v", err)
	}
	if after.Type != "" {
		t.Fatalf("SessionByID rejected row type = %q, want unchanged empty type", after.Type)
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("SessionByID provider calls before rejection: %+v", calls)
	}
	if provider.routes != 0 || provider.unroutes != 0 {
		t.Fatalf("SessionByID ACP route mutations before rejection: routes=%d unroutes=%d", provider.routes, provider.unroutes)
	}
}

func TestSessionHandleExactAuthorizationPinsRuntimeTriggerEnvironment(t *testing.T) {
	store := beads.NewMemStore()
	provider := runtime.NewFake()
	manager := sessionpkg.NewManagerWithOptions(store, provider)
	created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
		BeadOnly: true,
		Template: "strict-worker",
		Title:    "Strict Worker",
		Command:  "codex",
		WorkDir:  t.TempDir(),
		Provider: "codex",
		ExtraMeta: map[string]string{
			beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
			beadmeta.TriggerBeadStoreRefMetadataKey: "city:fixture-city",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession(deferred): %v", err)
	}
	handle, err := NewSessionHandle(SessionHandleConfig{
		Manager:            manager,
		SessionStore:       store,
		CanonicalCityStore: store,
		Session:            SessionSpec{ID: created.ID, Template: "strict-worker", Command: "codex", WorkDir: created.WorkDir, Provider: "codex"},
		AuthorizeLaunch: func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
			return RequireExactSessionTriggerAuthority(req, "city:fixture-city")
		},
	})
	if err != nil {
		t.Fatalf("NewSessionHandle: %v", err)
	}
	if err := handle.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	started := provider.LastStartConfig(created.SessionName)
	if started == nil {
		t.Fatalf("provider calls = %+v, want Start", provider.SnapshotCalls())
	}
	want := map[string]string{
		"GC_TRIGGER_BEAD_ID":        "work-exact",
		"GC_TRIGGER_WORK_BEAD_ID":   "work-exact",
		"GC_TRIGGER_BEAD_STORE_REF": "city:fixture-city",
		"GC_TRIGGER_WORK_STORE_REF": "city:fixture-city",
	}
	for key, value := range want {
		if got := started.Env[key]; got != value {
			t.Fatalf("Start env %s = %q, want %q", key, got, value)
		}
	}
}

func TestSessionHandleExactAuthorizationAllowsRespondAndInheritRemainsCompatible(t *testing.T) {
	for _, tc := range []struct {
		name      string
		strict    bool
		withExact bool
	}{
		{name: "strict exact trigger", strict: true, withExact: true},
		{name: "inherit without trigger"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := beads.NewMemStore()
			provider := runtime.NewFake()
			manager := sessionpkg.NewManagerWithOptions(store, provider)
			extra := map[string]string{}
			if tc.withExact {
				extra[beadmeta.TriggerBeadIDMetadataKey] = "work-exact"
				extra[beadmeta.TriggerBeadStoreRefMetadataKey] = "city:fixture-city"
			}
			created, err := manager.CreateSession(context.Background(), sessionpkg.CreateOptions{
				BeadOnly:  true,
				Template:  "worker",
				Title:     "Worker",
				Command:   "codex",
				WorkDir:   t.TempDir(),
				Provider:  "codex",
				ExtraMeta: extra,
			})
			if err != nil {
				t.Fatalf("CreateSession(deferred): %v", err)
			}
			provider.SetPendingInteraction(created.SessionName, &runtime.PendingInteraction{
				RequestID: "request-1",
				Kind:      "approval",
				Prompt:    "approve?",
			})
			var authorizer LaunchAuthorizer
			if tc.strict {
				authorizer = func(_ context.Context, req LaunchAuthorizationRequest) (LaunchAuthorization, error) {
					return RequireExactSessionTriggerAuthority(req, "city:fixture-city")
				}
			}
			handle, err := NewSessionHandle(SessionHandleConfig{
				Manager:            manager,
				SessionStore:       store,
				CanonicalCityStore: store,
				Session:            SessionSpec{ID: created.ID, Template: "worker", Command: "codex", WorkDir: created.WorkDir, Provider: "codex"},
				AuthorizeLaunch:    authorizer,
			})
			if err != nil {
				t.Fatalf("NewSessionHandle: %v", err)
			}
			if err := handle.Respond(context.Background(), InteractionResponse{Action: "approve"}); err != nil {
				t.Fatalf("Respond: %v", err)
			}
			responses := provider.Responses[created.SessionName]
			if len(responses) != 1 || responses[0].RequestID != "request-1" || responses[0].Action != "approve" {
				t.Fatalf("responses = %+v, want exact approval", responses)
			}
		})
	}
}

func TestFactoryThreadsLaunchAuthorizerToEverySessionHandle(t *testing.T) {
	store := beads.NewMemStore()
	provider := runtime.NewFake()
	called := 0
	factory, err := NewFactory(FactoryConfig{
		Store:    store,
		Provider: provider,
		AuthorizeLaunch: func(context.Context, LaunchAuthorizationRequest) (LaunchAuthorization, error) {
			called++
			return LaunchAuthorization{}, errTestLaunchUnauthorized
		},
	})
	if err != nil {
		t.Fatalf("NewFactory: %v", err)
	}
	handle, err := factory.Session(SessionSpec{Template: "strict-worker", Command: "codex", WorkDir: t.TempDir(), Provider: "codex"})
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	if err := handle.Start(context.Background()); !errors.Is(err, errTestLaunchUnauthorized) {
		t.Fatalf("Start error = %v, want authorization error", err)
	}
	if called != 1 {
		t.Fatalf("authorizer calls = %d, want 1", called)
	}
}

func TestRequireExactSessionTriggerAuthority(t *testing.T) {
	const cityRef = "city:fixture-city"
	store := beads.NewMemStore()
	for _, tc := range []struct {
		name    string
		req     LaunchAuthorizationRequest
		wantErr bool
	}{
		{name: "new handle has no persisted authority", req: LaunchAuthorizationRequest{}, wantErr: true},
		{name: "missing pair", req: LaunchAuthorizationRequest{Info: &sessionpkg.Info{ID: "session-1"}}, wantErr: true},
		{name: "partial pair", req: LaunchAuthorizationRequest{Info: &sessionpkg.Info{ID: "session-1", TriggerBeadID: "work-1"}}, wantErr: true},
		{name: "malformed bead", req: LaunchAuthorizationRequest{Info: &sessionpkg.Info{ID: "session-1", TriggerBeadID: "work-1\nwrong", TriggerBeadStoreRef: cityRef}}, wantErr: true},
		{name: "rig pair is not colocated", req: LaunchAuthorizationRequest{Info: &sessionpkg.Info{ID: "session-1", TriggerBeadID: "work-1", TriggerBeadStoreRef: "rig:fixture"}}, wantErr: true},
		{name: "other city pair is not colocated", req: LaunchAuthorizationRequest{Info: &sessionpkg.Info{ID: "session-1", TriggerBeadID: "work-1", TriggerBeadStoreRef: "city:other"}}, wantErr: true},
		{name: "exact city pair", req: LaunchAuthorizationRequest{Info: &sessionpkg.Info{ID: "session-1", TriggerBeadID: "work-1", TriggerBeadStoreRef: cityRef}, SessionStore: store, CanonicalCityStore: store}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RequireExactSessionTriggerAuthority(tc.req, cityRef)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, ErrLaunchUnauthorized) {
				t.Fatalf("error = %v, want ErrLaunchUnauthorized", err)
			}
		})
	}
}
