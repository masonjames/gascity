package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/pathutil"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/testutil"
	"github.com/gastownhall/gascity/internal/worker"
)

type projectHooksStartSignalProvider struct {
	*runtime.Fake
	started chan runtime.Config
}

func newProjectHooksStartSignalProvider() *projectHooksStartSignalProvider {
	return &projectHooksStartSignalProvider{
		Fake:    runtime.NewFake(),
		started: make(chan runtime.Config, 2),
	}
}

func (p *projectHooksStartSignalProvider) Start(ctx context.Context, name string, cfg runtime.Config) error {
	err := p.Fake.Start(ctx, name, cfg)
	p.started <- cfg
	return err
}

func (p *projectHooksStartSignalProvider) SupportsProjectHookIsolation(transport string) bool {
	return transport == "" || transport == config.SessionTransportTmux
}

type projectHooksAttestingProvider struct {
	*runtime.Fake
}

func (p *projectHooksAttestingProvider) SupportsProjectHookIsolation(transport string) bool {
	return transport == "" || transport == config.SessionTransportTmux
}

func configureProjectHooksForbiddenAgent(t *testing.T, fs *fakeState) string {
	t.Helper()
	externalRoot := t.TempDir()
	fs.cfg.Agents = []config.Agent{{
		Name:              "worker",
		Dir:               "myrig",
		Provider:          "codex",
		WorkDir:           filepath.Join(externalRoot, "{{.AgentBase}}"),
		ProjectHooks:      config.ProjectHooksForbid,
		MaxActiveSessions: intPtr(1),
	}}
	fs.cfg.Providers["codex"] = config.ProviderSpec{
		DisplayName: "Codex",
		Command:     "codex",
		PathCheck:   "true",
	}
	return filepath.Join(externalRoot, "worker")
}

func projectHooksStartCalls(provider *runtime.Fake) int {
	count := 0
	for _, call := range provider.SnapshotCalls() {
		if call.Method == "Start" {
			count++
		}
	}
	return count
}

func setProjectHooksExactCityTrigger(t *testing.T, fs *fakeState, sessionID string) {
	t.Helper()
	if err := fs.cityBeadStore.SetMetadataBatch(sessionID, map[string]string{
		beadmeta.TriggerBeadIDMetadataKey:       "work-exact",
		beadmeta.TriggerBeadStoreRefMetadataKey: "city:" + apiCityName(fs.cfg, fs.cityPath),
	}); err != nil {
		t.Fatalf("persist exact trigger: %v", err)
	}
}

func TestHumaHandleSessionCreateRejectsForbiddenProjectHooksWithoutPersistedTriggerBeforeMutationOrStart(t *testing.T) {
	fs := newSessionFakeState(t)
	configureProjectHooksForbiddenAgent(t, fs)
	fs.sessionProvider = &projectHooksAttestingProvider{Fake: fs.sp}
	srv := New(fs)

	out, err := srv.humaHandleSessionCreate(context.Background(), &SessionCreateInput{
		Body: sessionCreateBody{Kind: "agent", Name: "myrig/worker"},
	})
	if err != nil {
		t.Fatalf("humaHandleSessionCreate: %v", err)
	}
	success, failure := waitForSessionCreateResult(t, fs.eventProv, out.Body.RequestID)
	if success != nil {
		t.Fatalf("session create unexpectedly succeeded: %+v", success)
	}
	if failure == nil || failure.ErrorCode != "create_failed" || !strings.Contains(failure.ErrorMessage, "persisted session row") {
		t.Fatalf("session create failure = %+v, want missing persisted authority", failure)
	}
	if got := projectHooksStartCalls(fs.sp); got != 0 {
		t.Fatalf("provider Start calls = %d, want 0", got)
	}
	sessions, err := session.NewManagerWithOptions(fs.cityBeadStore, fs.sp).List("", "")
	if err != nil {
		t.Fatalf("List sessions: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("sessions = %+v, want zero unauthorized mutations", sessions)
	}
}

func TestLegacyHandleSessionCreatePersistsForbiddenProjectHooksAgentInertlyWithoutStart(t *testing.T) {
	fs := newSessionFakeState(t)
	configureProjectHooksForbiddenAgent(t, fs)
	fs.sessionProvider = &projectHooksAttestingProvider{Fake: fs.sp}
	srv := New(fs)

	rec := httptest.NewRecorder()
	req := newPostRequest("/v0/sessions", strings.NewReader(`{"kind":"agent","name":"myrig/worker"}`))
	srv.legacySessionHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	var response sessionResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.ID == "" {
		t.Fatal("deferred response session id is empty")
	}
	if got := projectHooksStartCalls(fs.sp); got != 0 {
		t.Fatalf("provider Start calls = %d, want 0 for inert create", got)
	}
	persisted, err := fs.cityBeadStore.Get(response.ID)
	if err != nil {
		t.Fatalf("Get(deferred session): %v", err)
	}
	if got := persisted.Metadata["state"]; got != string(session.StateStartPending) {
		t.Fatalf("deferred state = %q, want %q", got, session.StateStartPending)
	}
	if got := persisted.Metadata[beadmeta.TriggerBeadIDMetadataKey]; got != "" {
		t.Fatalf("deferred trigger id = %q, want empty until controller binding", got)
	}
	if got := persisted.Metadata[beadmeta.TriggerBeadStoreRefMetadataKey]; got != "" {
		t.Fatalf("deferred trigger store = %q, want empty until controller binding", got)
	}
}

func TestHumaHandleSessionRespondRejectsForbiddenProjectHooksWithoutTriggerBeforeProviderOrSessionMutation(t *testing.T) {
	fs := newSessionFakeState(t)
	wantWorkDir := configureProjectHooksForbiddenAgent(t, fs)
	provider := &projectHooksAttestingProvider{Fake: fs.sp}
	fs.sessionProvider = provider
	srv := New(fs)

	mgr := session.NewManagerWithOptions(fs.cityBeadStore, provider)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template: "myrig/worker",
		Title:    "Worker",
		Command:  "codex",
		WorkDir:  wantWorkDir,
		Provider: "codex",
		ExtraMeta: map[string]string{
			"agent_name":     "myrig/worker",
			"session_origin": "manual",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	provider.SetPendingInteraction(info.SessionName, &runtime.PendingInteraction{
		RequestID: "request-1",
		Kind:      "approval",
		Prompt:    "approve?",
	})
	before, err := fs.cityBeadStore.Get(info.ID)
	if err != nil {
		t.Fatalf("Get(before): %v", err)
	}
	providerCallsBefore := provider.SnapshotCalls()
	input := &SessionRespondInput{ID: info.ID}
	input.Body.Action = "approve"

	_, err = srv.humaHandleSessionRespond(context.Background(), input)
	if err == nil || !strings.Contains(err.Error(), "complete exact trigger pair") {
		t.Fatalf("humaHandleSessionRespond error = %v, want missing exact trigger refusal", err)
	}
	after, err := fs.cityBeadStore.Get(info.ID)
	if err != nil {
		t.Fatalf("Get(after): %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("session mutated by rejected response:\n got %+v\nwant %+v", after, before)
	}
	providerCallsAfter := provider.SnapshotCalls()
	if !reflect.DeepEqual(providerCallsAfter, providerCallsBefore) {
		t.Fatalf("provider calls changed after rejected response:\n got %+v\nwant %+v", providerCallsAfter, providerCallsBefore)
	}
	if pending := provider.PendingInteractions[info.SessionName]; pending == nil || pending.RequestID != "request-1" {
		t.Fatalf("pending interaction changed after rejected response: %+v", pending)
	}
}

func TestHumaHandleSessionCreateRejectsForbiddenProjectHooksUnattestableProviderBeforeStart(t *testing.T) {
	fs := newSessionFakeState(t)
	configureProjectHooksForbiddenAgent(t, fs)
	srv := New(fs)

	_, err := srv.humaHandleSessionCreate(context.Background(), &SessionCreateInput{
		Body: sessionCreateBody{Kind: "agent", Name: "myrig/worker"},
	})
	if err == nil {
		t.Fatal("humaHandleSessionCreate error = nil, want unattestable provider rejection")
	}
	if !strings.Contains(err.Error(), "cannot attest project hook isolation") {
		t.Fatalf("humaHandleSessionCreate error = %v, want project hook attestation diagnostic", err)
	}
	if got := projectHooksStartCalls(fs.sp); got != 0 {
		t.Fatalf("provider Start calls = %d, want 0", got)
	}
	sessions, listErr := session.NewManagerWithOptions(fs.cityBeadStore, fs.sp).List("", "")
	if listErr != nil {
		t.Fatalf("list sessions after rejected create: %v", listErr)
	}
	if len(sessions) != 0 {
		t.Fatalf("materialized sessions = %d, want 0", len(sessions))
	}
}

func TestHumaHandleSessionCreateRejectsForbiddenProjectHooksCityFallbackBeforeStart(t *testing.T) {
	fs := newSessionFakeState(t)
	fs.cfg.Agents[0].ProjectHooks = config.ProjectHooksForbid
	fs.cfg.Agents[0].WorkDir = ""
	fs.cfg.Providers["test-agent"] = config.ProviderSpec{Command: "/bin/echo", PathCheck: "true"}
	srv := New(fs)

	_, err := srv.humaHandleSessionCreate(context.Background(), &SessionCreateInput{
		Body: sessionCreateBody{Kind: "agent", Name: "myrig/worker"},
	})
	if err == nil {
		t.Fatal("humaHandleSessionCreate error = nil, want unsafe fallback rejection")
	}
	if !strings.Contains(err.Error(), "explicit work_dir") {
		t.Fatalf("humaHandleSessionCreate error = %v, want explicit work_dir diagnostic", err)
	}
	if got := projectHooksStartCalls(fs.sp); got != 0 {
		t.Fatalf("provider Start calls = %d, want 0", got)
	}
}

func TestHumaHandleConfigValidateRejectsForbiddenProjectHooksInheritedWorkspaceInstaller(t *testing.T) {
	fs := newSessionFakeState(t)
	fs.cfg.Agents[0].ProjectHooks = config.ProjectHooksForbid
	fs.cfg.Workspace.InstallAgentHooks = []string{"codex"}
	srv := New(fs)

	out, err := srv.humaHandleConfigValidate(context.Background(), &ConfigValidateInput{})
	if err != nil {
		t.Fatalf("humaHandleConfigValidate: %v", err)
	}
	if out.Body.Valid {
		t.Fatal("config validate valid = true, want inherited hook installer conflict")
	}
	if len(out.Body.Errors) != 1 || !strings.Contains(out.Body.Errors[0], "workspace install_agent_hooks") {
		t.Fatalf("config validate errors = %#v, want workspace install_agent_hooks conflict", out.Body.Errors)
	}
}

func TestMaterializeNamedSessionRejectsForbiddenProjectHooksWithoutPersistedTriggerBeforeMutationOrStart(t *testing.T) {
	fs := newSessionFakeState(t)
	configureProjectHooksForbiddenAgent(t, fs)
	fs.sessionProvider = &projectHooksAttestingProvider{Fake: fs.sp}
	fs.cfg.NamedSessions = []config.NamedSession{{Template: "worker", Dir: "myrig"}}
	srv := New(fs)

	spec, ok, err := srv.findNamedSessionSpecForTarget(fs.cityBeadStore, "myrig/worker")
	if err != nil {
		t.Fatalf("findNamedSessionSpecForTarget: %v", err)
	}
	if !ok {
		t.Fatal("expected named session spec")
	}
	_, err = srv.materializeNamedSession(fs.cityBeadStore, spec)
	if !errors.Is(err, worker.ErrLaunchUnauthorized) {
		t.Fatalf("materializeNamedSession error = %v, want ErrLaunchUnauthorized", err)
	}
	if got := projectHooksStartCalls(fs.sp); got != 0 {
		t.Fatalf("provider Start calls = %d, want 0", got)
	}
	sessions, err := session.NewManagerWithOptions(fs.cityBeadStore, fs.sp).List("", "")
	if err != nil {
		t.Fatalf("List sessions: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("sessions = %+v, want zero unauthorized mutations", sessions)
	}
}

func TestMaterializeNamedSessionRejectsForbiddenProjectHooksUnattestableProviderBeforeStart(t *testing.T) {
	fs := newSessionFakeState(t)
	configureProjectHooksForbiddenAgent(t, fs)
	fs.cfg.NamedSessions = []config.NamedSession{{Template: "worker", Dir: "myrig"}}
	srv := New(fs)

	spec, ok, err := srv.findNamedSessionSpecForTarget(fs.cityBeadStore, "myrig/worker")
	if err != nil {
		t.Fatalf("findNamedSessionSpecForTarget: %v", err)
	}
	if !ok {
		t.Fatal("expected named session spec")
	}
	_, err = srv.materializeNamedSession(fs.cityBeadStore, spec)
	if err == nil {
		t.Fatal("materializeNamedSession error = nil, want unattestable provider rejection")
	}
	if !strings.Contains(err.Error(), "cannot attest project hook isolation") {
		t.Fatalf("materializeNamedSession error = %v, want project hook attestation diagnostic", err)
	}
	if got := projectHooksStartCalls(fs.sp); got != 0 {
		t.Fatalf("provider Start calls = %d, want 0", got)
	}
	sessions, listErr := session.NewManagerWithOptions(fs.cityBeadStore, fs.sp).List("", "")
	if listErr != nil {
		t.Fatalf("list sessions after rejected materialization: %v", listErr)
	}
	if len(sessions) != 0 {
		t.Fatalf("materialized sessions = %d, want 0", len(sessions))
	}
}

func TestHumaHandleSessionWakePropagatesProjectHooksForbidFromAttestedCWD(t *testing.T) {
	fs := newSessionFakeState(t)
	wantWorkDir := configureProjectHooksForbiddenAgent(t, fs)
	provider := newProjectHooksStartSignalProvider()
	fs.sessionProvider = provider
	srv := New(fs)

	mgr := session.NewManagerWithOptions(fs.cityBeadStore, provider)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template: "myrig/worker",
		Title:    "Worker",
		Command:  "codex",
		WorkDir:  wantWorkDir,
		Provider: "codex",
		ExtraMeta: map[string]string{
			"agent_name":     "myrig/worker",
			"session_origin": "manual",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	<-provider.started
	if err := mgr.Suspend(info.ID); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	setProjectHooksExactCityTrigger(t, fs, info.ID)

	if _, err := srv.humaHandleSessionWake(context.Background(), &SessionIDInput{ID: info.ID}); err != nil {
		t.Fatalf("humaHandleSessionWake: %v", err)
	}
	select {
	case start := <-provider.started:
		if !start.ProjectHooksForbidden {
			t.Fatal("resume Start ProjectHooksForbidden = false, want true")
		}
		if !pathutil.SamePath(start.WorkDir, wantWorkDir) {
			t.Fatalf("resume Start WorkDir = %q, want canonical %q", start.WorkDir, wantWorkDir)
		}
	case <-time.After(testutil.GoroutineRaceTimeout):
		t.Fatal("timed out waiting for resume provider Start")
	}
}

func TestExistingHandleFailsClosedWhenCurrentConfigFlipsToForbiddenDifferentCWD(t *testing.T) {
	fs := newSessionFakeState(t)
	originalWorkDir := configureProjectHooksForbiddenAgent(t, fs)
	fs.cfg.Agents[0].ProjectHooks = config.ProjectHooksInherit
	provider := &projectHooksAttestingProvider{Fake: fs.sp}
	fs.sessionProvider = provider
	srv := New(fs)

	mgr := session.NewManagerWithOptions(fs.cityBeadStore, provider)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		BeadOnly: true,
		Template: "myrig/worker",
		Title:    "Worker",
		Command:  "codex",
		WorkDir:  originalWorkDir,
		Provider: "codex",
		ExtraMeta: map[string]string{
			"agent_name":     "myrig/worker",
			"session_origin": "manual",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	setProjectHooksExactCityTrigger(t, fs, info.ID)
	factory, err := srv.workerFactory(fs.cityBeadStore)
	if err != nil {
		t.Fatalf("workerFactory: %v", err)
	}
	handle, err := factory.SessionByID(info.ID)
	if err != nil {
		t.Fatalf("SessionByID under inherit: %v", err)
	}

	fs.cfg.Agents[0].ProjectHooks = config.ProjectHooksForbid
	fs.cfg.Agents[0].WorkDir = filepath.Join(t.TempDir(), "{{.AgentBase}}")
	if err := handle.Start(context.Background()); err == nil {
		t.Fatal("Start error = nil, want current forbidden cwd mismatch refusal")
	}
	if got := projectHooksStartCalls(fs.sp); got != 0 {
		t.Fatalf("provider Start calls = %d, want 0 after policy/cwd drift", got)
	}
}

func TestHumaHandleSessionWakeRejectsForbiddenProjectHooksUnattestableProviderBeforeMutationOrStart(t *testing.T) {
	fs := newSessionFakeState(t)
	wantWorkDir := configureProjectHooksForbiddenAgent(t, fs)
	srv := New(fs)

	mgr := session.NewManagerWithOptions(fs.cityBeadStore, fs.sp)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template: "myrig/worker",
		Title:    "Worker",
		Command:  "codex",
		WorkDir:  wantWorkDir,
		Provider: "codex",
		ExtraMeta: map[string]string{
			"agent_name":     "myrig/worker",
			"session_origin": "manual",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := mgr.Suspend(info.ID); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	setProjectHooksExactCityTrigger(t, fs, info.ID)
	startsBefore := projectHooksStartCalls(fs.sp)

	_, err = srv.humaHandleSessionWake(context.Background(), &SessionIDInput{ID: info.ID})
	if err == nil {
		t.Fatal("humaHandleSessionWake error = nil, want unattestable provider rejection")
	}
	if !strings.Contains(err.Error(), "cannot attest project hook isolation") {
		t.Fatalf("humaHandleSessionWake error = %v, want project hook attestation diagnostic", err)
	}
	if got := projectHooksStartCalls(fs.sp); got != startsBefore {
		t.Fatalf("provider Start calls = %d, want unchanged %d", got, startsBefore)
	}
	got, getErr := mgr.Get(info.ID)
	if getErr != nil {
		t.Fatalf("get session after rejected wake: %v", getErr)
	}
	if got.State != session.StateSuspended {
		t.Fatalf("session state = %q, want %q after pre-mutation rejection", got.State, session.StateSuspended)
	}
}

func TestHumaHandleSessionWakeRejectsPersistedCWDThatDiffersFromCurrentAttestationBeforeMutationOrStart(t *testing.T) {
	fs := newSessionFakeState(t)
	configureProjectHooksForbiddenAgent(t, fs)
	provider := newProjectHooksStartSignalProvider()
	fs.sessionProvider = provider
	srv := New(fs)

	mgr := session.NewManagerWithOptions(fs.cityBeadStore, provider)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template: "myrig/worker",
		Title:    "Worker",
		Command:  "codex",
		WorkDir:  fs.cityPath,
		Provider: "codex",
		ExtraMeta: map[string]string{
			"agent_name":     "myrig/worker",
			"session_origin": "manual",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	<-provider.started
	if err := mgr.Suspend(info.ID); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	setProjectHooksExactCityTrigger(t, fs, info.ID)
	startsBefore := projectHooksStartCalls(provider.Fake)

	_, err = srv.humaHandleSessionWake(context.Background(), &SessionIDInput{ID: info.ID})
	if err == nil {
		t.Fatal("humaHandleSessionWake error = nil, want persisted cwd mismatch rejection")
	}
	if !strings.Contains(err.Error(), "persisted work_dir") {
		t.Fatalf("humaHandleSessionWake error = %v, want persisted work_dir diagnostic", err)
	}
	if got := projectHooksStartCalls(provider.Fake); got != startsBefore {
		t.Fatalf("provider Start calls = %d, want unchanged %d", got, startsBefore)
	}
	got, getErr := mgr.Get(info.ID)
	if getErr != nil {
		t.Fatalf("get session after rejected wake: %v", getErr)
	}
	if got.State != session.StateSuspended {
		t.Fatalf("session state = %q, want %q after pre-mutation rejection", got.State, session.StateSuspended)
	}
}

func TestLegacyHandleSessionWakeRejectsForbiddenProjectHooksUnattestableProviderBeforeMutationOrStart(t *testing.T) {
	fs := newSessionFakeState(t)
	wantWorkDir := configureProjectHooksForbiddenAgent(t, fs)
	srv := New(fs)
	h := srv.legacySessionHandler()

	mgr := session.NewManagerWithOptions(fs.cityBeadStore, fs.sp)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template: "myrig/worker",
		Title:    "Worker",
		Command:  "codex",
		WorkDir:  wantWorkDir,
		Provider: "codex",
		ExtraMeta: map[string]string{
			"agent_name":     "myrig/worker",
			"session_origin": "manual",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := mgr.Suspend(info.ID); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// Supply the exact ownership prerequisite so this control reaches the
	// provider-attestation fence it is intended to exercise.
	setProjectHooksExactCityTrigger(t, fs, info.ID)
	startsBefore := projectHooksStartCalls(fs.sp)

	rec := httptest.NewRecorder()
	req := newPostRequest("/v0/session/"+info.ID+"/wake", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "cannot attest project hook isolation") {
		t.Fatalf("body = %q, want project hook attestation diagnostic", rec.Body.String())
	}
	if got := projectHooksStartCalls(fs.sp); got != startsBefore {
		t.Fatalf("provider Start calls = %d, want unchanged %d", got, startsBefore)
	}
	got, getErr := mgr.Get(info.ID)
	if getErr != nil {
		t.Fatalf("get session after rejected wake: %v", getErr)
	}
	if got.State != session.StateSuspended {
		t.Fatalf("session state = %q, want %q after pre-mutation rejection", got.State, session.StateSuspended)
	}
}

func TestLegacyHandleSessionWakePropagatesProjectHooksForbidFromAttestedCWD(t *testing.T) {
	fs := newSessionFakeState(t)
	wantWorkDir := configureProjectHooksForbiddenAgent(t, fs)
	provider := newProjectHooksStartSignalProvider()
	fs.sessionProvider = provider
	srv := New(fs)
	h := srv.legacySessionHandler()

	mgr := session.NewManagerWithOptions(fs.cityBeadStore, provider)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template: "myrig/worker",
		Title:    "Worker",
		Command:  "codex",
		WorkDir:  wantWorkDir,
		Provider: "codex",
		ExtraMeta: map[string]string{
			"agent_name":     "myrig/worker",
			"session_origin": "manual",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	<-provider.started
	if err := mgr.Suspend(info.ID); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	setProjectHooksExactCityTrigger(t, fs, info.ID)

	rec := httptest.NewRecorder()
	req := newPostRequest("/v0/session/"+info.ID+"/wake", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	select {
	case start := <-provider.started:
		if !start.ProjectHooksForbidden {
			t.Fatal("resume Start ProjectHooksForbidden = false, want true")
		}
		if !pathutil.SamePath(start.WorkDir, wantWorkDir) {
			t.Fatalf("resume Start WorkDir = %q, want canonical %q", start.WorkDir, wantWorkDir)
		}
	case <-time.After(testutil.GoroutineRaceTimeout):
		t.Fatal("timed out waiting for resume provider Start")
	}
}

func TestLegacyHandleSessionWakeRejectsPersistedCWDThatDiffersFromCurrentAttestationBeforeMutationOrStart(t *testing.T) {
	fs := newSessionFakeState(t)
	configureProjectHooksForbiddenAgent(t, fs)
	provider := newProjectHooksStartSignalProvider()
	fs.sessionProvider = provider
	srv := New(fs)
	h := srv.legacySessionHandler()

	mgr := session.NewManagerWithOptions(fs.cityBeadStore, provider)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template: "myrig/worker",
		Title:    "Worker",
		Command:  "codex",
		WorkDir:  fs.cityPath,
		Provider: "codex",
		ExtraMeta: map[string]string{
			"agent_name":     "myrig/worker",
			"session_origin": "manual",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	<-provider.started
	if err := mgr.Suspend(info.ID); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	setProjectHooksExactCityTrigger(t, fs, info.ID)
	startsBefore := projectHooksStartCalls(provider.Fake)

	rec := httptest.NewRecorder()
	req := newPostRequest("/v0/session/"+info.ID+"/wake", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "persisted work_dir") {
		t.Fatalf("body = %q, want persisted work_dir diagnostic", rec.Body.String())
	}
	if got := projectHooksStartCalls(provider.Fake); got != startsBefore {
		t.Fatalf("provider Start calls = %d, want unchanged %d", got, startsBefore)
	}
	got, getErr := mgr.Get(info.ID)
	if getErr != nil {
		t.Fatalf("get session after rejected wake: %v", getErr)
	}
	if got.State != session.StateSuspended {
		t.Fatalf("session state = %q, want %q after pre-mutation rejection", got.State, session.StateSuspended)
	}
}
