package api

import (
	"context"
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/worker"
)

func TestServerWorkerLaunchAuthorizerUsesCurrentConfigAndExactCityTrigger(t *testing.T) {
	fs := newSessionFakeState(t)
	wantWorkDir := configureProjectHooksForbiddenAgent(t, fs)
	fs.sessionProvider = &projectHooksAttestingProvider{Fake: fs.sp}
	srv := New(fs)
	authorize := srv.workerLaunchAuthorizer()

	newReq := worker.LaunchAuthorizationRequest{Session: worker.SessionSpec{Template: "myrig/worker"}}
	if _, err := authorize(context.Background(), newReq); !errors.Is(err, worker.ErrLaunchUnauthorized) {
		t.Fatalf("new forbidden launch error = %v, want ErrLaunchUnauthorized", err)
	}
	exact := worker.LaunchAuthorizationRequest{Info: &session.Info{
		ID:                  "session-strict",
		Template:            "myrig/worker",
		Provider:            "codex",
		WorkDir:             wantWorkDir,
		TriggerBeadID:       "work-exact",
		TriggerBeadStoreRef: "city:" + apiCityName(fs.cfg, fs.cityPath),
	}, Metadata: map[string]string{"agent_name": "myrig/worker"}, SessionStore: fs.CityBeadStore(), CanonicalCityStore: fs.CityBeadStore()}
	if _, err := authorize(context.Background(), exact); err != nil {
		t.Fatalf("exact trigger authorization: %v", err)
	}

	// The callback reads state at invocation time, not factory construction
	// time: a config reload to inherit immediately restores ordinary behavior.
	fs.cfg.Agents[0].ProjectHooks = config.ProjectHooksInherit
	if _, err := authorize(context.Background(), newReq); err != nil {
		t.Fatalf("current inherit config rejected new launch: %v", err)
	}
	// No configured agent is an ad-hoc provider session and remains compatible.
	if _, err := authorize(context.Background(), worker.LaunchAuthorizationRequest{Session: worker.SessionSpec{Template: "adhoc"}}); err != nil {
		t.Fatalf("unconfigured provider launch: %v", err)
	}
}
