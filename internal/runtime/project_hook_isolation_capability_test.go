package runtime_test

import (
	"testing"

	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/runtime/auto"
	"github.com/gastownhall/gascity/internal/runtime/tmux"
)

func TestProjectHookIsolationCapabilityStaysOnAttestableLocalRoute(t *testing.T) {
	local := tmux.NewSeamBackedWithConfig(tmux.DefaultConfig())
	localCapability, ok := local.(runtime.ProjectHookIsolationCapabilityProvider)
	if !ok {
		t.Fatal("tmux provider does not report project-hook isolation capability")
	}
	if !localCapability.SupportsProjectHookIsolation("") || !localCapability.SupportsProjectHookIsolation("tmux") {
		t.Fatal("tmux provider rejected its local transport")
	}
	if localCapability.SupportsProjectHookIsolation("acp") {
		t.Fatal("tmux provider accepted unattestable ACP transport")
	}

	routed := auto.New(local, runtime.NewFake())
	routedCapability, ok := any(routed).(runtime.ProjectHookIsolationCapabilityProvider)
	if !ok {
		t.Fatal("auto provider does not forward project-hook isolation capability")
	}
	if !routedCapability.SupportsProjectHookIsolation("tmux") {
		t.Fatal("auto provider rejected its attestable local route")
	}
	if routedCapability.SupportsProjectHookIsolation("acp") {
		t.Fatal("auto provider accepted its unattestable ACP route")
	}
}

func TestAutoStartProjectHookIsolationValidatesActualRoutedBackend(t *testing.T) {
	local := tmux.NewSeamBackedWithConfig(tmux.DefaultConfig())
	remote := runtime.NewFake()
	routed := auto.New(local, remote)
	routed.RouteACP("remote-session")

	err := routed.Start(t.Context(), "remote-session", runtime.Config{ProjectHooksForbidden: true})
	if err == nil {
		t.Fatal("Start error = nil, want routed-backend project-hook-isolation failure")
	}
	if len(remote.Calls) != 0 {
		t.Fatalf("remote runtime calls = %#v, want zero", remote.Calls)
	}
}

func TestAutoRelaunchProjectHookIsolationValidatesActualRoutedBackend(t *testing.T) {
	local := tmux.NewSeamBackedWithConfig(tmux.DefaultConfig())
	remote := runtime.NewFake()
	if err := remote.Start(t.Context(), "remote-session", runtime.Config{Command: "remote-agent"}); err != nil {
		t.Fatalf("seed remote session: %v", err)
	}
	remote.Calls = nil

	routed := auto.New(local, remote)
	routed.RouteACP("remote-session")

	err := routed.Relaunch(t.Context(), "remote-session", runtime.Config{
		Command:               "remote-agent --resume",
		ProjectHooksForbidden: true,
	})
	if err == nil {
		t.Fatal("Relaunch error = nil, want routed-backend project-hook-isolation failure")
	}
	if len(remote.Calls) != 0 {
		t.Fatalf("remote runtime calls = %#v, want zero", remote.Calls)
	}
}

func TestAutoRunLiveProjectHookIsolationValidatesActualRoutedBackend(t *testing.T) {
	local := tmux.NewSeamBackedWithConfig(tmux.DefaultConfig())
	remote := runtime.NewFake()
	routed := auto.New(local, remote)
	routed.RouteACP("remote-session")

	err := routed.RunLive("remote-session", runtime.Config{
		ProjectHooksForbidden: true,
		SessionLive:           []string{"must-not-run"},
	})
	if err == nil {
		t.Fatal("RunLive error = nil, want routed-backend project-hook-isolation failure")
	}
	if len(remote.Calls) != 0 {
		t.Fatalf("remote runtime calls = %#v, want zero", remote.Calls)
	}
}

type projectHookIsolationFake struct {
	*runtime.Fake
}

func (*projectHookIsolationFake) SupportsProjectHookIsolation(string) bool { return true }

func TestAutoRunLiveProjectHookIsolationForwardsToCapableRoutedBackend(t *testing.T) {
	local := runtime.NewFake()
	remote := &projectHookIsolationFake{Fake: runtime.NewFake()}
	routed := auto.New(local, remote)
	routed.RouteACP("remote-session")

	if err := routed.RunLive("remote-session", runtime.Config{
		ProjectHooksForbidden: true,
		SessionLive:           []string{"safe-live-command"},
	}); err != nil {
		t.Fatalf("RunLive: %v", err)
	}
	if got := remote.CountCalls("RunLive", "remote-session"); got != 1 {
		t.Fatalf("capable remote RunLive calls = %d, want 1", got)
	}
	if got := local.CountCalls("RunLive", "remote-session"); got != 0 {
		t.Fatalf("default RunLive calls = %d, want 0", got)
	}
}
