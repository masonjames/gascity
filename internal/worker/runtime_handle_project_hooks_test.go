package worker

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gastownhall/gascity/internal/runtime"
)

func TestRuntimeHandleStartResolvedRejectsForbiddenProjectHookLaunchWithoutPersistedWitness(t *testing.T) {
	provider := runtime.NewFake()
	handle, err := NewRuntimeHandle(RuntimeHandleConfig{
		Provider:     provider,
		SessionName:  "strict-runtime",
		ProviderName: "codex",
	})
	if err != nil {
		t.Fatalf("NewRuntimeHandle: %v", err)
	}
	err = handle.StartResolved(context.Background(), "codex --model gpt-5.6-sol", runtime.Config{
		WorkDir:               filepath.Join(t.TempDir(), "strict-instance", "work"),
		ProjectHooksForbidden: true,
	})
	if err == nil {
		t.Fatal("StartResolved error = nil, want bead-backed ownership refusal")
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls = %+v, want none before strict ownership refusal", calls)
	}
}

func TestRuntimeHandleStartResolvedRejectsUnsafeForbiddenCommandBeforeProvider(t *testing.T) {
	provider := runtime.NewFake()
	handle, err := NewRuntimeHandle(RuntimeHandleConfig{Provider: provider, SessionName: "strict-runtime", ProviderName: "codex"})
	if err != nil {
		t.Fatalf("NewRuntimeHandle: %v", err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}
	err = handle.StartResolved(context.Background(), "sh -c 'exec codex'", runtime.Config{
		WorkDir:               filepath.Join(root, "strict-instance", "work"),
		ProjectHooksForbidden: true,
	})
	if err == nil {
		t.Fatal("StartResolved error = nil, want unsafe command refusal")
	}
	if calls := provider.SnapshotCalls(); len(calls) != 0 {
		t.Fatalf("provider calls = %+v, want none before strict ownership refusal", calls)
	}
}
