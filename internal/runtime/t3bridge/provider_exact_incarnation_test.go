package t3bridge

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/runtime"
)

func TestT3BridgeDoesNotExposeUnsoundSnapshotThenDispatchExactCapabilities(t *testing.T) {
	providers := []struct {
		name     string
		provider runtime.Provider
	}{
		{name: "raw", provider: &Provider{}},
		{name: "seam", provider: NewSeamBacked()},
	}

	for _, tc := range providers {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := tc.provider.(runtime.ExactIncarnationProvider); ok {
				t.Fatalf("%T exposes snapshot-then-dispatch ExactIncarnationProvider", tc.provider)
			}
			if _, ok := tc.provider.(runtime.ExactIncarnationMetadataProvider); ok {
				t.Fatalf("%T exposes snapshot-then-dispatch ExactIncarnationMetadataProvider", tc.provider)
			}
			if _, ok := tc.provider.(runtime.ExactIncarnationObserver); ok {
				t.Fatalf("%T exposes snapshot-then-dispatch ExactIncarnationObserver", tc.provider)
			}
			if _, ok := tc.provider.(runtime.ExactIncarnationPeekProvider); ok {
				t.Fatalf("%T exposes snapshot-then-dispatch ExactIncarnationPeekProvider", tc.provider)
			}
			if _, ok := tc.provider.(runtime.ExactIncarnationStartProvider); ok {
				t.Fatalf("%T exposes exact start without immutable runtime ownership", tc.provider)
			}
		})
	}
}

func TestGetMetaReadsRuntimeIdentityFromThreadSessionEnv(t *testing.T) {
	expected := runtime.Incarnation{
		SessionID:      "session-1",
		InstanceToken:  "instance-1",
		Epoch:          "7",
		OperationToken: "operation-1",
	}
	provider, _ := newExactIncarnationTestProvider(t, exactIncarnationSnapshot(
		exactIncarnationThread("thread-attested", "agent-a", expected),
	))

	for key, want := range map[string]string{
		"GC_SESSION_ID":              expected.SessionID,
		"GC_INSTANCE_TOKEN":          expected.InstanceToken,
		"GC_RUNTIME_EPOCH":           expected.Epoch,
		"GC_RUNTIME_OPERATION_TOKEN": expected.OperationToken,
	} {
		got, err := provider.GetMeta("agent-a", key)
		if err != nil {
			t.Fatalf("GetMeta(%s): %v", key, err)
		}
		if got != want {
			t.Fatalf("GetMeta(%s) = %q, want %q", key, got, want)
		}
	}
}

func TestThreadReuseRequiresExactRuntimeIncarnation(t *testing.T) {
	incarnation := runtime.Incarnation{
		SessionID:      "session-1",
		InstanceToken:  "instance-1",
		Epoch:          "7",
		OperationToken: "operation-1",
	}
	thread := exactIncarnationThread("thread-attested", "agent-a", incarnation)
	desired := ParseSessionEnv(threadCustomMetadata(thread)["gc.sessionEnv"])
	if !threadRuntimeIncarnationReusable(thread, desired) {
		t.Fatal("matching runtime incarnation was not reusable")
	}

	for _, key := range []string{
		"GC_SESSION_ID",
		"GC_INSTANCE_TOKEN",
		"GC_RUNTIME_EPOCH",
		"GC_RUNTIME_OPERATION_TOKEN",
	} {
		t.Run(key, func(t *testing.T) {
			changed := make(map[string]string, len(desired))
			for envKey, value := range desired {
				changed[envKey] = value
			}
			changed[key] += "-replacement"
			if threadRuntimeIncarnationReusable(thread, changed) {
				t.Fatalf("thread remained reusable after %s changed", key)
			}
		})
	}

	if !threadRuntimeIncarnationReusable(thread, map[string]string{"GC_AGENT": "agent-a"}) {
		t.Fatal("legacy config without incarnation evidence changed reuse behavior")
	}
}

func TestAutomaticStartRefusesDifferentIncarnationWithoutMutatingExistingThread(t *testing.T) {
	expected := runtime.Incarnation{
		SessionID:      "session-1",
		InstanceToken:  "instance-1",
		Epoch:          "7",
		OperationToken: "operation-1",
	}
	replacement := expected
	replacement.InstanceToken = "instance-2"
	replacement.OperationToken = "operation-2"
	provider, server := newExactIncarnationTestProvider(t, exactIncarnationSnapshot(
		exactIncarnationThread("thread-replacement", "agent-a", replacement),
	))
	cfg := runtime.Config{
		WorkDir: "/tmp/agent-a",
		Command: "codex",
		Env: map[string]string{
			"GC_CITY_PATH":               "/tmp/city",
			"GC_ALIAS":                   "agent-a",
			"GC_TEMPLATE":                "agent-a",
			"GC_PROVIDER":                "codex",
			"GC_MODEL":                   "gpt-5.6",
			"GC_SESSION_ID":              expected.SessionID,
			"GC_INSTANCE_TOKEN":          expected.InstanceToken,
			"GC_RUNTIME_EPOCH":           expected.Epoch,
			"GC_RUNTIME_OPERATION_TOKEN": expected.OperationToken,
		},
	}

	err := provider.Start(context.Background(), "agent-a", cfg)
	if !errors.Is(err, runtime.ErrIncarnationMismatch) {
		t.Fatalf("Start error = %v, want ErrIncarnationMismatch", err)
	}
	if commands := server.dispatchedCommands(); len(commands) != 0 {
		t.Fatalf("commands = %#v, want none", commands)
	}
}

func newExactIncarnationTestProvider(t *testing.T, snapshot map[string]interface{}) (*Provider, *t3BridgeTestServer) {
	t.Helper()
	server := newT3BridgeTestServer(t, snapshot)
	t.Cleanup(server.Close)
	t.Setenv("T3_BEARER_TOKEN", "test-bearer")
	t.Setenv("T3_HOME", t.TempDir())
	t.Setenv("T3_WS_URL", server.wsURL())
	t.Setenv("GC_T3BRIDGE_STATE_DIR", t.TempDir())
	return &Provider{
		watchers:     make(map[string]context.CancelFunc),
		recentStarts: make(map[string]time.Time),
	}, server
}

func exactIncarnationSnapshot(threads ...map[string]interface{}) map[string]interface{} {
	items := make([]interface{}, len(threads))
	for i := range threads {
		items[i] = threads[i]
	}
	return map[string]interface{}{"threads": items}
}

func exactIncarnationThread(id, name string, incarnation runtime.Incarnation) map[string]interface{} {
	env := map[string]string{
		"GC_SESSION_NAME":   name,
		"GC_SESSION_ID":     incarnation.SessionID,
		"GC_INSTANCE_TOKEN": incarnation.InstanceToken,
		"GC_RUNTIME_EPOCH":  incarnation.Epoch,
	}
	if incarnation.OperationToken != "" {
		env["GC_RUNTIME_OPERATION_TOKEN"] = incarnation.OperationToken
	}
	encoded, _ := json.Marshal(env)
	return map[string]interface{}{
		"id":        id,
		"projectId": "project-1",
		"updatedAt": "2026-08-08T12:00:00.000Z",
		"customMetadata": map[string]interface{}{
			"gc.sessionName":     name,
			"gc.sessionEnv":      string(encoded),
			"gc.runtimeProvider": "codex",
			"gc.startupModel":    "gpt-5.6",
			"gc.bead":            "work-1",
		},
		"session": map[string]interface{}{"status": "ready"},
	}
}
