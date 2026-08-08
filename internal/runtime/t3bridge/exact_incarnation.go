package t3bridge

import (
	"strings"
)

const (
	runtimeSessionIDEnv      = "GC_SESSION_ID"
	runtimeInstanceTokenEnv  = "GC_INSTANCE_TOKEN"
	runtimeEpochEnv          = "GC_RUNTIME_EPOCH"
	runtimeOperationTokenEnv = "GC_RUNTIME_OPERATION_TOKEN"
)

// threadRuntimeIncarnationReusable prevents one T3 thread ID from being
// rebound to a different automatic runtime incarnation. Legacy starts that do
// not carry incarnation evidence retain their existing reuse behavior.
func threadRuntimeIncarnationReusable(thread map[string]interface{}, desired map[string]string) bool {
	keys := []string{
		runtimeSessionIDEnv,
		runtimeInstanceTokenEnv,
		runtimeEpochEnv,
		runtimeOperationTokenEnv,
	}
	hasEvidence := false
	stored := ParseSessionEnv(threadCustomMetadata(thread)["gc.sessionEnv"])
	for _, key := range keys {
		want := strings.TrimSpace(desired[key])
		if want == "" {
			continue
		}
		hasEvidence = true
		if strings.TrimSpace(stored[key]) != want {
			return false
		}
	}
	return !hasEvidence || stored != nil
}

func hasRuntimeIncarnationEvidence(env map[string]string) bool {
	for _, key := range []string{
		runtimeSessionIDEnv,
		runtimeInstanceTokenEnv,
		runtimeEpochEnv,
		runtimeOperationTokenEnv,
	} {
		if strings.TrimSpace(env[key]) != "" {
			return true
		}
	}
	return false
}

func runtimeIdentityMetadataKey(key string) bool {
	switch key {
	case runtimeSessionIDEnv, runtimeInstanceTokenEnv, runtimeEpochEnv, runtimeOperationTokenEnv:
		return true
	default:
		return false
	}
}

func t3RuntimeMetadataKey(key string) string {
	return "gc.runtimeMeta." + strings.TrimSpace(key)
}

func threadRuntimeMetadataValue(thread map[string]interface{}, key string) (string, bool) {
	raw, _ := thread["customMetadata"].(map[string]interface{})
	if raw == nil {
		return "", false
	}
	value, ok := raw[t3RuntimeMetadataKey(key)]
	if !ok {
		return "", false
	}
	return strings.TrimSpace(parseMetadataValue(value)), true
}
