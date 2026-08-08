package hookartifact

import (
	"reflect"
	"sort"
	"testing"
)

func TestCompleteProvidersExactSet(t *testing.T) {
	want := []string{
		"codex",
	}
	got := make([]string, 0, len(completeProviders))
	for provider := range completeProviders {
		got = append(got, provider)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("completeProviders = %v, want authoritative exact set %v", got, want)
	}
}
