package beadmeta

import "testing"

// TestDispatchHoldLabelsMatchCanonicalHoldValues pins beadmeta as the single
// named home for the two canonical hold values documented in
// engdocs/contributors/hold-label-conventions.md (hold:mayor, hold:external)
// so internal/config and cmd/gc can share one definition instead of each
// re-spelling the label strings (ga-x9kptu / ga-5736js).
func TestDispatchHoldLabelsMatchCanonicalHoldValues(t *testing.T) {
	if HoldMayorLabel != "hold:mayor" {
		t.Fatalf("HoldMayorLabel = %q, want %q", HoldMayorLabel, "hold:mayor")
	}
	if HoldExternalLabel != "hold:external" {
		t.Fatalf("HoldExternalLabel = %q, want %q", HoldExternalLabel, "hold:external")
	}
	want := []string{HoldMayorLabel, HoldExternalLabel}
	if len(DispatchHoldLabels) != len(want) {
		t.Fatalf("DispatchHoldLabels = %#v, want %#v", DispatchHoldLabels, want)
	}
	for i, v := range want {
		if DispatchHoldLabels[i] != v {
			t.Fatalf("DispatchHoldLabels[%d] = %q, want %q", i, DispatchHoldLabels[i], v)
		}
	}
}

func TestHasDispatchHoldLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		labels []string
		want   bool
	}{
		{name: "none"},
		{name: "mayor", labels: []string{HoldMayorLabel}, want: true},
		{name: "external", labels: []string{HoldExternalLabel}, want: true},
		{name: "both", labels: []string{HoldMayorLabel, HoldExternalLabel}, want: true},
		{name: "unrelated", labels: []string{"priority:high", "human"}},
		{name: "retired hold spelling", labels: []string{"on-hold"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := HasDispatchHoldLabel(tt.labels); got != tt.want {
				t.Fatalf("HasDispatchHoldLabel(%v) = %v, want %v", tt.labels, got, tt.want)
			}
		})
	}
}
