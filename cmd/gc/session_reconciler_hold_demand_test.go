package main

import "testing"

// TestCanonicalHoldCompletedBarrierPreventsSequentialReplacementAfterSuspendedSessionDrain
// owns the incident-B composition boundary in the reconciler package surface.
// The implementation helper lives beside the demand fixtures it shares.
func TestCanonicalHoldCompletedBarrierPreventsSequentialReplacementAfterSuspendedSessionDrain(t *testing.T) {
	runCanonicalHoldCompletedBarrierPreventsSequentialReplacementAfterSuspendedSessionDrain(t)
}
