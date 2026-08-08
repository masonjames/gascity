package beads_test

import (
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/beadstest"
)

func TestNativeDoltStoreConformance(t *testing.T) {
	beadstest.RunStoreTests(t, beads.NewNativeDoltStoreForConformance)
}

func TestNativeDoltStoreAssignmentClaimConformance(t *testing.T) {
	beadstest.RunGuardedAssignmentClaimConformanceWithOptions(t, "NativeDoltStore",
		func(_ *testing.T) beads.Store { return beads.NewNativeDoltStoreForConformance() },
		beadstest.GuardedAssignmentClaimOptions{
			FixtureLacksIsolationReason: "nativeDoltMemStorage models rollback but not isolation; " +
				"single-winner contention is covered against real embedded Dolt by " +
				"TestNativeDoltStoreGuardedAssignmentContentionAgainstRealDolt (-tags=integration)",
		})
}

func TestNativeDoltStoreAssignmentReleaseConformance(t *testing.T) {
	beadstest.RunAssignmentReleaseConformanceWithOptions(t, "NativeDoltStore",
		func(_ *testing.T) beads.Store { return beads.NewNativeDoltStoreForConformance() },
		beadstest.AssignmentReleaseOptions{
			FixtureLacksIsolationReason: "nativeDoltMemStorage models rollback but not transaction isolation",
		})
}

func TestNativeDoltStoreGuardedClaimDoesNotAdvertiseConditionalWriter(t *testing.T) {
	store := beads.NewNativeDoltStoreForConformance()
	if _, ok := beads.GuardedAssignmentClaimerFor(store); !ok {
		t.Fatal("NativeDoltStore does not expose GuardedAssignmentClaimer")
	}
	if _, ok := beads.ConditionalWriterFor(store); ok {
		t.Fatal("NativeDoltStore exposes ConditionalWriter; guarded assignment must not widen revision-CAS support")
	}
}
