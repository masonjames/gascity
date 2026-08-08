package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

type policyAtomicProviderProxy struct{ beads.Store }

func (s *policyAtomicProviderProxy) CreateAssignmentClaimerHandle() (beads.CreateAssignmentClaimer, bool) {
	return s, true
}

func (s *policyAtomicProviderProxy) CreateAssignmentClaim(ctx context.Context, req beads.CreateAssignmentClaimRequest) (beads.CreateAssignmentClaimResult, bool, error) {
	inner, ok := beads.CreateAssignmentClaimerFor(s.Store)
	if !ok {
		return beads.CreateAssignmentClaimResult{}, false, beads.ErrCreateAssignmentClaimUnsupported
	}
	return inner.CreateAssignmentClaim(ctx, req)
}

func policyCreateAssignmentRequest(workID string) beads.CreateAssignmentClaimRequest {
	return beads.CreateAssignmentClaimRequest{
		Claim: beads.AssignmentClaimRequest{
			ID:               workID,
			Actor:            "worker-1",
			ExpectedStatus:   "open",
			ExpectedAssignee: "",
			ExpectedMetadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
			ForbiddenLabels:  beadmeta.DispatchHoldLabels,
			AssignmentMetadata: map[string]string{
				beadmeta.SessionNameMetadataKey:          "runtime-1",
				beadmeta.SessionInstanceTokenMetadataKey: "token-1",
			},
		},
		Witness: beads.Bead{
			Title:     "fresh session witness",
			Type:      session.BeadType,
			Labels:    []string{session.LabelSession},
			Ephemeral: true,
			Metadata:  map[string]string{"witness.kind": "fresh"},
		},
		CreatedWitnessIDMetadataKeys: []string{beadmeta.SessionIDMetadataKey},
	}
}

func TestBeadPolicyStoreForwardsGuardedAssignmentCapability(t *testing.T) {
	inner := beads.NewMemStore()
	created, err := inner.Create(beads.Bead{
		Status: "open",
		Metadata: map[string]string{
			beadmeta.RoutedToMetadataKey: "worker",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wrapped := wrapStoreWithBeadPolicies(inner, &config.City{})
	claimer, ok := beads.GuardedAssignmentClaimerFor(wrapped)
	if !ok {
		t.Fatalf("policy store %T hides guarded assignment capability", wrapped)
	}
	claimed, ok, err := claimer.ClaimAssignment(context.Background(), beads.AssignmentClaimRequest{
		ID:               created.ID,
		Actor:            "worker-1",
		ExpectedStatus:   "open",
		ExpectedAssignee: "",
		ExpectedMetadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
		ForbiddenLabels:  beadmeta.DispatchHoldLabels,
		AssignmentMetadata: map[string]string{
			beadmeta.SessionIDMetadataKey:            "session-1",
			beadmeta.SessionNameMetadataKey:          "runtime-1",
			beadmeta.SessionInstanceTokenMetadataKey: "token-1",
		},
	})
	if err != nil || !ok || claimed.ID != created.ID {
		t.Fatalf("ClaimAssignment = (%+v, %v, %v), want exact success", claimed, ok, err)
	}
}

func TestBeadPolicyStoreDoesNotInventGuardedAssignmentCapability(t *testing.T) {
	inner := storeWithoutGuard{Store: beads.NewMemStore()}
	wrapped := wrapStoreWithBeadPolicies(inner, &config.City{})
	if claimer, ok := beads.GuardedAssignmentClaimerFor(wrapped); ok || claimer != nil {
		t.Fatalf("policy store invented guarded assignment capability: (%T, %v)", claimer, ok)
	}
}

func TestBeadPolicyStoreForwardsCreateAssignmentCapabilityWithWitnessStoragePolicy(t *testing.T) {
	providers := []struct {
		name string
		open func() (beads.Store, beads.Store)
	}{
		{name: "mem", open: func() (beads.Store, beads.Store) {
			store := beads.NewMemStore()
			return store, store
		}},
		{name: "atomic provider proxy", open: func() (beads.Store, beads.Store) {
			store := beads.NewMemStore()
			return &policyAtomicProviderProxy{Store: store}, store
		}},
	}
	policies := []struct {
		name          string
		cfg           *config.City
		wantNoHistory bool
	}{
		{name: "default no history", cfg: &config.City{}, wantNoHistory: true},
		{name: "configured no history", cfg: &config.City{Beads: config.BeadsConfig{Policies: map[string]config.BeadPolicyConfig{
			beadPolicySession: {Storage: beadStorageNoHistory},
		}}}, wantNoHistory: true},
		{name: "explicit history", cfg: &config.City{Beads: config.BeadsConfig{Policies: map[string]config.BeadPolicyConfig{
			beadPolicySession: {Storage: beadStorageHistory},
		}}}, wantNoHistory: false},
	}

	for _, provider := range providers {
		for _, policy := range policies {
			t.Run(provider.name+"/"+policy.name, func(t *testing.T) {
				inner, persisted := provider.open()
				work, err := inner.Create(beads.Bead{
					Title:    "exact work",
					Type:     "task",
					Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
				})
				if err != nil {
					t.Fatal(err)
				}
				wrapped := wrapStoreWithBeadPolicies(inner, policy.cfg)
				claimer, ok := beads.CreateAssignmentClaimerFor(wrapped)
				if !ok {
					t.Fatalf("policy store %T hides create-assignment capability", wrapped)
				}
				req := policyCreateAssignmentRequest(work.ID)
				result, won, err := claimer.CreateAssignmentClaim(context.Background(), req)
				if err != nil || !won || result.Created.ID == "" || result.Claimed.ID != work.ID {
					t.Fatalf("CreateAssignmentClaim = (%+v, %v, %v), want exact success", result, won, err)
				}
				if got := result.Claimed.Metadata[beadmeta.SessionIDMetadataKey]; got != result.Created.ID {
					t.Fatalf("dynamic witness metadata = %q, want created ID %q", got, result.Created.ID)
				}
				storedWitness, err := persisted.Get(result.Created.ID)
				if err != nil {
					t.Fatal(err)
				}
				if storedWitness.NoHistory != policy.wantNoHistory || storedWitness.Ephemeral {
					t.Fatalf("persisted witness storage = (no_history=%v ephemeral=%v), want (no_history=%v ephemeral=false)", storedWitness.NoHistory, storedWitness.Ephemeral, policy.wantNoHistory)
				}
				if !req.Witness.Ephemeral || req.Witness.NoHistory {
					t.Fatalf("caller witness mutated in place: %+v", req.Witness)
				}
			})
		}
	}
}

func TestBeadPolicyStoreCreateAssignmentLoserMutatesNeitherRow(t *testing.T) {
	for _, proxy := range []bool{false, true} {
		name := "mem"
		if proxy {
			name = "atomic provider proxy"
		}
		t.Run(name, func(t *testing.T) {
			persisted := beads.NewMemStore()
			var inner beads.Store = persisted
			if proxy {
				inner = &policyAtomicProviderProxy{Store: persisted}
			}
			work, err := inner.Create(beads.Bead{
				Title:    "held work",
				Labels:   []string{beadmeta.HoldExternalLabel},
				Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
			})
			if err != nil {
				t.Fatal(err)
			}
			before, err := persisted.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			wrapped := wrapStoreWithBeadPolicies(inner, &config.City{})
			claimer, ok := beads.CreateAssignmentClaimerFor(wrapped)
			if !ok {
				t.Fatal("policy wrapper hid create-assignment capability")
			}
			result, won, err := claimer.CreateAssignmentClaim(t.Context(), policyCreateAssignmentRequest(work.ID))
			if err != nil || won || result.Created.ID != "" || result.Claimed.ID != "" {
				t.Fatalf("CreateAssignmentClaim loser = (%+v, %v, %v), want zero,false,nil", result, won, err)
			}
			after, err := persisted.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("predicate loser mutated work: before=%+v after=%+v", before, after)
			}
			rows, err := persisted.List(beads.ListQuery{AllowScan: true, IncludeClosed: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("rows after predicate loser = %+v, want only work", rows)
			}
		})
	}
}

func TestBeadPolicyStoreDoesNotInventCreateAssignmentCapability(t *testing.T) {
	inner := storeWithoutGuard{Store: beads.NewMemStore()}
	wrapped := wrapStoreWithBeadPolicies(inner, &config.City{})
	if claimer, ok := beads.CreateAssignmentClaimerFor(wrapped); ok || claimer != nil {
		t.Fatalf("policy store invented create-assignment capability: (%T, %v)", claimer, ok)
	}
}

func TestBeadPolicyStoreForwardsAssignmentReleaseCapabilityHonestly(t *testing.T) {
	wrapped := wrapStoreWithBeadPolicies(beads.NewMemStore(), &config.City{})
	if releaser, ok := beads.AssignmentReleaserFor(wrapped); !ok || releaser == nil {
		t.Fatalf("AssignmentReleaserFor(policy(MemStore)) = (%T, %v), want capability", releaser, ok)
	}

	unsupported := wrapStoreWithBeadPolicies(storeWithoutGuard{Store: beads.NewMemStore()}, &config.City{})
	if releaser, ok := beads.AssignmentReleaserFor(unsupported); ok || releaser != nil {
		t.Fatalf("AssignmentReleaserFor(policy(store-only)) = (%T, %v), want nil,false", releaser, ok)
	}
}

var (
	_ beads.CreateAssignmentClaimer               = (*policyAtomicProviderProxy)(nil)
	_ beads.CreateAssignmentClaimerHandleProvider = (*policyAtomicProviderProxy)(nil)
)
