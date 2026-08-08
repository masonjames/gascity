package beads

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestAssignmentReleaserForDoesNotInventCapability(t *testing.T) {
	type storeOnly struct{ Store }
	wrapped := storeOnly{Store: NewMemStore()}
	if releaser, ok := AssignmentReleaserFor(wrapped); ok || releaser != nil {
		t.Fatalf("AssignmentReleaserFor(store-only wrapper) = (%T, %v), want nil,false", releaser, ok)
	}

	runnerCalls := 0
	bd := NewBdStore("/not-used", func(_, _ string, _ ...string) ([]byte, error) {
		runnerCalls++
		return nil, nil
	})
	if releaser, ok := AssignmentReleaserFor(bd); ok || releaser != nil {
		t.Fatalf("AssignmentReleaserFor(BdStore) = (%T, %v), want nil,false", releaser, ok)
	}
	cache := NewCachingStoreForTest(bd, nil)
	if releaser, ok := AssignmentReleaserFor(cache); ok || releaser != nil {
		t.Fatalf("AssignmentReleaserFor(cache(BdStore)) = (%T, %v), want nil,false", releaser, ok)
	}
	if runnerCalls != 0 {
		t.Fatalf("capability checks executed bd %d times, want zero", runnerCalls)
	}
}

func TestSQLiteStoreAssignmentReleaseCapabilityRequiresWritableRevisionSchema(t *testing.T) {
	for _, tc := range []struct {
		name     string
		revision bool
		readOnly bool
	}{
		{name: "legacy schema without revision", revision: false},
		{name: "current schema read only", revision: true, readOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			createSQLiteSchemaFixture(t, dir, tc.revision, false, nil)
			options := []SQLiteStoreOption{WithSQLiteStoreIDPrefix(sqliteGraphPrefix)}
			if tc.readOnly {
				options = append(options, WithSQLiteStoreReadOnly())
			}
			opened, err := OpenSQLiteStore(dir, options...)
			if err != nil {
				t.Fatal(err)
			}
			store := opened.(*SQLiteStore)
			t.Cleanup(func() { _ = store.CloseStore() })
			if releaser, ok := AssignmentReleaserFor(store); ok || releaser != nil {
				t.Fatalf("AssignmentReleaserFor(SQLiteStore) = (%T, %v), want nil,false", releaser, ok)
			}

			revision := int64(0)
			_, won, err := store.ReleaseAssignment(t.Context(), AssignmentReleaseRequest{
				ID:                   "gc-missing",
				ExpectedStatus:       "in_progress",
				ExpectedAssignee:     "worker-1",
				ExpectedRevision:     &revision,
				ExpectedMetadata:     map[string]string{"route": "pool"},
				ForbiddenLabels:      []string{"hold"},
				AbsentCoLocatedMatch: &CoLocatedMatchPredicate{ExpectedStatus: "open", ClassAnyOf: []CoLocatedClassPredicate{{ExpectedType: "coordination"}}, MatchValue: "worker-1", MatchID: true},
			})
			if won || !errors.Is(err, ErrAssignmentReleaseUnsupported) {
				t.Fatalf("ReleaseAssignment = (won %v, err %v), want false,ErrAssignmentReleaseUnsupported", won, err)
			}
		})
	}
}

func TestCoLocatedMatchPredicateMatchesGenericRowIDSources(t *testing.T) {
	predicate := &CoLocatedMatchPredicate{
		ExpectedStatus: "open",
		ClassAnyOf: []CoLocatedClassPredicate{
			{ExpectedType: "coordination-row"},
			{RequiredLabels: []string{"coordination:owner"}},
		},
		MatchValue: "owner-1",
		MatchID:    true,
	}
	if !coLocatedMatchPredicateMatches(Bead{
		ID:     "owner-1",
		Status: "open",
		Type:   "coordination-row",
		Labels: []string{"coordination:owner"},
	}, predicate) {
		t.Fatal("generic row ID identity did not match")
	}
	if coLocatedMatchPredicateMatches(Bead{
		ID:     "owner-1",
		Status: "closed",
		Type:   "coordination-row",
		Labels: []string{"coordination:owner"},
	}, predicate) {
		t.Fatal("closed generic row matched open-only predicate")
	}
}

func TestAssignmentReleaseRejectsIncompletePredicatesWithoutMutation(t *testing.T) {
	store := NewMemStore()
	work, err := store.Create(Bead{
		Title:    "assigned work",
		Assignee: "worker-1",
		Metadata: map[string]string{"route": "pool"},
	})
	if err != nil {
		t.Fatal(err)
	}
	status := "in_progress"
	if err := store.Update(work.ID, UpdateOpts{Status: &status}); err != nil {
		t.Fatal(err)
	}
	work, err = store.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	valid := func() AssignmentReleaseRequest {
		revision := work.Revision
		return AssignmentReleaseRequest{
			ID:                   work.ID,
			ExpectedStatus:       "in_progress",
			ExpectedAssignee:     "worker-1",
			ExpectedRevision:     &revision,
			ExpectedMetadata:     map[string]string{"route": "pool"},
			ForbiddenLabels:      []string{"hold:external"},
			AbsentCoLocatedMatch: &CoLocatedMatchPredicate{ExpectedStatus: "open", ClassAnyOf: []CoLocatedClassPredicate{{ExpectedType: "coordination-row"}}, MatchValue: "worker-1", MatchID: true},
		}
	}
	tests := []struct {
		name   string
		mutate func(*AssignmentReleaseRequest)
	}{
		{name: "revision required", mutate: func(req *AssignmentReleaseRequest) { req.ExpectedRevision = nil }},
		{name: "metadata required", mutate: func(req *AssignmentReleaseRequest) { req.ExpectedMetadata = nil }},
		{name: "forbidden labels required", mutate: func(req *AssignmentReleaseRequest) { req.ForbiddenLabels = nil }},
		{name: "absence predicate required", mutate: func(req *AssignmentReleaseRequest) { req.AbsentCoLocatedMatch = nil }},
		{name: "absence class alternatives required", mutate: func(req *AssignmentReleaseRequest) { req.AbsentCoLocatedMatch.ClassAnyOf = nil }},
		{name: "absence class alternative cannot be empty", mutate: func(req *AssignmentReleaseRequest) {
			req.AbsentCoLocatedMatch.ClassAnyOf = []CoLocatedClassPredicate{{}}
		}},
		{name: "absence identity must equal assignee", mutate: func(req *AssignmentReleaseRequest) { req.AbsentCoLocatedMatch.MatchValue = "worker-2" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := valid()
			tt.mutate(&req)
			released, won, err := store.ReleaseAssignment(context.Background(), req)
			if err == nil || won || released.ID != "" {
				t.Fatalf("ReleaseAssignment = (%+v, %v, %v), want zero,false,error", released, won, err)
			}
			after, getErr := store.Get(work.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if !reflect.DeepEqual(after, work) {
				t.Fatalf("invalid request mutated work:\nbefore=%+v\nafter=%+v", work, after)
			}
		})
	}
}
